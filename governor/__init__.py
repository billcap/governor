import logging
import time
import os
import subprocess as sp

from governor.etcd import Client as Etcd
from governor.postgresql import Postgresql
from governor.ha import Ha

import etcd

class Governor:
    INIT_SCRIPT_DIR = '/docker-entrypoint-initdb.d'

    def __init__(self, config, psql_config):
        self.loop_time = config.loop_time

        self.connect_to_etcd(config)
        self.psql = Postgresql(config, psql_config)
        self.ha = Ha(self.psql, self.etcd)

        self.name = self.psql.name

    def run_init_scripts(self):
        # run all the scripts /docker-entrypoint-initdb.d/*.sh
        if not os.path.isdir(self.INIT_SCRIPT_DIR):
            return
        for file in os.listdir(self.INIT_SCRIPT_DIR):
            file = os.path.join(self.INIT_SCRIPT_DIR, file)
            if not file.endswith('.sh') or not os.path.isfile(file):
                continue
            logging.info('Running init script: %s', file)
            if sp.call(['sh', file]) != 0:
                logging.warn('Failed to run init script: %s', file)

    def connect_to_etcd(self, config):
        while True:
            logging.info('waiting on etcd')
            try:
                self.etcd = Etcd(config)
            except (ConnectionRefusedError, etcd.EtcdConnectionFailed) as e:
                logging.error('Error communicating with etcd: %s', e)
            else:
                return
            time.sleep(5)

    def keep_alive(self):
        value = self.psql.connection_string
        try:
            self.etcd.write_scoped(self.name, value, ttl=self.etcd.ttl, prevValue=value)
        except etcd.EtcdKeyNotFound:
            self.etcd.write_scoped(self.name, value, ttl=self.etcd.ttl, prevExist=False)

    def initialize(self, force_leader=False):
        self.keep_alive()

        # is data directory empty?
        if not self.psql.data_directory_empty():
            self.load_psql()
        elif not self.init_cluster(force_leader):
            self.sync_from_leader()
        self.run_init_scripts()

    def init_cluster(self, force_leader=False):
        try:
            self.etcd.init_cluster(self.name)
        except etcd.EtcdAlreadyExist:
            if not force_leader:
                return False
        self.psql.initialize()
        self.etcd.take_leadership(self.name, first = not force_leader)
        self.psql.start()
        self.psql.create_users()
        return True

    def sync_from_leader(self):
        while True:
            logging.info('resolving leader')
            try:
                cluster = self.etcd.get_cluster()
            except etcd.EtcdKeyNotFound:
                cluster = None

            if cluster and cluster.leader:
                logging.info('syncing with leader')
                if self.psql.sync_from_leader(cluster.leader):
                    self.psql.write_recovery_conf(cluster.leader)
                    self.psql.start()
                    return True
            time.sleep(5)

    def load_psql(self):
        self.psql.start()
        if self.psql.is_running():
            self.psql.load_replication_slots()

    def run(self):
        while True:
            self.keep_alive()
            logging.info(self.ha.run_cycle())
            self.ha.sync_replication_slots()
            time.sleep(self.loop_time)

    def cleanup(self):
        self.psql.stop()
        self.etcd.delete(os.path.join(self.etcd.scope, self.name))
        try:
            self.etcd.vacate_leadership(self.name)
        except (etcd.EtcdCompareFailed, etcd.EtcdKeyNotFound):
            pass
