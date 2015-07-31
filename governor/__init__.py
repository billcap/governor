import logging
import time

import etcd

from governor.etcd import Client as Etcd
from governor.postgresql import Postgresql
from governor.ha import Ha

class Governor:
    def __init__(self, config, psql_config):
        self.nap_time = config.loop_time

        logging.info('waiting on etcd')
        self.etcd = Etcd(config)
        self.psql = Postgresql(config, psql_config)
        self.ha = Ha(self.psql, self.etcd)
        self.name = self.psql.name

    def keep_alive(self):
        self.etcd.write(self.name, self.postgresql.connection_string())

    def initialize(self, force_leader=False):
        self.keep_alive()

        # is data directory empty?
        if not self.psql.data_directory_empty():
            self.load_psql()
        elif not self.init_cluster(force_leader):
            self.sync_from_leader()

    def init_cluster(self, force_leader=False):
        if not force_leader:
            try:
                self.etcd.init_cluster(self.INIT_KEY, self.name)
            except etcd.EtcdAlreadyExist:
                return False
        self.postgresql.initialize()
        self.etcd.take_leadership(self.LEADER_KEY, self.name)
        self.postgresql.start()
        self.postgresql.create_users()
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
        if self.psql.is_running():
            self.psql.load_replication_slots()

    def run(self):
        while True:
            self.keep_alive()
            logging.info(self.ha.run_cycle())
            time.sleep(self.nap_time)

    def cleanup(self):
        self.psql.stop()
        self.etcd.delete(self.name)
        try:
            self.etcd.delete(self.LEADER_KEY, self.name, prevValue=self.name)
        except etcd.EtcdCompareFailed:
            pass
