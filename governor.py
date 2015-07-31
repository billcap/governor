#!/usr/bin/env python

import logging
import os
import signal
import sys
import socket
import time
import yaml
import subprocess
import argparse

import etcd

from helpers.etcd import Client as Etcd
from helpers.postgresql import Postgresql
from helpers.ha import Ha


def sigterm_handler(signo, stack_frame):
    sys.exit()

# handle SIGCHILD, since we are the equivalent of the INIT process
def sigchld_handler(signo, stack_frame):
    try:
        while True:
            ret = os.waitpid(-1, os.WNOHANG)
            if ret == (0, 0):
                break
    except OSError:
        pass

class Governor:
    def __init__(self, config):
        self.nap_time = config['loop_wait']

        logging.info('waiting on etcd')
        self.etcd = Etcd()
        self.psql = Postgresql(config['postgresql'])
        self.ha = Ha(self.psql, self.etcd)
        self.name = self.psql.name

    def keep_alive(self):
        self.etcd.write(self.name, self.postgresql.connection_string)

    def initialize(self, force_leader=False):
        self.keep_alive()

        # is data directory empty?
        if not self.psql.data_directory_empty():
            self.load_psql()
        elif not self.init_cluster(force_leader):
            self.sync_from_leader()

    def init_cluster(self, force_leader=False):
        try:
            self.etcd.init_cluster(self.INIT_KEY, self.name) or force_leader:
        except etcd.EtcdAlreadyExist:
            return False
        else:
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
                continue

            if not cluster.leader:
                continue

            logging.info('syncing with leader')
            self.psql.sync_from_leader(cluster.leader)
            self.psql.write_recovery_conf(cluster.leader)
            self.psql.start()
            return True

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

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.signal(signal.SIGCHLD, sigchld_handler)

    parser = argparse.ArgumentParser(description='Postgresql node with self-registration on etcd')
    parser.add_argument('config', help='config file')
    parser.add_argument('--name', default=socket.gethostname(), help='name of node (defaults to hostname)')
    parser.add_argument('--force-leader', action='store_true', help='forcibly become the leader')
    parser.add_argument('--advertise-url', help='URL to advertise to the rest of the cluster')
    parser.add_argument('--etcd-url', default='http://127.0.0.1:4001', help='url to etcd')

    parser.add_argument('--allow-address', default='127.0.0.1/32',
                        help='space separated list of addresses to allow client access')
    parser.add_argument('--allow-replication-address',
                        help='space separated list of addresses to allow replication')

    parser.add_argument('--ca-file', help='path to TLS CA file')
    parser.add_argument('--cert-file', help='path to TLS cert file')
    parser.add_argument('--key-file', help='path to TLS key file')

    args = parser.parse_args()
    config = load_config(args.config, args)

    gov = Governor(args)
    try:
        gov.initialize(force_leader=args.force_leader)
        gov.run()
    except KeyboardInterrupt:
        pass
    finally:
        gov.cleanup()
