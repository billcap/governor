#!/usr/bin/env python

import logging
import os
import signal
import sys
import time
import yaml
import argparse

from helpers.etcd import Etcd
from helpers.postgresql import Postgresql
from helpers.ha import Ha
from helpers.config import load_config


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
        self.etcd = Etcd(config['etcd'])
        self.postgresql = Postgresql(config['postgresql'])
        self.ha = Ha(self.postgresql, self.etcd)

        self.name = self.postgresql.name

    def touch_member(self):
        return self.etcd.touch_member(self.name, self.postgresql.connection_string)

    def initialize(self, force_leader=False):
        # wait for etcd to be available
        while not self.touch_member():
            logging.info('waiting on etcd')
            time.sleep(5)

        # is data directory empty?
        if not self.postgresql.data_directory_empty():
            self.load_postgresql()
        elif not self.init_cluster(force_leader):
            self.sync_from_leader()

    def init_cluster(self, force_leader=False):
        if self.etcd.race('/initialize', self.name) or force_leader:
            self.postgresql.initialize()
            self.etcd.take_leader(self.name)
            self.postgresql.start()
            self.postgresql.create_replication_user()
            return True

    def sync_from_leader(self):
        while True:
            leader = self.etcd.current_leader()
            if leader and self.postgresql.sync_from_leader(leader):
                self.postgresql.write_recovery_conf(leader)
                self.postgresql.start()
                break
            time.sleep(5)

    def load_postgresql():
        if self.postgresql.is_running():
            self.postgresql.load_replication_slots()

    def run(self):
        while True:
            self.touch_member()
            logging.info(self.ha.run_cycle())
            time.sleep(self.nap_time)

    def cleanup(self):
        self.postgresql.stop()
        self.etcd.delete_member(self.name)
        self.etcd.delete_leader(self.name)


def main():
    parser = argparse.ArgumentParser(description='Postgresql node with self-registration on etcd')
    parser.add_argument('config', help='config file')
    parser.add_argument('--force-leader', action='store_true', help='forcibly become the leader')
    args = parser.parse_args()

    config = load_config(args.config)
    governor = Governor(config)
    try:
        governor.initialize(force_leader=args.force_leader)
        governor.run()
    except KeyboardInterrupt:
        pass
    finally:
        governor.cleanup()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.signal(signal.SIGCHLD, sigchld_handler)
    main()

