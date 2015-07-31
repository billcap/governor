#!/usr/bin/env python

import logging
import os
import signal
import sys
import socket
import argparse

from governor import Governor

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
