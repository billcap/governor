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
    parser.add_argument('--name', default=socket.gethostname(),
                        help='name of node (defaults to hostname)')
    parser.add_argument('--force-leader', action='store_true',
                        help='forcibly become the leader')
    parser.add_argument('--advertise-url',
                        help='URL to advertise to the rest of the cluster')
    parser.add_argument('--loop-time', default=10, type=int,
                        help='length of time (seconds) for each loop, until members re-register themselves')

    group = parser.add_argument_group('etcd')
    group.add_argument('--etcd-url', metavar='PROTOCOL://HOST:PORT',
                       default='http://127.0.0.1:4001',
                        help='url to etcd (default: http://127.0.0.1:4001)')
    group.add_argument('--etcd-prefix', default='/governor',
                       help='etcd key prefix (default: /governor)')
    group.add_argument('--etcd-ttl', type=int,
                       help='time-to-live for member registration (default: double --loop-time)')
    group.add_argument('--ca-file', help='path to TLS CA file')
    group.add_argument('--cert-file', help='path to TLS cert file')
    group.add_argument('--key-file', help='path to TLS key file')

    group = parser.add_argument_group('psql')
    group.add_argument('--dbname', help='database name')
    group.add_argument('--listen-address', metavar='HOST:PORT', default='0.0.0.0:5432',
                       help='addresses for psql to listen on')
    group.add_argument('--data-dir', default=os.environ.get("PGDATA"),
                       help='data directory for psql (default: $PGDATA)')
    group.add_argument('--maximum-lag', default=0,
                       help='the maximum bytes a follower may lag before it is not eligible become leader')

    group = parser.add_argument_group('auth')
    group.add_argument('--user', default=os.environ.get('POSTGRES_USER'),
                       help='psql username (default: $POSTGRES_USER)')
    group.add_argument('--password', default=os.environ.get('POSTGRES_PASS'),
                       help='psql password (default: $POSTGRES_PASS)')
    group.add_argument('--allow-address', default='127.0.0.1/32',
                       help='space separated list of addresses to allow client access (default: 127.0.0.1/32)')

    group = parser.add_argument_group('replication')
    group.add_argument('--repl-user', default='replication',
                       help='psql username for replication (default: replication)')
    group.add_argument('--repl-password', default=os.environ.get('REPLICATION_PASS'),
                       help='psql password (default $REPLICATION_PASS)')
    group.add_argument('--repl-allow-address',
                       help='space separated list of addresses to allow replication (default: same as --allow-address)')

    config, psql_config = parser.parse_known_args()

    if bool(config.cert_file) != bool(config.key_file):
        raise ValueError("Expected both or none of --cert-file and --key-file options")

    if config.etcd_ttl is None:
        config.etcd_ttl = config.loop_time * 2
    if config.repl_allow_address is None:
        config.repl_allow_address = config.allow_address

    gov = Governor(config, psql_config)
    try:
        gov.initialize(force_leader=config.force_leader)
        gov.run()
    except KeyboardInterrupt:
        pass
    finally:
        gov.cleanup()
