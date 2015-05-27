import yaml
import socket
import os

def load_config(filename):
    with open(filename) as f:
        config = yaml.load(f)

    config.setdefault('etcd', {})
    etcd = config['etcd']
    etcd.setdefault('host', os.environ.get('ETCD_HOST', '127.0.0.1:4001'))
    etcd.setdefault('scope', '')
    etcd['scope'] = etcd['scope'].lstrip('/')

    config.setdefault('postgresql', {})
    psql = config['postgresql']
    psql.setdefault('name', socket.gethostname())
    psql.setdefault('data_dir', os.environ['PGDATA'])

    return config
