import yaml
import socket
import os

def load_config(filename, args):
    with open(filename) as f:
        config = yaml.load(f)

    config.setdefault('etcd', {})
    etcd = config['etcd']
    etcd.setdefault('host', os.environ.get('ETCD_HOST', 'http://127.0.0.1:4001'))
    etcd.setdefault('scope', '')

    config.setdefault('postgresql', {})
    psql = config['postgresql']
    psql.setdefault('name', socket.gethostname())
    psql.setdefault('connect_address', args.advertise_url)
    psql.setdefault('listen', '0.0.0.0:5432')
    psql.setdefault('data_dir', os.environ['PGDATA'])

    return config
