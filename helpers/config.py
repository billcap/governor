import yaml
import socket

def load_config(filename):
    with open(filename) as f:
        config = yaml.load(f)

    config.setdefault('etcd', {})
    etcd = config['etcd']
    etcd.setdefault('scope', '/')

    config.setdefault('postgresql', {})
    psql = config['postgresql']
    psql.setdefault('name', socket.gethostname())
    psql.setdefault('data_dir', '/data')

    return config
