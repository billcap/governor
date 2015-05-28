import yaml
import logging
import os

def load_config(filename, args):
    with open(filename) as f:
        config = yaml.load(f)

    config.setdefault('etcd', {})
    etcd = config['etcd']
    etcd.setdefault('host', args.etcd_url)
    etcd.setdefault('scope', '/service')
    etcd.setdefault('member_ttl', etcd['ttl'])
    etcd.setdefault('ca_file', args.ca_file)
    etcd.setdefault('cert_file', args.cert_file)
    etcd.setdefault('key_file', args.key_file)

    config.setdefault('postgresql', {})
    psql = config['postgresql']
    psql.setdefault('name', args.name)
    psql.setdefault('connect_address', args.advertise_url)
    psql.setdefault('listen', '0.0.0.0:5432')
    psql.setdefault('data_dir', os.environ['PGDATA'])
    psql.setdefault('maximum_lag_on_failover', 0)
    psql.setdefault('replication', {})
    psql['auth'] = _setup_auth_config(psql, args)

    repl = psql['replication']
    repl.setdefault('network', args.allow_replication_address or args.allow_address)

    return config

def _setup_auth_config(config, args):
    auth = config.get('auth')
    if not auth:
        return {}
    if 'password' not in auth:
        logging.warning('no password set, ignoring auth')
        return {}
    auth.setdefault('network', args.allow_address)
    auth.setdefault('dbname', 'postgres')
    auth.setdefault('user', 'postgres')
    return auth
