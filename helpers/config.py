import yaml
import os

def load_config(filename, args):
    with open(filename) as f:
        config = yaml.load(f)

    config.setdefault('etcd', {})
    etcd = config['etcd']
    etcd.setdefault('host', args.etcd_url)
    etcd.setdefault('scope', '')
    etcd.setdefault('member_ttl', etcd['ttl'])

    config.setdefault('postgresql', {})
    psql = config['postgresql']
    psql.setdefault('name', args.name)
    psql.setdefault('connect_address', args.advertise_url)
    psql.setdefault('listen', '0.0.0.0:5432')
    psql.setdefault('data_dir', os.environ['PGDATA'])
    psql.setdefault('maximum_lag_on_failover', 0)
    psql.setdefault('replication', {})

    repl = psql['replication']
    repl.setdefault('network', args.replication_subnets)

    return config
