import etcd
import os
import re

class Client(etcd.Client):
    LEADER_KEY = 'leader'
    OPTIME_KEY = 'optime'
    INIT_KEY = 'initialize'

    url_regex = re.compile('^(?P<protocol>http(s?))://(?P<host>.*?):(?P<port>\d+)$')

    def __init__(self, config):
        match = self.url_regex.match(config.etcd_url).groupdict()

        cert = (config.cert_file, config.key_file)
        if not all(cert):
            cert = None

        super().__init__(
            host=match['host'],
            port=int(match['port']),
            protocol=match['protocol'],
            #allow_reconnect=True,
            ca_cert=config.ca_file,
            cert=cert,
        )
        self.ttl = config.etcd_ttl
        self.scope = config.etcd_prefix

    def write_scoped(self, key, value, **kwargs):
        key = os.path.join(self.scope, key)
        return self.write(key, value, **kwargs)

    def write_optime(self, value):
        return self.write_scoped(self.OPTIME_KEY, value)

    def init_cluster(self, value):
        return self.write_scoped(self.INIT_KEY, value, prevExist=False)

    def take_leadership(self, value, force=False, first=False):
        kwargs = {}
        if first:
            kwargs['prevExist'] = False
        elif not force:
            kwargs['prevValue'] = value
        return self.write_scoped(self.LEADER_KEY, value, ttl=self.ttl, **kwargs)

    def vacate_leadership(self, value):
        key = os.path.join(self.scope, self.LEADER_KEY)
        return self.delete(key, value, prevValue=value)

    def get_leader(self):
        key = os.path.join(self.scope, self.LEADER_KEY)
        return self.read(key)

    def get_cluster(self, recursive=True):
        cluster = self.read(self.scope, recursive=recursive)
        return Cluster(cluster, self)

class Cluster:
    __slots__ = ('members', 'leader', 'leader_node', 'optime')

    def __init__(self, nodes, client):
        self.members = {os.path.basename(m.key): m for m in nodes.leaves}
        self.optime = self.members.pop(Client.OPTIME_KEY, None)
        self.leader_node = self.members.pop(Client.LEADER_KEY, None)
        self.leader = None

        if not self.leader_node:
            return

        self.leader = self.members.get(self.leader_node.value)
        if not self.leader:
            try:
                # leader is not a member! delete
                client.delete(self.leader_node.key, prevValue=self.leader_node.value)
            except (etcd.EtcdCompareFailed, EtcdKeyNotFound):
                pass
            self.leader_node = None
