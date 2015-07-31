import logging
import time
import etcd
import os
from functools import partial

from collections import namedtuple
from helpers.errors import CurrentLeaderError, EtcdError

logger = logging.getLogger(__name__)

Member = namedtuple('Member', 'hostname,address,ttl')

class Client(etcd.Client):
    LEADER_KEY = 'leader'
    OPTIME_KEY = 'optime'
    INIT_KEY = 'initialize'
    scope = '/'
    ttl = None

    def write_optime(self, value):
        key = os.path.join(self.scope, self.OPTIME_KEY)
        return self.write(key, value)

    def init_cluster(self, value):
        key = os.path.join(self.scope, self.INIT_KEY)
        return self.write(key, value, prevExist=False)

    def take_leadership(self, value, force=False, first=False):
        key = os.path.join(self.scope, self.LEADER_KEY)
        prevValue = (None if force else prevValue)
        prevExist = (not first)
        return self.write(key, value, prevValue=prevValue, prevExist=prevExist, ttl=self.ttl)

    def get_leader(self):
        key = os.path.join(self.scope, self.LEADER_KEY)
        return self.read(key)

    def get_cluster(self, recursive=True):
        cluster = self.read(self.scope, recursive=recursive)
        return Cluster(cluster, self)

class Cluster:
    __slots__ = ('members', 'leader', 'leader_node', 'optime')

    def __init__(self, nodes, client):
        self.members = {os.path.basename(m.key): m for m in self.members}
        self.optime = self.members.pop(Client.OPTIME_KEY, None)
        self.leader_node = self.members.pop(Client.LEADER_KEY, None)
        self.leader = None

        if not self.leader_key:
            return

        self.leader = self.members.get(self.leader_node.value)
        if not self.leader:
            try:
                # leader is not a member! delete
                client.delete(self.leader_node.key, prevValue=self.leader_node.value)
            except (etcd.EtcdCompareFailed, EtcdKeyNotFound):
                pass
            self.leader_node = None

class Cluster(namedtuple('Cluster', 'leader,last_leader_operation,members')):
    def is_unlocked(self):
        return not (self.leader and self.leader.hostname)


def urljoin(*args):
    return '/'.join(i.strip('/') for i in args)


class _Client(requests.Session):
    def __init__(self, config):
        super().__init__()

        if config['host'].startswith('https://'):
            self.cert = (config['cert_file'], config['key_file'])
            self.verify = config['ca_file']

        self.url = urljoin(config['host'], 'v2/keys', config['scope'])
        self.make_url = partial(urljoin, self.url)

    def get(self, path):
        return super().get(self.make_url(path))
    def set(self, path, **data):
        return super().put(self.make_url(path), data=data)
    def delete(self, path):
        return super().delete(self.make_url(path))

class Etcd:

    @staticmethod
    def validate_cert_config(config):
        if config['host'].startswith('https://'):
            # TLS
            if not config['ca_file']:
                raise Exception('Expected a CA file')
            if not config['cert_file']:
                raise Exception('Expected a cert file')
            if not config['key_file']:
                raise Exception('Expected a key file')

    def __init__(self, config):
        self.ttl = config['ttl']
        self.member_ttl = config['member_ttl']
        self.postgres_cluster = None
        self.client = _Client(config)

    def get_client_path(self, path, max_attempts=1):
        for i in range(max_attempts):
            ex = None
            if i != 0:
                logger.info('Failed to return %s, trying again. (%s of %s)', path, attempts, max_attempts)
                time.sleep(3)

            try:
                response = self.client.get(path)
                if response.status_code == 200:
                    break
            except Exception as e:
                logger.exception('get_client_path')
                ex = e

        if ex:
            raise ex

        return response.json(), response.status_code

    def put_client_path(self, path, **data):
        try:
            response = self.client.set(path, **data)
            return response.status_code in [200, 201, 202, 204]
        except:
            logger.exception('PUT %s data=%s', path, data)
            return False

    def delete_client_path(self, path):
        try:
            response = self.client.delete(path)
            return response.status_code in [200, 202, 204]
        except:
            logger.exception('DELETE %s', path)
            return False

    @staticmethod
    def find_node(node, key):
        """
        >>> Etcd.find_node({}, None)
        >>> Etcd.find_node({'dir': True, 'nodes': [], 'key': '/test/'}, 'test')
        """
        if not node.get('dir', False):
            return None
        key = node['key'] + key
        for n in node['nodes']:
            if n['key'] == key:
                return n
        return None

    def get_cluster(self):
        try:
            response, status_code = self.get_client_path('?recursive=true')
            if status_code == 200:
                # get list of members
                node = self.find_node(response['node'], '/members') or {'nodes': []}
                members = [Member(n['key'].split('/')[-1], n['value'], n.get('ttl', None)) for n in node['nodes']]

                # get last leader operation
                last_leader_operation = 0
                node = self.find_node(response['node'], '/optime')
                if node:
                    node = self.find_node(node, '/leader')
                    if node:
                        last_leader_operation = int(node['value'])

                # get leader
                leader = None
                node = self.find_node(response['node'], '/leader')
                if node:
                    leader = next((m for m in members if m.hostname == node['value']), None)
                    # leader is not a member! delete
                    if not leader:
                        self.delete_leader(node['value'])

                return Cluster(leader, last_leader_operation, members)
            elif status_code == 404:
                return Cluster(None, None, [])
        except:
            logger.exception('get_cluster')

        raise EtcdError('Etcd is not responding properly')

    def current_leader(self):
        try:
            cluster = self.get_cluster()
            return None if cluster.is_unlocked() else cluster.leader
        except:
            raise CurrentLeaderError("Etcd is not responding properly")

    def touch_member(self, member, connection_string):
        return self.put_client_path('/members/' + member, value=connection_string, ttl=self.member_ttl)

    def take_leader(self, value):
        return self.put_client_path('/leader', value=value, ttl=self.ttl)

    def attempt_to_acquire_leader(self, value):
        ret = self.put_client_path('/leader', value=value, ttl=self.ttl, prevExist=False)
        ret or logger.info('Could not take out TTL lock')
        return ret

    def update_leader(self, state_handler):
        optime = state_handler.last_operation()
        ret = self.put_client_path('/leader', value=state_handler.name, ttl=self.ttl, prevValue=state_handler.name)
        ret and self.put_client_path('/optime/leader', value=optime)
        return ret

    def race(self, path, value):
        return self.put_client_path(path, value=value, prevExist=False)

    def delete_member(self, member):
        return self.delete_client_path('/members/' + member)

    def delete_leader(self, value):
        return self.delete_client_path('/leader?prevValue=' + value)

