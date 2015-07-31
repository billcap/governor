import logging
import etcd

from psycopg2 import InterfaceError, OperationalError

logger = logging.getLogger(__name__)

class Ha:

    def __init__(self, psql, etcd):
        self.psql = psql
        self.etcd = etcd
        self.cluster = None

    def refresh_cluster(self):
        self.cluster = self.etcd.get_cluster()

    def acquire_leadership(self):
        try:
            self.etcd.take_leadership(self.psql.name, first=True)
        except ectd.EtcdAlreadyExist:
            return False
        return True

    def update_leadership(self):
        optime = self.psql.last_operation()
        try:
            self.etcd.take_leadership(self.psql.name)
        except etcd.EtcdCompareFailed:
            return False
        self.etcd.write_optime(optime)
        return True

    def is_leader(self):
        leader = (self.cluster.leader_node and self.cluster.leader_node.value)
        logger.info('Lock owner: %s; I am %s', leader, self.psql.name)
        return leader == self.psql.name

    def recover(self):
        if not self.psql.is_healthy():
            locked = self.is_leader()
            self.psql.write_recovery_conf(None if locked else self.cluster.leader_node)
            self.psql.start()
            if locked:
                logging.info('Started as readonly because I had the session lock')
                self.refresh_cluster()
            return True

    def become_leader(self):
        if self.acquire_lock():
            if self.psql.is_leader():
                return 'Acquired session lock as a leader'
            self.psql.promote()
            return 'Promoted self to leader by acquiring session lock'

    def follow_leader(self, refresh=True):
        if refresh:
            self.refresh_cluster()

        if self.psql.is_leader():
            self.psql.follow_the_leader(self.cluster.leader_node)
            return 'Demoted self'
        self.psql.follow_the_leader(self.cluster)
        return 'Following the leader'

    def run_cycle(self):
        try:
            self.refresh_cluster()
            if self.recover() and not self.is_leader():
                return 'Started as secondary'

            if not self.cluster.leader:
                if self.psql.is_healthiest_node(self.cluster):
                    status = self.become_leader()
                    if status:
                        return status

                return self.follow_leader()

            if not self.is_leader() or not self.update_leadership():
                logger.info('Does not have lock')
                return self.follow_leader()

            if self.psql.is_leader():
                return 'No action. I am the leader with the lock'
            self.psql.promote()
            return 'Promoted self to leader'

        except etcd.EtcdException:
            logger.error('Error communicating with Etcd')
            if self.psql.is_leader():
                self.psql.follow_the_leader(None)
                return 'Demoted self because etcd is not accessible and I was a leader'
        except (InterfaceError, OperationalError):
            logger.error('Error communicating with Postgresql.  Will try again')

    def sync_replication_slots(self):try:
        try:
            if not self.psql.is_leader():
                self.psql.drop_replication_slots()
            elif self.cluster:
                self.psql.create_replication_slots(self.cluster)
        except:
            logging.exception('Exception when changing replication slots')
