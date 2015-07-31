import logging
import os
import psycopg2
import sys
import time
import subprocess

from urllib.parse import urlparse

logger = logging.getLogger(__name__)

def parseurl(url):
    r = urlparse(url)
    return {
        'host': r.hostname,
        'port': r.port or 5432,
        'user': r.username,
        'password': r.password,
        'database': r.path[1:],
        'fallback_application_name': 'Governor',
        'connect_timeout': 5,
    }

class Postgresql:
    _conn = None
    _cursor_holder = None

    connection_format = 'postgres://{username}:{password}@{connect_address}/postgres'.format

    def __init__(self, config):
        self.config = config
        self.name = config['name']
        self.listen_addresses, self.port = config['listen'].split(':')
        self.data_dir = config['data_dir']
        self.auth = config['auth']
        self.replication = config['replication']
        self.recovery_conf = os.path.join(self.data_dir, 'recovery.conf')
        self.pid_path = os.path.join(self.data_dir, 'postmaster.pid')

        self._pg_ctl = ['pg_ctl', '-w', '-D', self.data_dir]

        self.local_address = self.get_local_address()
        connect_address = config.get('connect_address', None) or self.local_address
        self.connection_string = self.connection_format(connect_address=connect_address, **self.replication)

        self.members = []  # list of already existing replication slots

    def pg_ctl(self, *args, **kwargs):
        return subprocess.call(self._pg_ctl + args, **kwargs)

    def get_local_address(self):
        # TODO: try to get unix_socket_directory from postmaster.pid
        return self.listen_addresses.split(',')[0].strip() + ':' + self.port

    def connection(self):
        if not self._conn or self._conn.closed != 0:
            self._conn = psycopg2.connect(
                dbname = self.auth.get('dbname', 'postgres'),
                user = self.auth.get('username', 'postgres'),
                password = self.auth.get('password'),
            )
            self._conn.autocommit = True
        return self._conn

    def _cursor(self):
        if not self._cursor_holder or self._cursor_holder.closed:
            self._cursor_holder = self.connection().cursor()
        return self._cursor_holder

    def disconnect(self):
        if self._conn:
            self._conn.close()
        self._conn = self._cursor_holder = None

    def query(self, sql, *params):
        max_attempts = 3

        for i in range(max_attempts):
            ex = None
            try:
                cursor = self._cursor()
                cursor.execute(sql, params)
                return cursor
            except psycopg2.InterfaceError as e:
                ex = e
            except psycopg2.OperationalError as e:
                if self._conn and self._conn.closed == 0:
                    raise e
                ex = e
            self.disconnect()
            time.sleep(5)

        if ex:
            raise ex

    def data_directory_empty(self):
        return not (os.path.exists(self.data_dir) and os.listdir(self.data_dir))

    def initialize(self):
        if self.pg_ctl('initdb', '-o', '--encoding', 'UTF-8') == 0:
            self.write_pg_hba()
            return True
        return False

    def sync_from_leader(self, leader):
        r = parseurl(leader.value)

        pgpass = os.environ['PGPASS']
        with open(pgpass, 'w') as f:
            os.fchmod(f.fileno(), 0o600)
            f.write('{host}:{port}:*:{user}:{password}\n'.format(**r))

        env = os.environ.copy()
        env['PGPASSFILE'] = pgpass
        subprocess.check_call([
            'pg_basebackup', '-R', '-P',
            '-D', self.data_dir,
            '--host', r['host'],
            '--port', str(r['port']),
            '-U', r['user'],
        ], env=env)

        os.chmod(self.data_dir, 0o700)

    def is_leader(self):
        return not self.query('SELECT pg_is_in_recovery()').fetchone()[0]

    def is_running(self):
        return self.pg_ctl('status') == 0

    def start(self):
        if self.is_running():
            self.load_replication_slots()
            logger.error('Cannot start PostgreSQL because one is already running.')
            return False

        if os.path.exists(self.pid_path):
            os.remove(self.pid_path)
            logger.info('Removed %s', self.pid_path)

        if self.pg_ctl('start', '-o', self.server_options) == 0:
            self.load_replication_slots()
            return True
        return False

    def stop(self):
        return self.pg_ctl('stop', '-m', 'fast') != 0

    def reload(self):
        return self.pg_ctl('reload') == 0

    def restart(self):
        return self.pg_ctl('restart', '-m', 'fast') == 0

    def server_options(self):
        options = "--listen_addresses='{}' --port={}".format(self.listen_addresses, self.port)
        for setting, value in self.config['parameters'].items():
            options += " --{}='{}'".format(setting, value)
        return options

    def is_healthy(self):
        if not self.is_running():
            logger.warning('Postgresql is not running.')
            return False
        return True

    def is_healthiest_node(self, cluster):
        if self.is_leader():
            return True

        if cluster.optime - self.xlog_position() > self.config['maximum_lag_on_failover']:
            return False

        for name, m in cluster.members.items():
            if name == self.name:
                continue
            try:
                member_conn = psycopg2.connect(**parseurl(m.value))
                member_conn.autocommit = True
                member_cursor = member_conn.cursor()
                member_cursor.execute(
                    "SELECT pg_is_in_recovery(), %s - (pg_last_xlog_replay_location() - '0/0000000'::pg_lsn)",
                    (self.xlog_position(), ))
                row = member_cursor.fetchone()
                member_cursor.close()
                member_conn.close()
                logger.error([self.name, name, row])
                if not row[0] or row[1] < 0:
                    return False
            except psycopg2.Error:
                continue
        return True

    def write_pg_hba(self):
        with open(os.path.join(self.data_dir, 'pg_hba.conf'), 'w') as f:
            # always allow local socket access
            f.write('\nlocal all all trust')

            if self.auth:
                for subnet in self.auth['network'].split():
                    f.write('\nhost {dbname} {username} {subnet} md5\n'.format(subnet=subnet, **self.auth))

            for subnet in self.replication['network'].split():
                f.write('\nhost replication {username} {subnet} md5\n'.format(subnet=subnet, **self.replication))

    @staticmethod
    def primary_conninfo(leader_url):
        r = parseurl(leader_url)
        return 'user={user} password={password} host={host} port={port} sslmode=prefer sslcompression=1'.format(**r)

    def check_recovery_conf(self, leader):
        if not os.path.isfile(self.recovery_conf):
            return False

        pattern = (leader and self.primary_conninfo(leader.value))

        with open(self.recovery_conf, 'r') as f:
            for line in f:
                if line.startswith('primary_conninfo'):
                    if not pattern:
                        return False
                    return pattern in line

        return not pattern

    def write_recovery_conf(self, leader):
        contents = [
            "standby_mode = 'on'",
            "recovery_target_timeline = 'latest'",
        ]
        if leader:
            contents.append( "primary_slot_name = '{}'".format(self.name) )
            contents.append( "primary_conninfo = '{}'".format(self.primary_conninfo(leader.value)) )
            for name, value in self.config.get('recovery_conf', {}).items():
                contents.append( "{} = '{}'".format(name, value) )

        with open(self.recovery_conf, 'w') as f:
            f.write('\n'.join(contents) + '\n')

    def follow_the_leader(self, leader):
        if not self.check_recovery_conf(leader):
            self.write_recovery_conf(leader)
            self.restart()

    def promote(self):
        return self.pg_ctl('promote') == 0

    def create_users(self):
        if self.auth:
            op = ('ALTER' if self.auth['username'] == 'postgres' else 'CREATE')
            # normal client user
            self.query('{} USER "{}" WITH SUPERUSER ENCRYPTED PASSWORD %s'.format(
                op, self.auth['username']), self.auth['password'])
        # replication user
        self.query('CREATE USER "{}" WITH REPLICATION ENCRYPTED PASSWORD %s'.format(
            self.replication['username']), self.replication['password'])

    def xlog_position(self):
        return self.query("""SELECT CASE WHEN pg_is_in_recovery()
                                         THEN pg_last_xlog_replay_location() - '0/0000000'::pg_lsn
                                         ELSE pg_current_xlog_location() - '0/00000'::pg_lsn END""").fetchone()[0]

    def load_replication_slots(self):
        cursor = self.query("SELECT slot_name FROM pg_replication_slots WHERE slot_type='physical'")
        self.members = [r[0] for r in cursor]

    def create_replication_slots(self, members):
        # drop unused slots
        for slot in set(self.members) - set(members):
            self.query("""SELECT pg_drop_replication_slot(%s)
                           WHERE EXISTS(SELECT 1 FROM pg_replication_slots
                           WHERE slot_name = %s)""", slot, slot)

        # create new slots
        for slot in set(members) - set(self.members):
            self.query("""SELECT pg_create_physical_replication_slot(%s)
                           WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots
                           WHERE slot_name = %s)""", slot, slot)
        self.members = members

    def last_operation(self):
        return self.xlog_position()
