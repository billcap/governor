import logging
import os
import psycopg2
import time
import shlex
import subprocess
import shutil
import threading

from urllib.parse import urlparse

logger = logging.getLogger(__name__)

def parseurl(url):
    r = urlparse(url)
    options = {
        'host': r.hostname,
        'port': r.port or 5432,
        'user': r.username,
        'password': r.password,
        'database': r.path[1:],
        'fallback_application_name': 'Governor',
    }
    options.update(Postgresql.CONN_OPTIONS)
    return options

class Postgresql:
    CONN_OPTIONS = {
        'connect_timeout': 3,
        'options': '-c statement_timeout=2000',
    }

    _conn = None
    _cursor_holder = None

    connection_format = 'postgres://{username}:{password}@{connect_address}/postgres'.format

    def __init__(self, config, psql_config):
        self.config = config
        self.psql_config = psql_config

        self.name = config.name
        self.listen_addresses, self.port = config.listen_address.split(':')
        self.data_dir = config.data_dir

        self.recovery_conf = os.path.join(self.data_dir, 'recovery.conf')
        self.pid_path = os.path.join(self.data_dir, 'postmaster.pid')
        self._pg_ctl = ('pg_ctl', '-w', '-D', self.data_dir)

        self.members = set()    # list of already existing replication slots
        self.promoted = False

        self.connection_string = self.connection_format(
            connect_address=self.config.advertise_url,
            username=self.config.repl_user,
            password=self.config.repl_password,
        )

    def pg_ctl(self, *args, **kwargs):
        cmd = self._pg_ctl + args
        logger.info(cmd)
        return subprocess.call(cmd, **kwargs)

    def connection(self):
        if not self._conn or self._conn.closed:
            self._conn = psycopg2.connect(
                dbname=self.config.dbname,
                port=self.port,
                user=self.config.user,
                password=self.config.password,
                **self.CONN_OPTIONS
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
        if subprocess.call(['initdb', '-D', self.data_dir, '--encoding', 'UTF-8']) == 0:
            self.write_pg_hba()
            return True
        return False

    def sync_from_leader(self, leader):
        r = parseurl(leader.value)

        pgpass = os.path.join(os.environ['ROOT'], 'pgpass')
        with open(pgpass, 'w') as f:
            os.fchmod(f.fileno(), 0o600)
            f.write('{host}:{port}:*:{user}:{password}\n'.format(**r))

        env = os.environ.copy()
        env['PGPASSFILE'] = pgpass
        try:
            subprocess.check_call([
                'pg_basebackup', '-R', '-P',
                '-D', self.data_dir,
                '--host', r['host'],
                '--port', str(r['port']),
                '-U', r['user'],
            ], env=env)
        except subprocess.CalledProcessError:
            return False
        finally:
            os.chmod(self.data_dir, 0o700)
        return True

    def is_leader(self):
        is_leader = not self.query('SELECT pg_is_in_recovery()').fetchone()[0]
        if is_leader:
            self.promoted = False
        return is_leader

    def is_running(self):
        return self.pg_ctl('status', stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0

    def start_threaded(self):
        logger = logging.getLogger('postgres')
        cmd = [
            'postgres', '-i',
            '-p', self.port,
            '-h', self.listen_addresses,
            ] + self.psql_config
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        while True:
            line = proc.stdout.readline()
            if not line:
                break
            logging.info(line)

    def start(self):
        if self.is_running():
            self.load_replication_slots()
            logger.error('Cannot start PostgreSQL because one is already running.')
            return False

        if os.path.exists(self.pid_path):
            os.remove(self.pid_path)
            logger.info('Removed %s', self.pid_path)

        self.disconnect()
        thread = threading.Thread(target=self.start_threaded)
        thread.daemon = True
        thread.start()
        return True

    def stop(self):
        self.disconnect()
        return self.pg_ctl('stop', '-m', 'fast') != 0

    def reload(self):
        return self.pg_ctl('reload') == 0

    def restart(self):
        self.disconnect()
        return self.pg_ctl('restart', '-m', 'fast') == 0

    def is_healthy(self):
        if not self.is_running():
            logger.warning('Postgresql is not running.')
            return False
        return True

    def is_healthiest_node(self, cluster):
        if self.is_leader():
            return True

        if int(cluster.optime.value) - self.xlog_position() > self.config.maximum_lag:
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
        if self.config.password:
            method = 'md5'
        else:
            logger.warning('No password specified')
            method = 'trust'

        hba = ['local all all trust']
        for subnet in self.config.allow_address.split():
            hba.append(' '.join(['host', self.config.dbname, self.config.user, subnet, method]))

        for subnet in self.config.repl_allow_address.split():
            hba.append(' '.join(['host', 'replication', self.config.repl_user, subnet, method]))

        config = ConfigFile(os.path.join(self.data_dir, 'pg_hba.conf'))
        config.write_config(*hba)

    @staticmethod
    def primary_conninfo(leader_url):
        r = parseurl(leader_url)
        return 'user={user} password={password} host={host} port={port} sslmode=prefer sslcompression=1'.format(**r)

    def check_recovery_conf(self, leader):
        if not os.path.isfile(self.recovery_conf):
            return False

        pattern = (leader and self.primary_conninfo(leader.value))
        for key, value in RecoveryConf(self.recovery_conf).load_config():
            if key == 'primary_conninfo':
                if not pattern:
                    return False
                return value[1:-1] == pattern

        return not pattern

    def write_recovery_conf(self, leader):
        contents = [
            ('standby_mode', 'on'),
            ('recovery_target_timeline', 'latest'),
        ]
        if leader:
            contents.append(('primary_slot_name', self.name))
            contents.append(('primary_conninfo', self.primary_conninfo(leader.value)))

        config = RecoveryConf(self.recovery_conf)
        config.write_config(*contents, truncate = not leader)

    def follow_the_leader(self, leader):
        if not self.check_recovery_conf(leader):
            self.write_recovery_conf(leader)
            self.restart()

    def promote(self):
        self.promoted = (self.pg_ctl('promote') == 0)
        return self.promoted

    def create_users(self):
        op = ('ALTER' if self.config.user == 'postgres' else 'CREATE')
        query = '{} USER "{}" WITH {}'.format
        # normal client user
        self.create_user(query(op, self.config.user, 'SUPERUSER'), self.config.password)
        # replication user
        self.create_user(query('CREATE', self.config.repl_user, 'REPLICATION'), self.config.repl_password)

    def create_user(self, query, password):
        if password:
            return self.query(query + ' ENCRYPTED PASSWORD %s', password)
        return self.query(query)

    def xlog_position(self):
        return self.query("""SELECT CASE WHEN pg_is_in_recovery()
                                         THEN pg_last_xlog_replay_location() - '0/0000000'::pg_lsn
                                         ELSE pg_current_xlog_location() - '0/00000'::pg_lsn END""").fetchone()[0]

    def load_replication_slots(self):
        cursor = self.query("SELECT slot_name FROM pg_replication_slots WHERE slot_type='physical'")
        self.members = set(r[0] for r in cursor)

    def sync_replication_slots(self, members):
        members = set(name for name in members if name != self.name)
        # drop unused slots
        for slot in self.members - members:
            self.query("""SELECT pg_drop_replication_slot(%s)
                           WHERE EXISTS(SELECT 1 FROM pg_replication_slots
                           WHERE slot_name = %s)""", slot, slot)

        # create new slots
        for slot in members - self.members:
            self.query("""SELECT pg_create_physical_replication_slot(%s)
                           WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots
                           WHERE slot_name = %s)""", slot, slot)
        self.members = members

    def create_replication_slots(self, cluster):
        self.sync_replication_slots([name for name in cluster.members if name != self.name])

    def drop_replication_slots(self):
        self.sync_replication_slots([])

    def last_operation(self):
        return self.xlog_position()

class ConfigFile:
    __slots__ = ('path',)

    def __init__(self, path):
        self.path = path
        backup = self.path + '.backup'
        if not os.path.exists(backup):
            if os.path.exists(self.path):
                os.rename(self.path, backup)
            else:
                with open(backup, 'w'): pass

    def reload_backup(self):
        shutil.copy(self.path + '.backup', self.path)

    def load_config(self):
        with open(self.path) as file:
            for line in file:
                if not line.startswith('#'):
                    yield from file

    def write_config(self, *lines, reload=True, check_duplicates=True, truncate=False):
        if reload:
            self.reload_backup()
        if check_duplicates:
            config = set(self.load_config())
        else:
            config = ()
        mode = ('w' if truncate else 'a')
        with open(self.path, mode) as file:
            for l in lines:
                if l not in config:
                    file.write('\n' + l)
            file.write('\n')

class RecoveryConf(ConfigFile):
    def load_config(self):
        for line in super().load_config():
            yield line.partition(' = ')

    def write_config(self, *args, reload=True, check_duplicates=True, **kwargs):
        if reload:
            self.reload_backup()
        if check_duplicates:
            config = set(i[0] for i in self.load_config())
        else:
            config = ()
        args = ("{} = '{}'".format(k, v) for k, v in args if k not in config)
        return super().write_config(*args, reload=False, check_duplicates=False, **kwargs)
