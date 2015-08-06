FILE=$PGDATA/postgresql.conf
cat <<EOS > $FILE
archive_mode = 'on'
wal_level = 'hot_standby'
archive_command = 'mkdir -p ../wal_archive && cp %p ../wal_archive/%f'
max_wal_senders = '5'
wal_keep_segments = '8'
archive_timeout = '1800s'
max_replication_slots = '5'
hot_standby = 'on'
EOS
cp $FILE $FILE.backup
