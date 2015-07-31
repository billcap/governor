#!/bin/sh

export PGPASS=/pgpass
touch "$PGPASS"
chown -R postgres "$PGDATA" "$WAL_ARCHIVE" "$PGPASS"
exec su -c "$*" postgres
