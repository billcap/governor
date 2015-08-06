#!/bin/sh

export PGPASS=/pgpass
touch "$PGPASS"
chown -R postgres "$PGDATA" "$PGPASS"
exec su -c "$*" postgres
