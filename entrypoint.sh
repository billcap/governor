#!/bin/sh

export PGPASS=/pgpass
touch "$PGPASS"
chown -R postgres "$PGDATA" "$PGPASS"
exec sudo -E -u postgres "$@"
