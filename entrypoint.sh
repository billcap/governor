#!/bin/bash

chown -R postgres "$PGDATA" "$WAL_ARCHIVE"
exec "$@"