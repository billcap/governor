#!/bin/sh

chown -R postgres "$ROOT"
exec sudo -E -u postgres "$@"
