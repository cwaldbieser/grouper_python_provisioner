#! /bin/bash

SCRIPT=$(readlink -f "$0")
THISDIR=$(dirname "$SCRIPT")
PIDFILE="$THISDIR/twistd.pid"

if [ -f "$PIDFILE" ]; then
    pid=$(cat "$PIDFILE")
    if [ ! -z "$pid" ]; then
        kill -HUP "$pid"
        exit $?
    fi
fi
exit 1

