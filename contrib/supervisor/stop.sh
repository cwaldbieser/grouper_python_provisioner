#! /bin/sh

THISDIR="$( cd $(dirname $0); pwd)"
SCRIPT="$THISDIR/$(basename $0)"
PIDFILE=/tmp/supervisord.pid

if [ -f "$PIDFILE" ]; then
    pid=$(cat "$PIDFILE")
    if [ ! -z "$pid" ]; then
        kill -INT "$pid"
        exit $?
    fi
fi
exit 1

