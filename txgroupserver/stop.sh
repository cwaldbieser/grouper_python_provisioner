#! /bin/sh

THISDIR="$( cd $(dirname $0); pwd)"
SCRIPT="$THISDIR/$(basename $0)"
PIDFILE="$THISDIR/twistd.pid"

if [ -f "$PIDFILE" ]; then
    pid=$(cat "$PIDFILE")
    if [ ! -z "$pid" ]; then
        kill -HUP "$pid"
        exit $?
    fi
fi
exit 1

