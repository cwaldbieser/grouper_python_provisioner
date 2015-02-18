#! /bin/sh

THISDIR="$( cd $(dirname $0); pwd)"
SCRIPT="$THISDIR/$(basename $0)"

PIDFILE="$THISDIR/twistd.pid"
if [ -f "$PIDFILE" ]; then
    pid=$(cat "$PIDFILE")
    if [ ! -z "$pid" ]; then
        kill -HUP "$pid"
        retval=$?
    fi
fi
if [ "$retval" -ne 0 ]; then
    exit 1
fi

PIDFILE="$THISDIR/status.pid"
if [ -f "$PIDFILE" ]; then
    pid=$(cat "$PIDFILE")
    if [ ! -z "$pid" ]; then
        kill -HUP "$pid"
        retval=$?
    fi
fi
if [ "$retval" -ne 0 ]; then
    exit 1
fi

exit "$retval"
