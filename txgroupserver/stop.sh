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
retval0="$retval"

PIDFILE="$THISDIR/status.pid"
if [ -f "$PIDFILE" ]; then
    pid=$(cat "$PIDFILE")
    if [ ! -z "$pid" ]; then
        kill -HUP "$pid"
        retval1=$?
    fi
fi
if [ "$retval0" -ne 0 ]; then
    exit 1
fi
if [ "$retval1" -ne 0 ]; then
    exit 1
fi

exit 0
