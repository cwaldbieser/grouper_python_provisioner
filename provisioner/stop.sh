#! /bin/sh

THISDIR="$( cd $(dirname $0); pwd)"
SCRIPT="$THISDIR/$(basename $0)"
CONFD="$THISDIR/conf.d"

cd "$THISDIR"
ls "$CONFD"/*.cfg | while read fname; do
    BASE=$(basename "$fname" .cfg)
    PIDFILE="$BASE.pid"
    if [ -f "$PIDFILE" ]; then
        PID=$(cat "$PIDFILE")
        if [ ! -z "$PID" ]; then
            kill -INT "$PID"
        fi
    fi
done

