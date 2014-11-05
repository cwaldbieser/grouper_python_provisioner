#! /bin/sh

THISDIR="$( cd $(dirname $0); pwd)"
SCRIPT="$THISDIR/$(basename $0)"
PYENV="$THISDIR/pyenv"
GROUPER_USER=grouper
LOGFILE=/var/log/txgroupserver/txgroupserver.log
PIDFILE="$THISDIR/twistd.pid"

. "$PYENV/bin/activate"
twistd -u $(id -u "$GROUPER_USER") -g$(id -g "$GROUPER_USER") -y "$THISDIR/txgroupserver.py" -l "$LOGFILE" --pidfile "$PIDFILE"
