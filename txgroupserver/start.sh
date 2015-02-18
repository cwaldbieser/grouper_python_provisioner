#! /bin/sh

THISDIR="$( cd $(dirname $0); pwd)"
SCRIPT="$THISDIR/$(basename $0)"
PYENV="$THISDIR/pyenv"
GROUPER_USER=grouper
LOGFILE=/var/log/txgroupserver/txgroupserver.log
STATUS_LOGFILE=/var/log/txgroupserver/status.log
PIDFILE="$THISDIR/twistd.pid"
STATUS_PIDFILE="$THISDIR/status.pid"

. "$PYENV/bin/activate"
twistd -u $(id -u "$GROUPER_USER") -g$(id -g "$GROUPER_USER") -y "$THISDIR/txgroupserver.py" -l "$LOGFILE" --pidfile "$PIDFILE"
twistd -u $(id -u "$GROUPER_USER") -g$(id -g "$GROUPER_USER") -y "$THISDIR/status2http.py" -l "$STATUS_LOGFILE" --pidfile "$STATUS_PIDFILE"
