#! /bin/sh

THISDIR="$( cd $(dirname $0); pwd)"
SCRIPT="$THISDIR/$(basename $0)"
PYENV="$THISDIR/pyenv"
GROUPER_USER=grouper
GROUPER_GROUP=grouper
LOGFILE=/var/log/txgroupserver/txgroupserver.log
STATUS_LOGFILE=/var/log/txgroupserver/status.log
PIDFILE="$THISDIR/twistd.pid"
STATUS_PIDFILE="$THISDIR/status.pid"

sudo -u "$GROUPER_USER" touch "$LOGFILE"
sudo -u "$GROUPER_USER" touch "$STATUS_LOGFILE"
chown "$GROUPER_USER":"$GROUPER_GROUP" "$LOGFILE" "$STATUS_LOGFILE"
cd "$THISDIR"
. "$PYENV/bin/activate"
twistd -u $(id -u "$GROUPER_USER") -g$(id -g "$GROUPER_USER") -y "$THISDIR/txgroupserver.py" --syslog --prefix txgroupserver --pidfile "$PIDFILE"
twistd -u $(id -u "$GROUPER_USER") -g$(id -g "$GROUPER_USER") -y "$THISDIR/status2http.py" --syslog --prefix txgroupstatus --pidfile "$STATUS_PIDFILE"

