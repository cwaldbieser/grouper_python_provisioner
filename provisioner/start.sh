#! /bin/sh

THISDIR="$( cd $(dirname $0); pwd)"
SCRIPT="$THISDIR/$(basename $0)"
PYENV="$THISDIR/pyenv"
GROUPER_USER=grouper
GROUPER_GROUP=grouper
LOGFILE=/var/log/txgroupprovisioner/txgroupprovisioner.log
PIDFILE="$THISDIR/twistd.pid"

sudo -u "$GROUPER_USER" touch "$LOGFILE"
chown "$GROUPER_USER":"$GROUPER_GROUP" "$LOGFILE"
cd "$THISDIR"
. "$PYENV/bin/activate"
twistd -u $(id -u "$GROUPER_USER") -g$(id -g "$GROUPER_USER") -y "$THISDIR/txgroupprovisioner/txgroupprovisioner.py" --syslog --prefix txgroupprovisioner --pidfile "$PIDFILE"

