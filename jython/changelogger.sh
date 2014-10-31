#! /bin/sh

trap "kill -- -$$" EXIT

THISDIR="$( cd $(dirname $0); pwd)"
SCRIPT="$iTHISDIR/$(basename $0)"
CHANGELOGGER="$THISDIR/process_changelog.py"
CONFIG="$THISDIR/process_changelog.cfg"
export GROUPER_HOME=/opt/internet2/grouper/grouper.apiBinary-2.2.0
GSH_JYTHON="$GROUPER_HOME/bin/gsh.jython"

cd "$THISDIR"
"$GSH_JYTHON" "$CHANGELOGGER" -c "$CONFIG"
