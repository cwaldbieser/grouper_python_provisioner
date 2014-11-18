#! /bin/sh

trap "kill -- -$$" EXIT

THISDIR="$( cd $(dirname $0); pwd)"
SCRIPT="$THISDIR/$(basename $0)"
CHANGELOGGER="$THISDIR/process_changelog.py"
export GROUPER_HOME=/opt/internet2/grouper/grouper.apiBinary
GSH_JYTHON="$GROUPER_HOME/bin/gsh.jython"
CACHEDIR="$THISDIR/cachedir"

cd "$THISDIR"
"$GSH_JYTHON" -p "python.cachedir=$CACHEDIR" "$CHANGELOGGER"
