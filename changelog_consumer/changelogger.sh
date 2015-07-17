#! /bin/sh

trap "kill -- -$$" EXIT

THISDIR="$( cd $(dirname $0); pwd)"
SCRIPT="$THISDIR/$(basename $0)"
CHANGELOGGER="$THISDIR/process_changelog.py"
GROUPER_HOME=${GROUPER_HOME:-/opt/internet2/grouper/grouper.apiBinary}
export GROUPER_HOME
GSH_JYTHON="$GROUPER_HOME/bin/gsh.jython"
CACHEDIR="$THISDIR/cachedir"
RABBITMQ_JAR="$THISDIR/lib/rabbitmq-client.jar"
if [ -z "$CLASSPATH" ]; then
    CLASSPATH="$RABBITMQ_JAR"
else
    CLASSPATH="$CLASSPATH:$RABBITMQ_JAR"
fi
export CLASSPATH
cd "$THISDIR"
"$GSH_JYTHON" -p "python.cachedir=$CACHEDIR" "$CHANGELOGGER"
