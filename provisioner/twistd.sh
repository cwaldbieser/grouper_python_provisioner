#! /bin/sh

THISDIR="$( cd $(dirname $0); pwd)"
SCRIPT="$THISDIR/$(basename $0)"
TXSSHADMIN="$THISDIR/txsshadmin"

if [ -z "$PYTHONPATH" ]; then
    export PYTHONPATH="$TXSSHADMIN"
else
    export PYTHONPATH="$PYTHONPATH:$TXSSHADMIN"
fi
twistd $@

