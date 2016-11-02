#! /bin/sh

THISDIR="$( cd $(dirname $0); pwd)"
SCRIPT="$THISDIR/$(basename $0)"
TXSSHADMIN="$THISDIR/txsshadmin"

if [ -z "$PYTHONPATH" ]; then
    export PYTHONPATH="$THISDIR:$TXSSHADMIN"
else
    export PYTHONPATH="$PYTHONPATH:$THISDIR:$TXSSHADMIN"
fi
twistd $@

