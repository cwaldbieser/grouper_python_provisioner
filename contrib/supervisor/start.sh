#! /bin/sh

THISDIR="$( cd $(dirname $0); pwd)"
SCRIPT="$THISDIR/$(basename $0)"
PYENV="$THISDIR/pyenv"
CONFIG="$THISDIR/supervisord.conf"

. "$PYENV/bin/activate"
cd "$THISDIR"
supervisord -c "$CONFIG"

