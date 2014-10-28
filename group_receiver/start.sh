#! /bin/bash

SCRIPT=$(readlink -f "$0")
THISDIR=$(dirname "$SCRIPT")
PYENV="$THISDIR/pyenv"

. "$PYENV/bin/activate"
twistd -y "$THISDIR/txgroupserver.py"

