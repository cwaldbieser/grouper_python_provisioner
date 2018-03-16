#! /bin/bash

THISDIR="$( cd $(dirname $0); pwd)"
SCRIPT="$THISDIR/$(basename $0)"
ITERATION=${ITERATION:-1}
FPM=${FPM:-/usr/bin/fpm}
OS_DEPS='jython >= 2.7.0'
RPM_STEM="pychangelogger"
SOFTWARE=$(readlink -f "/opt/pychangelogger")
AFTER_INSTALL="$THISDIR/post-install.sh"
OUTDIR=${OUTDIR:-/var/local/rpms}

function get_version
{
    pushd "$WEBROOT" > /dev/null
    git describe --abbrev=0 --tags
    popd > /dev/null
}
VERSION=$(get_version)

pushd "$OUTDIR" > /dev/null
"$FPM" \
    --verbose -s dir -t rpm -d "$OS_DEPS" -n "$RPM_STEM" \
    -v "$VERSION" --iteration "$ITERATION" \
    -x '*.pyc' -x '*.pem' -x '*.pid' -x '*.log' -x '*.gitignore' \
    -x '*.cfg' -x '*.tgz' -x '*.zip' -x '*.tar.gz' -x '*.pkc' \
    -x '*.log' \
    --exclude '*/.git' \
    --rpm-use-file-permissions \
    --after-install "$AFTER_INSTALL" \
    -p "$OUTDIR" \
    "$SOFTWARE"
popd > /dev/null

