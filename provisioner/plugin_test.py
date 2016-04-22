#! /usr/bin/env python

from txgroupprovisioner.interface import (
    IAttributeResolverFactory,
    IMessageParserFactory,
    IProvisionerFactory,
)
from twisted.plugin import getPlugins
import os
import sys

sys.path.append(
    os.path.join(os.path.dirname(__file__), "txsshadmin")
)

print("== IAttributeResolverFactory  test ==")
for n, thing in enumerate(getPlugins(IAttributeResolverFactory)):
    print("%02d %s" % (n, thing))
    print(thing.tag)
    print("")

print("== IMessageParserFactory test ==")
for n, thing in enumerate(getPlugins(IMessageParserFactory)):
    print("%02d %s" % (n, thing))
    print(thing.tag)
    print("")

print("== IProvisionerFactory test ==")
for n, thing in enumerate(getPlugins(IProvisionerFactory)):
    print("%02d %s" % (n, thing))
    print(thing.tag)
    print("")

