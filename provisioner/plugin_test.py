#! /usr/bin/env python

from txgroupprovisioner.interface import (
    IAttributeResolverFactory,
    IGroupMapperFactory,
    IMessageParserFactory,
    IProvisionerFactory,
    IRouterFactory,
)
from twisted.plugin import getPlugins
import os
import sys

sys.path.append(
    os.path.join(os.path.dirname(__file__), "txsshadmin")
)

interfaces = (
    IAttributeResolverFactory,
    IGroupMapperFactory,
    IMessageParserFactory,
    IProvisionerFactory,
    IRouterFactory,
)

for iface in interfaces:
    print("== {0} test ==".format(iface.__name__))
    for n, thing in enumerate(getPlugins(iface)):
        print("%02d %s" % (n, thing))
        print(thing.tag)
        print("")
