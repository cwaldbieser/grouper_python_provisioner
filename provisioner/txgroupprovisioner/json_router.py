
from __future__ import print_function
import json
from twisted.internet.defer import (
    inlineCallbacks, 
    returnValue,
)
from twisted.plugin import IPlugin
from zope.interface import implements
from interface import (
    IRouterFactory,
    IRouter,
)
from errors import (
    NoMatchingRouteError,
)
from kikiroute import RouteInfo


class JSONRouterFactory(object):
    implements(IPlugin, IRouterFactory)
    tag = "json_router"

    def generate_router(self, config_parser):
        """
        Create an object that implements IRouter.
        """
        section = "JSON Router"
        options = config_parser.options(section)
        path = config_parser.get(section, "json_file")
        router = JSONRouter(path) 
        return router


class JSONRouter(object):
    implements(IGroupMapper)
    log = None

    def __init__(self, path):
        """
        JSON routing map format:

        [
            {
                "name": "Splunk",
                "stem": "lc:app:splunk:exports",
                "recursive": false,
                "include_attributes": false,
                "route_key": "splunk"
            },
            {
                "name": "VPN",
                "group": "lc:app:vpn:vpn",
                "include_attributes": false,
                "route_key": "vpn"
            },
            {
                "name": "OrgSync",
                "stem": "lc:app:orgsync:exports",
                "include_attributes": true,
                "route_key": "orgsync"
            },
            {
                "name": "Default",
                "group_pattern": "*",
                "discard": true
            }
        ]
        """
        with open(path, "r") as f:
            doc = json.load(f)
        self.create_route_map(doc)

    def create_route_map(self, doc):
        """
        Create the internal routing map from the JSON representiation.
        """
        routes = []
        for n, entry in enumerate(doc):
            routes.append(RouteEntry(n, entry))
        self.routes = routes

    @inlineCallbacks
    def get_route(self, instructions, groups):
        """
        Return a Deferred that fires with a RouteInfo
        object or raises a NoMatchingRouteError.
        If a message should be discarded, the route should
        map to None.
        """
        routes = self.routes
        route_keys = []
        attributes_required = False
        for group in groups:
            for route in routes:
                match = route.match(group):
                if match = RouteEntry.match_result:
                    route_keys.append(route.route_key)
                    attributes_required = attributes_required or route.attributes_required
                    continue
                elif match == RouteEntry.discard_result:
                    returnValue(RouteInfo(None, False))
                else:
                    raise NoMatchingRouteError(
                        "There is not route that matches group '{0}'.".format(
                            group))
        route_info = RouteInfo(
            '.'.join(route_keys),
            attributes_required)
        returnValue(route_info)        


class RouteEntry(object):
    match_result = 0
    discard_result = 1

    def __init__(self, n, props):
        self.index = n
        if "group" in props and "stem" in props:
            msg = (
                "Cannot have both 'group' and 'stem' patterns "
                "in a route entry number {0}.").format(n)
            raise JSONRouteEntryError(msg)
        if "group" not in props and "stem" not in props:
            msg = (
                "Must have either 'group' or 'stem' pattern "
                "in route entry number {0}.").format(n)
            raise JSONRouteEntryError(msg)
        self.group = props.get("group", None)
        self.stem = props.get("stem", None)
        #recursive, include_attributes, route_key, discard
        if self.stem is None and "recursive" in props:
            msg = (
                "'recursive' property is only valid for 'stem' pattern"
                "in route entry number {0}.").format(n)
            raise JSONRouteEntryError(msg)
        self.recursive = bool(props.get("recursive", False))
        self.include_attributes = bool(props.get("include_attributes", False))
        self.discard = bool(props.get("discard", False))
        self.route_key = props.get("route_key", None)
        if self.route_key is None:
            msg = (
                "Missing 'route_key' "
                "in route entry number {0}.").format(n)
            raise JSONRouteEntryError(msg)
        if self.discard and self.include_attributes:
            msg = (
                "'include_attributes' and 'discard' are mutally exclusive "
                "in route entry number {0}.").format(n)

    def match(self, group):
        pass 


class JSONRouteEntryError(Exception):
    pass

        
