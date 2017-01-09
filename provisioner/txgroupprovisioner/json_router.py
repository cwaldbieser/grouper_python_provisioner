
from __future__ import print_function
from commentjson import load as load_json
from twisted.internet import defer
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
    implements(IRouter)
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
                "allow_actions": ["add", "delete", "update"],
                "route_key": "orgsync"
            },
            {
                "name": "Default",
                "group": "*",
                "discard": true
            }
        ]
        """
        with open(path, "r") as f:
            doc = load_json(f)
        self.create_route_map(doc)

    def create_route_map(self, doc):
        """
        Create the internal routing map from the JSON representiation.
        """
        log = self.log
        routes = []
        for n, entry in enumerate(doc):
            route_entry = RouteEntry(n, entry)
            routes.append(route_entry)
        self.routes = routes

    def get_route(self, instructions, groups):
        """
        Return a Deferred that fires with a RouteInfo
        object or raises a NoMatchingRouteError.
        If a message should be discarded, the route should
        map to None.
        """
        log = self.log
        action = instructions.action
        routes = self.routes
        route_keys = []
        attributes_required = False
        for group in groups:
            matched = False
            for route in routes:
                route.log = self.log
                if route.match(group, action):
                    matched = True
                    if route.discard:
                        log.debug(
                            "Discarding group '{group}' from routing consideration.",
                            group=group)
                        break
                    else:
                        route_keys.append(route.route_key)
                        attributes_required = attributes_required or route.include_attributes
                        break
            if not matched:
                raise NoMatchingRouteError(
                    "There is not route that matches group '{0}'.".format(
                        group))
        if len(route_keys) == 0:
            route_info = RouteInfo(None, False)
        else:
            # Remove duplicate route keys.
            routekey_set = set([])
            temp = []
            for k in route_keys:
                if not k in routekey_set:
                    temp.append(k)
                    routekey_set.add(k)
            route_keys = temp
            del temp
            route_info = RouteInfo(
                '.'.join(route_keys),
                attributes_required)
        return defer.succeed(route_info)


class RouteEntry(object):
    log = None

    def __init__(self, n, props):
        self.index = n + 1
        if "group" in props and "stem" in props:
            msg = (
                "Cannot have both 'group' and 'stem' patterns "
                "in a route entry number {0}.").format(n+1)
            raise JSONRouteEntryError(msg)
        if "group" not in props and "stem" not in props:
            msg = (
                "Must have either 'group' or 'stem' pattern "
                "in route entry number {0}.").format(n+1)
            raise JSONRouteEntryError(msg)
        self.group = props.get("group", None)
        self.stem = props.get("stem", None)
        if "allowed_actions" in props:
            self.allowed_actions = set(action.lower() for action in props["allowed_actions"])
        else:
            self.allowed_actions = None
        if self.stem is not None and not self.stem.endswith(":"):
            self.stem = "{0}:".format(self.stem)
        if self.stem is None and "recursive" in props:
            msg = (
                "'recursive' property is only valid for 'stem' pattern"
                "in route entry number {0}.").format(n+1)
            raise JSONRouteEntryError(msg)
        self.recursive = bool(props.get("recursive", False))
        self.include_attributes = bool(props.get("include_attributes", False))
        self.discard = bool(props.get("discard", False))
        self.route_key = props.get("route_key", None)
        if self.route_key is None and not self.discard:
            msg = (
                "Missing 'route_key' "
                "in route entry number {0}.").format(n+1)
            raise JSONRouteEntryError(msg)
        if self.discard and self.include_attributes:
            msg = (
                "'include_attributes' and 'discard' are mutally exclusive "
                "in route entry number {0}.").format(n+1)
            raise JSONRouteEntryError(msg)
        if self.discard and self.route_key is not None:
            msg = (
                "'route_key' and 'discard' are mutally exclusive "
                "in route entry number {0}.").format(n+1)
            raise JSONRouteEntryError(msg)

    def match(self, group, action):
        """
        Return True if the group matches the entry; False otherwise.
        """
        log = self.log
        allowed_actions = self.allowed_actions
        log.debug(
            "Testing route for group '{group}', action '{action}'",
            group=group, 
            action=action)
        log.debug(
            "Route group: '{group}', stem: '{stem}', allowed_actions: {allowed_actions}",
            group=self.group,
            stem=self.stem,
            allowed_actions=allowed_actions)
        if self.group == group or self.group == "*":
            if (allowed_actions is None) or action.lower() in allowed_actions:
                return True
        elif self.stem is not None and group.startswith(self.stem):
            if self.recursive:
                if (allowed_actions is None) or action.lower() in allowed_actions:
                    return True
            suffix = group[len(self.stem):]
            if ":" not in suffix:
                if (allowed_actions is None) or action.lower() in allowed_actions:
                    return True
        return False


class JSONRouteEntryError(Exception):
    pass

        
