
from __future__ import print_function
from collections import namedtuple
import json
import os
import re
from textwrap import dedent
import commentjson
from twisted.internet import task
from twisted.internet.defer import (
    inlineCallbacks, 
    returnValue,
)
from twisted.internet.endpoints import clientFromString, connectProtocol
from twisted.logger import Logger
from twisted.plugin import IPlugin
from txamqp.client import TwistedDelegate
from txamqp.content import Content
from txamqp.protocol import AMQClient
from txamqp.queue import Closed as QueueClosedError
import txamqp.spec
from zope.interface import implements
from config import load_config, section2dict
from errors import (
    OptionMissingError,
    NoMatchingMessageParserError,
    UnknownAttributeResolverError,
)
from interface import (
    IAttributeResolverFactory,
    IGroupMapperFactory,
    IMessageParserFactory,
    IProvisionerFactory,
    IProvisioner,
    IRouterFactory,
)
import kikimessage
from utils import get_plugin_factory


ParserMatcher = namedtuple(
    'ParserMatcher',
    ['exchange_pattern', 'route_key_pattern', 'parser'])


class KikiProvisionerFactory(object):
    implements(IPlugin, IProvisionerFactory)

    tag = "kiki"
    opt_help = "A provisioner delivery service provisioner."
    opt_usage = "This plugin does not support any options."

    def generateProvisioner(self, argstring=""):
        """
        Create an object that implements IProvisioner
        """
        return KikiProvisioner()


class KikiProvisioner(object):
    implements(IProvisioner)

    service_state = None
    reactor = None
    log = None

    @inlineCallbacks
    def load_config(self, config_file, default_log_level, logObserverFactory):
        """                                                             
        Load the configuration for this provisioner and initialize it.  
        """             
        log = Logger(observer=logObserverFactory("ERROR"))
        try:
            # Load config.
            config_parser = load_config(config_file, defaults=self.get_config_defaults())
            section = "PROVISIONER"
            config = section2dict(config_parser, section)
            self.config = config
            # Start logger.
            log_level = config.get('log_level', default_log_level)
            log = Logger(observer=logObserverFactory(log_level))
            self.log = log
            log.debug("Initialized logging for Kiki provisioner delivery service.",
                event_type='init_provisioner_logging')
            # Load and configure the attribute resolver.
            attrib_resolver_tag = get_config_opt(config, section, "attrib_resolver")
            self.install_attribute_resolver(attrib_resolver_tag, config_parser)
            # Load parse map.
            parser_map_filename = get_config_opt(config, section, "parser_map")
            self.load_parser_map(parser_map_filename)
            # Install group mapper.
            group_mapper_tag = get_config_opt(config, section, "group_mapper")
            self.install_group_mapper(group_mapper_tag, config_parser)
            # Install the router
            router_tag = get_config_opt(config, section, "router")
            self.install_router(router_tag, config_parser)
            # Connect to exchange for publishing.
            self.configure_target_exchange(config_parser)
            yield self.connect_to_exchange() 
        except Exception as ex:
            d = self.reactor.callLater(0, self.reactor.stop)
            log.failure("Provisioner failed to initialize: {0}".format(ex))
            raise
                     
    @inlineCallbacks                                                   
    def provision(self, amqp_message):             
        """                                                
        Provision an entry based on an AMQP message.  
        """                                              
        log = self.log
        try:
            log.debug("Begin processing message.")
            msg_parser = self.get_message_parser(amqp_message)
            log.debug(
                "Created message parser '{parser_type}'.",
                parser_type=msg_parser.__class__.__name__)
            parsed = msg_parser.parse_message(amqp_message)
            msg_type = parsed.__class__.__name__
            log.debug("Parsed message of type '{msg_type}'.", msg_type=msg_type)
            mapper = self.group_mapper
            log.debug("Getting groups for message ...")
            groups = yield parsed.get_groups(mapper)
            log.debug("Groups used for routing: {groups}", groups=groups)
            if len(groups) == 0:
                log.debug(
                    "Message did not produce any groups of interest.")
                returnValue(None)
            target_route_key, attributes_required = yield self.get_route_info(parsed, groups)
            log.debug(
                "Routing results: route_key={route_key}, "
                "attributes_required={attributes_required}",
                route_key=target_route_key,
                attributes_required=attributes_required)
            if target_route_key is None:
                log.debug("Discarding message based on route.")
                returnValue(None)
            if attributes_required:
                log.debug("Resolving attributes for message ...")
                yield parsed.resolve_attributes(self.attrib_resolver)
            log.debug("Delivering message to exchange ...")
            yield self.send_message(target_route_key, parsed)
        except Exception as ex:
            log.warn("Error provisioning target: {error}", error=ex)
            raise

    def get_config_defaults(self):
        """
        Return option defaults.
        """
        spec_dir = os.path.join(os.path.split(os.path.split(__file__)[0])[0], "spec")
        spec_path = os.path.join(spec_dir, "amqp0-9-1.stripped.xml")
        return dedent("""\
            [PROVISIONER]
            parser_map = parser_map.json
            attrib_resolver = rdbms_attrib_resolver
            group_mapper = null_group_mapper
            router = json_router
            
            [AMQP_TARGET]
            endpoint = tcp:host=127.0.0.1:port=5672
            log_level = INFO
            exchange = grouper_exchange
            queue = kiki_q
            vhost = /
            user = guest
            passwd = guest
            spec = {spec_path}
            """).format(spec_path=spec_path)

    def configure_target_exchange(self, scp):
        """
        Configure the parameters for connection to the target exchange.
        """
        section = "AMQP_TARGET"
        publisher_config = section2dict(scp, section)
        self.pub_exchange = get_config_opt(publisher_config, section, "exchange")
        self.pub_vhost = get_config_opt(publisher_config, section, "vhost")
        self.pub_user = get_config_opt(publisher_config, section, "user")
        self.pub_passwd = get_config_opt(publisher_config, section, "passwd")
        self.pub_endpoint_s = get_config_opt(publisher_config, section, "endpoint")
        spec_path = get_config_opt(publisher_config, section, "spec")
        self.pub_spec = txamqp.spec.load(spec_path)

    def install_attribute_resolver(self, tag, config_parser):
        """
        Configure the component that will be used to perform attribute
        resolution.
        """
        factory = get_plugin_factory(tag, IAttributeResolverFactory)
        if factory is None:
            raise UnknownAttributeResolverError(
                "The attribute resolver identified by tag '{0}' is unknown.".format(
                    tag))
        attrib_resolver = factory.generate_attribute_resolver(config_parser)
        attrib_resolver.log = self.log
        self.attrib_resolver = attrib_resolver

    def install_router(self, tag, config_parser):
        """
        Configure the component that will be used to route messages to
        provisioner targets.
        """
        factory = get_plugin_factory(tag, IRouterFactory)
        if factory is None:
            raise UnknownRouterError(
                "The router identified by tag '{0}' is unknown.".format(tag))
        router = factory.generate_router(config_parser)
        router.log = self.log
        self.router = router 

    def install_group_mapper(self, tag, config_parser):
        """
        Configure the component that will be used to map a bare subject to
        groups of interest.
        """
        factory = get_plugin_factory(tag, IGroupMapperFactory)
        if factory is None:
            raise UnknownRouterError(
                "The group mapper identified by tag '{0}' is unknown.".format(
                    tag))
        group_mapper = factory.generate_group_mapper(config_parser)
        group_mapper.log = self.log
        self.group_mapper = group_mapper 

    def load_parser_map(self, parser_map_filename):
        """
        Load the parser map.
        """
        log = self.log
        self.parser_mappings = []
        with open(parser_map_filename) as f:
            doc = commentjson.load(f)
        for entry_index, entry in enumerate(doc):
            valid = True
            for required in ('exchange', 'route_key', 'parser'):
                if not required in entry:
                    log.error("Parse map entry {entry_index} missing required field '{required}'.  This entry will be skipped.", 
                        entry_index=entry_index,
                        required=required)
                    valid = False
            if not valid:
                continue
            try:
                exchange = re.compile(entry["exchange"])
            except re.error as ex:
                log.error("Parser map entry {entry_index}: Exchange pattern '{exchange}' is not a valid regular expression.  Error was {regex_error}.",
                    entry_index=entry_index,
                    exchange=entry["exchange"],
                    regex_error=ex)
                valid = False
            try:
                route_key = re.compile(entry["route_key"])
            except re.error as ex:
                log.error("Parser map entry {entry_index}: Route key pattern '{route_key}' is not a valid regular expression.  Error was {regex_error}.",
                    entry_index=entry_index,
                    route_key=entry["route_key"],
                    regex_error=ex)
                valid = False
            parser_tag = entry["parser"]
            parser_args = entry.get("parser_args")
            if parser_args is None:
                parser_args = {}
            factory = get_plugin_factory(parser_tag, IMessageParserFactory)
            if factory is None:
                log.error("Parser map entry {entry_index}: Could not find a matching parser for tag '{parser_tag}'.",
                entry_index=entry_index,
                parser_tag=parser_tag)
                continue
            parser = factory.generate_message_parser(**parser_args)
            matcher = ParserMatcher(exchange, route_key, parser)
            self.parser_mappings.append(matcher)

    def get_message_parser(self, msg):
        """
        Return a message parser (IMessageParser) based on the message
        characteristics.
        """
        consumer_tag, delivery_tag, redelivered, exchange_name, route_key = msg.fields
        parser_mappings = self.parser_mappings
        for exchange_pattern, route_pattern, parser in parser_mappings:
            if exchange_pattern.match(exchange_name) is not None:
                if route_pattern.match(route_key) is not None:
                    return parser
        raise NoMatchingMessageParserError(
            "A parser for exchange '{0}' and route_key '{1}' could not be determined.".format(
                exchange_name,
                route_key))

    @inlineCallbacks
    def connect_to_exchange(self):
        """
        Connect to an AMQP exchange as a publisher.
        """
        exchange = self.pub_exchange
        vhost = self.pub_vhost
        user = self.pub_user
        passwd = self.pub_passwd
        endpoint_s = self.pub_endpoint_s
        spec = self.pub_spec
        e = clientFromString(self.reactor, endpoint_s)
        delegate = TwistedDelegate()
        amqp_protocol = AMQClient(
            delegate=delegate,
            vhost=vhost,
            spec=spec)
        try:
            conn = yield connectProtocol(e, amqp_protocol)
        except Exception:
            self.log.failure(
                "Failed to establish AMQP connection to endpoint '{0}'".format(
                    endpoint_s))
            raise
        yield conn.authenticate(user, passwd)
        self.pub_channel = yield conn.channel(1)
        yield self.pub_channel.channel_open()

    @inlineCallbacks
    def get_route_info(self, instructions, groups):
        """
        Get the target route information  based on the instructions parsed from the 
        original message.
        Returns `RouteData` named tuple.
        """
        route_info = yield self.router.get_route(instructions, groups)
        returnValue(route_info)

    @inlineCallbacks
    def send_message(self, route_key, instructions):
        """
        Compose a provisioning message and deliver it to an exchange with
        routing key `route_key`.
        """
        log = self.log
        exchange = self.pub_exchange
        serialized = instructions.serialize()
        msg = Content(serialized)
        msg["delivery-mode"] = 2
        success = False
        reconnect = False
        delay_time = 20
        while not success:
            try:
                if reconnect:
                    log.INFO("Attempting to recconect to publisher exchange ...")
                    yield self.connect_to_exchange()
                channel = self.pub_channel
                channel.basic_publish(
                    exchange=exchange,
                    content=msg,
                    routing_key=route_key)
                success = True
                reconnect = False
            except Exception as ex:
                log.warn("Error attempting to publish message: {error}", error=ex)
                log.warn("Will attempt to reconnect.")
                yield delay(self.reactor, delay_time)
                reconnect = True
        log.debug(
            "Sent message to target exchange '{exchange}' with routing key '{route_key}'.",
            event_type="amqp_send",
            exchange=exchange,
            route_key=route_key)
        log.debug("Send message: {msg}",
            event_type="amqp_send_msg",
            msg=serialized)

@inlineCallbacks            
def delay(reactor, seconds):
    """
    A Deferred that fires after `seconds` seconds.
    """
    yield task.deferLater(reactor, seconds, lambda : None)

def get_config_opt(config, section, opt):
    """
    Return the config option from the mapping `config` or
    raise `OptionMissingError`.
    """
    try:
        return config[opt]
    except KeyError as ex:
        raise OptionMissingError(
            "A require option was missing: '{0}:{1}'.".format(
                section, ex.args[0]))
    except KeyError as ex:
        raise OptionMissingError(
            "A require option was missing: '{0}:{1}'.".format(
                section, ex.args[0]))




