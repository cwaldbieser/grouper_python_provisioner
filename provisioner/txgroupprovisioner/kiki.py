
from __future__ import print_function
from collections import namedtuple
import json
import os
import re
from textwrap import dedent
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
    IMessageParserFactory,
    IProvisionerFactory,
    IProvisioner,
)
from utils import get_plugin_factory


ParserMatcher = namedtuple(
    'ParserMatcher',
    ['exchange_pattern', 'route_key_pattern', 'parser'])


RouteData = namedtuple(
    'RouteData',
    ['route_key', 'attributes_required'])


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
            scp = load_config(config_file, defaults=self.get_config_defaults())
            config = section2dict(scp, "PROVISIONER")
            self.config = config
            # Start logger.
            log_level = config.get('log_level', default_log_level)
            log = Logger(observer=logObserverFactory(log_level))
            self.log = log
            log.debug("Initialized logging for Kiki provisioner delivery service.",
                event_type='init_provisioner_logging')
            # Load and configure the attribute resolver.
            attrib_resolver_tag = config["attrib_resolver"]
            factory = get_plugin_factory(attrib_resolver_tag, IAttributeResolverFactory)
            if factory is None:
                raise UnknownAttributeResolverError(
                    "The attribute resolver identified by tag '{0}' is unknown.".format(
                        attrib_resolver_tag))
            attrib_resolver = factory.generate_attribute_resolver(scp)
            attrib_resolver.log = self.log
            self.attrib_resolver = attrib_resolver
            # Load parse map.
            parser_map_filename = config["parser_map"]
            self.load_parser_map(parser_map_filename)
            # Connect to exchange for publishing.
            section = "AMQP_TARGET"
            publisher_config = section2dict(scp, section)
            try:
                self.pub_exchange = publisher_config["exchange"]
                self.pub_vhost = publisher_config["vhost"]
                self.pub_user = publisher_config["user"]
                self.pub_passwd = publisher_config["passwd"]
                self.pub_endpoint_s = publisher_config["endpoint"]
                spec_path = publisher_config["spec"]
            except KeyError as ex:
                raise OptionMissingError(
                    "A require option was missing: '{0}:{1}'.".format(
                        section, ex.args[0]))
            self.pub_spec = txamqp.spec.load(spec_path)
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
            msg_parser = self.get_message_parser(amqp_message)
            instructions = msg_parser.parse_message(amqp_message)
            target_route_key, attributes_required = yield self.get_route_info(instructions)
            if attributes_required:
                attribs = yield self.query_subject(instructions)
                instructions.attributes.update(attribs)
            yield self.send_message(target_route_key, instructions)
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
            group_filter = rdbms_group_filter
            
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

    def load_parser_map(self, parser_map_filename):
        """
        Load the parser map.
        """
        log = self.log
        self.parser_mappings = []
        with open(parser_map_filename) as f:
            doc = json.load(f)
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
    def query_subject(self, instructions):
        """
        Return a dictionary of attribute mappings for a subject.
        """
        # Attributes requested/returned should be based on some kind of 
        # filter / mapping / logic.
        subject = instructions.subject
        attributes = yield self.attrib_resolver.resolve_attributes(subject)
        returnValue(attributes)

    @inlineCallbacks
    def get_target_route_key(self, instructions):
        """
        Get the target route key based on the instructions parsed from the 
        original message.
        Returns `RouteData` named tuple.
        """
        returnValue(RouteData("todo.route_key", False))

    @inlineCallbacks
    def send_message(self, route_key, instructions):
        """
        Compose a provisioning message and deliver it to an exchange with
        routing key `route_key`.
        """
        log = self.log
        exchange = self.pub_exchange
        message = {
            "action": instructions.action,
            "subject": instructions.subject
        }
        if instructions.requires_attributes:
            message["attributes"] = dict(instructions.attributes)
        serialized = json.dumps(message)
        msg = Content(serialized)
        msg["delivery mode"] = 2
        success = False
        reconnect = False
        delay = 20
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
                yield task.deferLater(reactor, delay, lambda x: None)
                reconnect = True
        log.debug(
            "Sent message to target exchange '{exchange}' with routing key '{route_key}'.",
            event_type="amqp_send",
            exchange=exchange,
            route_key=route_key)
        log.debug("Send message: {msg}",
            event_type="amqp_send_msg",
            msg=serialized)

    def query_groups(self, instructions):
        """
        """
            
def delay(reactor, seconds):
    """
    A Deferred that fires after `seconds` seconds.
    """
    yield task.deferLater(reactor, seconds, provisioner.provision, msg)









