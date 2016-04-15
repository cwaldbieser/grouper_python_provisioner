
from __future__ import print_function
from collections import namedtuple
import re
from textwrap import dedent
from twisted.internet.defer import (
    inlineCallbacks, 
    returnValue,
)
from twisted.enterprise import adbapi
from twisted.logger import Logger
from twisted.plugin import IPlugin
from zope.interface import implements
from config import load_config, section2dict
from errors import (
    UnknownAttributeResolverError,
    NoMatchingMessageParserError,
)
from interface import (
    IAttributeResolverFactory,
    IMessageParserFactory,
)
from utils import get_plugin_factory
#
from twisted.internet.endpoints import clientFromString, connectProtocol
from txamqp.client import TwistedDelegate
from txamqp.protocol import AMQClient
from txamqp.queue import Closed as QueueClosedError
import txamqp.spec
#

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


class KikiProvisioner(Interface):                                          
    implements(IProvisioner)

    service_state = None

    def load_config(self, config_file, default_log_level, logObserverFactory):
        """                                                             
        Load the configuration for this provisioner and initialize it.  
        """             
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
        attrib_resolver = factory.generateResolver(scp)
        # Load parse map.
        parser_map_filename = config["parser_map"]
        self.load_parser_map(parser_map_filename)
                     
    @inlineCallbacks                                                   
    def provision(self, amqp_message):             
        """                                                
        Provision an entry based on an AMQP message.  
        """                                              
        msg_parser = self.get_message_parser(amqp_message)
        instructions = msg_parser.parse_message(amqp_message)
        attribs = {}
        if instructions.requires_attributes:
            attribs = yield self.query_subject(instructions)
            instructions.attributes.update(attribs)
        target_route_key = self.get_target_route_key(amqp_message)
        yield self.send_message(target_route_key, instructions)

    def get_config_defaults(self):
        """
        Return option defaults.
        """
        return dedent("""\
            [PROVISIONER]
            parser_map = parser_map.json
            attrib_resolver = rdbms_attrib_resolver
            
            [AMQP_TARGET]
            endpoint = tcp:host=127.0.0.1:port=5672
            log_level = INFO
            exchange = grouper_exchange
            queue = kiki_q
            vhost = /
            user = guest
            passwd = guest

            [KIKI_RDMS]
            driver = sqlite3
            """)

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
                exchange = re.parse(entry["exchange"])
            except re.error as ex:
                log.error("Parser map entry {entry_index}: Exchange pattern '{exchange}' is not a valid regular expression.  Error was {regex_error}.",
                    entry_index=entry_index,
                    exchange=entry["exchange"],
                    regex_error=ex)
                valid = False
            try:
                route_key = entry["route_key"]
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
            parser = factory.generateParser(**parser_args)
            matcher = ParserMatcher(exchange, route_key, parser)
            self.parser_mappings.append(matcher)

    def get_message_parser(self, msg):
        """
        Return a message parser (IMessageParser) based on the message
        characteristics.
        """
        consumer_tag, delivery_tag, redelivered, exchange_name, route_key = amqp_message.fields
        parser_mappings = self.parser_mappings
        for exchange_pattern, route_pattern, parser in parser_mappings:
            if exchange_pattern.match(exchange_name) is not None:
                if route_pattern.match(route_key) is not None:
                    return parser
        raise NoMatchingMessageParserError(
            "A parser for exchange '{0}' and route_key '{1}' could not be determined.".format(
                exchange_name,
                route_key))

    def query_subject(self, instructions):
        """
        Return a dictionary of attribute mappings for a subject.
        """
        # Attributes requested/returned should be based on some kind of 
        # filter / mapping / logic.
        subject = instructions.subject
        return {}

    def get_target_route_key(self, msg):
        """
        Get the target route key from the original route key.
        This provisioner expects the original route key to be of the form:

        KIKI.FROM.TARGET

        It transforms the target key to:

            TARGET.FROM
        """
        consumer_tag, delivery_tag, redelivered, exchange_name, route_key = msg.fields
        parts = route_key.split(".")
        parts = parts[1:]
        parts = parts[1:].append(parts[0])
        return ".".join(parts)

    @inlineCallbacks
    def send_message(self, route_key, instructions):
        """
        Compose a provisioning message and deliver it to an exchange with
        routing key `route_key`.
        """
        message = {
            "action": instructions.action,
            "subject": instructions.subject
        }
        if instructions.requires_attributes:
            message["attributes"] = dict(instructions.attributes)
        serialized = json.dumps(message)
        exchange = self.exchange
        vhost = self.vhost
        user = self.user
        passwd = self.passwd
        endpoint_s = self.endpoint_s
        spec = self.spec
        endpoint_s = args.endpoint
        e = clientFromString(self.reactor, endpoint_s)
        delegate = TwistedDelegate()
        amqp_protocol = AMQClient(
            delegate=delegate,
            vhost=vhost,
            spec=spec)
        conn = yield connectProtocol(e, amqp_protocol)
        yield conn.authenticate(user, passwd)
        channel = yield conn.channel(1)
        # TODO: Send message to exchange.

        
