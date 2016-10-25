
from __future__ import print_function
from collections import namedtuple
import datetime
import json
import commentjson
import jinja2 
from textwrap import dedent
from twisted.conch.client.knownhosts import KnownHostsFile
from twisted.conch.endpoints import SSHCommandClientEndpoint
from twisted.conch.ssh.keys import EncryptedKeyError, Key
from twisted.internet import defer
from twisted.internet.defer import (
    inlineCallbacks, 
    returnValue,
)
from twisted.internet.endpoints import clientFromString, connectProtocol
from twisted.internet.endpoints import UNIXClientEndpoint
from twisted.internet.protocol import Factory, Protocol
from twisted.logger import Logger
from twisted.plugin import IPlugin
from twisted.python.filepath import FilePath
from zope.interface import implements, implementer
from config import load_config, section2dict
import constants
from errors import (
    OptionMissingError,
)
from interface import (
    IProvisionerFactory,
    IProvisioner,
)
from utils import get_plugin_factory


ParsedMessage = namedtuple(
    'ParsedMessage', 
    ["action", "subject"])


class UnknowActionError(Exception):
    pass


class SSHProvisionerFactory(object):
    implements(IPlugin, IProvisionerFactory)
    tag = "ssh"
    opt_help = "SSH Provisioner"
    opt_usage = "This plugin does not support any options."

    def generateProvisioner(self, argstring=""):
        """
        Create an object that implements IProvisioner
        """
        return SSHProvisioner()


class SSHProvisioner(object):
    implements(IProvisioner)
    service_state = None
    reactor = None
    log = None

    def load_config(self, config_file, default_log_level, logObserverFactory):
        """                                                             
        Load the configuration for this provisioner and initialize it.  
        """             
        log = Logger(observer=logObserverFactory("ERROR"))
        try:
            # Load config.
            scp = load_config(config_file, defaults=self.get_config_defaults())
            section = "PROVISIONER"
            config = section2dict(scp, section)
            self.config = config
            # Start logger.
            log_level = config.get('log_level', default_log_level)
            log = Logger(observer=logObserverFactory(log_level))
            self.log = log
            log.info("Initializing SSH provisioner.",
                event_type='init_provisioner')
            # Load SSH configuration info.
            try:
                self.diagnostic_mode = bool(config.get("diagnostic_mode", False))
                self.endpoint_s = config.get("endpoint", None)
                self.add_memb_command = config["add_memb_command"]
                self.remove_memb_command = config["remove_memb_command"]
                # known hosts path
                # ssh-agent endpoint
                # keys; passwords (optional)
                # host
                # user
                # command type; simple, argument driven OR input driven
                # SSH timeout - how long should the connection stay established before 
                # deciding no more commands are going to be issued for a while?
            except KeyError as ex:
                raise OptionMissingError(
                    "A require option was missing: '{0}:{1}'.".format(
                        section, ex.args[0]))
        except Exception as ex:
            d = self.reactor.callLater(0, self.reactor.stop)
            log.failure("Provisioner failed to initialize: {0}".format(ex))
            raise
        return defer.succeed(None)

    @inlineCallbacks                                                   
    def provision(self, amqp_message):             
        """                                                
        Provision an entry based on an AMQP message.  
        """                                              
        log = self.log
        try:
            msg = self.parse_message(amqp_message)
        except Exception as ex:
            log.warn("Error parsing message: {error}", error=ex)
            raise
        try:
            if msg.action == constants.ACTION_ADD:
                yield self.provision_subject(msg)
            elif msg.action == constants.ACTION_DELETE:
                yield self.deprovision_subject(msg)
            else:
                raise UnknownActionError(
                    "Don't know how to handle action '{0}'.".format(msg.action))
        except Exception as ex:
            log.warn("Error provisioning message: {error}", error=ex)
            raise

    def get_config_defaults(self):
        return dedent("""\
            [PROVISIONER]
            diagnostic_mode = 0
            """)

    def parse_message(self, msg):
        """
        Parse message into a standard form.
        """
        serialized = msg.content.body
        doc = json.loads(serialized)
        action = doc['action']
        subject = doc['subject']
        attributes = None
        return ParsedMessage(action, subject)

    @inlineCallbacks
    def provision_subject(self, msg):
        """
        Provision a subject.
        """
        log = self.log
        log.debug(
            "Attempting to provision subject '{subject}'.",
            subject=msg.subject)
        #TODO: Logic to provision subject goes here.
        yield self.todo()
        returnValue(None)

    @inlineCallbacks
    def deprovision_subject(self, msg):
        """
        Deprovision a subject.
        """
        log = self.log
        log.debug(
            "Attempting to deprovision subject '{subject}'.",
            subject=msg.subject)
        #TODO: Logic to de-provision subject goes here.
        yield self.todo()
        returnValue(None)
