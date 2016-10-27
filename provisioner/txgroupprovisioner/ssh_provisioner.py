
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
    ["action", "group", "subject"])


ParseSyncMessage = namedtuple(
    'ParsedSyncMessage',
    ["action", "group", "subjects"])


class UnknowActionError(Exception):
    pass


class AnchorProtocol(Protocol):
    """
    Protocol used by the "anchor" ssh connection, which is usually just some
    kind of shell.  It stays connected to the remote host and additional
    channels are opened on its connection in order to execute programs on
    the remote host.
    """

    reactor = None
    log = None

    def __init__(self):
        self.connected = defer.Deferred()
        self.finished = defer.Deferred()

    def connectionMade(self):
        """
        Fires when the initial connection is made.
        """
        log.info("Connected to target host.")
        self.connected.callback()

    def dataReceived(self, data):
        pass

    def connectionLost(self, reason):
        """
        Fires if the connection to the remote host is lost.
        This event fires even if the dropped connection was self-initiated.
        """
        log.info("Connection to target host lost.")
        self.finished.callback(reason)


class CommandProtocol(Protocol):
    """
    Protocol used to issue a remote command and receive responses.
    """

    reactor = None
    log = None
    data_callback = None

    def __init__(self):
        self.connected = defer.Deferred()
        self.finished = defer.Deferred()

    def connectionMade(self):
        """
        Fires when the initial connection is made.
        """
        log.debug("Opened new channel to target host.")
        self.connected.callback(None)

    def dataReceived(self, data):
        cb = self.data_callback
        if cb is not None:
            cb(data)

    def connectionLost(self, reason):
        """
        Fires if the connection to the remote host is lost.
        This event fires even if the dropped connection was self-initiated.
        """
        log.debug("Closed channel to target host.")
        self.finished.callback(None)



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
    connected_to_target = False
    ssh_conn = None

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
            elif msg.action = constants.ACTION_MEMBERSHIP_SYNC:
                yield self.sync_membership(msg)
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
        group = doc['group']
        if action in (constants.ACTION_ADD, constants.ACTION_DELETE):
            subject = doc['subject']
            return ParsedMessage(action, group, subject)
        elif action == constants.ACTION_MEMBERSHIP_SYNC:
            subjects = doc["subjects"]
            return ParsedSyncMessage(action, group, subjects)
        else:
            raise UnknownActionError(
                "Don't know how to handle action '{0}'.".format(msg.action))

    @inlineCallbacks
    def connect_to_target(self):
        """
        Establish an SSH connection to the target host.
        """
        if self.connected_to_target:
            returnValue(None)
        known_hosts = self.get_known_hosts()
        keys = self.get_keys()
        command = b"/bin/bash"
        endpoint = SSHCommandClientEndpoint.newConnection(
            self.reactor,
            command,
            self.user,
            self.host,
            keys=keys,
            knownHosts=known_hosts)
        proto = AnchorProtocol()
        proto.reactor = self.reactor
        proto.connected.addCallback(self.set_connected)
        proto.finished.addCallback(self.set_disconnected)
        proto = yield connectProtocol(endpoint, proto)
        self.ssh_conn = proto.transport.conn
        returnValue(proto.connected)

    def set_connected(self):
        self.connected_to_target = True

    def set_disconnected(self, reason):
        self.connected_to_target = False    

    def get_known_hosts(self):
        """
        """
        pass

    def get_keys(self):
        """
        """
        pass

    @inlineCallbacks
    def provision_subject(self, msg):
        """
        Provision a subject.
        """
        log = self.log
        log.debug(
            "Attempting to provision subject '{subject}' for group '{group}'.",
            subject=msg.subject, group=msg.group)
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
            "Attempting to deprovision subject '{subject}' from group '{group}'.",
            subject=msg.subject, group=msg.group)
        #TODO: Logic to de-provision subject goes here.
        yield self.todo()
        returnValue(None)

    @inlineCallbacks
    def sync_membership(self, msg):
        """
        Synchronize membership.
        """
        log = self.log
        log.debug(
            "Attempting to synchronize membership for group '{0}'.",
            group=msg.group)
        #TODO: Logic to de-provision subject goes here.
        yield self.todo()
        returnValue(None)
