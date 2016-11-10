
from __future__ import print_function
import collections
from collections import namedtuple
import datetime
import json
import commentjson
import jinja2 
import os.path
import re
import struct
from textwrap import dedent
import types
import commentjson
from twisted.conch.client.knownhosts import KnownHostsFile
from twisted.conch.endpoints import SSHCommandClientEndpoint
from twisted.conch.ssh.keys import EncryptedKeyError, Key
from twisted.internet import defer, error
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

def filter_shellquote(s):
    """
    Jinja2 template filter for quoting shell arguments.
    """
    if isinstance(s, jinja2.runtime.Undefined):
        return ""
    return "'" + s.replace("'", "'\\''") + "'"

def filter_newline(s):
    """
    Jinja2 template filter adds a newline to the end of a string.
    """
    if isinstance(s, jinja2.runtime.Undefined):
        return "\n"
    return "{0}\n".format(s)

def patch_channel(proto):
    """
    Patches a Protocol instance's channel (transport) so that the exit status
    is actually recorded on the Protocol instance.
    This monkeypatching is intended to work with SSH client protocols.
    """

    def request_exit_status(self, data):
        proto.exit_status = int(struct.unpack('>L', data)[0])

    proto.transport.request_exit_status = types.MethodType(request_exit_status, proto.transport)


ParsedMessage = namedtuple(
    'ParsedMessage', 
    ["action", "group", "subject"])


ParsedSyncMessage = namedtuple(
    'ParsedSyncMessage',
    ["action", "group", "subjects"])


class UnknowActionError(Exception):
    pass


class AnchorProtocol(Protocol):
    """
    Protocol used by the "anchor" ssh connection, which is usually just some
    kind of blocking command.  It stays connected to the remote host and additional
    channels are opened on its connection in order to execute programs on
    the remote host.
    """

    reactor = None
    log = None

    def __init__(self):
        self.connected_future = defer.Deferred()
        self.finished = defer.Deferred()

    def connectionMade(self):
        """
        Fires when the initial connection is made.
        """
        self.log.info("Connected to target host.")
        self.connected_future.callback(None)

    def dataReceived(self, data):
        pass

    def connectionLost(self, reason):
        """
        Fires if the connection to the remote host is lost.
        This event fires even if the dropped connection was self-initiated.
        """
        self.log.info("Connection to target host lost.")
        self.finished.callback(reason)


class CommandProtocol(Protocol):
    """
    Protocol used to issue a remote command and receive responses.
    """

    reactor = None
    log = None
    data_callback = None
    exit_status = None

    def __init__(self):
        self.connected_future = defer.Deferred()
        self.finished = defer.Deferred()

    def connectionMade(self):
        """
        Fires when the initial connection is made.
        """
        self.log.debug("Opened new channel to target host.")
        self.connected_future.callback(None)

    def dataReceived(self, data):
        cb = self.data_callback
        if cb is not None:
            cb(data)

    def connectionLost(self, reason):
        """
        Fires if the connection to the remote host is lost.
        This event fires even if the dropped connection was self-initiated.
        """
        self.log.debug("Closed channel to target host.")
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
    CMD_TYPE_SIMPLE = 0
    CMD_TYPE_INPUT = 1
    service_state = None
    reactor = None
    log = None
    connected_to_target = False
    ssh_conn = None
    anchor_cmd = b"/bin/cat"
    provision_cmd = None
    provision_cmd_type = CMD_TYPE_SIMPLE
    provision_input = None
    provision_ok_result = None
    deprovision_cmd = None
    deprovision_cmd_type = CMD_TYPE_SIMPLE
    deprovision_input = None
    deprovision_ok_result = None
    sync_cmd = None
    sync_cmd_type = CMD_TYPE_SIMPLE
    sync_input = None
    sync_ok_result = None
    ssh_user = None
    host = "localhost"
    port = 22
    keys = None
    known_hosts = None
    cmd_timeout = 10
    group_map = None

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
            # Initialize template environment.
            self.template_env = jinja2.Environment(trim_blocks=True, lstrip_blocks=True)
            self.template_env.filters['shellquote'] = filter_shellquote
            self.template_env.filters['newline'] = filter_newline
            # Load SSH configuration info.
            try:
                self.provision_cmd = self.template_env.from_string(
                    config["provision_cmd"].strip())
                self.deprovision_cmd = self.template_env.from_string(
                    config["deprovision_cmd"].strip())
                template_str = config.get("sync_cmd", None)
                if template_str is not None:
                    self.sync_cmd = self.template_env.from_string(
                        template_str.strip())
                self.provision_cmd_type = self.parse_command_type(
                    config.get("provision_cmd_type", "simple"))
                self.deprovision_cmd_type = self.parse_command_type(
                    config.get("deprovision_cmd_type", "simple"))
                self.sync_cmd_type = self.parse_command_type(
                    config.get("sync_cmd_type", "simple"))
                if self.provision_cmd_type == self.CMD_TYPE_INPUT:
                    self.provision_input = self.template_env.from_string(
                        config["provision_input"].strip())
                if self.deprovision_cmd_type == self.CMD_TYPE_INPUT:
                    self.deprovision_input = self.template_env.from_string(
                        config["deprovision_input"].strip())
                if self.sync_cmd_type == self.CMD_TYPE_INPUT:
                    template_str = config.get("sync_input", None)
                    self.sync_input = self.template_env.from_string(
                        template_str.strip())
                result = config.get("provision_ok_result", None)
                if result is not None:
                    self.provision_ok_result = int(result.strip())
                result = config.get("deprovision_ok_result", None)
                if result is not None:
                    self.deprovision_ok_result = int(result.strip())
                result = config.get("sync_ok_result", None)
                if result is not None:
                    self.sync_ok_result = int(result.strip())
                self.cmd_timeout = int(config['cmd_timeout'])
                self.host = config["host"]
                self.port = int(config["port"])
                self.ssh_user = config["user"]
                self.known_hosts = os.path.expanduser(config["known_hosts"])
                if "keys" in config:
                    self.keys = config["keys"].split(",")
            except KeyError as ex:
                raise OptionMissingError(
                    "A require option was missing: '{0}:{1}'.".format(
                        section, ex.args[0]))
            self.load_groupmap(config.get("group_map", None))
        except Exception as ex:
            d = self.reactor.callLater(0, self.reactor.stop)
            log.failure("Provisioner failed to initialize: {0}".format(ex))
            raise
        return defer.succeed(None)

    def load_groupmap(self, path):
        """
        Load JSON group map.
        """
        with open(path, "r") as f:
            doc = commentjson.load(f)
        template_env = self.template_env
        pattern_map = []
        for pattern, template_str in doc:
             compiled = re.compile(pattern)
             template = template_env.from_string(template_str)
             pattern_map.append((compiled, template))
        self.group_map = pattern_map

    def translate_group(self, group):
        """
        Attempt to translate a group name into target group name.
        """
        log = self.log
        group_map = self.group_map
        for compiled, template in group_map:
            m = compiled.match(group)
            if not m is  None:
                groupdict = m.groupdict()
                groupdict["orig_group"] = group
                log.debug("Group translation symbols: {symbols}", symbols=groupdict)
                result = template.render(**groupdict)
                log.debug("Translated group: '{group}'", group=result)
                return result
        raise Exception("No pattern matched group '{0}'.".format(group))

    def parse_command_type(self, t):
        s = t.lower().strip()
        if s == "simple":
            return self.CMD_TYPE_SIMPLE
        elif s == "input":
            return self.CMD_TYPE_INPUT
        else:
            raise Exception("Unknown command type '{0}'.".format(t))

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
            elif msg.action == constants.ACTION_MEMBERSHIP_SYNC:
                log.info("msg => {msg}", msg=msg)
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
            host = localhost
            port = 22
            known_hosts = ~/.ssh/known_hosts
            cmd_timeout = 10
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
        command = self.anchor_cmd
        agent = self.get_agent()
        endpoint = SSHCommandClientEndpoint.newConnection(
            self.reactor,
            command,
            self.ssh_user,
            self.host,
            keys=keys,
            knownHosts=known_hosts,
            agentEndpoint=agent)
        proto = AnchorProtocol()
        proto.log = self.log
        proto.reactor = self.reactor
        proto.connected_future.addCallback(self.set_connected).addTimeout(
            self.cmd_timeout, self.reactor)
        proto.finished.addCallback(self.set_disconnected)
        proto = yield connectProtocol(endpoint, proto)
        yield proto.connected_future 
        self.anchor = proto
        self.ssh_conn = proto.transport.conn
        returnValue(None)

    def disconnect_from_target(self):
        """
        Disconnect the anchor SSH connection from the target host.
        """
        if not self.connected_to_target:
            return
        self.anchor.transport.loseConnection()
        self.anchor = None
        self.ssh_conn = None
        self.set_disconnected()

    def set_connected(self, ignored=None):
        self.log.debug("set_connected() called.")
        self.connected_to_target = True

    def set_disconnected(self, *args):
        self.log.debug("set_disconnected() called.")
        self.connected_to_target = False    

    def get_keys(self):
        """
        Get SSH private keys. 
        """
        log = self.log
        keys = []
        if self.keys is None:
            return None
        for keyPath in self.keys:
            log.debug("Looking up SSH private key at '{path}' ...", path=keyPath)
            keyPath = os.path.expanduser(keyPath)
            if os.path.exists(keyPath):
                keys.append(self.readKey(keyPath))
                log.debug("Read SSH private key at '{path}'.", path=keyPath)
        log.info("Loaded {count} SSH private keys.", count=len(keys))
        return keys

    def readKey(self, path):
        try:
            return Key.fromFile(path)
        except EncryptedKeyError as ex:
            passphrase = getpass.getpass("%r keyphrase: " % (path,))
            return Key.fromFile(path, passphrase=passphrase)
        except Exception as ex:
            print(ex)
            raise

    def get_agent(self):
        """
        Get the SSH agent endpoint.
        """
        reactor = self.reactor
        if "SSH_AUTH_SOCK" in os.environ:
            agent_endpoint = UNIXClientEndpoint(reactor, os.environ["SSH_AUTH_SOCK"])
        else:
            agent_endpoint = None
        return agent_endpoint

    def get_known_hosts(self):
        knownHostsPath = FilePath(self.known_hosts)
        if knownHostsPath.exists():
            knownHosts = KnownHostsFile.fromPath(knownHostsPath)
        else:
            knownHosts = None
        return knownHosts

    @inlineCallbacks
    def create_command_channel(self, cmd):
        """
        Create an SSH channel over the existing connection and issue a command.
        `cmd` should already be encoded as a byte string.
        Return the `CommandProtocol` instance.
        """
        log = self.log
        if not self.connected_to_target:
            raise Exception("Tried to open channel, but was not connected to the target host.")
        ssh_conn = self.ssh_conn
        endpoint = SSHCommandClientEndpoint.existingConnection(
            ssh_conn,
            cmd)
        proto = CommandProtocol()
        proto.log = self.log
        d = connectProtocol(endpoint, proto)

        def _on_timeout(result, timeout):
            self.disconnect_from_target()
            raise Exception("Timed out while attempting to establish command channel.")

        d.addTimeout(self.cmd_timeout, self.reactor, onTimeoutCancel=_on_timeout)
        log.debug("Establishing channel ...")
        proto = yield d
        log.debug("Channel established.")
        returnValue(proto)

    @inlineCallbacks
    def provision_subject(self, msg):
        """
        Provision a subject.
        """
        log = self.log
        log.debug(
            "Attempting to provision subject '{subject}' for group '{group}'.",
            subject=msg.subject, group=msg.group)
        target_group = self.translate_group(msg.group)
        log.debug("Target group: '{group}'", group=target_group)
        yield self.connect_to_target()
        command = self.provision_cmd
        command_type = self.provision_cmd_type
        # Evaluate command template
        command = self.provision_cmd.render(
            subject=msg.subject, 
            group=target_group)
        # Create channel with command.
        log.debug("Creating command channel with command: {command}", command=command)
        cmd_protocol = yield self.create_command_channel(command.encode('utf-8'))
        patch_channel(cmd_protocol)
        transport = cmd_protocol.transport
        ssh_conn = transport.conn
        if command_type == self.CMD_TYPE_INPUT:
            log.debug("Provision command type is INPUT.")
            # Evaluate input template
            data = self.provision_input.render(
                subject=msg.subject,
                group=target_group)
            # Write input to channel.
            log.debug("Writing data to channel: {data}", data=data)
            cmd_protocol.transport.write(data.encode('utf-8'))
        # Send EOF
        ssh_conn.sendEOF(transport)
        # Wait for connection to close.
        finished = cmd_protocol.finished
        finished.addTimeout(self.cmd_timeout, self.reactor)
        try:
            yield finished
        except error.TimeoutError as ex:
            log.error("Timed out while waiting for response to provisioning command '{command}.",
                command=command)
            raise
        # Evaluate response.
        expected_status = self.provision_ok_result
        if expected_status is not None:
            exit_status = cmd_protocol.exit_status 
            if exit_status != expected_status:
                raise Exception(
                    ("Actual provisioning command status '{actual}' did not "
                    "match expected status '{expected}' for command '{command}'.").format(
                    actual=exit_status,
                    expected=expected_status,
                    command=command))
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
        target_group = self.translate_group(msg.group)
        log.debug("Target group: '{group}'", group=target_group)
        yield self.connect_to_target()
        command = self.deprovision_cmd
        command_type = self.deprovision_cmd_type
        # Evaluate command template
        command = self.deprovision_cmd.render(
            subject=msg.subject, 
            group=target_group)
        # Create channel with command.
        log.debug("Creating command channel with command: {command}", command=command)
        cmd_protocol = yield self.create_command_channel(command.encode('utf-8'))
        patch_channel(cmd_protocol)
        transport = cmd_protocol.transport
        ssh_conn = transport.conn
        if command_type == self.CMD_TYPE_INPUT:
            # Evaluate input template
            data = self.deprovision_input.render(
                subject=msg.subject,
                group=target_group)
            # Write input to channel.
            cmd_protocol.transport.write(data.encode('utf-8'))
        # Send EOF
        ssh_conn.sendEOF(transport)
        # Wait for connection to close.
        finished = cmd_protocol.finished
        finished.addTimeout(self.cmd_timeout, self.reactor)
        try:
            yield finished
        except error.TimeoutError as ex:
            log.error("Timed out while waiting for response to deprovisioning command '{command}.",
                command=command)
            raise
        # Evaluate response.
        expected_status = self.provision_ok_result
        if expected_status is not None:
            exit_status = cmd_protocol.exit_status 
            if exit_status != expected_status:
                raise Exception(
                    ("Actual deprovisioning command status '{actual}' did not "
                    "match expected status '{expected}' for command '{command}'.").format(
                    actual=exit_status,
                    expected=expected_status,
                    command=command))
        returnValue(None)

    @inlineCallbacks
    def sync_membership(self, msg):
        """
        Synchronize membership.
        """
        log = self.log
        log.debug(
            "Attempting to synchronize membership for group '{group}'.",
            group=msg.group)
        target_group = self.translate_group(msg.group)
        log.debug("Target group: '{group}'", group=target_group)
        yield self.connect_to_target()
        command = self.sync_cmd
        if command is None:
            raise Exception("Membership synchronization command is not configured.")
        command_type = self.sync_cmd_type
        # Evaluate command template
        command = self.sync_cmd.render(
            group=target_group, 
            subjects=msg.subjects)
        # Create channel with command.
        log.debug("Creating command channel with command: {command}", command=command)
        cmd_protocol = yield self.create_command_channel(command.encode('utf-8'))
        log.debug("Command channel created.")
        patch_channel(cmd_protocol)
        transport = cmd_protocol.transport
        ssh_conn = transport.conn
        if command_type == self.CMD_TYPE_INPUT:
            for subject in msg.subjects:
                # Evaluate input template
                data = self.sync_input.render(
                    subject=subject,
                    group=target_group)
                # Write input to channel.
                cmd_protocol.transport.write(data.encode('utf-8'))
        # Send EOF
        ssh_conn.sendEOF(transport)
        # Wait for connection to close.
        finished = cmd_protocol.finished
        finished.addTimeout(self.cmd_timeout, self.reactor)
        log.debug("Command timeout: {timeout} seconds", timeout=self.cmd_timeout)
        try:
            yield finished
        except error.TimeoutError as ex:
            log.error("Timed out while waiting for response to sync command '{command}.",
                command=command)
            raise
        log.debug("Command was sent.")
        # Evaluate response.
        expected_status = self.provision_ok_result
        if expected_status is not None:
            exit_status = cmd_protocol.exit_status 
            if exit_status != expected_status:
                raise Exception(
                    ("Actual sync command status '{actual}' did not "
                    "match expected status '{expected}' for command '{command}'.").format(
                    actual=exit_status,
                    expected=expected_status,
                    command=command))
        returnValue(None)

