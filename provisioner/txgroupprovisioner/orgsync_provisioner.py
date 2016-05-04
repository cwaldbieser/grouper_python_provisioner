
from __future__ import print_function
from collections import namedtuple
import datetime
import json
from textwrap import dedent
import treq
from twisted.internet import defer
from twisted.internet.defer import (
    inlineCallbacks, 
    returnValue,
)
from twisted.internet.endpoints import clientFromString, connectProtocol
from twisted.internet.task import LoopingCall
from twisted.logger import Logger
from twisted.plugin import IPlugin
from zope.interface import implements
from config import load_config, section2dict
from errors import (
    OptionMissingError,
)
from interface import (
    IProvisionerFactory,
    IProvisioner,
)
from kikimessage import (
    ADD_ACTION,
    DELETE_ACTION,
    UPDATE_ACTION,
)
from utils import get_plugin_factory


ParsedMessage = namedtuple(
    'ParsedMessage', 
    ["action", "subject", "attributes"])


class UnknowActionError(Exception):
    pass


class OrgsyncProvisionerFactory(object):
    implements(IPlugin, IProvisionerFactory)
    tag = "orgsync"
    opt_help = "Orgsync RESTful API Provisioner"
    opt_usage = "This plugin does not support any options."

    def generateProvisioner(self, argstring=""):
        """
        Create an object that implements IProvisioner
        """
        return OrgsyncProvisioner()


class OrgsyncProvisioner(object):
    implements(IProvisioner)
    service_state = None
    reactor = None
    log = None
    max_per_day = 20

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
            log.info("Initializing Orgsync RESTful API provisioner.",
                event_type='init_provisioner')
            # Load API configuration info-- endpoint info, URL, API key.
            try:
                self.endpoint_s = config.get("endpoint", None)
                self.url_prefix = config["url_prefix"]
                self.api_key = config["api_key"]
                self.max_per_day = config.get("max_per_day", 20)
            except KeyError as ex:
                raise OptionMissingError(
                    "A require option was missing: '{0}:{1}'.".format(
                        section, ex.args[0]))
            # Start the daiy countdown timer.
            self.daily_reset = LoopingCall(self.reset_daily_countdown)
            d = self.daily_reset.start(60*60*24, now=True)
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
        if self.daily_count >= self.max_per_day:
            log.warn("Maximum provisioning threshold has already been reached.")
            returnValue(defer.fail(Exception(
                "Maximum provisioning threshold has already been reached.")))
        try:
            msg = self.parse_message(amqp_message)
        except Exception as ex:
            log.warn("Error parsing message: {error}", error=ex)
            raise
        try:
            if msg.action in (ADD_ACTION, UPDATE_ACTION):
                yield self.provision_subject(msg)
                self.daily_count = self.daily_count + 1
            elif msg.action == DELETE_ACTION:
                yield self.deprovision_subject(msg)
                self.daily_count = self.daily_count + 1
            else:
                raise UnknownActionError(
                    "Don't know how to handle action '{0}'.".format(msg.action))
        except Exception as ex:
            log.warn("Error provisioning message: {error}", error=ex)
            raise

    def get_config_defaults(self):
        return dedent("""\
            [PROVISIONER]
            url_prefix = https://api.orgsync.com/api/v2
            """)

    def reset_daily_countdown(self):
        """
        Reset the daily countdown and schedule the next one.
        """
        self.daily_count = 0

    def parse_message(self, msg):
        """
        Parse message into a standard form.
        """
        serialized = msg.content.body
        doc = json.loads(serialized)
        action = doc['action']
        subject = doc['subject']
        attributes = None
        if action  != DELETE_ACTION:
            attributes = doc['attributes']
        return ParsedMessage(action, subject, attributes)

    @inlineCallbacks
    def provision_subject(self, msg):
        """
        Provision a subject to Orgsync.
        """
        log = self.log
        # Check if subject exists.
        # Add or update subject record via API.
        if False:
            yield None
        log.debug(
            "Attempting to provision subject '{subject}'.",
            subject=msg.subject)
        returnValue(None)

    @inlineCallbacks
    def deprovision_subject(self, msg):
        """
        Deprovision a subject from Orgsync.
        """
        # Look up subject's Orgsync ID.
        # Remove subject from Orgsync.
        log = self.log
        if False:
            yield None
        log.debug(
            "Attempting to deprovision subject '{subject}'.",
            subject=msg.subject)
        returnValue(None)

