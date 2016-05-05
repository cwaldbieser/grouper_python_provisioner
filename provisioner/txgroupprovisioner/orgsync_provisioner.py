
from __future__ import print_function
from collections import namedtuple
import datetime
import json
import commentjson
import jinja2 
from textwrap import dedent
import treq
from twisted.internet import defer
from twisted.internet.defer import (
    inlineCallbacks, 
    returnValue,
)
from twisted.internet.endpoints import clientFromString, connectProtocol
from twisted.internet.task import LoopingCall
from twisted.web.client import(
    Agent,
    HTTPConnectionPool,
)
from twisted.web.iweb import IAgentEndpointFactory
from twisted.logger import Logger
from twisted.plugin import IPlugin
from zope.interface import implements, implementer
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


@implementer(IAgentEndpointFactory)
class WebClientEndpointFactory(object):
    """
    An Agent endpoint factory based on endpoint strings.
    """
    def __init__(self, reactor, endpoint_s):
        self.reactor = reactor
        self.endpoint_s = endpoint_s

    def endpointForURI(self, uri):
        return clientFromString(self.reactor, self.endpoint_s)


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
                self.account_query = jinja2.Template(config['account_query'])
                self.account_update = jinja2.Template(config['account_update'])
                attrib_map_path = config["attribute_map"]
            except KeyError as ex:
                raise OptionMissingError(
                    "A require option was missing: '{0}:{1}'.".format(
                        section, ex.args[0]))
            # Start the daiy countdown timer.
            self.daily_reset = LoopingCall(self.reset_daily_countdown)
            d = self.daily_reset.start(60*60*24, now=True)
            # Create the web client.
            self.make_web_client()
            # Create the attribute map.
            self.make_attribute_map(attrib_map_path)
        except Exception as ex:
            d = self.reactor.callLater(0, self.reactor.stop)
            log.failure("Provisioner failed to initialize: {0}".format(ex))
            raise
        return defer.succeed(None)

    def make_attribute_map(self, path):
        with open(path, "r") as f:
            doc = commentjson.load(f)
        self.attribute_map = {}
        for k, v in doc.items():
            self.attribute_map[k.lower()] = v
                     
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
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}{1}".format(
            prefix,
            self.account_query.render(
                subject=msg.subject,
                attributes=msg.attributes))
        params = {'key': self.api_key}
        headers = {'Accept': ['application/json']}
        log.debug("url: {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        log.debug("params: {params}", params=params)
        try:
            resp = yield http_client.get(url, headers=headers, params=params)
        except Exception as ex:
            log.error("Error attempting to retrieve existing account.")
            raise
        resp_code = resp.code
        log.debug("Response code: {code}", code=resp_code)
        if resp_code == 200:
            try:
                doc = yield resp.json()
            except Exception as ex:
                log.error("Error attempting to parse response.")
                raise
            log.debug(
                "Received response: {response}",
                response=doc)
            yield self.update_subject(doc, msg)
        elif resp_code == 404:
            self.add_subject(msg)
        else:
            raise Exception("Invalid response code: {0}".format(resp_code))
        returnValue(None)

    @inlineCallbacks
    def update_subject(self, account, msg):
        """
        Update an OrgSync account.
        """
        log = self.log
        subject = msg.subject
        log.debug("Updating subject '{subject}'", subject=subject)
        attributes = msg.attributes
        attrib_map = self.attribute_map
        props = {}
        for k, v in attributes.items():
            prop_name = attrib_map.get(k.lower(), None)
            if prop_name is not None:
                props[prop_name] = v
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}{1}".format(
            prefix,
            self.account_update.render(
                account=account,
                subject=msg.subject,
                attributes=props))
        params = {'key': self.api_key}
        params.update(props)
        headers = {'Accept': ['application/json']}
        log.debug("url: {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        log.debug("params: {params}", params=params)
        try:
            resp = yield http_client.put(url, headers=headers, params=params)
        except Exception as ex:
            log.error("Error attempting to update existing account.")
            raise
        resp_code = resp.code
        log.debug("Response code: {code}", code=resp_code)
        yield resp.content()

    def add_subject(self, msg):
        """
        Add an Orgsync account.
        """
        log = self.log
        log.debug("Adding a new account.")

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

    def make_web_agent(self):
        """
        Configure a `Twisted.web.client.Agent` to be used to make REST calls.
        """
        self.pool = HTTPConnectionPool(self.reactor)
        self.agent = Agent.usingEndpointFactory(
            self.reactor,
            WebClientEndpointFactory(self.reactor, self.endpoint_s),
            pool=self.pool)

    def make_web_client(self):
        self.make_web_agent()
        self.http_client = treq.client.HTTPClient(self.agent)

