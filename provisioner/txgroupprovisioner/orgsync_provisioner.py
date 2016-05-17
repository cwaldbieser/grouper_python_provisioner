
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
from twisted.web.iweb import (
    IAgentEndpointFactory,
    IBodyProducer,
)
from twisted.logger import Logger
from twisted.plugin import IPlugin
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


class StringProducer(object):
    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return defer.succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
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
                self.diagnostic_mode = bool(config.get("diagnostic_mode", False))
                self.endpoint_s = config.get("endpoint", None)
                self.url_prefix = config["url_prefix"]
                self.api_key = config["api_key"]
                self.max_per_day = config.get("max_per_day", 20)
                self.account_query = jinja2.Template(config['account_query'])
                self.account_update = jinja2.Template(config['account_update'])
                self.account_delete = jinja2.Template(config['account_delete'])
                self.account_add = jinja2.Template(config['account_add'])
                account_template_path = config['account_template']
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
            # Create the account template.
            self.make_account_template(account_template_path)
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
            try:
                self.attribute_map[k.lower()] = jinja2.Template(v)
            except jinja2.exceptions.TemplateError as ex:
                log.error(
                    "Error parsing attribute template for '{attribute}'",
                    attribute=k)
                raise
                     
    def make_account_template(self, path):
        with open(path, "r") as f:
            data = f.read()
        self.account_template = jinja2.Template(data)

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
            if msg.action in (constants.ACTION_ADD, constants.ACTION_UPDATE):
                yield self.provision_subject(msg)
                self.daily_count = self.daily_count + 1
            elif msg.action == constants.ACTION_DELETE:
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
            diagnostic_mode = 0
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
        if action  != constants.ACTION_DELETE:
            attributes = doc['attributes']
        return ParsedMessage(action, subject, attributes)

    @inlineCallbacks
    def provision_subject(self, msg):
        """
        Provision a subject to OrgSync.
        """
        log = self.log
        log.debug(
            "Attempting to provision subject '{subject}'.",
            subject=msg.subject)
        account = yield self.fetch_existing_account(msg)
        if account is not None:
            yield self.update_subject(account, msg)
        else:
            yield self.add_subject(msg)
        returnValue(None)

    @inlineCallbacks
    def fetch_existing_account(self, msg):
        """
        Fetch an existing account and return it or None if it does not
        exist.
        """
        log = self.log
        log.debug("Attempting to fetch existing account.")
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}{1}".format(
            prefix,
            self.account_query.render(
                subject=msg.subject,
                attributes=msg.attributes,
                action=msg.action))
        params = {'key': self.api_key}
        headers = {'Accept': ['application/json']}
        log.debug("URL (GET): {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        log.debug("params: {params}", params=params)
        try:
            resp = yield http_client.get(url, headers=headers, params=params)
        except Exception as ex:
            log.error("Error attempting to retrieve existing account.")
            raise
        resp_code = resp.code
        if resp_code == 200:
            try:
                doc = yield resp.json()
            except Exception as ex:
                log.error("Error attempting to parse response.")
                raise
            log.debug(
                "Received response: {response}",
                response=doc)
            returnValue(doc)
        elif resp_code == 404:
            returnValue(None)
        else:
            raise Exception("Invalid response code: {0}".format(resp_code))

    @inlineCallbacks
    def update_subject(self, account, msg):
        """
        Update an OrgSync account.
        """
        log = self.log
        subject = msg.subject
        log.debug("Updating subject '{subject}'", subject=subject)
        attributes = msg.attributes
        props = self.map_attributes(attributes, subject, msg.action)
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
        if not self.diagnostic_mode:
            try:
                resp = yield http_client.put(url, headers=headers, params=params)
            except Exception as ex:
                log.error("Error attempting to update existing account.")
                raise
            resp_code = resp.code
            log.debug("Response code: {code}", code=resp_code)
            yield resp.content()

    def map_attributes(self, attribs, subject, action):
        """
        Map subject attributes to OrgSync attributes.
        Returns OrgSync attributes mapping.
        """
        attrib_map = self.attribute_map
        props = {}
        for prop_name, template in attrib_map.items():
            value = template.render(
                subject=subject,
                attributes=attribs,
                action=action)
            if value != u'\x00':
                props[prop_name] = value
        return props

    @inlineCallbacks
    def add_subject(self, msg):
        """
        Add an Orgsync account.
        """
        log = self.log
        if False:
            yield None
        log.debug("Adding a new account.")
        attributes = msg.attributes
        props = self.map_attributes(attributes, subject, msg.action)
        account_doc = self.account_template.render(
            attributes=props,
            subject=msg.subject,
            action=msg.action)
        log.debug("Account doc: {doc}", doc=account_doc)
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}{1}".format(
            prefix,
            self.account_add.render(
                account=json.dumps(account_doc),
                subject=msg.subject))
        params = {'key': self.api_key}
        headers = {
            'Accept': ['application/json'], 
            'Content-Type': ['application/json']}
        log.debug("url: {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        log.debug("params: {params}", params=params)
        if not self.diagnostic_mode:
            try:
                resp = yield http_client.post(
                    url, 
                    data=StringProducer(account_doc.encode('utf-8')), 
                    headers=headers, 
                    params=params)
            except Exception as ex:
                log.error("Error attempting to add new account.")
                raise
            resp_code = resp.code
            log.debug("Response code: {code}", code=resp_code)
            yield resp.content()

    @inlineCallbacks
    def deprovision_subject(self, msg):
        """
        Deprovision a subject from Orgsync.
        """
        log = self.log
        log.debug(
            "Attempting to deprovision subject '{subject}'.",
            subject=msg.subject)
        account = yield self.fetch_existing_account(msg)
        if account is None:
            log.debug("Account already does not exist.")
            returnValue(None)
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}{1}".format(
            prefix,
            self.account_delete.render(
                account=account,
                subject=msg.subject))
        params = {'key': self.api_key}
        headers = {
            'Accept': ['application/json']}
        log.debug("url: {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        log.debug("params: {params}", params=params)
        if not self.diagnostic_mode:
            try:
                resp = yield http_client.delete(url, headers=headers, params=params)
            except Exception as ex:
                log.error("Error attempting to delete existing account.")
                raise
            resp_code = resp.code
            log.debug("Response code: {code}", code=resp_code)
            yield resp.content()

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

