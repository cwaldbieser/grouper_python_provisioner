
from __future__ import print_function
from collections import namedtuple
import datetime
import json
import commentjson
import jinja2 
from textwrap import dedent
import attr
import pylru
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


@attr.attrs
class ParsedMessage(object):
    action = attr.attrib()
    subject = attr.attrib()
    attributes = attr.attrib(default=attr.Factory(list))


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


class BoardEffectProvisionerFactory(object):
    implements(IPlugin, IProvisionerFactory)
    tag = "board_effect"
    opt_help = "Board Effect RESTful API Provisioner"
    opt_usage = "This plugin does not support any options."

    def generateProvisioner(self, argstring=""):
        """
        Create an object that implements IProvisioner
        """
        provisioner = BoardEffectProvisioner()
        return provisioner


class BoardEffectProvisioner(object):
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
            log.info("Initializing provisioner.",
                event_type='init_provisioner')
            # Load API configuration info-- endpoint info, URL, API key.
            try:
                self.diagnostic_mode = bool(int(config.get("diagnostic_mode", 0)))
                self.endpoint_s = config.get("endpoint", None)
                self.url_prefix = config["url_prefix"]
                self.api_key = config["api_key"]
                self.cache_size = int(config["cache_size"])
                self.authenticate = config['authenticate']
                self.accounts_query = config['accounts_query']
                self.account_update = jinja2.Template(config['account_update'])
                self.account_delete = jinja2.Template(config['account_delete'])
                self.account_add = config['account_add']
                account_template_path = config['account_template']
                attrib_map_path = config["attribute_map"]
            except KeyError as ex:
                raise OptionMissingError(
                    "A require option was missing: '{0}:{1}'.".format(
                        section, ex.args[0]))
            # Create the web client.
            self.make_web_client()
            # Create the attribute map.
            self.make_attribute_map(attrib_map_path)
            # Create the account template.
            self.make_account_template(account_template_path)
            # Create account cache.
            self.__account_cache = pylru.lrucache(self.cache_size)
            # Initialize access token.
            self.__auth_token = None
            log.info("Diagnostic mode: {diagnostic}", diagnostic=self.diagnostic_mode)
        except Exception as ex:
            d = self.reactor.callLater(0, self.reactor.stop)
            log.failure("Provisioner failed to initialize: {0}".format(ex))
            raise
        return defer.succeed(None)

    def make_attribute_map(self, path):
        log = self.log
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
        try:
            msg = self.parse_message(amqp_message)
        except Exception as ex:
            log.warn("Error parsing message: {error}", error=ex)
            raise
        try:
            if msg.action in (constants.ACTION_ADD, constants.ACTION_UPDATE):
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
            url_prefix = https://lafayette.boardeffect.com/api/v3
            cache_size = 1000
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
        if action  != constants.ACTION_DELETE:
            attributes = doc['attributes']
        return ParsedMessage(action, subject, attributes)

    def map_attributes(self, attribs, subject, action):
        """
        Map subject attributes to remote service attributes.
        Returns attributes mapping.
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
    def provision_subject(self, msg):
        """
        Provision a subject to Board Effect.
        """
        log = self.log
        log.debug(
            "Attempting to provision subject '{subject}'.",
            subject=msg.subject)
        remote_id = yield self.fetch_account_id(msg)
        if remote_id is not None:
            yield self.update_subject(remote_id, msg)
        else:
            yield self.add_subject(msg)
        returnValue(None)

    @inlineCallbacks
    def make_authenticated_api_call(self, method, url, **http_options):
        log = self.log
        log.debug("Making authenticated API call ...")
        http_client = self.http_client
        auth_token = self.__auth_token
        if auth_token is None:
            log.debug("Must obtain auth token ...")
            prefix = self.url_prefix
            auth_url = "{0}{1}".format(
                prefix,
                self.authenticate 
            )
            headers = {
                'Accept': ['application/json'],
            }
            api_key = self.api_key
            data = dict(api_key=api_key)
            log.debug("Making API call to obtain auth token ...")
            log.debug("method: POST, URL: {url}", url=auth_url)
            response = yield http_client.post(auth_url, data=data, headers=headers)
            resp_code = response.code
            log.debug("API call complete.  Response code: {code}", code=resp_code)
            if resp_code == 200:
                try:
                    doc = yield response.json()
                except Exception as ex:
                    log.error("Error attempting to parse response to authentication request.")
                    raise
                if not "data" in doc:
                    log.error("Error attempting to parse response to authentication request.")
                    raise Exception("Error parsing authentication response.")
                data = doc["data"]
                if not "token" in data:
                    log.error("Error attempting to parse response to authentication request.")
                    raise Exception("Error parsing authentication response.")
                self.__auth_token = data["token"]
                auth_token =  self.__auth_token
            else:
                content = yield response.content()
                raise Exception(
                    "Could not obtain auth token.  Response ({code}) was:\n{content}".format(
                        code=resp_code,
                        content=content))
        log.debug("Have auth token.")
        headers = http_options.setdefault("headers", {})
        headers["Authorization"] = [auth_token]
        method = method.lower()
        log.debug("Making API call.  method: {method}, URL: {url}", method=method, url=url)
        response = yield getattr(http_client, method)(url, **http_options)
        log.debug("API call complete.  Response code: {code}", code=response.code)
        returnValue(response)

    @inlineCallbacks
    def fetch_all_users(self):
        """
        Load all remote user accounts from the sevice.
        """
        log = self.log
        log.debug("Attempting to fetch all remote user IDs ...")
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}{1}".format(prefix, self.accounts_query)
        headers = {
            'Accept': ['application/json'],
        }
        log.debug("URL (GET): {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        try:
            resp = yield self.make_authenticated_api_call("GET", url, headers=headers)
        except Exception as ex:
            log.error("Error attempting to retrieve existing account.")
            raise
        log.debug("HTTP GET complete.  Response code was: {code}", code=resp.code)
        resp_code = resp.code
        if resp_code != 200:
            raise Exception("Invalid response code: {0}".format(resp_code))
        try:
            doc = yield resp.json()
        except Exception as ex:
            log.error("Error attempting to parse response.")
            raise
        log.debug("Received valid JSON response")
        returnValue(doc)

    @inlineCallbacks
    def fetch_account_id(self, msg):
        """
        Fetch an existing remote account ID and return it or None if the remote 
        account does not exist.
        """
        log = self.log
        log.debug("Attempting to fetch existing account.")
        account_cache = self.__account_cache
        cache_size = self.cache_size
        log.debug("cache max size: {cache_size}", cache_size=cache_size)
        log.debug("cache current size: {cache_size}", cache_size=len(account_cache))
        account_data = None
        if len(account_cache) == 0: 
            # Prefill cache.
            log.debug("Prefilling cache ...")
            doc = yield self.fetch_all_users()
            account_data = doc["data"]
            for entry in account_data: 
                if len(account_cache) >= cache_size:
                    break
                login = entry["login"]
                identifier = entry["id"]
                account_cache[login] = identifier 
            log.debug("Cache size after prefill: {cache_size}", cache_size=len(account_cache))
        subject = msg.subject.lower()
        if subject in account_cache:
            remote_id = account_cache[subject]
            returnValue(remote_id)
        log.debug("Account ID not in cache for '{subject}.", subject=subject)
        if account_data is None:
            account_data = yield self.fetch_all_users()
        for entry in account_data:
            if entry["login"].lower() == subject:
                remote_id = entry["id"]
                account_cache[subject] = remote_id
                log.debug("Added entry to cache: {login}: {identifier}", login=login, identifier=identifier)
                returnValue(remote_id)
        returnValue(None)

    @inlineCallbacks
    def update_subject(self, remote_id, msg):
        """
        Update a remote account.
        """
        log = self.log
        subject = msg.subject
        log.debug("Updating subject '{subject}'", subject=subject)
        attributes = msg.attributes
        props = self.map_attributes(attributes, subject, msg.action)
        prefix = self.url_prefix
        url = "{0}{1}".format(
            prefix,
            self.account_update.render(
                remote_id=remote_id,
                subject=msg.subject,
                attributes=props))
        headers = {'Accept': ['application/json']}
        log.debug("url: {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        log.debug("data: {props}", props=props)
        if not self.diagnostic_mode:
            try:
                resp = yield self.makeauthenticated_api_call(
                    'PUT',  
                    url, 
                    data=props, 
                    headers=headers)
            except Exception as ex:
                log.error("Error attempting to update existing account.")
                raise
            resp_code = resp.code
            log.debug("Response code: {code}", code=resp_code)
            yield resp.content()

    @inlineCallbacks
    def add_subject(self, msg):
        """
        Add a remote service account.
        """
        log = self.log
        subject = msg.subject.lower()
        action = msg.action
        log.debug("Adding a new account ...")
        attributes = msg.attributes
        props = self.map_attributes(attributes, subject, action)
        account_doc = self.account_template.render(
            props=props,
            subject=subject,
            action=action)
        log.debug("Account doc: {doc}", doc=account_doc)
        prefix = self.url_prefix
        url = "{0}{1}".format(
            prefix,
            self.account_add)
        headers = {
            'Accept': ['application/json'], 
            'Content-Type': ['application/json']}
        log.debug("url: {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        log.debug("data: {account_doc}", account_doc=account_doc)
        if not self.diagnostic_mode:
            try:
                resp = yield self.make_authenticated_api_call(
                    'POST',
                    url, 
                    data=StringProducer(account_doc.encode('utf-8')), 
                    headers=headers) 
            except Exception as ex:
                log.error("Error attempting to add new account.")
                raise
            resp_code = resp.code
            log.debug("Response code: {code}", code=resp_code)
            doc = yield resp.json()
            remote_id = doc["id"]
            self.__account_cache[subject.lower()] = remote_id

    @inlineCallbacks
    def deprovision_subject(self, msg):
        """
        Deprovision a subject from the remote service.
        """
        log = self.log
        subject = msg.subject.lower()
        log.debug(
            "Attempting to deprovision subject '{subject}'.",
            subject=subject)
        remote_id = yield self.fetch_account_id(msg)
        if remote_id is None:
            log.debug("Account '{subject}' does not exist on the remote service.",
                subject=subject)
            returnValue(None)
        prefix = self.url_prefix
        url = "{0}{1}".format(
            prefix,
            self.account_delete.render(remote_id=remote_id))
        headers = {
            'Accept': ['application/json']}
        log.debug("url: {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        if not self.diagnostic_mode:
            try:
                resp = yield self.make_authenticated_api_call(
                    'DELETE',
                    url, 
                    headers=headers)
            except Exception as ex:
                log.error(
                    "Error attempting to delete existing account.  subject: {subject}",
                    subject=subject
                )
                raise
            resp_code = resp.code
            log.debug("Response code: {code}", code=resp_code)
            content = yield resp.content()
            if resp_code != 200:
                log.error(
                    "API error attempting to delete subject {subject}:\n{content}",
                    subject=subject,
                    content=content)
                raise Exception("API error attempting to delete remote subject.")
            account_cache = self.__account_cache
            if subject in account_cache:
                del account_cache[subject]

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

