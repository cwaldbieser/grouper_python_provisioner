
from __future__ import print_function
from collections import namedtuple
import datetime
import json
import commentjson
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
class MembershipMessage(object):
    action = attr.attrib()
    subject = attr.attrib()
    group = attr.attrib()


@attr.attrs
class SyncMessage(object):
    action = attr.attrib()
    subjects = attr.attrib(default=attr.Factory(list))
    group = attr.attrib()    


@attr.attrs
class User(object):
    user_id = attr.attrib()
    email = attr.attrib()


@attr.attrs
class UserGroup(object):
    remote_id = attr.attrib()
    handle = attr.attrib()


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


class SlackProvisionerFactory(object):
    implements(IPlugin, IProvisionerFactory)
    tag = "slack"
    opt_help = "Slack Web API Provisioner"
    opt_usage = "This plugin does not support any options."

    def generateProvisioner(self, argstring=""):
        """
        Create an object that implements IProvisioner
        """
        provisioner = SlackProvisioner()
        return provisioner


class SlackProvisioner(object):
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
            provider_config = section2dict(scp, i"SLACK")
            # Load configuration.
            try:
                self.endpoint_s = config.get("endpoint", None)
                self.load_provider_config(provider_config)
            except KeyError as ex:
                raise OptionMissingError(
                    "A require option was missing: '{0}:{1}'.".format(
                        section, ex.args[0]))
            # Create the web client.
            self.make_web_client()
            # Initialize the provider.
            self.init_provider()
        except Exception as ex:
            d = self.reactor.callLater(0, self.reactor.stop)
            log.failure("Provisioner failed to initialize: {0}".format(ex))
            raise
        return defer.succeed(None)

    def load_provider_config(self, config):
        """
        Load provider-specific configuration.
        """
        log = self.log
        self.diagnostic_mode = bool(int(config.get("diagnostic_mode", 0)))
        log.info("Diagnostic mode: {diagnostic}", diagnostic=self.diagnostic_mode)
        self.url_prefix = config["url_prefix"]
        self.api_key = config["api_key"]
        self.team_id = config["team_id"]
        self.user_cache_size = int(config["user_cache_size"])
        self.usergroup_cache_size = int(config["usergroup_cache_size"])
        self.groupmap_path = config['groupmap']

    def init_provider(self):
        """
        Initialize any provider-specific state.
        """
        self.__user_cache = pylru.lrucache(self.user_cache_size)
        self.__usergroup_cache = pylru.lrucache(self.usergroup_cache_size)
        with open(self.groupmap_path, "r") as f:
            doc = json.load(f)
        self.groupmap = doc

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
                yield self.add_member(msg)
            elif msg.action == constants.ACTION_DELETE:
                yield self.remove_member(msg)
            elif msg.action == constants.ACTION_MEMBERSHIP_SYNC:
                yield self.sync_membership(msg)
            else:
                raise UnknownActionError(
                    "Don't know how to handle action '{0}'.".format(msg.action))
        except Exception as ex:
            log.warn("Error provisioning message: {error}", error=ex)
            raise

    def get_config_defaults(self):
        return dedent("""\
            [SLACK]
            diagnostic_mode = 0
            url_prefix = https://slack.com/api
            user_cache_size = 1000
            usergroup_cache_size = 1000
            """)

    def parse_message(self, msg):
        """
        Parse message into a standard form.
        """
        serialized = msg.content.body
        doc = json.loads(serialized)
        action = doc['action']
        if action in (constants.ACTION_ADD, constants.ACTION_DELETE):
            subject = doc['subject']
            group = doc['group']
            return ParsedMessage(action, subject)
        elif action == constants.ACTION_MEMBERSHIP_SYNC:
            subjects = list(doc['subjects'])
            group = doc['group']
            return SyncMessage(action, subjects, group) 

    def check_response_for_errors(self, doc):
        """
        Check the parsed JSON resonse for errors.
        """
        log = self.log
        if "warning" in doc:
            log.warn(
                "Warning in HTTP Response: {{warning}}",
                warning=doc["warning"])
        if not doc["ok"]:
            raise Exception(
                "Error in web API: {error}".format(
                    error=doc["error"]))

    @inlineCallbacks
    def fetch_all_users(self):
        """
        Load all remote user accounts from the sevice.
        """
        log = self.log
        log.debug("Attempting to fetch all remote user IDs ...")
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}{1}".format(prefix, self.users_list)
        headers = {
            'Accept': ['application/json'],
        }
        log.debug("URL (GET): {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        params = {
            'token': self.api_key,
            'presence': '0'
        }
        try:
            resp = yield self.make_authenticated_api_call("GET", url, headers=headers, params=params)
        except Exception as ex:
            log.error("Error attempting to retrieve users list.")
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
        self.check_response_for_errors(doc)
        members = doc["members"]
        team_id = self.team_id
        team_members = []
        for member in members:
            if member["team_id"] != team_id:
                continue
            if member["is_bot"]:
                continue
            if "profile" in member:
                profile = member["profile"]
                email = profile.get("email")
                user_id = member["id"]
                team_members.append(User(user_id, email))
        returnValue(team_members)

    @inlineCallbacks
    def get_user_id(self, subject):
        """
        Fetch an existing remote account ID and return it or None if the remote 
        account does not exist.
        """
        log = self.log
        log.debug("Attempting to fetch existing account.")
        user_cache = self.__user_cache
        cache_size = self.user_cache_size
        log.debug("cache max size: {cache_size}", cache_size=cache_size)
        log.debug("cache current size: {cache_size}", cache_size=len(user_cache))
        members = None
        if len(user_cache) == 0: 
            # Prefill cache.
            log.debug("Prefilling cache ...")
            members = yield self.fetch_all_users()
            for entry in members: 
                if len(user_cache) >= cache_size:
                    break
                user_cache[entry.email] = entry.user_id
            log.debug("Cache size after prefill: {cache_size}", cache_size=len(user_cache))
        subject = subject.lower()
        email = "{0}@lafayette.edu".format(subject)
        if email in user_cache:
            remote_id = user_cache[email]
            returnValue(remote_id)
        log.debug("User ID not in cache for '{subject}.", subject=subject)
        if members is None:
            members = yield self.fetch_all_users()
        for entry in members:
            log.debug("Looping through entries ...")
            if entry.email.lower() == email:
                remote_id = entry.user_id
                user_cache[email] = remote_id
                log.debug("Added entry to cache: {email}: {identifier}", email=email, identifier=remote_id)
                returnValue(remote_id)
        returnValue(None)

    @inlineCallbacks
    def get_usergroup_id(self, name):
        """
        Fetch an existing remote usergroup ID and return it or None if the remote 
        usergroup does not exist.
        """
        log = self.log
        log.debug("Attempting to fetch existing usergroup.")
        cache = self.__usergroup_cache
        cache_size = self.usergroup_cache_size
        log.debug("cache max size: {cache_size}", cache_size=cache_size)
        log.debug("cache current size: {cache_size}", cache_size=len(cache))
        entries = None
        if len(user_cache) == 0: 
            # Prefill cache.
            log.debug("Prefilling cache ...")
            entries = yield self.fetch_all_usergroups()
            for entry in account_data: 
                if len(cache) >= cache_size:
                    break
                cache[entry.handle] = entry.remote_id
            log.debug("Cache size after prefill: {cache_size}", cache_size=len(cache))
        if name in cache:
            remote_id = cache[name]
            returnValue(remote_id)
        log.debug("UserGroup ID not in cache for '{name}.", name=name)
        if entries is None:
            entries = yield self.fetch_all_users()
        for entry in members:
            log.debug("Looping through entries ...")
            if entry.email.lower() == email:
                remote_id = entry.user_id
                user_cache[email] = remote_id
                log.debug("Added entry to cache: {email}: {identifier}", email=email, identifier=remote_id)
                returnValue(remote_id)
        returnValue(None)

    @inlineCallbacks
    def add_member(self, msg):
        """
        Add a member to a usergroup.
        """
        log = self.log
        log.debug("Entered add_member().")
        subject = msg.subject
        groupmap = self.groupmap
        group = msg.group
        if not group in groupmap:
            log.warn(
                "Group '{{group}}' is not listed in the provider group map.  Discarding.",
                group=group)
            returnValue(None)
        mapped_group = groupmap[group]
        user_id = yield self.get_user_id(subject)
        if user_id is None:
            log.warn(
                "Subject '{{subject}}' does not exist on the remote end.  Discarding.",
                subject=subject) 
            returnValue(None)
        usergroup_id = yield self.get_usergroup_id(mapped_group)
        if usergroup_id is None:
            log.warn(
                "UserGroup '{{group}}' does not exist on the remote end.  Discarding.",
                group=mapped_group) 
            returnValue(None)
        members = yield self.get_usergroup_members(usergroup_id)
        mset = set(members)
        if user_id in mset:
            log.debug(
                "Subject '{{subject}}' already a member of '{{group}}'.",
                subject=subject,
                group=mapped_group)
            returnValue(None)
        mset.add(user_id)
        members = list(mset)
        yield self.set_usergroup_members(usergroup_id, members) 
        returnValue(None)



    @inlineCallbacks
    def update_subject(self, remote_id, msg):
        """
        Update a remote account.
        """
        log = self.log
        log.debug("Entered update_subject().")
        subject = msg.subject
        log.debug("Updating subject '{subject}'", subject=subject)
        attributes = msg.attributes
        props = self.map_attributes(attributes, subject, msg.action)
        props['active'] = '1'
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
                resp = yield self.make_authenticated_api_call(
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
        log.debug("Entered add_subject().")
        subject = msg.subject.lower()
        action = msg.action
        log.debug("Adding a new account ...")
        attributes = msg.attributes
        props = self.map_attributes(attributes, subject, action)
        props['active'] = '1'
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
            self.__user_cache[subject.lower()] = remote_id

    @inlineCallbacks
    def deprovision_subject(self, msg):
        """
        Deprovision a subject from the remote service.
        """
        log = self.log
        log.debug("Entered deprovision_subject().")
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
        data = {'active': '0'}
        if not self.diagnostic_mode:
            try:
                resp = yield self.make_authenticated_api_call(
                    'PUT',
                    url, 
                    headers=headers,
                    data=data)
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
            user_cache = self.__user_cache
            if subject in user_cache:
                del user_cache[subject]

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

