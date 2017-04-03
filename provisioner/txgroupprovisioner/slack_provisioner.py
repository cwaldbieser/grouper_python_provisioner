
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
class MembershipMessage(object):
    action = attr.attrib()
    subject = attr.attrib()
    group = attr.attrib()


@attr.attrs
class SyncMessage(object):
    action = attr.attrib()
    subjects = attr.attrib()
    group = attr.attrib()    


@attr.attrs
class IdentifierPair(object):
    local_id = attr.attrib()
    remote_id = attr.attrib()


class UnknowActionError(Exception):
    pass


@attr.attrs
class APIError(Exception):
    message = attr.attrib()
    api_error = attr.attrib()


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
            provider_config = section2dict(scp, "SLACK")
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
        self.user_id_formatter = jinja2.Template(config['user_id_formatter'])
        self.users_list = config['users_list']
        self.usergroups_list = config['usergroups_list']
        self.usergroups_users_list = config['usergroups_users_list']
        self.usergroups_users_update = config['usergroups_users_update']

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
            return MembershipMessage(action, subject, group)
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
                "Warning in HTTP Response: {warning}",
                warning=doc["warning"])
        if not doc["ok"]:
            log.debug("Parsed document is NOT ok ...")
            if "error" in doc:
                api_error = doc["error"]
            else:
                api_error = None
            log.debug("api_error == {api_error}", api_error=api_error)
            ex = APIError(
                "Error in web API: {error}".format(error=api_error),
                api_error)
            raise ex

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
            resp = yield http_client.get(url, headers=headers, params=params)
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
            if member.get("is_restricted", False) == True:
                continue
            if member.get("is_ultra_restricted", False) == True:
                continue
            if "profile" in member:
                profile = member["profile"]
                email = profile.get("email", None)
                user_id = member.get("id", None)
                if email is not None and user_id is not None:
                    team_members.append(IdentifierPair(email.lower(), user_id))
        returnValue(team_members)

    @inlineCallbacks
    def fetch_all_usergroups(self):
        """
        Load all remote usergroups from the sevice.
        """
        log = self.log
        log.debug("Attempting to fetch all remote usergroup IDs ...")
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}{1}".format(prefix, self.usergroups_list)
        headers = {
            'Accept': ['application/json'],
        }
        log.debug("URL (GET): {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        params = {
            'token': self.api_key,
            'include_disabled': '0',
            'include_count': '0',
            'include_users': '0'
        }
        try:
            resp = yield http_client.get(url, headers=headers, params=params)
        except Exception as ex:
            log.error("Error attempting to retrieve usergroups list.")
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
        entries = doc["usergroups"]
        team_id = self.team_id
        results = []
        for entry in entries:
            if entry["team_id"] != team_id:
                continue
            remote_id = entry.get("id", None)
            local_id = entry.get("handle", None)
            if local_id is not None and remote_id is not None:
                results.append(IdentifierPair(local_id.lower(), remote_id))
        returnValue(results)

    @inlineCallbacks
    def get_remote_id(self, local_id, cache, cache_size, entry_list_func):
        """
        Fetch an existing remote ID and return it or None if the remote 
        entry does not exist.
        """
        log = self.log
        log.debug("Entered get_remote_id().")
        log.debug("cache max size: {cache_size}", cache_size=cache_size)
        log.debug("cache current size: {cache_size}", cache_size=len(cache))
        entries = None
        if len(cache) == 0: 
            # Prefill cache.
            log.debug("Prefilling cache ...")
            entries = yield entry_list_func()
            for entry in entries: 
                if len(cache) >= cache_size:
                    break
                cache[entry.local_id] = entry.remote_id
            log.debug("Cache size after prefill: {cache_size}", cache_size=len(cache))
        if name in cache:
            remote_id = cache[local_id]
            returnValue(remote_id)
        log.debug("Remote ID not in cache for '{local_id}.", local_id=local_id)
        if entries is None:
            entries = yield entry_list_func()
        for entry in entries:
            if entry.local_id == local_id:
                remote_id = entry.remote_id
                cache[local_id] = remote_id
                log.debug("Added entry to cache: {local_id}: {remote_id}", local_id=local_id, remote_id=remote_id)
                returnValue(remote_id)
        returnValue(None)

    @inlineCallbacks
    def get_remote_id(self, local_id, cache, cache_size, entry_list_func):
        """
        Fetch an existing remote ID and return it or None if the remote 
        entry does not exist.
        """
        log = self.log
        log.debug("Entered get_remote_id().")
        log.debug("cache max size: {cache_size}", cache_size=cache_size)
        log.debug("cache current size: {cache_size}", cache_size=len(cache))
        entries = None
        if len(cache) == 0: 
            # Prefill cache.
            log.debug("Prefilling cache ...")
            entries = yield entry_list_func()
            for entry in entries: 
                if len(cache) >= cache_size:
                    break
                cache[entry.local_id] = entry.remote_id
            log.debug("Cache size after prefill: {cache_size}", cache_size=len(cache))
        if local_id in cache:
            remote_id = cache[local_id]
            returnValue(remote_id)
        log.debug("Remote ID not in cache for '{local_id}.", local_id=local_id)
        if entries is None:
            entries = yield entry_list_func()
        for entry in entries:
            if entry.local_id == local_id:
                remote_id = entry.remote_id
                cache[local_id] = remote_id
                log.debug("Added entry to cache: {local_id}: {remote_id}", local_id=local_id, remote_id=remote_id)
                returnValue(remote_id)
        returnValue(None)

    @inlineCallbacks
    def get_user_id(self, subject):
        """
        Fetch an existing remote account ID and return it or None if the remote 
        account does not exist.
        """
        local_id = self.user_id_formatter.render(subject=subject)
        cache = self.__user_cache
        cache_size = self.user_cache_size
        entry_list_func = self.fetch_all_users
        result = yield self. get_remote_id(local_id, cache, cache_size, entry_list_func)
        returnValue(result)

    @inlineCallbacks
    def get_usergroup_id(self, usergroup):
        """
        Fetch an existing remote usergroup ID and return it or None if the remote 
        usergroup does not exist.
        """
        local_id = usergroup.lower()
        cache = self.__usergroup_cache
        cache_size = self.usergroup_cache_size
        entry_list_func = self.fetch_all_usergroups
        result = yield self. get_remote_id(local_id, cache, cache_size, entry_list_func)
        returnValue(result)

    @inlineCallbacks
    def get_usergroup_members(self, remote_id):
        """
        Get the remote user IDs that are mebers of the usergroup
        identifier by `remote_id`.
        """
        log = self.log
        log.debug(
            "Attempting to fetch members of remote usergroup identified by '{remote_id}' ...",
            remote_id=remote_id)
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}{1}".format(prefix, self.usergroups_users_list)
        headers = {
            'Accept': ['application/json'],
        }
        log.debug("URL (GET): {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        params = {
            'token': self.api_key,
            'usergroup': remote_id,
            'include_disabled': '0',
        }
        try:
            resp = yield http_client.get(url, headers=headers, params=params)
        except Exception as ex:
            log.error("Error attempting to retrieve usergroup members.")
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
        entries = doc["users"]
        results = list(entries)
        returnValue(results)

    @inlineCallbacks
    def set_usergroup_members(self, remote_id, user_ids):
        """
        Set the remote user IDs that are mebers of the usergroup
        identifier by `remote_id`.
        `user_ids` is a complete list of user IDs that belong to the usergroup.
        """
        log = self.log
        log.debug(
            "Attempting to set members of remote usergroup identified by '{remote_id} ...",
            remote_id=remote_id)
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}{1}".format(prefix, self.usergroups_users_update)
        headers = {
            'Accept': ['application/json'],
        }
        log.debug("URL (GET): {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        params = {
            'token': self.api_key,
            'usergroup': remote_id,
            'users': ','.join(user_ids),
            'include_count': '0',
        }
        try:
            resp = yield http_client.get(url, headers=headers, params=params)
        except Exception as ex:
            log.error("Error attempting to set usergroup members.")
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
                "Group '{group}' is not listed in the provider group map.  Discarding.",
                group=group)
            returnValue(None)
        mapped_group = groupmap[group]
        user_id = yield self.get_user_id(subject)
        if user_id is None:
            log.warn(
                "Subject '{subject}' does not exist on the remote end.  Discarding.",
                subject=subject) 
            returnValue(None)
        usergroup_id = yield self.get_usergroup_id(mapped_group)
        if usergroup_id is None:
            log.warn(
                "UserGroup '{group}' does not exist on the remote end.  Discarding.",
                group=mapped_group) 
            returnValue(None)
        members = yield self.get_usergroup_members(usergroup_id)
        mset = set(members)
        if user_id in mset:
            log.debug(
                "Subject '{subject}' already a member of '{group}'.",
                subject=subject,
                group=mapped_group)
            returnValue(None)
        mset.add(user_id)
        members = list(mset)
        yield self.set_usergroup_members(usergroup_id, members) 
        returnValue(None)

    @inlineCallbacks
    def remove_member(self, msg):
        """
        Remove a member from a usergroup.
        """
        log = self.log
        log.debug("Entered remove_member().")
        subject = msg.subject
        groupmap = self.groupmap
        group = msg.group
        if not group in groupmap:
            log.warn(
                "Group '{group}' is not listed in the provider group map.  Discarding.",
                group=group)
            returnValue(None)
        mapped_group = groupmap[group]
        user_id = yield self.get_user_id(subject)
        if user_id is None:
            log.warn(
                "Subject '{subject}' does not exist on the remote end.  Discarding.",
                subject=subject) 
            returnValue(None)
        usergroup_id = yield self.get_usergroup_id(mapped_group)
        if usergroup_id is None:
            log.warn(
                "UserGroup '{group}' does not exist on the remote end.  Discarding.",
                group=mapped_group) 
            returnValue(None)
        members = yield self.get_usergroup_members(usergroup_id)
        mset = set(members)
        if user_id not in mset:
            log.debug(
                "Subject '{subject}' already isn't a member of '{group}'.",
                subject=subject,
                group=mapped_group)
            returnValue(None)
        mset.discard(user_id)
        members = list(mset)
        yield self.set_usergroup_members(usergroup_id, members) 
        returnValue(None)

    @inlineCallbacks
    def sync_membership(self, msg):
        """
        Sync a usergroup's membership to an exact list of users.
        """
        log = self.log
        log.debug("Entered sync_membership().")
        subjects = msg.subjects
        groupmap = self.groupmap
        group = msg.group
        if not group in groupmap:
            log.warn(
                "Group '{group}' is not listed in the provider group map.  Discarding.",
                group=group)
            returnValue(None)
        mapped_group = groupmap[group]
        usergroup_id = yield self.get_usergroup_id(mapped_group)
        if usergroup_id is None:
            log.warn(
                "UserGroup '{group}' does not exist on the remote end.  Discarding.",
                group=mapped_group) 
            returnValue(None)
        user_ids = []
        for subject in subjects:
            user_id = yield self.get_user_id(subject)
            if user_id is None:
                log.warn(
                    "Subject '{subject}' does not exist on the remote end.  Discarding.",
                    subject=subject) 
            else:
                user_ids.append(user_id)
        try:
            yield self.set_usergroup_members(usergroup_id, user_ids) 
        except APIError as ex:
            if ex.api_error == "invalid_users":
                self.remove_subjects_from_cache(subjects)
            raise ex
        returnValue(None)

    def remove_subjects_from_cache(self, subjects):
        """
        Remove subjects from cache.
        """
        log = self.log
        log.debug("Removing subjects from cache: {subjects}", subjects=subjects)
        cache = self.__user_cache
        for subject in subjects:
            local_id = self.user_id_formatter.render(subject=subject)
            if local_id in cache:
                del cache[local_id]
        log.debug("Subjects removed from cache.")

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

