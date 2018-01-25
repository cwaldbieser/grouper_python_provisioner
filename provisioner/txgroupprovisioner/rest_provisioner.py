
from __future__ import print_function
from collections import namedtuple
import datetime
import json
import commentjson
import jinja2 
from textwrap import dedent
import traceback
import attr
import pylru
from six import iteritems
import treq
from twisted.internet import defer, task
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
class ParsedSubjectMessage(object):
    action = attr.attrib()
    group = attr.attrib()
    subject = attr.attrib()
    attributes = attr.attrib(default=attr.Factory(dict))


@attr.attrs
class ParsedSyncMessage(object):
    action = attr.attrib()
    group = attr.attrib()
    subjects = attr.attrib(default=attr.Factory(list))
    attributes = attr.attrib(default=attr.Factory(dict))


@attr.attrs
class ParsedWorkroomMessage(object):
    action = attr.attrib()
    group = attr.attrib()
    subject = attr.attrib()
    attributes = attr.attrib(default=attr.Factory(dict))


@attr.attrs
class ParsedSyncWorkroomMessage(object):
    action = attr.attrib()
    group = attr.attrib()
    subjects = attr.attrib()
    attributes = attr.attrib(default=attr.Factory(dict))


class UnknowActionError(Exception):
    pass


class APIResponseError(Exception):
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

def useless_provisioner_factory__():
    raise NotImplementedError(
        "Set `provisioner_factory` to an instance of an actual provisioner factory.  "
        "HINT: Subclass `RESTProvisioner`.")

class RESTProvisionerFactory(object):
    implements(IPlugin, IProvisionerFactory)
    tag = "override_this_tag"
    opt_help = "RESTful API Provisioner"
    opt_usage = "This plugin does not support any options."
    provisioner_factory = useless_provisioner_factory__

    def generateProvisioner(self, argstring=""):
        """
        Create an object that implements IProvisioner
        """
        provisioner = self.provisioner_factory()
        return provisioner


class RESTProvisioner(object):
    implements(IProvisioner)
    service_state = None
    reactor = None
    log = None
    account_sync_rate_limit_ms = 0
    member_sync_rate_limit_ms = 0
    provision_strategy = "query-first"
    group_sync_strategy = "add-members-first"

    def get_match_value_from_remote_account(self, remote_account):
        """
        Given a remote account, `remote_account`, extract the
        value that will be used to match the remote account 
        to the local subject.
        Returns None if a match value cannot be constructed for the remote
        account.
        """
        raise NotImplementedError()

    def get_match_value_from_local_subject(self, subject, attributes):
        """
        Given a local subject and attributes, compute the value that
        will be used to match the remote account to the local subject.
        """
        raise NotImplementedError()

    def get_api_id_from_remote_account(self, remote_account):
        """
        Given a remote account, `remote_account`, extract the
        value that is used as an account identifier in API
        calls that reference the account.
        """
        raise NotImplementedError()

    @inlineCallbacks
    def api_get_auth_token(self):
        """
        Make API call to obtain valid auth token.
        Should set `self.auth_token`.
        Logic in the REST provisioner will attempt to determine if an
        authirization token has expired and needs to be renewed.  This
        method will be called when that happens.

        .. note::

            Some APIs don't require a separate step for obtaining an authorization
            token.  In that case, just set `self.auth_token` to True.  The actual
            authorization of a HTTP request occurs in `authorize_api_call()`.
            In that method, you may use `self.auth_token` or not.

            E.g. If a request uses a simple shared secret for authorization, or
            a JSON web token (JWT) which is based on time and a shared secret, then
            `self.auth_token` would not be relevant to authorization.  Instead,
            `self.client_secret` would likely be used directly.
        """
        if False:
            yield None
        raise NotImplementedError()

    @inlineCallbacks
    def authorize_api_call(self, method, url, **http_options):
        """
        Given the components of an *unauthenticated* HTTP client request, 
        return the components of an authenticated request.

        Should return a tuple of (method, url, http_options)
        """
        if False:
            yield None
        raise NotImplementedError()

    @inlineCallbacks
    def api_get_all_target_groups(self):
        """
        Load all target_groups from the sevice.
        Must return an iterable that yields tuples of
        (local_group_id, remote_group_id).
        """
        if False:
            yield None
        raise NotImplementedError()
    
    @inlineCallbacks
    def api_add_subject_to_group(self, subject_id, target_group_id):
        """
        Make an authenticated API call to add the remote subject ID
        to the remote group ID.
        Should raise on error on failure.
        """
        if False:
            yield None
        raise NotImplementedError()

    @inlineCallbacks
    def api_remove_subject_from_group(self, subject_id, target_group_id):
        """
        Make an authenticated API call to remove the remote subject ID
        from the remote group ID.
        Should raise on error on failure.
        """
        if False:
            yield None
        raise NotImplementedError()

    @inlineCallbacks
    def get_subjects_for_target_group(self, target_group_id):
        """
        Retireve a list of remote subject IDs that belong to a target_group identified
        by remote target_group_id.
        """
        if False:
            yield None
        raise NotImplementedError()

    @inlineCallbacks
    def get_all_api_ids_and_match_values(self):
        """
        Load all the remote API IDs and match values from the 
        user accounts that exist on the remote sevice.
        Returns an iterable of (api_id, match_value).
        """
        if False:
            yield None
        raise NotImplementedError()

    @inlineCallbacks
    def api_deprovision_subject(self, api_id):
        """
        Make the API call require to deprovision the subject identified by
        `api_id`.
        """
        if False:
            yield None
        raise NotImplementedError()

    @inlineCallbacks
    def api_update_subject(self, subject, api_id, attributes):
        """
        Make API request to update remote account.
        Returns the HTTP response.
        """
        if False:
            yield None
        raise NotImplementedError()

    @inlineCallbacks
    def api_add_subject(self, subject, attributes):
        """
        Use the API to add subjects.
        
        Returns the API ID of the newly created remote account or None.
        If None is returned, the API ID will not be cached and require
        a lookup on future use.
        """
        if False:
            yield None
        raise NotImplementedError()

    @inlineCallbacks
    def api_get_remote_account(self, api_id):
        """
        Get the remote account information using its API ID.
        If the remote account cannot be found, return None.
        """
        if False:
            yield None
        raise NotImplementedError()

    def parse_config(self, scp):
        """
        Parse any additional configuration this provisioner might need.
        """
        pass

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
                self.unmanaged_logins = set(
                    login.lower() 
                        for login in config.get("unmanaged_logins", "").split())
                log.debug(
                    "unmanaged_logins: {unmanaged_logins}",
                    unmanaged_logins=list(self.unmanaged_logins))
                self.provision_group = config.get("provision_group", None)
                if self.provision_group is not None:
                    self.provision_group = self.provision_group.lower()
                target_group_map_path = config.get("target_group_map", None)
                self.endpoint_s = config.get("endpoint", None)
                self.url_prefix = config["url_prefix"]
                self.client_secret = config["client_secret"]
                self.account_cache_size = int(config["account_cache_size"])
                if target_group_map_path:
                    self.target_group_cache_size = int(config.get("target_group_cache_size", 100))
                    self.target_group_retry_delay = int(config.get("target_group_retry_delay", 20))
                self.account_sync_rate_limit_ms = int(config.get("account_sync_rate_limit_ms", 0))
                self.member_sync_rate_limit_ms = int(config.get("member_sync_rate_limit_ms", 0))
                self.provision_strategy = config.get("provision_strategy", "query-first").lower()
                self.group_sync_strategy = config.get("group_sync_strategy", "add-members-first").lower()
                if not self.group_sync_strategy in ('query-first', 'add-members-first'):
                    raise Exception("Unknown group_sync_strategy: {}".format(self.group_sync_strategy))
            except KeyError as ex:
                raise OptionMissingError(
                    "A require option was missing: '{0}:{1}'.".format(
                        section, ex.args[0]))
            if self.provision_group is None and target_group_map_path is None:
                raise OptionMissingError(
                    "Must provide at least one of `provision_group` (account "
                    "provisioning) or `target_group_map` (target_group mapping).")
            if not self.provision_strategy in ('query-first', 'create-first'):
                raise Exception("`provision_strategy` must be one of: query-first, create-first")
            # Create the web client.
            self.make_default_web_client()
            self.__target_group_cache = None
            if target_group_map_path:
                # Create the target_group cache.
                self.__target_group_cache = pylru.lrucache(self.target_group_cache_size)
            # Create the target_group map.
            self.make_target_group_map(target_group_map_path)
            # Create account cache.
            self.__account_cache = pylru.lrucache(self.account_cache_size)
            # Initialize access token.
            self.auth_token = None
        except Exception as ex:
            d = self.reactor.callLater(0, self.reactor.stop)
            log.failure("Provisioner failed to initialize: {0}".format(ex))
            raise
        self.parse_config(scp)
        return defer.succeed(None)

    def make_target_group_map(self, target_group_map_path):
        """
        Create source group to target group mappings from JSON
        file.
        """
        log = self.log
        if target_group_map_path is None:
            log.info("No target_group map.  Permissions will not be mapped.")
            self.target_group_map = {}
            return
        with open(target_group_map_path, "r") as f:
            self.target_group_map = commentjson.load(f)
        log.info("Created source group to target group map.")

    @inlineCallbacks                                                   
    def provision(self, amqp_message):             
        """                                                
        Provision an entry based on an AMQP message.  
        """                                              
        log = self.log
        target_group_map = self.target_group_map
        try:
            msg = self.parse_message(amqp_message)
            src_group = msg.group.lower()
            target_group = target_group_map.get(src_group, None)
            if src_group == self.provision_group:
                if target_group is not None:
                    log.warn(
                        "Group '{group}' is the account provisioning group AND in the target_group map."
                        "  It will NEVER be used for target_group mapping.",
                        group=src_group)
                if msg.action in (constants.ACTION_ADD, constants.ACTION_UPDATE):
                    yield self.provision_subject(msg.subject, msg.attributes)
                elif msg.action == constants.ACTION_DELETE:
                    yield self.deprovision_subject(msg.subject, msg.attributes)
                elif msg.action == constants.ACTION_MEMBERSHIP_SYNC:
                    yield self.sync_members(msg.subjects, msg.attributes)
                else:
                    raise UnknownActionError(
                        "Don't know how to handle action '{0}' for provisioning.".format(msg.action))
            elif target_group is not None:
                target_group = target_group.lower()
                if msg.action == constants.ACTION_ADD:
                    yield self.add_subject_to_target_group(target_group, msg.subject, msg.attributes)    
                elif msg.action == constants.ACTION_DELETE:
                    yield self.remove_subject_from_target_group(target_group, msg.subject, msg.attributes)
                elif msg.action == constants.ACTION_MEMBERSHIP_SYNC:
                    yield self.sync_subjects_to_target_group(target_group, msg.subjects, msg.attributes)
                else:
                    raise UnknownActionError(
                        "Don't know how to handle action '{0}' for target_groups.".format(msg.action))
            else:
                log.warn(
                    "Not sure what to do with group '{group}'.  Discarding ...",
                    group=src_group)
        except Exception as ex:
            log.warn("Error provisioning message: {error}", error=ex)
            tb = traceback.format_exc()
            log.debug("{traceback}", traceback=tb)
            raise

    def get_config_defaults(self):
        return dedent("""\
            [PROVISIONER]
            url_prefix = https://graph.microsoft.com/v1.0
            account_cache_size = 1000
            """)

    def parse_message(self, msg):
        """
        Parse message into a standard form.
        """
        log = self.log
        provision_group = self.provision_group
        serialized = msg.content.body
        doc = json.loads(serialized)
        action = doc['action']
        group = doc['group'].lower()
        single_subject_actions = (
            constants.ACTION_ADD,
            constants.ACTION_DELETE,
            constants.ACTION_UPDATE)
        if group == provision_group:
            if action in single_subject_actions:
                subject = doc['subject'].lower()
                attributes = None
                attributes = doc['attributes']
                return ParsedSubjectMessage(action, group, subject, attributes)
            elif action == constants.ACTION_MEMBERSHIP_SYNC:
                subjects = doc['subjects']
                attributes = doc['attributes']
                return ParsedSyncMessage(action, group, subjects, attributes)
        else:
            if action in single_subject_actions:
                subject = doc["subject"].lower()
                attributes = doc.get("attributes", None)
                return ParsedWorkroomMessage(action, group, subject, attributes)
            elif action == constants.ACTION_MEMBERSHIP_SYNC:
                subjects = doc["subjects"]
                attributes = doc.get("attributes", None)
                return ParsedSyncWorkroomMessage(action, group, subjects, attributes)
        raise Exception("Could not parse message: {0}".format(msg))

    @inlineCallbacks
    def sync_members(self, subjects, attrib_map):
        """
        Sync all local subjects to remote accounts.
        (Except non-managed accounts).
        """
        log = self.log
        reactor = self.reactor
        unmanaged_logins = self.unmanaged_logins
        subject_list = [s.lower() for s in subjects]
        subject_list.sort()
        rate_limit_ms = self.account_sync_rate_limit_ms
        if rate_limit_ms != 0:
            rate_limit_td = datetime.timedelta(milliseconds=rate_limit_ms)
        else:
            rate_limit_td = None
        process_next_at = None
        for subject in subject_list:
            subject = subject.lower()
            attributes = attrib_map[subject]
            if not process_next_at is None:
                yield delayUntil(reactor, process_next_at)
            yield self.provision_subject(subject, attributes)
            if not rate_limit_td is None:
                process_next_at = datetime.datetime.today() + rate_limit_td
        match_set = set([])
        for subject in subject_list:
            match_value = self.get_match_value_from_local_subject(
                subject=subject,
                attributes=attrib_map[subject])
            match_set.add(match_value)
        api_ids = yield self.get_all_api_ids_and_match_values()
        for api_id, match_value in api_ids:
            if match_value in unmanaged_logins:
                continue
            if not match_value in match_set:
                if not process_next_at is None:
                    yield delayUntil(reactor, process_next_at)
                yield self.deprovision_subject(None, None, api_id=api_id) 
                if not rate_limit_td is None:
                    process_next_at = datetime.datetime.today() + rate_limit_td

    @inlineCallbacks
    def provision_subject(self, subject, attributes):
        """
        Provision a subject to the remote service.
        """
        log = self.log
        provision_strategy = self.provision_strategy
        log.debug(
            "Attempting to provision subject '{subject}' with strategy {strategy}.",
            subject=subject,
            strategy=provision_strategy)
        if self.is_subject_unmanaged(subject, attributes):
            returnValue(None)
        api_id = yield self.get_account_id_from_cache(subject)
        if not api_id is None:
            yield self.update_subject(subject, api_id, attributes)
            returnValue(None)
        elif provision_strategy == 'query-first':
            api_id = yield self.fetch_account_id(subject, attributes)
            if api_id is not None:
                yield self.update_subject(subject, api_id, attributes)
            else:
                yield self.add_subject(subject, attributes)
            returnValue(None)
        elif provision_strategy == 'create-first':
            try:
                yield self.add_subject(subject, attributes)
            except Exception as ex:
                api_id = yield self.fetch_account_id(subject, attributes)
                if api_id is not None:
                    yield self.update_subject(subject, api_id, attributes)
                else:
                    raise Exception("Could not create remote account nor query matching API ID for '{}'.".format(subject))
        else:
            raise Exception("Unknown strategy '{}'.".format(provision_strategy))

    def check_unauthorized_response(self, response):
        """
        Check if an API response is 4xx representing unauthorized.
        If so, raise an exception.
        """
        log = self.log
        if response.code in (401, 419):
            self.auth_token = None
            content = yield response.content()
            raise Exception(
                "Unauthorized.  Response ({code}):\n{content}".format(
                    code=resp_code,
                    content=content))

    @inlineCallbacks
    def fetch_auth_token(self):
        """
        Obtain valid auth token.
        If `self.auth_token` is not None, this is a no-op.
        Sets `self.auth_token`.
        """
        log = self.log
        if self.auth_token is None:
            log.debug("Must obtain auth token ...")
            yield self.api_get_auth_token()
            log.debug("Auth token obtained.")

    @inlineCallbacks
    def make_authenticated_api_call(self, method, url, **http_options):
        """
        Given the components of an HTTP client request, make an
        authenticated request.
        """
        log = self.log
        log.debug("Making authenticated API call ...")
        http_client = self.http_client
        method = method.lower()
        yield self.fetch_auth_token()
        new_method, new_url, new_http_options = yield self.authorize_api_call(
            method, url, **http_options)
        log.debug(
            "Making API call.  method: {method}, URL: {url}", 
            method=new_method, 
            url=new_url)
        response = yield getattr(http_client, new_method)(new_url, **new_http_options)
        log.debug("API call complete.  Response code: {code}", code=response.code)
        if response.code in (401, 419):
            log.debug("Got unauthorized response.  Will reauthorize and retry.")
            self.auth_token = None
            yield self.fetch_auth_token()
            new_method, new_url, new_http_options = yield self.authorize_api_call(
                method, url, **http_options)
            response = yield getattr(http_client, new_method)(new_url, **new_http_options)
            log.debug("API call complete.  Response code: {code}", code=response.code)
        returnValue(response)

    @inlineCallbacks
    def fetch_target_group_id(self, target_group):
        """
        Fetch an existing remote target_group ID and return it or None if the remote 
        account does not exist.
        """
        log = self.log
        target_group = target_group.lower()
        log.debug("Attempting to fetch existing target_group.")
        target_group_cache = self.__target_group_cache
        cache_size = self.target_group_cache_size
        log.debug("cache max size: {cache_size}", cache_size=cache_size)
        log.debug("cache current size: {cache_size}", cache_size=len(target_group_cache))
        all_groups = None
        if len(target_group_cache) == 0: 
            # Prefill cache.
            log.debug("Prefilling target_group cache ...")
            all_groups = yield self.api_get_all_target_groups()
            for local_id, api_id in all_groups: 
                if len(target_group_cache) >= cache_size:
                    break
                local_id = local_id.lower()
                target_group_cache[local_id] = api_id
            log.debug("Cache size after prefill: {cache_size}", cache_size=len(target_group_cache))
        if target_group in target_group_cache:
            target_group_id = target_group_cache[target_group]
            returnValue(target_group_id)
        log.debug("Remote ID not in cache for '{target_group}.", target_group=target_group)
        if all_groups is None:
            all_groups = yield self.api_get_all_target_groups()
        for local_id, api_id in all_groups:
            log.debug("Looping through entries ...")
            local_id = local_id.lower()
            if local_id == target_group:
                target_group_cache[target_group] = api_id
                log.debug(
                    "Added entry to target_group cache: {name}: {identifier}", 
                    name=target_group, 
                    identifier=api_id)
                returnValue(api_id)
        returnValue(None)
    
    @inlineCallbacks
    def add_subject_to_target_group(self, target_group, subject, attributes, target_group_id=None, subject_id=None):
        """
        Add a subject to a target_group.
        `subject_id` may be provided as an optimization.  If it is not 
        provided, it is looked up using `subject` and `attributes`.
        """
        log = self.log
        assert (subject is not None), "Must provide `subject`!"
        assert (target_group is not None) or (target_group_id is not None), "Must provide `target_group` or `target_group_id`!"
        if not subject is None:
            if self.is_subject_unmanaged(subject, attributes):
                returnValue(None)
        else:
            is_unmanaged = yield self.is_api_id_unmanaged(subject_id)
            if is_unmanaged:
                returnValue(None)
        if target_group_id is None:
            target_group_id = yield self.fetch_target_group_id(target_group)
            if target_group_id is None:
                log.warn(
                    "Unable to find target_group ID for '{target_group}'.  Discarding ...",
                    target_group=target_group)
                returnValue(None)
        if subject_id is None:
            subject_id = yield self.fetch_account_id(subject, attributes)
            if subject_id is None:
                yield delay(self.reactor, self.target_group_retry_delay) 
                subject_id = yield self.fetch_account_id(subject, attributes)
                if subject_id is None:
                    log.warn(
                        "Unable to find api_id for subject '{subject}'.  Discarding ...",
                        subject=subject)
                    returnValue(None)
        yield self.api_add_subject_to_group(subject_id, target_group_id)

    @inlineCallbacks
    def remove_subject_from_target_group(self, target_group, subject, attributes, target_group_id=None, subject_id=None):
        """
        Remove a subject from a target_group.
        """
        log = self.log
        assert (subject is not None) or (subject_id is not None), "Must provide `subject` or `subject_id`!"
        assert (target_group is not None) or (target_group_id is not None), "Must provide `target_group` or `target_group_id`!"
        if not subject is None:
            if self.is_subject_unmanaged(subject, attributes):
                returnValue(None)
        else:
            is_unmanaged = yield self.is_api_id_unmanaged(subject_id)
            if is_unmanaged:
                returnValue(None)
        subject_identifier = subject or subject_id
        if target_group_id is None:
            target_group_id = yield self.fetch_target_group_id(target_group)
            if target_group_id is None:
                log.warn(
                    "Unable to find target_group ID for '{target_group}'.  Discarding ...",
                    target_group=target_group)
                returnValue(None)
        if subject_id is None:
            subject_id = yield self.fetch_account_id(subject, attributes)
            if subject_id is None:
                log.warn(
                    "Unable to find api_id for subject '{subject}'.  Discarding ...",
                    subject=subject)
                returnValue(None)
        yield self.api_remove_subject_from_group(subject_id, target_group_id)

    @inlineCallbacks
    def sync_subjects_to_target_group(self, target_group, subjects, attributes):
        """
        Sync target_group membership.
        """
        log = self.log
        reactor = self.reactor
        group_sync_strategy = self.group_sync_strategy
        rate_limit_ms = self.member_sync_rate_limit_ms
        if rate_limit_ms != 0:
            rate_limit_td = datetime.timedelta(milliseconds=rate_limit_ms)
        else:
            rate_limit_td = None
        process_next_at = None
        target_group_id = yield self.fetch_target_group_id(target_group)
        if target_group_id is None:
            log.warn(
                "Unable to find target_group ID for '{target_group}'.  Discarding ...",
                target_group=target_group)
            returnValue(None)
        subject_api_ids = []
        if group_sync_strategy == 'query-first':
            log.debug("Group sync strategy is 'query-first'.")
            existing_member_ids = yield self.get_subjects_for_target_group(target_group_id)
        else:
            existing_member_ids = set([])
        log.debug("`existing_member_ids`: {existing_ids}", existing_ids=existing_member_ids)
        subject_list = list(subjects)
        subject_list.sort()
        for subject in subjects:
            subj_attribs = None
            if attributes is not None:
                subj_attribs = attributes.get(subject, None)
            subject_api_id = yield self.fetch_account_id(subject, subj_attribs)
            if subject_api_id is None:
                log.warn(
                    "Could not find api ID for subject '{subject}'."
                    "  Ignoring for sync to target_group.",
                    subject=subject)
                continue
            subject_api_ids.append((subject, subject_api_id))
        log.debug(
            "Adding {count} subjects to target_group '{target_group}' ...",
            count=len(subject_api_ids),
            target_group=target_group)
        for subject, subject_api_id in subject_api_ids:
            if subject_api_id in existing_member_ids:
                continue
            is_unmanaged = yield self.is_api_id_unmanaged(subject_api_id)
            if is_unmanaged:
                continue
            subj_attribs = None
            if attributes is not None:
                subj_attribs = attributes.get(subject, None)
            if not process_next_at is None:
                yield delayUntil(reactor, process_next_at)
            yield self.add_subject_to_target_group(
                target_group,
                subject,
                subj_attribs,
                target_group_id=target_group_id,
                subject_id=subject_api_id)
            if not rate_limit_td is None:
                process_next_at = datetime.datetime.today() + rate_limit_td
        subject_api_id_set = set(identifier for junk, identifier in subject_api_ids)
        actual_subject_ids = yield self.get_subjects_for_target_group(target_group_id)
        for api_id in actual_subject_ids:
            if not api_id in subject_api_id_set:
                is_unmanaged = yield self.is_api_id_unmanaged(api_id)
                if is_unmanaged:
                    continue
                if not process_next_at is None:
                    yield delayUntil(reactor, process_next_at)
                yield self.remove_subject_from_target_group(
                    target_group,
                    subject=None,
                    attributes=None,
                    target_group_id=target_group_id,
                    subject_id=api_id)
                if not rate_limit_td is None:
                    process_next_at = datetime.datetime.today() + rate_limit_td

    def get_account_id_from_cache(self, subject):
        """
        Fetch an existing remote account ID from the cache and return it 
        or None if the remote account does not exist.
        """
        log = self.log
        log.debug("Attempting to fetch existing account from cache.")
        account_cache = self.__account_cache
        cache_size = self.account_cache_size
        log.debug("cache max size: {cache_size}", cache_size=cache_size)
        log.debug("cache current size: {cache_size}", cache_size=len(account_cache))
        account_data = None
        if subject in account_cache:
            api_id = account_cache[subject]
            return api_id
        log.debug("Account ID not in cache for '{subject}'.", subject=subject)

    @inlineCallbacks
    def fetch_account_id(self, subject, attributes):
        """
        Fetch an existing remote account ID and return it or None if the remote 
        account does not exist.
        """
        log = self.log
        api_id = self.get_account_id_from_cache(subject)
        if not api_id is None:
            returnValue(api_id)
        account_cache = self.__account_cache
        api_id = yield self.api_get_account_id(subject, attributes)
        if api_id is not None:
            account_cache[subject] = api_id
        returnValue(api_id)

    def get_subject_api_id_from_cache(self, subject):
        """
        Return the API ID for a subject or None if it is not in the cache.
        """
        log = self.log
        account_cache = self.__account_cache
        return account_cache.get(subject, None)

    def invalidate_cached_subject_api(self, api_id):
        """
        Remove all entries from the subject cache which have the given `api_id`.
        """
        log = self.log
        account_cache = self.__account_cache
        subjects = set([])
        for k, v in account_cache.items():
            if v == api_id:
                subjects.add(k)
        for k in subjects:
            del account_cache[k]
            log.debug(
                "Invalidated subject cache fo subject '{subject}', API ID '{api_id}'.",
                subject=k,
                api_id=api_id)

    def fill_account_cache(self, account_id_map):
        """
        Sometimes, a service API only allows looking up a complete list of
        accounts.  In this case, it can make sense for subclasses of the
        `RESTProvisioner` to fill the cache with the queried information
        rather than looking it up multiple times.

        `account_id_map` should be a mapping of subjects to API IDs.
        
        The cache will not be filled beyond its capacity.
        """
        log = self.log
        log.debug("Filling account cache with {size} entries ...", size=len(account_id_map))
        account_cache = self.__account_cache
        for subject, api_id in iteritems(account_id_map):
            account_cache[subject] = api_id

    @inlineCallbacks
    def update_subject(self, subject, api_id, attributes):
        """
        Update a remote account.
        """
        log = self.log
        if self.is_subject_unmanaged(subject, attributes):
            returnValue(None)
        try:
            resp = yield self.api_update_subject(subject, api_id, attributes)
        except Exception as ex:
            #log.error(
            #    "Error attempting to update subject '{subject}' identified by '{api_id}'.",
            #    subject,
            #    api_id)
            raise
        resp_code = resp.code
        log.debug("Response code: {code}", code=resp_code)
        yield resp.content()

    @inlineCallbacks
    def add_subject(self, subject, attributes):
        """
        Add an account to the remote service.
        """
        log = self.log
        if self.is_subject_unmanaged(subject, attributes):
            returnValue(None)
        log.debug("Adding a new account ...")
        try:
            api_id = yield self.api_add_subject(subject, attributes)
        except Exception as ex:
            #log.error(
            #    "Error attempting to add subject '{subject}'.",
            #    subject=subject)
            raise
        if api_id is not None:    
            self.__account_cache[subject.lower()] = api_id

    @inlineCallbacks
    def deprovision_subject(self, subject, attributes, api_id=None):
        """
        Deprovision a subject from the remote service.
        """
        log = self.log
        log.debug("Entered deprovision_subject().")
        assert (subject is not None) or (api_id is not None), (
            "Must provide `subject` or `api_id`!")
        subject_identifier = subject or api_id
        log.debug(
            "Attempting to deprovision subject identified by '{identifier}'.",
            identifier=subject_identifier)
        if api_id is None:
            subject = subject.lower()
            if self.is_subject_unmanaged(subject, attributes):
                returnValue(None)
            api_id = yield self.fetch_account_id(subject, attributes)
        if api_id is None:
            log.debug("Account '{subject}' does not exist on the remote service.",
                subject=subject)
            returnValue(None)
        try:
            yield self.api_deprovision_subject(api_id)
        except Exception as ex:
            #log.error(
            #    "Error attempting to de-provision subject identified by '{identifier}'.",
            #    identifier=subject_identifier)
            raise
        account_cache = self.__account_cache
        if not subject is None:
            if subject in account_cache:
                del account_cache[subject]
        else:
            for subject, r_id in account_cache.items():
                if api_id == r_id:
                    del account_cache[subject]
                    break

    def is_subject_unmanaged(self, subject, attributes):
        """
        Returns True if subject is unmanaged; False otherwise.
        """
        log = self.log 
        unmanaged_logins = self.unmanaged_logins
        subject_match_value = self.get_match_value_from_local_subject(subject, attributes)
        if subject_match_value in unmanaged_logins:
            log.debug(
                "Subject '{subject}' has match value '{match_value}' which is unmanaged.  Skipping ...",
                subject=subject,
                match_value=subject_match_value)
            return True
        return False

    @inlineCallbacks
    def is_api_id_unmanaged(self, api_id):
        """
        Determine if the remote account identified by its API ID is unmanaged.
        """
        log = self.log
        remote_entry = yield self.api_get_remote_account(api_id)
        if remote_entry is None:
            returnValue(False)
        remote_match_value = self.get_match_value_from_remote_account(remote_entry)
        unmanaged_logins = self.unmanaged_logins
        if remote_match_value in unmanaged_logins:
            log.debug(
                "Remote account identified by '{api_id}' has match value '{match_value}' which is unmanaged.  Skipping ...",
                api_id=api_id,
                match_value=remote_match_value)
            returnValue(True)
        returnValue(False)

    def make_web_agent(self, endpoint_s, pool=None):
        """
        Configure a `Twisted.web.client.Agent` to be used to make REST calls.
        """
        if pool is None:
            pool = HTTPConnectionPool(self.reactor)
        agent = Agent.usingEndpointFactory(
            self.reactor,
            WebClientEndpointFactory(self.reactor, endpoint_s),
            pool=pool)
        return (pool, agent)

    def make_web_client(self, endpoint_s, pool=None):
        pool, agent = self.make_web_agent(endpoint_s, pool=pool)
        http_client = treq.client.HTTPClient(agent)
        return (pool, agent, http_client)

    def make_default_web_client(self):
        pool, agent, http_client = self.make_web_client(self.endpoint_s)
        self.pool = pool
        self.agent = agent
        self.http_client = http_client

@inlineCallbacks
def delay(reactor, seconds):
    """
    A Deferred that fires after `seconds` seconds.
    """
    yield task.deferLater(reactor, seconds, lambda : None)

@inlineCallbacks
def delayUntil(reactor, t):
    """
    Delay until time `t`.
    If `t` is None, don't delay.

    `params t`: A datetime object or None
    """
    if t is None:
        returnValue(None)
    instant = datetime.datetime.today()
    if instant < t:
        td = t - instant
        delay_seconds = td.total_seconds()
        yield task.deferLater(reactor, delay_seconds, lambda : None)
    
