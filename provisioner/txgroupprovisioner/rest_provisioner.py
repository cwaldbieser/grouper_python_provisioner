
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
        provisioner = self.provsioner_factory()
        return provisioner


class RESTProvisioner(object):
    implements(IProvisioner)
    service_state = None
    reactor = None
    log = None

    def get_match_value_from_remote_account(self, remote_account):
        """
        Given a remote account, `remote_account`, extract the
        value that will be used to match the remote account 
        to the local subject.
        """
        raise NotImplementedError()

    def get_api_id_from_remote_account(self, remote_account):
        """
        Given a remote account, `remote_account`, extract the
        value that is used as an account identifier in API
        calls that reference the account.
        """
        raise NotImplementedError()

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
                self.provision_group = config.get("provision_group", None)
                if self.provision_group is not None:
                    self.provision_group = self.provision_group.lower()
                target_group_map_path = config.get("target_group_map", None)
                self.endpoint_s = config.get("endpoint", None)
                self.url_prefix = config["url_prefix"]
                self.client_secret = config["client_secret"]
                self.cache_size = int(config["cache_size"])
                self.accounts_query = config['accounts_query']
                self.max_page = int(config.get('max_page', 100))
                self.local_computed_match_template = jinja2.Template(config['local_computed_match_template'])
                if self.provision_group:
                    self.account_update = jinja2.Template(config['account_update'])
                    self.account_delete = jinja2.Template(config['account_delete'])
                    self.account_add = config['account_add']
                    account_template_path = config['account_template']
                    attrib_map_path = config["attribute_map"]
                if target_group_map_path:
                    self.target_groups_query = config['target_groups_query']
                    self.target_group_members = jinja2.Template(config['target_group_members'])
                    self.target_group_subject = jinja2.Template(config['target_group_subject'])
                    self.target_group_cache_size = int(config.get("target_group_cache_size", 100))
                    self.target_group_retry_delay = int(config.get("target_group_retry_delay", 20))
            except KeyError as ex:
                raise OptionMissingError(
                    "A require option was missing: '{0}:{1}'.".format(
                        section, ex.args[0]))
            if self.provision_group is None and target_group_map_path is None:
                raise OptionMissingError(
                    "Must provide at least one of `provision_group` (account "
                    "provisioning) or `target_group_map` (target_group mapping).")
            # Create the web client.
            self.make_web_client()
            self.__target_group_cache = None
            if self.provision_group:
                # Create the attribute map.
                self.make_attribute_map(attrib_map_path)
                # Create the account template.
                self.make_account_template(account_template_path)
            if target_group_map_path:
                # Create the target_group cache.
                self.__target_group_cache = pylru.lrucache(self.target_group_cache_size)
            # Create the target_group map.
            self.make_target_group_map(target_group_map_path)
            # Create account cache.
            self.__account_cache = pylru.lrucache(self.cache_size)
            # Initialize access token.
            self.auth_token = None
        except Exception as ex:
            d = self.reactor.callLater(0, self.reactor.stop)
            log.failure("Provisioner failed to initialize: {0}".format(ex))
            raise
        self.parse_config(scp)
        return defer.succeed(None)

    def parse_config(self, scp):
        """
        Parse any additional configuration this provisioner might need.
        """
        pass

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
            url_prefix = https://lafayette.boardeffect.com/api/v3
            cache_size = 1000
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
                if action  != constants.ACTION_DELETE:
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
    def sync_members(self, subjects, attrib_map):
        """
        Sync all subects to Board Effect accounts.
        (Except non-SSO accounts).
        """
        log = self.log
        unmanaged_logins = self.unmanaged_logins
        for subject in subjects:
            subject = subject.lower()
            if subject in unmanaged_logins:
                continue
            attributes = attrib_map[subject]
            yield self.provision_subject(subject, attributes)
        local_computed_match_template = self.local_computed_match_template
        local_match_set = set([])
        for subject in subjects:
            local_match = local_computed_match_template.render(
                subject=subject,
                attributes=attrib_map[subject])
            local_match_set.add(local_match)
        remote_ids = yield self.get_all_subject_remote_ids()
        for remote_id, match_value in remote_ids:
            if match_value in unmanaged_logins:
                continue
            if not match_value in local_match_set:
                yield self.deprovision_subject(None, None, remote_id=remote_id) 

    @inlineCallbacks
    def provision_subject(self, subject, attributes):
        """
        Provision a subject to Board Effect.
        """
        log = self.log
        log.debug(
            "Attempting to provision subject '{subject}'.",
            subject=subject)
        remote_id = yield self.fetch_account_id(subject, attributes)
        if remote_id is not None:
            yield self.update_subject(subject, remote_id, attributes)
        else:
            yield self.add_subject(subject, attributes)
        returnValue(None)

    @inlineCallbacks
    def fetch_auth_token(self):
        """
        Make API call to obtain valid auth token.
        """
        if False:
            yield None
        raise NotImplementedError()

    def check_401_response(self, response):
        """
        Check if an API response is 401 - Unauthorized.
        If so, raise an exception.
        """
        log = self.log
        if response.code == 401:
            self.auth_token = None
            content = yield response.content()
            raise Exception(
                "Could not obtain auth token.  Response ({code}) was:\n{content}".format(
                    code=resp_code,
                    content=content))

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
    def make_authenticate_api_call(self, method, url, **http_options):
        """
        Given the *unauthenticated* of an HTTP client request, make an
        authenticated request.
        """
        log = self.log
        log.debug("Making authenticated API call ...")
        http_client = self.http_client
        method = method.lower()
        new_method, new_url, new_http_options = yield self.authorize_api_call(
            method, url, **http_options)
        log.debug(
            "Making API call.  method: {method}, URL: {url}", 
            method=new_method, 
            url=new_url)
        response = yield getattr(http_client, new_method)(new_url, **new_http_options)
        log.debug("API call complete.  Response code: {code}", code=response.code)
        if response.code == 401:
            log.debug("Got unauthorized response.  Will reauthorize and retry.")
            self.auth_token = None
            new_method, new_url, new_http_options = yield self.authorize_api_call(
                method, url, **http_options)
            response = yield getattr(http_client, new_method)(new_url, **new_http_options)
            log.debug("API call complete.  Response code: {code}", code=response.code)
        returnValue(response)

    @inlineCallbacks
    def fetch_all_target_groups(self):
        """
        Load all target_groups from the sevice.
        Must return an iterable that yields tuples of
        (local_group_id, remote_group_id).
        """
        if False:
            yield None
        raise NotImplementedError()
    
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
        cache_size = self.cache_size
        log.debug("cache max size: {cache_size}", cache_size=cache_size)
        log.debug("cache current size: {cache_size}", cache_size=len(target_group_cache))
        all_groups = None
        if len(target_group_cache) == 0: 
            # Prefill cache.
            log.debug("Prefilling target_group cache ...")
            all_groups = yield self.fetch_all_target_groups()
            for local_id, remote_id in all_groups: 
                if len(target_group_cache) >= cache_size:
                    break
                local_id = local_id.lower()
                target_group_cache[local_id] = remote_id
            log.debug("Cache size after prefill: {cache_size}", cache_size=len(target_group_cache))
        if target_group in target_group_cache:
            target_group_id = target_group_cache[target_group]
            returnValue(target_group_id)
        log.debug("Remote ID not in cache for '{target_group}.", target_group=target_group)
        if all_groups is None:
            all_groups = yield self.fetch_all_target_groups()
        for local_id, remote_id in all_groups:
            log.debug("Looping through entries ...")
            local_id = local_id.lower()
            if local_id == target_group:
                target_group_id = remote_id
                target_group_cache[target_group] = target_group_id
                log.debug(
                    "Added entry to target_group cache: {name}: {identifier}", 
                    name=target_group, 
                    identifier=target_group_id)
                returnValue(target_group_id)
        returnValue(None)
    
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
    def add_subject_to_target_group(self, target_group, subject, attributes, target_group_id=None, subject_id=None):
        """
        Add a subject to a target_group.
        """
        log = self.log
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
                        "Unable to find remote_id for subject '{subject}'.  Discarding ...",
                        subject=subject)
                    returnValue(None)
        yield self.api_add_subject_to_group(subject_id, target_group_id)

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
    def remove_subject_from_target_group(self, target_group, subject, attributes, target_group_id=None, subject_id=None):
        """
        Remove a subject from a target_group.
        """
        log = self.log
        assert (subject is not None) or (subject_id is not None), "Must provide `subject` or `subject_id`!"
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
                    "Unable to find remote_id for subject '{subject}'.  Discarding ...",
                    subject=subject)
                returnValue(None)
        yield self.api_remove_subject_from_group(subject_id, target_group_id)

    @inlineCallbacks
    def sync_subjects_to_target_group(self, target_group, subjects, attributes):
        """
        Sync target_group membership.
        """
        log = self.log
        target_group_id = yield self.fetch_target_group_id(target_group)
        if target_group_id is None:
            log.warn(
                "Unable to find target_group ID for '{target_group}'.  Discarding ...",
                target_group=target_group)
            returnValue(None)
        subject_ids = []
        for subject in subjects:
            subj_attribs = None
            if attributes is not None:
                subj_attribs = attributes.get(subject, None)
            subject_id = yield self.fetch_account_id(subject, subj_attribs)
            if subject_id is None:
                log.warn(
                    "Could not find remote ID for subject '{subject}'."
                    "  Ignoring for sync to target_group.",
                    subject=subject)
                continue
            subject_ids.append((subject, subject_id))
        log.debug(
            "Adding {count} subjects to target_group '{target_group}' ...",
            count=len(subject_ids),
            target_group=target_group)
        for subject, subject_id in subject_ids:
            subj_attribs = None
            if attributes is not None:
                subj_attribs = attributes.get(subject, None)
            yield self.add_subject_to_target_group(
                target_group,
                subject,
                subj_attribs,
                target_group_id=target_group_id,
                subject_id=subject_id)
        subject_id_set = set(identifier for junk, identifier in subject_ids)
        actual_subject_ids = yield self.get_subjects_for_target_group(target_group_id)
        for remote_id in actual_subject_ids:
            if not remote_id in subject_id_set:
                log.debug(
                    "Looking up subject for remote_id '{remote_id}' ...",
                    remote_id=remote_id)
                yield self.remove_subject_from_target_group(
                    target_group,
                    subject=None,
                    attributes=None,
                    target_group_id=target_group_id,
                    subject_id=remote_id)

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
    def get_all_subject_remote_ids(self):
        """
        Load all the remote subject IDs and match values from the 
        user accounts that exist on the remote sevice.
        """
        if False:
            yield None
        raise NotImplementedError()

    @inlineCallbacks
    def fetch_account_id(self, subject, attributes):
        """
        Fetch an existing remote account ID and return it or None if the remote 
        account does not exist.
        """
        log = self.log
        log.debug("Attempting to fetch existing account.")
        local_computed_match = self.local_computed_match_template.render(
            subject=subject,
            attributes=attributes)
        account_cache = self.__account_cache
        cache_size = self.cache_size
        log.debug("cache max size: {cache_size}", cache_size=cache_size)
        log.debug("cache current size: {cache_size}", cache_size=len(account_cache))
        account_data = None
        if subject in account_cache:
            remote_id = account_cache[subject]
            returnValue(remote_id)
        log.debug("Account ID not in cache for '{subject}'.", subject=subject)
        remote_id = yield self.api_get_account_id(self, subject, attributes)
        if remote_id is not None:
            account_cache[subject] = remote_id
        returnValue(remote_id)

    @inlineCallbacks
    def update_subject(self, subject, remote_id, attributes):
        """
        Update a remote account.
        """
        log = self.log
        log.debug("Entered update_subject().")
        log.debug("Updating subject '{subject}'", subject=subject)
        props = self.map_attributes(attributes, subject, constants.ACTION_UPDATE)
        props['active'] = '1'
        props['preferred_contact_address'] = 'Company'
        prefix = self.url_prefix
        url = "{0}{1}".format(
            prefix,
            self.account_update.render(
                remote_id=remote_id,
                subject=subject,
                attributes=props))
        headers = {'Accept': ['application/json']}
        log.debug("url: {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        log.debug("data: {props}", props=props)
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
    def add_subject(self, subject, attributes):
        """
        Add a remote service account.
        """
        log = self.log
        log.debug("Entered add_subject().")
        log.debug("Adding a new account ...")
        props = self.map_attributes(attributes, subject, constants.ACTION_ADD)
        props['active'] = '1'
        props['preferred_contact_address'] = 'Company'
        account_doc = self.account_template.render(
            props=props,
            subject=subject)
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
        entry = doc["data"]
        remote_id = self.get_api_id_from_remote_account(entry)
        self.__account_cache[subject.lower()] = remote_id

    @inlineCallbacks
    def api_deprovision_subject(self, remote_id):
        """
        Make the API call require to deprovision the subject identified by
        `remote_id`.
        """
        if False:
            yield None
        raise NotImplementedError()

    @inlineCallbacks
    def deprovision_subject(self, subject, attributes, remote_id=None):
        """
        Deprovision a subject from the remote service.
        """
        log = self.log
        log.debug("Entered deprovision_subject().")
        assert (subject is not None) or (remote_id is not None), (
            "Must provide `subject` or `remote_id`!")
        subject_identifier = subject or remote_id
        log.debug(
            "Attempting to deprovision subject identified by '{identifier}'.",
            identifier=subject_identifier)
        if remote_id is None:
            subject = subject.lower()
            remote_id = yield self.fetch_account_id(subject, attributes)
        if remote_id is None:
            log.debug("Account '{subject}' does not exist on the remote service.",
                subject=subject)
            returnValue(None)
        yield self.api_deprovision_subject(remote_id)
        account_cache = self.__account_cache
        if not subject is None:
            if subject in account_cache:
                del account_cache[subject]
        else:
            for subject, r_id in account_cache.items():
                if remote_id == r_id:
                    del account_cache[subject]
                    break

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


@inlineCallbacks
def delay(reactor, seconds):
    """
    A Deferred that fires after `seconds` seconds.
    """
    yield task.deferLater(reactor, seconds, lambda : None)

