
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


@attr.attrs
class ParsedSyncWorkroomMessage(object):
    action = attr.attrib()
    group = attr.attrib()
    subjects = attr.attrib()


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
                self.unmanaged_logins = set(
                    login.lower() 
                        for login in config.get("unmanaged_logins", "").split())
                self.provision_group = config.get("provision_group", None)
                if self.provision_group is not None:
                    self.provision_group = self.provision_group.lower()
                workroom_map_path = config.get("workroom_map", None)
                self.endpoint_s = config.get("endpoint", None)
                self.url_prefix = config["url_prefix"]
                self.api_key = config["api_key"]
                self.cache_size = int(config["cache_size"])
                self.authenticate = config['authenticate']
                self.accounts_query = config['accounts_query']
                if self.provision_group:
                    self.account_update = jinja2.Template(config['account_update'])
                    self.account_delete = jinja2.Template(config['account_delete'])
                    self.account_add = config['account_add']
                    account_template_path = config['account_template']
                    attrib_map_path = config["attribute_map"]
                if workroom_map_path:
                    self.workrooms_query = config['workrooms_query']
                    self.workroom_members = jinja2.Template(config['workroom_members'])
                    self.workroom_subject = jinja2.Template(config['workroom_subject'])
                    self.workroom_cache_size = int(config.get("workroom_cache_size", 100))
                    self.workroom_retry_delay = int(config.get("workroom_retry_delay", 20))
            except KeyError as ex:
                raise OptionMissingError(
                    "A require option was missing: '{0}:{1}'.".format(
                        section, ex.args[0]))
            if self.provision_group is None and workroom_map_path is None:
                raise OptionMissingError(
                    "Must provide at least one of `provision_group` (account "
                    "provisioning) or `workroom_map` (workroom mapping).")
            # Create the web client.
            self.make_web_client()
            self.__workroom_cache = None
            if self.provision_group:
                # Create the attribute map.
                self.make_attribute_map(attrib_map_path)
                # Create the account template.
                self.make_account_template(account_template_path)
            if workroom_map_path:
                # Create the workroom cache.
                self.__workroom_cache = pylru.lrucache(self.workroom_cache_size)
            # Create the workroom map.
            self.make_workroom_map(workroom_map_path)
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

    def make_workroom_map(self, workroom_map_path):
        """
        Create group to workroom mappings from JSON
        file.
        """
        log = self.log
        if workroom_map_path is None:
            log.info("No workroom map.  Permissions will not be mapped.")
            self.workroom_map = {}
            return
        with open(workroom_map_path, "r") as f:
            self.workroom_map = commentjson.load(f)
        log.info("Created group to workroom map.")

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
        workroom_map = self.workroom_map
        try:
            msg = self.parse_message(amqp_message)
            group = msg.group.lower()
            workroom = workroom_map.get(group, None)
            if group == self.provision_group:
                if workroom is not None:
                    log.warn(
                        "Group '{group}' is the account provisioning group AND in the workroom map."
                        "  It will NEVER be used for workroom mapping.",
                        group=group)
                if msg.action in (constants.ACTION_ADD, constants.ACTION_UPDATE):
                    yield self.provision_subject(msg.subject, msg.attributes)
                elif msg.action == constants.ACTION_DELETE:
                    yield self.deprovision_subject(msg.subject)
                elif msg.action == constants.ACTION_MEMBERSHIP_SYNC:
                    yield self.sync_members(msg.subjects, msg.attributes)
                else:
                    raise UnknownActionError(
                        "Don't know how to handle action '{0}' for provisioning.".format(msg.action))
            elif workroom is not None:
                workroom = workroom.lower()
                if msg.action == constants.ACTION_ADD:
                    yield self.add_subject_to_workroom(workroom, msg.subject)    
                elif msg.action == constants.ACTION_DELETE:
                    yield self.remove_subject_from_workroom(workroom, msg.subject)
                elif msg.action == constants.ACTION_MEMBERSHIP_SYNC:
                    yield self.sync_subjects_to_workroom(workroom, msg.subjects)
                else:
                    raise UnknownActionError(
                        "Don't know how to handle action '{0}' for workrooms.".format(msg.action))
            else:
                log.warn(
                    "Not sure what to do with group '{group}'.  Discarding ...",
                    group=group)
        except Exception as ex:
            log.warn("Error provisioning message: {error}", error=ex)
            tb = traceback.format_exc()
            log.debug("{traceback}", traceback=tb)
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
                return ParsedWorkroomMessage(action, group, subject)
            elif action == constants.ACTION_MEMBERSHIP_SYNC:
                subjects = doc["subjects"]
                return ParsedSyncWorkroomMessage(action, group, subjects)
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
        subject_set = set(subject.lower() for subject in subjects)
        doc = yield self.fetch_all_users()
        account_data = doc["data"]
        for entry in account_data:
            active = entry["active"]
            if active == 0:
                continue
            login = entry["login"].lower()
            if login in unmanaged_logins:
                continue
            if not login in subject_set:
                yield self.deprovision_subject(login) 

    @inlineCallbacks
    def provision_subject(self, subject, attributes):
        """
        Provision a subject to Board Effect.
        """
        log = self.log
        log.debug(
            "Attempting to provision subject '{subject}'.",
            subject=subject)
        remote_id = yield self.fetch_account_id(subject)
        if remote_id is not None:
            yield self.update_subject(subject, remote_id, attributes)
        else:
            yield self.add_subject(subject, attributes)
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
                if resp_code == 401:
                    self.__auth_token = None
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
        if response.code == 401:
            self.__auth_token = None
        returnValue(response)

    @inlineCallbacks
    def fetch_all_workrooms(self):
        """
        Load all workrooms from the sevice.
        """
        log = self.log
        log.debug("Attempting to fetch all workroom IDs ...")
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}{1}".format(prefix, self.workrooms_query)
        headers = {
            'Accept': ['application/json'],
        }
        log.debug("URL (GET): {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        try:
            resp = yield self.make_authenticated_api_call("GET", url, headers=headers)
        except Exception as ex:
            log.error("Error attempting to retrieve existing workrooms.")
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
    def fetch_workroom_id(self, workroom):
        """
        Fetch an existing remote workroom ID and return it or None if the remote 
        account does not exist.
        """
        log = self.log
        workroom = workroom.lower()
        log.debug("Attempting to fetch existing workroom.")
        workroom_cache = self.__workroom_cache
        cache_size = self.cache_size
        log.debug("cache max size: {cache_size}", cache_size=cache_size)
        log.debug("cache current size: {cache_size}", cache_size=len(workroom_cache))
        workroom_data = None
        if len(workroom_cache) == 0: 
            # Prefill cache.
            log.debug("Prefilling workroom cache ...")
            doc = yield self.fetch_all_workrooms()
            workroom_data = doc["data"]
            for entry in workroom_data: 
                if len(workroom_cache) >= cache_size:
                    break
                name = entry["name"].lower()
                identifier = entry["id"]
                workroom_cache[name] = identifier 
            log.debug("Cache size after prefill: {cache_size}", cache_size=len(workroom_cache))
        if workroom in workroom_cache:
            workroom_id = workroom_cache[workroom]
            returnValue(workroom_id)
        log.debug("Workroom ID not in cache for '{workroom}.", workroom=workroom)
        if workroom_data is None:
            doc = yield self.fetch_all_workrooms()
            workroom_data = doc["data"]
        for entry in workroom_data:
            log.debug("Looping through entries ...")
            name = entry["name"].lower()
            if name == workroom:
                workroom_id = entry["id"]
                workroom_cache[workroom] = workroom_id
                log.debug(
                    "Added entry to workroom cache: {name}: {identifier}", 
                    name=workroom, 
                    identifier=workroom_id)
                returnValue(workroom_id)
        returnValue(None)
    
    @inlineCallbacks
    def add_subject_to_workroom(self, workroom, subject, workroom_id=None, subject_id=None):
        """
        Add a subject to a workroom.
        """
        log = self.log
        if workroom_id is None:
            workroom_id = yield self.fetch_workroom_id(workroom)
            if workroom_id is None:
                log.warn(
                    "Unable to find workroom ID for '{workroom}'.  Discarding ...",
                    workroom=workroom)
                returnValue(None)
        if subject_id is None:
            subject_id = yield self.fetch_account_id(subject)
            if subject_id is None:
                yield delay(self.reactor, self.workroom_retry_delay) 
                subject_id = yield self.fetch_account_id(subject)
                if subject_id is None:
                    log.warn(
                        "Unable to find remote_id for subject '{subject}'.  Discarding ...",
                        subject=subject)
                    returnValue(None)
        prefix = self.url_prefix
        url = "{0}{1}".format(
            prefix,
            self.workroom_subject.render(
                workroom_id=workroom_id,
                subject_id=subject_id))
        headers = {'Accept': ['application/json']}
        log.debug("url: {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        if not self.diagnostic_mode:
            try:
                resp = yield self.make_authenticated_api_call(
                    'PUT',  
                    url, 
                    headers=headers)
            except Exception as ex:
                log.error(
                    "Error attempting to remove subject '{subject}' from workroom '{workroom}'.",
                    subject=subject,
                    workroom=workroom
                )
                raise
            resp_code = resp.code
            log.debug("Response code: {code}", code=resp_code)
            if resp_code == 200:
                yield resp.content()
                returnValue(None)
            try:
                doc = yield resp.json()
                error = doc["error"]["message"]
            except Exception as ex:
                raise Exception(
                    "Unknown error prevented adding subject '{0}' to workroom '{1}'.".format(
                    subject,
                    workroom))
            raise Exception(
                "Error adding subject '{0}' to workroom '{1}': {2}".format(
                subject,
                workroom,
                error))

    @inlineCallbacks
    def remove_subject_from_workroom(self, workroom, subject, workroom_id=None, subject_id=None):
        """
        Remove a subject from a workroom.
        """
        log = self.log
        if workroom_id is None:
            workroom_id = yield self.fetch_workroom_id(workroom)
            if workroom_id is None:
                log.warn(
                    "Unable to find workroom ID for '{workroom}'.  Discarding ...",
                    workroom=workroom)
                returnValue(None)
        if subject_id is None:
            subject_id = yield self.fetch_account_id(subject)
            if subject_id is None:
                log.warn(
                    "Unable to find remote_id for subject '{subject}'.  Discarding ...",
                    subject=subject)
                returnValue(None)
        prefix = self.url_prefix
        url = "{0}{1}".format(
            prefix,
            self.workroom_subject.render(
                workroom_id=workroom_id,
                subject_id=subject_id))
        headers = {'Accept': ['application/json']}
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
                    "Error attempting to remove subject '{subject}' from workroom '{workroom}'.",
                    subject=subject,
                    workroom=workroom
                )
                raise
            resp_code = resp.code
            log.debug("Response code: {code}", code=resp_code)
            if resp_code in (204, 404):
                yield resp.content()
                returnValue(None)
            try:
                doc = yield resp.json()
                error = doc["error"]["message"]
            except Exception as ex:
                raise Exception(
                    "Unknown error prevented removing subject '{0}' from workroom '{1}'.".format(
                    subject,
                    workroom))
            raise Exception(
                "Error removing subject '{0}' from workroom '{1}': {2}".format(
                subject,
                workroom,
                error))

    @inlineCallbacks
    def sync_subjects_to_workroom(self, workroom, subjects):
        """
        Sync workroom membership.
        """
        log = self.log
        workroom_id = yield self.fetch_workroom_id(workroom)
        if workroom_id is None:
            log.warn(
                "Unable to find workroom ID for '{workroom}'.  Discarding ...",
                workroom=workroom)
            returnValue(None)
        subject_ids = []
        for subject in subjects:
            subject_id = yield self.fetch_account_id(subject)
            if subject_id is None:
                log.warn(
                    "Could not find remote ID for subject '{subject}'."
                    "  Ignoring for sync to workroom.",
                    subject=subject)
                continue
            subject_ids.append((subject, subject_id))
        for subject, subject_id in subject_ids:
            yield self.add_subject_to_workroom(
                workroom,
                subject,
                workroom_id=workroom_id,
                subject_id=subject_id)
        subject_id_set = set(identifier for junk, identifier in subject_ids)
        actual_subjects = yield self.get_subjects_for_workroom(workroom_id)
        for login, remote_id in actual_subjects:
            if not remote_id in subject_id_set:
                yield self.remove_subject_from_workroom(
                    workroom,
                    login,
                    workroom_id=workroom_id,
                    subject_id=remote_id)

    @inlineCallbacks
    def get_subjects_for_workroom(self, workroom_id):
        """
        Retireve a list of (logins, subject_ids) that belong to a workroom identified
        by workroom_id.
        """
        log = self.log
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}{1}".format(
            prefix, 
            self.workroom_members.render(workroom_id=workroom_id))
        headers = {
            'Accept': ['application/json'],
        }
        log.debug("URL (GET): {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        try:
            resp = yield self.make_authenticated_api_call("GET", url, headers=headers)
        except Exception as ex:
            log.error("Error attempting to retrieve existing workroom members.")
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
        data = doc["data"]
        subjects = []
        for entry in data:
            login = entry["login"].lower()
            remote_id = entry["id"]
            subjects.append((login, remote_id))
        returnValue(subjects)

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
        params={'include_inactive': 'true'}
        try:
            resp = yield self.make_authenticated_api_call("GET", url, headers=headers, params=params)
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
    def fetch_account_id(self, subject):
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
        if subject in account_cache:
            remote_id = account_cache[subject]
            returnValue(remote_id)
        log.debug("Account ID not in cache for '{subject}'.", subject=subject)
        if account_data is None:
            doc = yield self.fetch_all_users()
            account_data = doc["data"]
        for entry in account_data:
            log.debug("Looping through entries ...")
            login = entry["login"].lower()
            if login == subject:
                remote_id = entry["id"]
                account_cache[subject] = remote_id
                log.debug("Added entry to cache: {login}: {identifier}", login=login, identifier=remote_id)
                returnValue(remote_id)
        returnValue(None)

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
    def add_subject(self, subject, attributes):
        """
        Add a remote service account.
        """
        log = self.log
        log.debug("Entered add_subject().")
        log.debug("Adding a new account ...")
        props = self.map_attributes(attributes, subject, constants.ACTION_ADD)
        props['active'] = '1'
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
            remote_id = doc["data"]["id"]
            self.__account_cache[subject.lower()] = remote_id

    @inlineCallbacks
    def deprovision_subject(self, subject):
        """
        Deprovision a subject from the remote service.
        """
        log = self.log
        log.debug("Entered deprovision_subject().")
        subject = subject.lower()
        log.debug(
            "Attempting to deprovision subject '{subject}'.",
            subject=subject)
        remote_id = yield self.fetch_account_id(subject)
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


@inlineCallbacks
def delay(reactor, seconds):
    """
    A Deferred that fires after `seconds` seconds.
    """
    yield task.deferLater(reactor, seconds, lambda : None)

