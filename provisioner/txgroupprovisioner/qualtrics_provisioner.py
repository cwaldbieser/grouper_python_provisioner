
from __future__ import print_function
import itertools
import json
import random
import string
import urlparse
import commentjson
from rest_provisioner import (
    APIResponseError,
    OptionMissingError,
    RESTProvisioner,
    RESTProvisionerFactory, 
    StringProducer,
)
from twisted.internet.defer import (
    inlineCallbacks, 
    returnValue,
)

def generate_password():
    """
    Generate a random password.
    """
    caps = [random.choice(string.ascii_uppercase) for n in range(2)]
    lowers = [random.choice(string.ascii_lowercase) for n in range(2)]
    punct = [random.choice(string.punctuation)]
    digit = [random.choice(string.digits)]
    others = [random.choice(string.ascii_letters + string.punctuation + string.digits) for n in range(9)]
    concat = list(itertools.chain(caps, lowers, punct, digit, others))
    random.shuffle(concat)
    return ''.join(concat)


class QualtricsProvisioner(RESTProvisioner):
    """
    Definitions:

    * match_value: Can be computed on either the local or remote side.
      On the local side, the subject and attributes can be used to compute it.
      On the remote side, any of the remote account attributes can be used to
      compute it.
    * local_id: The local ID used to refer to the subject.  The subject value
      received in the message to the provisioner.
    * api_id: The identifier used by the REST API to refer to a remote account.
    * unmanaged_id: Some remote accounts are specific to the remote service and
      should not be managed by the provisioner (e.g. a back door admin account).
      These accounts are identified by their match_values.
    """
    organization = None
    new_user_type = None
    language = 'en'
    
    def get_match_value_from_remote_account(self, remote_account):
        """
        Given a remote account, `remote_account`, extract the
        value that will be used to match the remote account 
        to the local subject.
        Returns None if a match value cannot be constructed for the remote
        account.
        """
        log = self.log
        organization = self.organization
        match_value = remote_account.get("username", None)
        if match_value is not None:
            match_value = match_value.lower()
            if not match_value.endswith("#{0}".format(organization.lower())):
                match_value = None
        return match_value

    def get_match_value_from_local_subject(self, subject, attributes):
        """
        Given a local subject and attributes, compute the value that
        will be used to match the remote account to the local subject.
        """
        organization = self.organization
        return "{0}#{1}".format(subject, organization)

    def get_api_id_from_remote_account(self, remote_account):
        """
        Given a remote account, `remote_account`, extract the
        value that is used as an account identifier in API
        calls that reference the account.
        """
        return remote_account.get("id", None)

    def parse_config(self, scp):
        """
        Parse any additional configuration this provisioner might need.
        """
        log = self.log
        config = self.config
        organization = config.get("organization", None)
        if organization is None:
            raise OptionMissingError(
                "The `organization` option is missing!") 
        self.organization = organization
        new_user_type = config.get("new_user_type", None)
        if new_user_type is None:
            raise OptionMissingError(
                "The `new_user_type` option is missing!")
        self.new_user_type = new_user_type
        language = config.get("language", 'en')
        self.language = language

    @inlineCallbacks
    def api_get_auth_token(self):
        """
        Make API call to obtain valid auth token.
        Should set `self.auth_token`.
        """
        log = self.log
        if False:
            yield None
        self.auth_token = self.client_secret
        returnValue(None)

    @inlineCallbacks
    def authorize_api_call(self, method, url, **http_options):
        """
        Given the components of an *unauthenticated* HTTP client request, 
        return the components of an authenticated request.

        Should return a tuple of (method, url, http_options)
        """
        log = self.log
        log.debug("Authorizing API call ...")
        if False:
            yield "Required for inlineCallbacks-- can't wait for async/await!"
        auth_token = self.auth_token
        headers = http_options.setdefault("headers", {})
        headers["X-API-TOKEN"] = [auth_token]
        returnValue((method, url, http_options))

    @inlineCallbacks
    def get_all_api_ids_and_match_values(self):
        """
        Load all the remote subject IDs and match values from the 
        user accounts that exist on the remote sevice.
        Note: If a match value cannot be constructed for a remote
        account, it will not be included in the output of this function.
        """
        log = self.log
        log.debug("Attempting to fetch local IDs from all remote user accounts ...")
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}/users".format(prefix)
        headers = {
            'Accept': ['application/json'],
        }
        identifiers = []
        while True:
            log.debug("URL (GET): {url}", url=url)
            log.debug("headers: {headers}", headers=headers)
            try:
                resp = yield self.make_authenticated_api_call(
                    "GET",
                    url,
                    headers=headers)
            except Exception as ex:
                log.error("Error fetching all remote user data.")
                raise
            parsed = yield resp.json()
            meta = parsed["meta"]
            http_status = meta["httpStatus"]
            if http_status != "200 - OK":
                raise Exception("Error making API call to fetch all users: {}".format(http_status))
            result = parsed['result']
            elements = result['elements']
            next_page = result.get('nextPage', None)
            for entry in elements:
                api_id = self.get_api_id_from_remote_account(entry)
                match_value = self.get_match_value_from_remote_account(entry)
                if not match_value is None:
                    identifiers.append((api_id, match_value))
            if next_page is not None:
                url = next_page
            else:
                break
        returnValue(identifiers)

    @inlineCallbacks
    def api_get_remote_account(self, api_id):
        """
        Get the remote account information using its API ID.
        """
        log = self.log
        log.debug("Attempting to fetch remote account ...")
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}/users/{1}".format(prefix, api_id)
        headers = {
            'Accept': ['application/json'],
        }
        identifiers = []
        log.debug("URL (GET): {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        resp = yield self.make_authenticated_api_call(
            "GET",
            url,
            headers=headers)
        remote_account = yield resp.json()
        returnValue(remote_account)

    @inlineCallbacks
    def api_deprovision_subject(self, api_id):
        """
        Make the API call require to deprovision the subject identified by
        `api_id`.
        """
        log = self.log
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}/users/{1}".format(prefix, api_id)
        headers = {
            'Accept': ['application/json'],
            'Content-Type': ['application/json'],
        }
        props = {
            'status': 'disabled',
        }
        serialized = json.dumps(props)
        body = StringProducer(serialized.encode('utf-8'))
        log.debug("url: {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        log.debug("body: {body}", body=serialized)
        resp = yield self.make_authenticated_api_call(
            "PUT",
            url,
            headers=headers,
            data=body)
        resp_code = resp.code
        if resp_code == 400:
            #Invalidate cache
            self.invalidate_cached_subject_api(api_id)
            raise Exception("api_deprovision_subject: Client error using API ID '{}'.  Cached subject API ID has been invalidated.".format(api_id))
        content = "" 
        try:
            content = yield resp.content()
        except Exception as ex:
            pass
        if resp_code != 200:
            content = '\n{}'.format(content)
            raise Exception("API call to deprovision subject returned HTTP status {}.{}".format(resp_code, content))
        returnValue(None)

    @inlineCallbacks
    def api_get_account_id(self, subject, attributes):
        """
        Fetch the remote ID for a subject.
        Return None if the account does not exist on the remote end.
        """
        log = self.log
        cached_result = self.get_subject_api_id_from_cache(subject)
        if not cached_result is None:
            returnValue(cached_result)
        organization = self.organization
        offset = len(organization) + 1
        offset = offset * -1
        computed_match_value = self.get_match_value_from_local_subject(subject, attributes)
        results = yield self.get_all_api_ids_and_match_values()
        log.debug("computed_match_value={computed_match_value}", computed_match_value=computed_match_value)
        log.debug("len(results) == {size}", size=len(results))
        account_map = {}
        for api_id, match_value in results:
            if match_value.lower().endswith("#{}".format(organization)):
                subject = match_value[:offset]
                account_map[subject] = api_id
        self.fill_account_cache(account_map)
        del account_map
        for api_id, match_value in results:
            if computed_match_value == match_value:
                log.debug("Match value found.")
                returnValue(api_id)
        log.debug("No match found.")
        returnValue(None)

    @inlineCallbacks
    def api_update_subject(self, subject, api_id, attributes):
        """
        Make API request to update remote account.
        Returns the HTTP response.
        """
        log = self.log
        prefix = self.url_prefix
        url = "{0}/users/{1}".format(prefix, api_id)
        headers = {
            'Accept': ['application/json'],
            'Content-Type': ['application/json'],
        }
        organization = self.organization
        username = "{}#{}".format(subject.lower(), organization.lower())
        surname = attributes.get("sn", [""])[0]
        givenname = attributes.get("givenName", [""])[0]
        email = attributes.get("mail", [""])[0]
        props = {
            'firstName': givenname,
            'lastName': surname,
            'email': email,
            'username': username,
            'status': 'active',
        }
        serialized = json.dumps(props)
        body = StringProducer(serialized.encode('utf-8'))
        log.debug("url: {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        log.debug("body: {body}", body=serialized)
        resp = yield self.make_authenticated_api_call(
            'PUT',  
            url, 
            data=body, 
            headers=headers)
        returnValue(resp)

    @inlineCallbacks
    def api_add_subject(self, subject, attributes):
        """
        Use the API to add subjects.
        
        Returns the API ID of the newly created remote account or None.
        If None is returned, the API ID will not be cached and require
        a lookup on future use.
        """
        log = self.log
        log.debug("Entered: api_add_subject().")
        prefix = self.url_prefix
        url = "{0}/users".format(prefix)
        headers = {
            'Accept': ['application/json'],
            'Content-Type': ['application/json'],
        }
        organization = self.organization
        username = "{}#{}".format(subject.lower(), organization.lower())
        surname = attributes.get("sn", [""])[0]
        givenname = attributes.get("givenName", [""])[0]
        email = attributes.get("mail", [""])[0]
        props = {
            'username': username,
            'password': generate_password(),
            'firstName': givenname,
            'lastName': surname,
            'userType': self.new_user_type,
            'email': email,
            'language': self.language,
        }
        serialized = json.dumps(props)
        body = StringProducer(serialized.encode('utf-8'))
        log.debug("url: {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        log.debug("body: {body}", body=serialized)
        resp = yield self.make_authenticated_api_call(
            'POST',  
            url, 
            data=body, 
            headers=headers)
        resp_code = resp.code
        log.debug("Add-subject API response code: {code}", code=resp_code)
        if resp_code != 200:
            content = yield resp.content()
            log.error(
                "API response {code}: {content}", 
                code=resp_code,
                content=content)
            raise Exception("API returned status {0}".format(resp_code))
        else:
            parsed = yield resp.json()
            if not "result" in parsed:
                raise Exception("Error in `api_add_subject()`: {}".format(
                    json.dumps(parsed))) 
            result = parsed['result']
            if not "id" in result:
                raise Exception("Error in `api_add_subject()`: {}".format(
                    json.dumps(parsed))) 
            api_id = result["id"]
            returnValue(api_id)

    @inlineCallbacks
    def api_add_subject_to_group(self, subject_id, target_group_id):
        """
        Make an authenticated API call to add the remote subject ID
        to the remote group ID.
        Should raise on error on failure.
        """
        log = self.log
        log.debug("Entered: api_add_subject_to_group().")
        if False:
            yield "Can't wait for async/await!"
        raise NotImplementedError()

    @inlineCallbacks
    def api_remove_subject_from_group(self, subject_id, target_group_id):
        """
        Make an authenticated API call to add the remote subject ID
        to the remote group ID.
        Should raise on error on failure.
        """
        log = self.log
        log.debug("Entered: api_add_subject_to_group().")
        if False:
            yield "Can't wait for async/await!"
        raise NotImplementedError()

    @inlineCallbacks
    def api_get_all_target_groups(self):
        """
        Load all target_groups from the sevice.
        Must return an iterable that yields tuples of
        (local_group_id, remote_group_id).
        """
        if False:
            yield "Can't wait for async/await!"
        log = self.log  
        raise NotImplementedError()

    @inlineCallbacks
    def get_subjects_for_target_group(self, target_group_id):
        """
        Retireve a list of remote subject IDs that belong to a target_group identified
        by remote target_group_id.
        """
        if False:
            yield "Can't wait for async/await!"
        log = self.log  
        raise NotImplementedError()


class QualtricsProvisionerFactory(RESTProvisionerFactory):
    tag = "qualtrics_provisioner"
    opt_help = "Qualtrics REST API Provisioner"
    opt_usage = "This plugin does not support any options."
    provisioner_factory = QualtricsProvisioner


