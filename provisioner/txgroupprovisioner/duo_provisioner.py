
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
import base64
import email
import hmac
import hashlib
from six.moves.urllib.parse import quote as quote_url
from six.moves.urllib.parse import (
    parse_qs,
    urlparse
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

def sign(method, host, path, params, skey, ikey):
    """
    Return HTTP Basic Authentication ("Authorization" and "Date") headers.
    method, host, path: strings from request
    params: dict of request parameters
    skey: secret key
    ikey: integration key
    """
    # create canonical string
    now = email.Utils.formatdate()
    canon = [now, method.upper(), host.lower(), path]
    args = []
    for key in sorted(params.keys()):
        val = params[key]
        if isinstance(val, unicode):
            val = val.encode("utf-8")
        args.append(
            '%s=%s' % (quote_url(key, '~'), quote_url(val, '~')))
    canon.append('&'.join(args))
    canon = '\n'.join(canon)
    # sign canonical string
    sig = hmac.new(skey, canon, hashlib.sha1)
    auth = '%s:%s' % (ikey, sig.hexdigest())
    # return headers
    return {'Date': now, 'Authorization': 'Basic %s' % base64.b64encode(auth)}


class DuoSecurityProvisioner(RESTProvisioner):
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
    http_authn_client = None
    
    def get_match_value_from_remote_account(self, remote_account):
        """
        Given a remote account, `remote_account`, extract the
        value that will be used to match the remote account 
        to the local subject.
        Returns None if a match value cannot be constructed for the remote
        account.
        """
        log = self.log
        match_value = remote_account.get("username", None)
        return match_value

    def get_match_value_from_local_subject(self, subject, attributes):
        """
        Given a local subject and attributes, compute the value that
        will be used to match the remote account to the local subject.
        """
        return subject.lower()

    def get_api_id_from_remote_account(self, remote_account):
        """
        Given a remote account, `remote_account`, extract the
        value that is used as an account identifier in API
        calls that reference the account.
        """
        return remote_account.get("user_id", None)

    def parse_config(self, scp):
        """
        Parse any additional configuration this provisioner might need.
        """
        log = self.log
        config = self.config
        client_id = config.get("client_id", None)
        if client_id is None:
            raise OptionMissingError(
                "The `client_id` option is missing!") 
        self.client_id = client_id

    @inlineCallbacks
    def api_get_auth_token(self):
        """
        Make API call to obtain valid auth token.
        Should set `self.auth_token`.
        """
        log = self.log
        if False:
            yield None
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
        client_id = self.client_id
        client_secret = self.client_secret
        p = urlparse(url)
        netloc = p.netloc
        host = netloc.split(':')[0]
        path = p.path
        params = parse_qs(p.query) 
        params = dict([(k, v[0]) for k, v in params.items()])
        other_params = http_options.setdefault("params", {})
        params.update(other_params)
        data = http_options.get('data', {})
        params.update(data)
        auth_headers = sign(method, host, path, params, client_secret, client_id)
        headers.update(auth_headers)
        returnValue((method, url, http_options))

    @inlineCallbacks
    def get_all_api_ids_and_match_values(self):
        """
        Load all the remote subject IDs and match values from the 
        user accounts that exist on the remote sevice.
        Note: If a match value cannot be constructed for a remote
        account, it will not be included in the output of this function.

        Return value should be a list of (API ID, match value) tuples.
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
        value = parsed["response"]
        for entry in value:
            api_id = self.get_api_id_from_remote_account(entry)
            match_value = self.get_match_value_from_remote_account(entry)
            if not match_value is None:
                identifiers.append((api_id, match_value))
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
        if False:
            yield None
        raise NotImplementedError()

    @inlineCallbacks
    def api_get_account_id(self, subject, attributes):
        """
        Fetch the remote ID for a subject.
        Return None if the account does not exist on the remote end.
        """
        log = self.log
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{}/users".format(prefix)
        headers = {
            'Accept': ['application/json'],
        }
        params = {
            'username': subject.lower()
        }
        resp = yield self.make_authenticated_api_call(
            "GET",
            url,
            headers=headers,
            params=params)
        resp_code = resp.code
        parsed = yield resp.json()
        if resp_code not in (200, ):
            raise Exception("API call `api_get_account_id()` returned HTTP status {0}".format(resp_code))
        api_id = None
        if "response" in parsed:
            response = parsed['response']
            if len(response) != 1:
                returnValue(None) 
            account = response[0]
            api_id = account["user_id"]
        returnValue(api_id)

    @inlineCallbacks
    def api_update_subject(self, subject, api_id, attributes):
        """
        Make API request to update remote account.
        Returns the HTTP response.
        """
        log = self.log
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
        log = self.log
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
        log = self.log
        log.debug("Entered: api_add_subject_to_group().")
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{}/users/{}/groups".format(prefix, subject_id)
        headers = {
            'Accept': ['application/json'],
        }
        data = {
            'group_id': target_group_id
        }
        resp = yield self.make_authenticated_api_call(
            "GET",
            url,
            headers=headers)
        resp_code = resp.code
        parsed = yield resp.json()
        if resp_code not in (200, ):
            raise Exception("API call `api_add_subject_to_group()`, fetching memberships returned HTTP status {0}".format(resp_code))
        groups = parsed['response']
        for ginfo in groups:
            gid = ginfo['group_id']
            if gid == target_group_id:
                log.debug(
                    "Subject ID '{subject_id}' is already a member of group ID '{group_id}'.",
                    subject_id=subject_id,
                    group_id=target_group_id)
                returnValue(None)
        resp = yield self.make_authenticated_api_call(
            "POST",
            url,
            headers=headers,
            data=data)
        resp_code = resp.code
        parsed = yield resp.json()
        if resp_code not in (200, ):
            raise Exception("API call `api_add_subject_to_group()` returned HTTP status {0}".format(resp_code))

    @inlineCallbacks
    def api_remove_subject_from_group(self, subject_id, target_group_id):
        """
        Make an authenticated API call to add the remote subject ID
        to the remote group ID.
        Should raise on error on failure.
        """
        log = self.log
        log.debug("Entered: api_remove_subject_from_group().")
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{}/users/{}/groups/{}".format(prefix, subject_id, target_group_id)
        headers = {
            'Accept': ['application/json'],
        }
        params = {
        }
        resp = yield self.make_authenticated_api_call(
            "DELETE",
            url,
            headers=headers,
            params=params)
        resp_code = resp.code
        parsed = yield resp.json()
        if resp_code not in (200, ):
            raise Exception("API call `api_remove_subject_from_group()` returned HTTP status {0}".format(resp_code))

    @inlineCallbacks
    def api_get_all_target_groups(self):
        """
        Load all target_groups from the sevice.
        Must return an iterable that yields tuples of
        (local_group_id, remote_group_id).
        """
        log = self.log
        log.debug("Entered: api_get_all_target_groups().")
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{}/groups".format(prefix)
        headers = {
            'Accept': ['application/json'],
        }
        params = {
        }
        resp = yield self.make_authenticated_api_call(
            "GET",
            url,
            headers=headers,
            params=params)
        resp_code = resp.code
        parsed = yield resp.json()
        if resp_code not in (200, ):
            raise Exception("API call `api_get_all_target_groups()` returned HTTP status {0}".format(resp_code))
        if not "response" in parsed:
            raise Exception(
                "API call `api_get_all_target_groups()` unexpected response: {}".format(
                    json.dumps(parsed).encode('utf-8')))
        response = parsed['response']
        groups = []
        for entry in response:
            api_id = entry['group_id']
            local_id = entry['name']
            groups.append((local_id, api_id))
        log.debug("Returning groups: {groups}", groups=groups)
        returnValue(groups)

    @inlineCallbacks
    def get_subjects_for_target_group(self, target_group_id):
        """
        Retireve a list of remote subject IDs that belong to a target_group identified
        by remote target_group_id.
        """
        log = self.log
        log.debug("Entered: get_subjects_for_target_group().")
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{}/groups/{}".format(prefix, target_group_id)
        headers = {
            'Accept': ['application/json'],
        }
        params = {
        }
        resp = yield self.make_authenticated_api_call(
            "GET",
            url,
            headers=headers,
            params=params)
        resp_code = resp.code
        parsed = yield resp.json()
        if resp_code not in (200, ):
            raise Exception("API call `api_get_all_target_groups()` returned HTTP status {0}".format(resp_code))
        if not "response" in parsed:
            raise Exception(
                "API call `api_get_all_target_groups()` unexpected response: {}".format(
                    json.dumps(parsed).encode('utf-8')))
        response = parsed['response']
        users = response['users']
        user_ids = [u['user_id'] for u in users]
        returnValue(user_ids)


class DuoSecurityProvisionerFactory(RESTProvisionerFactory):
    tag = "duosecurity_provisioner"
    opt_help = "Duo Security REST API Provisioner"
    opt_usage = "This plugin does not support any options."
    provisioner_factory = DuoSecurityProvisioner


