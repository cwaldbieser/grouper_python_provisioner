
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
from  six.moves.urllib.parse import urlencode
from twisted.internet.defer import (
    inlineCallbacks, 
    returnValue,
)


class FakeResponse(object):
    """
    A minimal fake HTTP response
    """
    code = 200

    @inlineCallbacks
    def content(self):
        if False:
            yield None


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


class MoodleProvisioner(RESTProvisioner):
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
        if match_value is not None:
            match_value = match_value.lower()
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
        return remote_account.get("username", None)

    @inlineCallbacks
    def api_get_auth_token(self):
        """
        Make API call to obtain valid auth token.
        Should set `self.auth_token`.
        """
        if False:
            yield None
        self.auth_token = self.client_secret

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
        returnValue((method, url, http_options))

    @inlineCallbacks
    def get_all_api_ids_and_match_values(self):
        """
        Load all the remote subject IDs and match values from the 
        user accounts that exist on the remote sevice.
        Note: If a match value cannot be constructed for a remote
        account, it will not be included in the output of this function.
    
        NOTE: This is used for deprovisioning subjects during full sync.
        Because this Moodle provisioner *never* deprovisions accounts,
        it can safely return an empty list.
        """
        log = self.log
        if False:
            yield None
        returnValue([])

    def check_for_error_response(self, json_doc):
        """
        Check if the JSON response contains an error message.
        If so, raise an exception.
        """
        if 'errorcode' in json_doc:
            raise Exception("{0} - {0}".format(
                json_doc['errorcode'],
                json_doc['message']))

    @inlineCallbacks
    def api_get_remote_account(self, api_id):
        """
        Get the remote account information using its API ID.
        """
        log = self.log
        log.debug("Attempting to fetch remote account ...")
        http_client = self.http_client
        prefix = self.url_prefix
        url = prefix
        headers = {
            'Accept': ['application/json'],
        }
        params = {
            'moodlewsrestformat': 'json'
        }
        data = {
            'wstoken': self.client_secret,
            'wsfunction': 'core_user_get_users_by_field',
            'field': 'username',
             'values[0]': api_id 
        }
        log.debug("URL (POST): {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        log.debug("params: {params}", params=params)
        log.debug("data: {data}", data=data)
        resp = yield self.make_authenticated_api_call(
            "POST",
            url,
            headers=headers,
            params=params,
            data=data)
        remote_accounts = yield resp.json()
        self.check_for_error_response(remote_accounts)
        if len(remote_accounts) == 0:
            returnValue(None)
        returnValue(remote_accounts[0])

    @inlineCallbacks
    def api_deprovision_subject(self, api_id):
        """
        Make the API call require to deprovision the subject identified by
        `api_id`.

        NOTE: This is a no-op since this provisioner does not deprovision accounts.
        """
        log = self.log
        if False:
            yield None

    @inlineCallbacks
    def api_get_account_id(self, subject, attributes):
        """
        Fetch the remote ID for a subject.
        Return None if the account does not exist on the remote end.
        """
        print("PATH B")
        log = self.log
        log.debug("Attempting to fetch remote account ...")
        http_client = self.http_client
        prefix = self.url_prefix
        url = prefix
        headers = {
            'Accept': ['application/json'],
        }
        params = {
            'moodlewsrestformat': 'json'
        }
        data = {
            'wstoken': self.client_secret,
            'wsfunction': 'core_user_get_users_by_field',
            'field': 'username',
             'values[0]': subject 
        }
        log.debug("URL (POST): {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        log.debug("params: {params}", params=params)
        log.debug("data: {data}", data=data)
        resp = yield self.make_authenticated_api_call(
            "POST",
            url,
            headers=headers,
            params=params,
            data=data)
        remote_accounts = yield resp.json()
        self.check_for_error_response(remote_accounts)
        if len(remote_accounts) == 0:
            returnValue(None)
        print("REMOTE_ACCOUNTS: {0}".format(remote_accounts))
        returnValue(remote_accounts[0].get('username', None))

    @inlineCallbacks
    def api_update_subject(self, subject, api_id, attributes):
        """
        Make API request to update remote account.
        Returns the HTTP response.

        NOTE: This provisioner will not update a remote account.
        """
        log = self.log
        if False:
            yield None
        returnValue(FakeResponse())

    @inlineCallbacks
    def api_add_subject(self, subject, attributes):
        """
        Use the API to add subjects.
        
        Returns the API ID of the newly created remote account or None.
        If None is returned, the API ID will not be cached and require
        a lookup on future use.
        """
        log = self.log
        log.debug("Attempting to create remote account ...")
        http_client = self.http_client
        prefix = self.url_prefix
        url = prefix
        headers = {
            'Accept': ['application/json'],
        }
        params = {
            'moodlewsrestformat': 'json'
        }
        data = {
            'wstoken': self.client_secret,
            'wsfunction': 'core_user_create_users',
            'users[0][username]': subject,
            'users[0][firstname]': attributes.get("givenName", [""])[0],
            'users[0][lastname]': attributes.get("sn", [""])[0],
            'users[0][email]': attributes.get("mail", [""])[0],
            'users[0][customfields][0][type]': 'lnumber',
            'users[0][customfields][0][value]': attributes.get("bannerLNumber", [""])[0],
            'users[0][password]': generate_password(),
            'users[0][auth]': 'casattras',
        }
        log.debug("URL (GET): {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        log.debug("params: {params}", params=params)
        log.debug("data: {data}", data=data)
        resp = yield self.make_authenticated_api_call(
            "POST",
            url,
            headers=headers,
            params=params,
            data=data)
        remote_account = yield resp.json()
        self.check_for_error_response(remote_account)
        returnValue(subject)

class MoodleProvisionerFactory(RESTProvisionerFactory):
    tag = "moodle_provisioner"
    opt_help = "Moodle Provisioner"
    opt_usage = "This plugin does not support any options."
    provisioner_factory = MoodleProvisioner

