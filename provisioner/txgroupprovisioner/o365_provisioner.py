
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


class O365Provisioner(RESTProvisioner):
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
        domain = self.domain
        match_value = remote_account.get("userPrincipalName", None)
        if match_value is not None:
            match_value = match_value.lower()
            if not match_value.endswith("@{0}".format(domain.lower())):
                match_value = None
        return match_value

    def get_match_value_from_local_subject(self, subject, attributes):
        """
        Given a local subject and attributes, compute the value that
        will be used to match the remote account to the local subject.
        """
        domain = self.domain
        return "{0}@{1}".format(subject, domain)

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
        client_id = config.get("client_id", None)
        if client_id is None:
            raise OptionMissingError(
                "The `client_id` option is missing!") 
        self.client_id = client_id
        domain = config.get("domain", None)
        if domain is None:
            raise OptionMissingError(
                "The `domain` option is missing!") 
        self.domain = domain
        license_map = config.get("license_map", None)
        self.license_map = self.parse_license_map(license_map)

    def parse_license_map(self, license_map):
        """
        Create a map of psuedo-group-name to license info from a path to an external
        JSON file.
        """
        if license_map is None:
            return {}
        with open(license_map, "r") as f:
             doc = commentjson.load(f)
        for group, info in doc.items():
            sku = info.get("sku", None)
            if sku is None:
                raise Exception(
                    "License map '{0}': group '{1}' does not have 'sku' property!".format(
                        license_map,
                        group)
                )
        return doc

    @inlineCallbacks
    def api_get_auth_token(self):
        """
        Make API call to obtain valid auth token.
        Should set `self.auth_token`.
        """
        log = self.log
        domain = self.domain
        if self.http_authn_client is None:
            pool, agent, client = self.make_web_client("tls:host=login.microsoftonline.com:port=443")
            self.http_authn_client = client
        http_client = self.http_authn_client
        auth_url = "https://login.microsoftonline.com/{0}/oauth2/token".format(domain)
        headers = {
            'Accept': ['application/json'],
        }
        client_secret = self.client_secret
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": client_secret,
            "resource": "https://graph.microsoft.com/"}
        log.debug("Making API call to obtain auth token ...")
        log.debug("method: POST, URL: {url}", url=auth_url)
        response = yield http_client.post(auth_url, data=data, headers=headers)
        resp_code = response.code
        log.debug("API call to obtain token is complete.  Response code: {code}", code=resp_code)
        if resp_code == 200:
            try:
                doc = yield response.json()
            except Exception as ex:
                log.error("Error attempting to parse response to authentication request.")
                raise
            if not "access_token" in doc:
                log.error("Error attempting to parse response to authentication request.")
                raise Exception("Error parsing authentication response.")
            self.auth_token = doc["access_token"]
            log.debug("New auth token obtained.")
        else:
            self.check_unauthorized_response(response)
            content = yield response.content()
            raise Exception(
                "Unable to obtain valid auth token.  Response {0}: {1}".format(
                response_code=resp_code,
                content=content)
            )

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
        headers["Authorization"] = ["Bearer {0}".format(auth_token)]
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
            value = parsed["value"]
            for entry in value:
                api_id = self.get_api_id_from_remote_account(entry)
                match_value = self.get_match_value_from_remote_account(entry)
                if not match_value is None:
                    identifiers.append((api_id, match_value))
            if "@odata.nextLink" in parsed:
                url = parsed["@odata.nextLink"]
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
            'accountEnabled': False,
        }
        serialized = json.dumps(props)
        body = StringProducer(serialized.encode('utf-8'))
        log.debug("url: {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        log.debug("body: {body}", body=serialized)
        resp = yield self.make_authenticated_api_call(
            "PATCH",
            url,
            headers=headers,
            data=body)
        resp_code = resp.code
        try:
            content = yield resp.content()
        except Exception as ex:
            pass
        if resp_code != 204:
            raise Exception("API call to deprovision subject returned HTTP status {0}".format(resp_code))
        returnValue(None)

    @inlineCallbacks
    def api_get_account_id(self, subject, attributes):
        """
        Fetch the remote ID for a subject.
        Return None if the account oes not exist on the remote end.
        """
        log = self.log
        http_client = self.http_client
        prefix = self.url_prefix
        upn = "{0}@{1}".format(subject, self.domain) 
        url = "{0}/users/{1}".format(prefix, upn)
        headers = {
            'Accept': ['application/json'],
        }
        resp = yield self.make_authenticated_api_call(
            "GET",
            url,
            headers=headers)
        resp_code = resp.code
        parsed = yield resp.json()
        if resp_code not in (200, 404):
            raise Exception("API call to deprovision subject returned HTTP status {0}".format(resp_code))
        if resp_code == 200 and "id" in parsed:
            api_id = parsed["id"]
        else:
            api_id = None
        returnValue(api_id)

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
        surname = attributes.get("sn", [""])[0]
        givenname = attributes.get("givenName", [""])[0]
        displayname = "{0}, {1}".format(surname, givenname)
        upn = "{0}@{1}".format(subject, self.domain)
        immutable_id = attributes.get("bannerLNumber", [None])[0]
        props = {
            'accountEnabled': True,
            'displayName': displayname,
            'givenName': givenname,
            'surname': surname,
            'userPrincipalName': upn,
            'mailNickname': subject,
            'usageLocation': 'US',
        }
        if not immutable_id is None:
            props["onPremisesImmutableId"] = immutable_id
        serialized = json.dumps(props)
        body = StringProducer(serialized.encode('utf-8'))
        log.debug("url: {url}", url=url)
        log.debug("headers: {headers}", headers=headers)
        log.debug("body: {body}", body=serialized)
        resp = yield self.make_authenticated_api_call(
            'PATCH',  
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
        surname = attributes.get("sn", [""])[0]
        givenname = attributes.get("givenName", [""])[0]
        displayname = "{0}, {1}".format(surname, givenname)
        upn = "{0}@{1}".format(subject, self.domain)
        immutable_id = attributes.get("bannerLNumber", [None])[0]
        if immutable_id is None:
            raise Exception(
                "No immutable ID found for subject '{0}'!".format(subject)
            )
        props = {
            'accountEnabled': True,
            'displayName': displayname,
            'givenName': givenname,
            'surname': surname,
            'userPrincipalName': upn,
            'passwordProfile': {
                "forceChangePasswordNextSignIn": False, 
                "password": generate_password(),
            },
            'mailNickname': subject,
            'usageLocation': 'US',
            'onPremisesImmutableId': immutable_id,
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
        if resp_code != 201:
            content = yield resp.content()
            log.error(
                "API response {code}: {content}", 
                code=resp_code,
                content=content)
            raise Exception("API returned status {0}".format(resp_code))
        else:
            parsed = yield resp.json()
            api_id = parsed["id"]
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
        license_info = self.license_map.get(target_group_id)
        if not license_info is None:
            yield self.api_add_license_to_subject(subject_id, license_info)
        else:
            raise NotImplementedError()

    @inlineCallbacks
    def api_add_license_to_subject(self, subject_id, license_info):
        """
        API call to add a license to a user.
        """
        log = self.log
        sku = license_info["sku"]
        disabled_products = license_info.get("disabled_products", [])
        prefix = self.url_prefix
        url = "{0}/users/{1}/assignLicense".format(prefix, subject_id)
        headers = {
            'Accept': ['application/json'],
            'Content-Type': ['application/json'],
        }
        props = {
            'addLicenses': [
                {
                    "disabledPlans": disabled_products, 
                    "skuId": sku,
                }
            ],
            'removeLicenses': [],
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
        log.debug("Add-license API response code: {code}", code=resp_code)
        if resp_code != 200:
            content = yield resp.content()
            log.error(
                "API response {code}: {content}", 
                code=resp_code,
                content=content)
            raise Exception("API returned status {0}".format(resp_code))
        else:
            parsed = yield resp.json()
            api_id = parsed["id"]
            returnValue(api_id)

    @inlineCallbacks
    def api_remove_subject_from_group(self, subject_id, target_group_id):
        """
        Make an authenticated API call to add the remote subject ID
        to the remote group ID.
        Should raise on error on failure.
        """
        log = self.log
        log.debug("Entered: api_add_subject_to_group().")
        license_info = self.license_map.get(target_group_id)
        if not license_info is None:
            yield self.api_remove_license_from_subject(subject_id, license_info)
        else:
            raise NotImplementedError()

    @inlineCallbacks
    def api_remove_license_from_subject(self, subject_id, license_info):
        """
        API call to remove a license from a user.
        """
        log = self.log
        sku = license_info["sku"]
        prefix = self.url_prefix
        url = "{0}/users/{1}/assignLicense".format(prefix, subject_id)
        headers = {
            'Accept': ['application/json'],
            'Content-Type': ['application/json'],
        }
        props = {
            'removeLicenses': [sku],
            'addLicenses': [],
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
        log.debug("Remove-license API response code: {code}", code=resp_code)
        content = yield resp.content()
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as ex:
            parsed = None 
        is_error = False
        if resp_code != 200:
            is_error = True
            if (resp_code == 400) and (not parsed is None):
                error = parsed.get("error", {})
                message = error.get("message", None)
                if message == "User does not have a corresponding license.":
                    is_error = False
        if is_error:
            log.error(
                "API response {code}: {content}", 
                code=resp_code,
                content=content)
            raise Exception("API returned status {0}".format(resp_code))

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
        rval = [(tgroup, tgroup) for tgroup in self.license_map.keys()]
        returnValue(rval)

    @inlineCallbacks
    def get_subjects_for_target_group(self, target_group_id):
        """
        Retireve a list of remote subject IDs that belong to a target_group identified
        by remote target_group_id.
        """
        log = self.log
        log.debug("Attempting to fetch subject API IDs that are members of a target group ...")
        license_info = self.license_map.get(target_group_id)
        if not license_info is None:
            api_ids = yield self.api_get_subjects_for_license(license_info)
            returnValue(api_ids)
        raise NotImplementedError()
    
    @inlineCallbacks
    def api_get_subjects_for_license(self, license_info):
        """
        Get the API IDs for subjects that have the given license.
        Do NOT include unmanaged logins.
        """
        log = self.log
        sku = license_info["sku"]
        disabled_products = set(license_info.get("disabled_products", []))
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}/users".format(prefix)
        headers = {
            'Accept': ['application/json'],
        }
        params = {
            '$select': 'accountEnabled,userPrincipalName,id'
        }
        identifiers = []
        qs = {}
        while True:
            log.debug("URL (GET): {url}", url=url)
            log.debug("headers: {headers}", headers=headers)
            params.update(qs)
            try:
                resp = yield self.make_authenticated_api_call(
                    "GET",
                    url,
                    headers=headers,
                    params=params)
            except Exception as ex:
                log.error("Error fetching all remote user data.")
                raise
            parsed = yield resp.json()
            try:
                value = parsed["value"]
            except Exception as ex:
                log.debug("No value; parsed: {parsed}", parsed=parsed)
                raise
            unmanaged_logins = self.unmanaged_logins
            for entry in value:
                api_id = self.get_api_id_from_remote_account(entry)
                match_value = self.get_match_value_from_remote_account(entry)
                if match_value in unmanaged_logins:
                    continue
                if match_value is None:
                    continue
                for license in entry.get("assignedLicenses", []):
                    if license.get("skuId", None) == sku:
                        azure_disabled_plans = set(license.get("disabledPlans", []))
                        if disabled_products == azure_disabled_plans:
                            identifiers.append(api_id)
                            break
            if "@odata.nextLink" in parsed:
                url = parsed["@odata.nextLink"]
                p = urlparse.urlparse(url)
                qs = urlparse.parse_qs(p.query)
                url = urlparse.urlunparse((p.scheme, p.netloc, p.path, p.params, {}, p.fragment))
            else:
                break
        returnValue(identifiers)


class O365ProvisionerFactory(RESTProvisionerFactory):
    tag = "o365_provisioner"
    opt_help = "MS Graph API Provisioner"
    opt_usage = "This plugin does not support any options."
    provisioner_factory = O365Provisioner


