
from __future__ import print_function
from rest_provisioner import (
    OptionMissingError,
    RESTProvisioner,
    RESTProvisionerFactory, 
)
from twisted.internet.defer import (
    inlineCallbacks, 
    returnValue,
)


class O365Provisioner(RESTProvisioner):
    
    def get_match_value_from_remote_account(self, remote_account):
        """
        Given a remote account, `remote_account`, extract the
        value that will be used to match the remote account 
        to the local subject.
        """
        match_value = remote_account.get("userPrincipalName", None)
        if match_value is not None:
            match_value = match_value[:match_value.find('@')]
            return match_value.lower()
        return match_value

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

    @inlineCallbacks
    def fetch_auth_token(self):
        """
        Make API call to obtain valid auth token.
        """
        log = self.log
        if self.auth_token is None:
            log.debug("Must obtain auth token ...")
            http_client = self.http_client
            prefix = self.url_prefix
            auth_url = "{0}/oauth2/token".format(prefix)
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
                self.check_401_response(response)
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
        yield self.fetch_auth_token()
        auth_token = self.auth_token
        headers = http_options.setdefault("headers", {})
        headers["Authorization"] = ["Bearer {0}".format(auth_token)]
        returnValue((method, url, http_options))

    @inlineCallbacks
    def get_all_subject_remote_ids(self):
        """
        Load all the remote subject IDs and match values from the 
        user accounts that exist on the remote sevice.
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
                    headers=headers,
                    params=params)    
            except Exception as ex:
                log.error("Error fetching all users.")
                raise
            parsed = yield resp.json()
            value = parsed["value"]
            for entry in value:
                remote_id = self.get_api_id_from_remote_account(entry)
                match_value = self.get_match_value_from_remote_account(entry)
                identifiers.append((remote_id, match_value))
            if "@odata.nextLink" in parsed:
                url = parsed["@odata.nextLink"]
            else:
                break
        returnValue(identifiers)

    @inlineCallbacks
    def api_deprovision_subject(self, remote_id):
        """
        Make the API call require to deprovision the subject identified by
        `remote_id`.
        """
        log = self.log
        http_client = self.http_client
        prefix = self.url_prefix
        url = "{0}/users/{1}".format(prefix, remote_id)
        headers = {
            'Accept': ['application/json'],
        }
        resp = yield self.make_authenticated_api_call(
            "PATCH",
            url,
            headers=headers)
        resp_code = resp.code
        try:
            content = yield resp.content()
        except Exception as ex:
            pass
        if resp_code != 204:
            raise Exception("API call to deprovision subject returned HTTP status {0}".format(resp_code))
        returnValue(None)

    @inlineCallbacks
    def api_get_account_id(subject, attributes):
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
            remote_id = parsed["id"]
        else:
            remote_id = None
        returnValue(remote_id)


class O365ProvisionerFactory(RESTProvisionerFactory):
    tag = "o365_provisioner"
    opt_help = "MS Graph API Provisioner"
    opt_usage = "This plugin does not support any options."
    provisioner_factory = O365Provisioner


