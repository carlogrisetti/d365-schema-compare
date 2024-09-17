# Portions of the original code taken from https://github.com/GearPlug/dynamics365crm-python
"""
MIT License

Copyright (c) 2018 GearPlug

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from urllib.parse import urlencode
import re

import requests


class Client:
    api_path = "api/data/v9.2"

    def __init__(self, domain, client_id=None, client_secret=None, access_token=None):
        self.domain = domain.strip("/")
        self.scopes = [f"{domain}/user_impersonation"]
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token

        self.headers = {
            "Accept": "application/json, */*",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
        }

        if access_token is not None:
            self.set_access_token(access_token)

    def set_access_token(self, token):
        """
        Sets the Token for its use in this library.
        :param token: A string with the Token.
        :return:
        """
        assert token is not None, "The token cannot be None."
        self.access_token = token
        self.headers["Authorization"] = "Bearer " + self.access_token

    def make_request(
        self,
        method,
        endpoint,
        expand=None,
        filter=None,
        orderby=None,
        select=None,
        skip=None,
        top=None,
        data=None,
        json=None,
        **kwargs,
    ):
        """
        this method do the request petition, receive the different methods (post, delete, patch, get) that the api allow, see the documentation to check how to use the filters
        https://msdn.microsoft.com/en-us/library/gg309461(v=crm.7).aspx
        :param method:
        :param endpoint:
        :param expand:
        :param filter:
        :param orderby:
        :param select:
        :param skip:
        :param top:
        :param data:
        :param json:
        :param kwargs:
        :return:
        """
        extra = {}
        if expand is not None and isinstance(expand, str):
            extra["$expand"] = str(expand)
        if filter is not None and isinstance(filter, str):
            extra["$filter"] = filter
        if orderby is not None and isinstance(orderby, str):
            extra["$orderby"] = orderby
        if select is not None and isinstance(select, str):
            extra["$select"] = select
        if skip is not None and isinstance(skip, str):
            extra["$skip"] = skip
        if top is not None and isinstance(top, str):
            extra["$top"] = str(top)

        assert self.domain is not None, "'domain' is required"
        assert self.access_token is not None, "You must provide a 'token' to make requests"
        url = f"{self.domain}/{self.api_path}/{endpoint}?" + urlencode(extra)
        if method == "get":
            response = requests.request(method, url, headers=self.headers, params=kwargs)
        else:
            response = requests.request(method, url, headers=self.headers, data=data, json=json)

        return self.parse_response(response)

    def _get(self, endpoint, data=None, **kwargs):
        return self.make_request("get", endpoint, data=data, **kwargs)

    def _post(self, endpoint, data=None, json=None, **kwargs):
        return self.make_request("post", endpoint, data=data, json=json, **kwargs)

    def _delete(self, endpoint, **kwargs):
        return self.make_request("delete", endpoint, **kwargs)

    def _patch(self, endpoint, data=None, json=None, **kwargs):
        return self.make_request("patch", endpoint, data=data, json=json, **kwargs)

    def parse_response(self, response):
        """
        This method get the response request and returns json data or raise exceptions
        :param response:
        :return:
        """
        if response.status_code == 204 or response.status_code == 201:
            if 'OData-EntityId' in response.headers:
                entity_id = response.headers['OData-EntityId']
                if entity_id[-38:-37] == '(' and entity_id[-1:] == ')':  # Check container
                    guid = entity_id[-37:-1]
                    guid_pattern = re.compile(r'^[\da-f]{8}-([\da-f]{4}-){3}[\da-f]{12}$', re.IGNORECASE)
                    if guid_pattern.match(guid):
                        return guid
                    else:
                        return True  # Not all calls return a guid
            else:
                return True
        elif response.status_code == 400:
            raise Exception(
                "The URL {0} retrieved an {1} error. Please check your request body and try again.\nRaw message: {2}".format(
                    response.url, response.status_code, response.text
                )
            )
        elif response.status_code == 401:
            raise Exception(
                "The URL {0} retrieved and {1} error. Please check your credentials, make sure you have permission to perform this action and try again.".format(
                    response.url, response.status_code
                )
            )
        elif response.status_code == 403:
            raise Exception(
                "The URL {0} retrieved and {1} error. Please check your credentials, make sure you have permission to perform this action and try again.".format(
                    response.url, response.status_code
                )
            )
        elif response.status_code == 404:
            raise Exception(
                "The URL {0} retrieved an {1} error. Please check the URL and try again.\nRaw message: {2}".format(
                    response.url, response.status_code, response.text
                )
            )
        elif response.status_code == 412:
            raise Exception(
                "The URL {0} retrieved an {1} error. Please check the URL and try again.\nRaw message: {2}".format(
                    response.url, response.status_code, response.text
                )
            )
        elif response.status_code == 413:
            raise Exception(
                "The URL {0} retrieved an {1} error. Please check the URL and try again.\nRaw message: {2}".format(
                    response.url, response.status_code, response.text
                )
            )
        elif response.status_code == 500:
            raise Exception(
                "The URL {0} retrieved an {1} error. Please check the URL and try again.\nRaw message: {2}".format(
                    response.url, response.status_code, response.text
                )
            )
        elif response.status_code == 501:
            raise Exception(
                "The URL {0} retrieved an {1} error. Please check the URL and try again.\nRaw message: {2}".format(
                    response.url, response.status_code, response.text
                )
            )
        elif response.status_code == 503:
            raise Exception(
                "The URL {0} retrieved an {1} error. Please check the URL and try again.\nRaw message: {2}".format(
                    response.url, response.status_code, response.text
                )
            )
        return response.json()

    def get_data(self, type=None, **kwargs):
        if type is not None:
            return self._get(type, **kwargs)
        raise Exception("A type is necessary. Example: contacts, leads, accounts, etc... check the library")

    def create_data(self, type=None, **kwargs):
        if type is not None and kwargs is not None:
            params = {}
            params.update(kwargs)
            return self._post(type, json=params)
        raise Exception("A type is necessary. Example: contacts, leads, accounts, etc... check the library")

    def update_data(self, type=None, id=None, **kwargs):
        if type is not None and id is not None:
            url = "{0}({1})".format(type, id)
            params = {}
            if kwargs is not None:
                params.update(kwargs)
            return self._patch(url, json=params)
        raise Exception("A type is necessary. Example: contacts, leads, accounts, etc... check the library")

    def delete_data(self, type=None, id=None):
        if type is not None and id is not None:
            return self._delete("{0}({1})".format(type, id))
        raise Exception("A type is necessary. Example: contacts, leads, accounts, etc... check the library")
