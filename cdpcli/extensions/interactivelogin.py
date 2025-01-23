# Copyright 2021 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

import http.server as httpserver
import os
import re
import socket
import socketserver
import sys
import time
from time import sleep
import urllib.parse as urlparse
import uuid
import webbrowser

from cdpcli import CDP_ACCESS_KEY_ID_KEY_NAME, CDP_PRIVATE_KEY_KEY_NAME
from cdpcli.endpoint import EndpointResolver
from cdpcli.exceptions import DeviceLoginError, InteractiveLoginError, \
    MissingArgumentError, ProfileNotFound
from cdpcli.extensions.commands import BasicCommand
from cdpcli.extensions.configure import CREDENTIAL_FILE_COMMENT
from cdpcli.extensions.writer import ConfigFileWriter
import requests

DEFAULT_LOGIN_TIMEOUT = 600
DEVICE_LOGIN_WAIT_SLEEP = 5


class LoginHttpServer(socketserver.TCPServer):
    """
    A HTTP-server to listen and accept then access-key when interactive login succeeded.
    """

    def __init__(self, port, context, config_writer):
        server_address = ('localhost', port)
        super(LoginHttpServer, self).__init__(server_address, LoginHttpHandler)
        self.context = context
        self.config_writer = config_writer

    def handle_timeout(self):
        """
        Raise an exception if timeout.
        The caller of this HTTP-server will catch the exception and stop the server.
        """
        raise InteractiveLoginError(err_msg='Login timeout')


class LoginHttpHandler(httpserver.SimpleHTTPRequestHandler):
    """
    A HTTP-handler to handle following request:
    http://localhost/interactiveLogin?accessKeyId=...&privateKey=...
    1) save the token.
    2) send CLOSE_BROWER_HTML as response.
    """

    CLOSE_BROWSER_HTML = b'''\
<!DOCTYPE html>
<html>
  <head>
    <link rel="icon" href="data:,">
    <meta charset="utf-8" />
  </head>
  <body onload="window.close()">
    <p>It is safe to close your browser.</p>
  </body>
</html>
'''

    def log_message(self, format, *args):
        pass

    def do_GET(self):
        if not self.path.startswith('/interactiveLogin'):
            sys.stderr.write('Login failed: URL path not supported\n')
            self._send_response(404, b'')
            return

        url_parsed = urlparse.urlsplit(self.path)
        url_params = urlparse.parse_qs(url_parsed.query)

        error = url_params.get('error')
        if error is not None:
            if isinstance(error, list):
                error = error[0]
            # print the error message
            sys.stderr.write('Login failed: %s\n' % error)
            # send empty response, do not close the browser
            self._send_response(200, b'')
            return

        access_key_id = url_params.get('accessKeyId')
        private_key = url_params.get('privateKey')

        # access_key_id and private_key are lists (url params are lists by default),
        # read the first element in the list.
        if isinstance(access_key_id, list):
            access_key_id = access_key_id[0]
        if isinstance(private_key, list):
            private_key = private_key[0]
        if access_key_id is None or private_key is None:
            sys.stderr.write(
                'Login failed: Missing access key id or private key\n')
            self._send_response(400, b'Missing access key id or private key')
            return

        # save the access token.
        save_access_token(
            self.server.context,
            self.server.config_writer,
            access_key_id,
            private_key)

        # send close browser HTML to the client.
        self._send_response(200, LoginHttpHandler.CLOSE_BROWSER_HTML)

    def _send_response(self, status_code, body):
        self.send_response(status_code)
        self.end_headers()
        self.wfile.write(body)


class LoginCommand(BasicCommand):
    """
    The 'cdp login' command handler.
    """

    NAME = 'login'
    DESCRIPTION = 'Login to CDP interactively.'
    SYNOPSIS = (
        'cdp login'
        ' [--account-id account-id]'
        ' [--identity-provider identity-provider-name]'
        ' [--use-device-code]'
    )
    EXAMPLES = (
        'To login interactively::\n'
        '\n'
        '    $ cdp login --account-id <guid>\n'
    )
    SUBCOMMANDS = []
    ARG_TABLE = [
        {'name': 'account-id',
         'help_text': (
             'The account-id of the tenant to login. This is a required '
             'parameter. \'cdp configure set account_id <account-id>\' '
             'could be used to set the default account-id to be used if '
             'this parameter is not provided.'),
         'action': 'store',
         'required': False,  # the account id could also come from config file.
         'cli_type_name': 'string'},
        {'name': 'identity-provider',
         'help_text': (
             'The name or CRN of IdP which will be used to authenticate users. '
             'The default IdP will be used if not provided.'),
         'action': 'store',
         'required': False,
         'cli_type_name': 'string'},
        {'name': 'login-url',
         'help_text': ('The URL (SP-initiated or IdP-initiated URL) to '
                       'start the interactive login with.'),
         'action': 'store',
         'required': False,
         'hidden': True,
         'no_paramfile': True,
         'cli_type_name': 'string'},
        {'name': 'port',
         'help_text': (
             'The listening port number for CLI to receive the access token. '
             'A random un-used port will be assigned if not provided.'),
         'action': 'store',
         'required': False,
         'hidden': True,
         'cli_type_name': 'integer'},
        {'name': 'timeout',
         'help_text': ('The login timeout duration in seconds. Default to '
                       '%d seconds if not provided.') % DEFAULT_LOGIN_TIMEOUT,
         'action': 'store',
         'required': False,
         'hidden': True,
         'cli_type_name': 'integer'},
        {'name': 'use-device-code',
         'help_text': (
             "Use CDP's authentication flow based on the device code. "
             "Use this to authorize the device if it can't launch a "
             "browser, e.g. in remote SSH."),
         'action': 'store_true',
         'required': False,
         'default': False,
         'cli_type_name': 'boolean'},
    ]

    def __init__(self, config_writer=None, open_new_browser=None):
        super(LoginCommand, self).__init__()
        if config_writer is None:
            config_writer = ConfigFileWriter()
        self._config_writer = config_writer
        if open_new_browser is None:
            open_new_browser = LoginCommand._open_new_browser
        self._open_new_browser = open_new_browser

    def _run_main(self, client_creator, parsed_args, parsed_globals):
        # Called when invoked with no sub-command "cdp login"

        # This is the config from the config file scoped to a specific
        # profile.
        try:
            context = client_creator.context
            config = context.get_scoped_config()
        except ProfileNotFound:
            config = {}

        port = parsed_args.port
        if port is None:
            port = LoginCommand._find_unused_port()
        timeout = parsed_args.timeout
        if timeout is None:
            timeout = DEFAULT_LOGIN_TIMEOUT

        if parsed_args.use_device_code:
            client_id = uuid.uuid4()
            login_url = self._resolve_login_url(parsed_args, parsed_globals,
                                                config, port, client_id)
            self._handle_device_login(login_url, client_id, context)
        else:
            login_url = self._resolve_login_url(parsed_args, parsed_globals,
                                                config, port)
            self._open_new_browser(login_url)
            self._run_http_server(port, timeout, context)

    @staticmethod
    def _open_new_browser(url):
        webbrowser.open_new(url)

    @staticmethod
    def _find_unused_port():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('localhost', 0))
        port = s.getsockname()[1]
        s.close()
        return port

    def _resolve_login_url(self, parsed_args, parsed_globals, config,
                           return_url_port, client_id=None):
        if parsed_args.use_device_code:
            service_name = "DEVICELOGIN"
        else:
            service_name = "LOGIN"

        # get the login URL from input parameter or config file.
        endpoint_resolver = EndpointResolver()
        login_url = endpoint_resolver.resolve(
            explicit_endpoint_url=parsed_args.login_url,
            config=config,
            region=parsed_globals.cdp_region,
            service_name=service_name,
            prefix=None,
            products=['CDP'],
            scheme='https',
            port=443)

        # get the account-id and idp from:
        # 1) login URL
        # 2) input parameter
        # 3) config file
        url_parsed = urlparse.urlsplit(login_url)
        url_params = urlparse.parse_qs(url_parsed.query)
        if 'accountId' not in url_params:
            account_id = parsed_args.account_id
            if account_id is None:
                account_id = config.get('account_id')
            if account_id is not None:
                url_params['accountId'] = account_id
            else:
                raise MissingArgumentError(arg_name='--account-id')
        if 'idp' not in url_params:
            idp = parsed_args.identity_provider
            if idp is None:
                idp = config.get('identity_provider')
            if idp is not None:
                url_params['idp'] = idp

        if parsed_args.use_device_code:
            url_params['clientId'] = client_id
        else:
            return_url = 'http://localhost:%d/interactiveLogin' % return_url_port
            url_params['returnUrl'] = return_url

        url_query = urlparse.urlencode(url_params, doseq=True)
        url_parsed = url_parsed._replace(query=url_query)
        return urlparse.urlunsplit(url_parsed)

    def _run_http_server(self, port, timeout, context):
        httpd = LoginHttpServer(port, context, self._config_writer)
        try:
            # handle_request() waits and handles one HTTP request.
            httpd.timeout = timeout
            httpd.handle_request()
        finally:
            httpd.server_close()

    def _handle_device_login(self, login_url, client_id, context):
        # Authorize device for cdpcli login
        authorize_device = requests.get(url=login_url)

        if (authorize_device.status_code != 200):
            raise DeviceLoginError(err_msg=authorize_device.text)

        # Print user instructions for completing device login
        authorize_device_response = authorize_device.json()
        device_code = authorize_device_response.get('deviceCode')
        user_code = authorize_device_response.get('userCode')
        device_login_timeout = authorize_device_response.get('expiresIn').get(
            'low')
        verification_url = authorize_device_response.get('verificationUrl')
        access_key_url = authorize_device_response.get('accessKeyUrl')

        sys.stdout.write(f"To sign in, use a web browser to open the page "
                         f"{verification_url} and enter the code "
                         f"{user_code} to authenticate.\n")

        # Wait for user to complete device login flow
        data = {
            "clientId": client_id,
            "deviceCode": device_code,
            "userCode": user_code
        }
        fetch_device_access_key = requests.post(url=access_key_url, data=data)

        device_login_timeout_time = time.time() + device_login_timeout
        while (fetch_device_access_key.status_code == 401 or
               fetch_device_access_key.status_code == 404) and \
                time.time() < device_login_timeout_time:
            fetch_device_access_key = requests.post(url=access_key_url,
                                                    data=data)
            sleep(DEVICE_LOGIN_WAIT_SLEEP)

        # Write cdp api keys to config file
        if fetch_device_access_key.status_code == 200:
            device_access_key_response = fetch_device_access_key.json()

            # write access key to config file.
            private_key = device_access_key_response.get("privateKey")
            access_key_id = device_access_key_response.get("accessKeyId")
            save_access_token(context, self._config_writer, access_key_id, private_key)
            sys.stdout.write("Device login is successful.\n")
        else:
            raise DeviceLoginError(err_msg='Login timeout')


def save_access_token(context, config_writer, access_key_id, private_key):
    # V3/ECDSA private key has '\n' in the string.
    private_key = re.sub(r'[\r\n]+', r'\\n', private_key)
    credential_file_values = {CDP_ACCESS_KEY_ID_KEY_NAME: access_key_id,
                              CDP_PRIVATE_KEY_KEY_NAME: private_key}
    profile_name = context.effective_profile
    if profile_name is not None:
        credential_file_values['__section__'] = profile_name
    shared_credentials_filename = os.path.expanduser(
        context.get_config_variable('credentials_file'))
    config_writer.update_config(
        credential_file_values,
        shared_credentials_filename,
        config_file_comment=CREDENTIAL_FILE_COMMENT)
