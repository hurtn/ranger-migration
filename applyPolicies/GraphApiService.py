# SOURCE:
# https://github.com/microsoftgraph/msgraph-sdk-python-core

from typing import Any
import os
import time
import enum

from azure.identity import ClientSecretCredential, DeviceCodeCredential
from msgraphcore import GraphSession, graph_session
from requests.adapters import Response

SCOPES = "basic user.read"
REDIRECT = "http://centricapoc"
AUTHORITY = "https://login.microsoftonline.com/common"
AUTH_ENDPOINT = "/oauth2/v2.0/authorize"
TOKEN_ENDPOINT = "/oauth2/v2.0/token"
APP_ID = os.environ["SPNID"]
APP_SECRET = os.environ["SPNSecret"]
TENANT_ID = os.environ["TenantId"]
GRAPH_URL = 'https://graph.microsoft.com/v1.0'


authorize_url = '{0}{1}{2}'.format(AUTHORITY, TENANT_ID, AUTH_ENDPOINT)
token_url = '{0}{1}'.format(AUTHORITY, TOKEN_ENDPOINT)


class FilterType(enum.Enum):
    User = 'users'
    Group = 'groups'


def find(filter_type, search_phase):

    graph_credentials = ClientSecretCredential(
        client_id=APP_ID,
        client_secret=APP_SECRET,
        tenant_id=TENANT_ID
    )

    scopes = ['.default']

    try:

        graph_session = GraphSession(graph_credentials, scopes)

        result = ''

        if (filter_type == FilterType.Group.value):
            result = graph_session.get(
                f"/groups?$filter=startswith(displayName, '{search_phase}')").json()

        elif (filter_type == FilterType.User.value):
            result = graph_session.get(
                f"/users?$filter=startswith(userPrincipalName, '{search_phase}')").json()

        id = result['value'][0]['id']
        
        return id

    except Exception as e:

        print(e)

        return ''
