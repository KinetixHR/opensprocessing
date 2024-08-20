import requests
import msal
import atexit
import os.path

TENANT_ID = '87272575-d7ac-4705-86e3-21cd39600d46'
CLIENT_ID = '514cf64c-692a-48b1-a791-1c0da37fcb0c'
SHAREPOINT_HOST_NAME = 'kinetixhr.sharepoint.com'
SITE_NAME = 'KinetixCoaches'

AUTHORITY = 'https://login.microsoftonline.com/' + TENANT_ID
ENDPOINT = 'https://graph.microsoft.com/v1.0'

SCOPES = [
    'Files.ReadWrite.All',
    'Sites.ReadWrite.All',
    'User.Read',
    'User.ReadBasic.All'
]

cache = msal.SerializableTokenCache()

if os.path.exists('token_cache.bin'):
    cache.deserialize(open('token_cache.bin', 'r').read())

atexit.register(lambda: open('token_cache.bin', 'w').write(cache.serialize()) if cache.has_state_changed else None)

app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)

accounts = app.get_accounts()
result = None
if len(accounts) > 0:
    result = app.acquire_token_silent(SCOPES, account=accounts[0])

if result is None:
    flow = app.initiate_device_flow(scopes=SCOPES)
    if 'user_code' not in flow:
        raise Exception('Failed to create device flow')

    print(flow['message'])

    result = app.acquire_token_by_device_flow(flow)

if 'access_token' in result:
    result = requests.get(f'{ENDPOINT}/me', headers={'Authorization': 'Bearer ' + result['access_token']})
    result.raise_for_status()
    print(result.json())

else:
    raise Exception('no access token in result')