import requests
import msal
import atexit
import os.path
import urllib

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

print(result)
access_token = result['access_token']
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

result = requests.get(f'{ENDPOINT}/sites/{SHAREPOINT_HOST_NAME}:/sites/{SITE_NAME}', headers={'Authorization': 'Bearer ' + access_token})
site_info = result.json()
site_id = site_info['id']
#print(site_id)
result = requests.get(f'{ENDPOINT}/sites/{site_id}/drive', headers={'Authorization': 'Bearer ' + access_token})
drive_info = result.json()
drive_id =  drive_info['id']
print(drive_info)

item_path = 'Daily New Job Opens/TCH'
item_url = urllib.parse.quote(item_path)
result = requests.get(f'{ENDPOINT}/drives/{drive_id}/root:/{item_url}', headers={'Authorization': 'Bearer ' + access_token})
item_info = result.json()
folder_id = item_info['id']



result = requests.get(f'{ENDPOINT}/drives/{drive_id}/items/{folder_id}/children', headers={'Authorization': 'Bearer ' + access_token})
children = result.json()['value']
for item in children:
    print(item['name'])



# DOWNLOAD A FILE - WORKS!!!
file_path = item_path + '/NGHS 20230821.csv'
file_url = urllib.parse.quote(file_path)
result = requests.get(f'{ENDPOINT}/drives/{drive_id}/root:/{file_url}', headers={'Authorization': 'Bearer ' + access_token})
file_info = result.json()
file_id = file_info['id']

result = requests.get(f'{ENDPOINT}/drives/{drive_id}/items/{file_id}/content', headers={'Authorization': 'Bearer ' + access_token})
open(file_info['name'], 'wb').write(result.content)

# UPLOAD A FILE
filename = 'test.txt'
folder_path = 'Daily New Job Opens/TCH/'

path_url = urllib.parse.quote(f'{folder_path}/{filename}')
result = requests.get(f'{ENDPOINT}/drives/{drive_id}/root:/{path_url}', headers={'Authorization': 'Bearer ' + access_token})
if result.status_code == 200:
    file_info = result.json()
    file_id = file_info['id']
    result = requests.put(
        f'{ENDPOINT}/drives/{drive_id}/items/{file_id}/content',
        headers={
            'Authorization': 'Bearer ' + access_token,
            'Content-type': 'application/binary'
        },
        data=open(filename, 'rb').read()
    )
    
elif result.status_code == 404:
    folder_url = urllib.parse.quote(folder_path)
    result = requests.get(f'{ENDPOINT}/drives/{drive_id}/root:/{folder_url}', headers={'Authorization': 'Bearer ' + access_token})
    result.raise_for_status()
    folder_info = result.json()
    folder_id = folder_info['id']

    file_url = urllib.parse.quote(filename)
    result = requests.put(
        f'{ENDPOINT}/drives/{drive_id}/items/{folder_id}:/{file_url}:/content',
        headers={
            'Authorization': 'Bearer ' + access_token,
            'Content-type': 'application/binary'
        },
        data=open(filename, 'rb').read()
    )

print(drive_id)