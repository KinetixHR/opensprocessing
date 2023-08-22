import logging
logging.basicConfig(filename='tch_opens_logging.log', level=logging.DEBUG,format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")

import requests
import msal
import atexit
import os.path
import urllib
import pandas as pd

logging.info("Loaded in Packages")

def loginToSharepointViaAzure():
    logging.info("Starting to log in to Azure.")
    try:
        os.chdir('opensprocessing')
        logging.info(os.getcwd())
    except Exception as e:
        logging.warning(e)
    cache = msal.SerializableTokenCache()
    logging.info(os.listdir())

    
    if os.path.exists('token_cache.bin'):
        cache.deserialize(open('token_cache.bin', 'r').read())

    atexit.register(lambda: open('token_cache.bin', 'w').write(cache.serialize()) if cache.has_state_changed else None)

    app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)

    accounts = app.get_accounts()
    result = None
    if len(accounts) > 0:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
    if len(accounts) == 0:
        logging.warning("Something went wrong with login - likely a token issue")
    #print(result)
    logging.info(result)
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
        logging.warning("No Access Token in Result.")
        raise Exception('no access token in result')
    
    return access_token

def getToSharepointFolderInCoachesSite(item_path_):
    result = requests.get(f'{ENDPOINT}/sites/{SHAREPOINT_HOST_NAME}:/sites/{SITE_NAME}', headers={'Authorization': 'Bearer ' + access_token})
    site_info = result.json()
    site_id = site_info['id']
    #print(site_id)
    result = requests.get(f'{ENDPOINT}/sites/{site_id}/drive', headers={'Authorization': 'Bearer ' + access_token})
    drive_info = result.json()
    drive_id =  drive_info['id']
    print(drive_info)

    item_path = item_path_
    item_url = urllib.parse.quote(item_path)
    result = requests.get(f'{ENDPOINT}/drives/{drive_id}/root:/{item_url}', headers={'Authorization': 'Bearer ' + access_token})
    item_info = result.json()
    folder_id = item_info['id']
    return [drive_id, folder_id]

def downloadFromSharepoint(file_path_):
    
    # Dynamically get file path and id for download
    file_path = file_path_
    file_url = urllib.parse.quote(file_path)
    result = requests.get(f'{ENDPOINT}/drives/{drive_id}/root:/{file_url}', headers={'Authorization': 'Bearer ' + access_token})
    file_info = result.json()
    file_id = file_info['id']

    # Download file locally
    result = requests.get(f'{ENDPOINT}/drives/{drive_id}/items/{file_id}/content', headers={'Authorization': 'Bearer ' + access_token})
    open(file_info['name'], 'wb').write(result.content)
    

    try:
        sharepoint_data = pd.read_csv(file_info['name'], encoding = "ISO-8859-1")
    except:
        sharepoint_data = pd.read_excel(file_info['name'])
    
    return sharepoint_data

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

# Log in to Azure AD
access_token = loginToSharepointViaAzure()
logging.info("Logged in!")

# Get Sharepoint Details for nghs folder download
item_path = 'Daily New Job Opens/tch_unprocessed'
drive_id,folder_id = getToSharepointFolderInCoachesSite(item_path)
logging.info("Got details.")

# Find NGHS file name and paths
result = requests.get(f'{ENDPOINT}/drives/{drive_id}/items/{folder_id}/children', headers={'Authorization': 'Bearer ' + access_token})
children = result.json()['value']
for item in children:
    if 'Output' in item['name']:
        tch_file_path = item_path + '/' + item['name']
    if 'report1' in item['name']:
        tr_file_path = item_path + '/' + item['name']
logging.info(tch_file_path)
logging.info(tr_file_path)

# Download NGHS Files and read into dataframes!
tr_data = downloadFromSharepoint(tr_file_path)
tch_data = downloadFromSharepoint(tch_file_path)
logging.info("Loaded in data - done!.")