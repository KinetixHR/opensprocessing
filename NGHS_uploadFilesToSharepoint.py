
import logging
logging.basicConfig(filename='nghs_opens_logging.log', level=logging.INFO,format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")

import requests
import msal
import atexit
import os.path
import urllib
import pandas as pd
import os.path
from datetime import date


def loginToSharepointViaAzure():
    cache = msal.SerializableTokenCache()

    try:
        os.chdir('opensprocessing')
        logging.info(os.getcwd())
    except Exception as e:
        logging.warning(e)


    if os.path.exists('token_cache.bin'):
        cache.deserialize(open('token_cache.bin', 'r').read())

    atexit.register(lambda: open('token_cache.bin', 'w').write(cache.serialize()) if cache.has_state_changed else None)

    app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)

    accounts = app.get_accounts()
    result = None
    if len(accounts) > 0:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])

    #print(result)
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

def genDate():
    '''
    Generate date in a format that is ready to be included in a filename, like
    NGHS 20YYMMDD
    '''
    today = date.today()
    day = str(today.day)
    month = str(today.month)
    year = str(today.year)
    if len(month) == 1:
        month = "0"+month
    if len(day) == 1:
        day = "0"+day
    return year+month+day

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

today = genDate()

# Log in to Azure AD
access_token = loginToSharepointViaAzure()

# Get Sharepoint Details for nghs folder uplad
item_path = 'Daily New Job Opens/NGHS'
drive_id,folder_id = getToSharepointFolderInCoachesSite(item_path)


# UPLOAD A FILE
files = os.listdir()
for el in files:
    if today in el:
        filename = el 

folder_path = 'Daily New Job Opens/NGHS/'

path_url = urllib.parse.quote(f'{folder_path}/{filename}')
result = requests.get(f'{ENDPOINT}/drives/{drive_id}/root:/{path_url}', headers={'Authorization': 'Bearer ' + access_token})
logging.info(f"result status code: {result.status_code}")

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
    logging.info("File is already in Sharepoint.")

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
    logging.info("Successfully uploaded the file to the NGHS folder")


logging.info("Removing files from local disk...")
for el in os.listdir():
    #logging.info(el)
    if "NGHS 2" in el:
      os.remove(el)
      logging.info(f"Removed file! {el}")
    if "report1" in el:
      os.remove(el)
      logging.info(f"Removed file! {el}")
    if "REC - " in el:
       os.remove(el)
       logging.info(f"Removed file! {el}")
