''' TCH Download file functionality '''

import logging
import requests
import tch_helpers as tchhelper
import os
logging.basicConfig(filename='opensprocessing/TCH_Opens_new/logs/tch_opens_logging.log', level=logging.DEBUG,format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")


logging.info("Loaded in Packages")

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
access_token = tchhelper.login_to_sharepoint_via_azure(CLIENT_ID,AUTHORITY,SCOPES,ENDPOINT)
logging.info("Logged in!")

# Get Sharepoint Details for nghs folder download
item_path = 'Daily New Job Opens/tch_unprocessed'
drive_id,folder_id = tchhelper.get_to_sharepoint_folder_in_coaches_site(item_path,SHAREPOINT_HOST_NAME,SITE_NAME,access_token,ENDPOINT)
logging.info("Got details.")

# Find file name and paths
result = requests.get(f'{ENDPOINT}/drives/{drive_id}/items/{folder_id}/children', headers={'Authorization': 'Bearer ' + access_token},timeout = 5)
children = result.json()['value']
for item in children:
    if 'Output' in item['name']:
        tch_file_path = item_path + '/' + item['name']
    if 'report1' in item['name']:
        tr_file_path = item_path + '/' + item['name']
logging.info(tch_file_path)
logging.info(tr_file_path)

# Download NGHS Files and read into dataframes!
tr_data = tchhelper.download_from_sharepoint(tr_file_path,ENDPOINT,drive_id,access_token)
tch_data = tchhelper.download_from_sharepoint(tch_file_path,ENDPOINT,drive_id,access_token)
logging.info("Loaded in data - done!.")

# Save file in /working_files directory
os.chdir("TCH_Opens_new")
os.chdir("working_files")
tr_data.to_csv("report1.csv",index = False)
tch_data.to_excel("Output1.xlsx", index = False)
# End of File