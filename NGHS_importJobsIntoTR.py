
import logging
logging.basicConfig(filename='nghs_opens_logging.log', level=logging.DEBUG,format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")


import os
import re
from datetime import date
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from simple_salesforce import Salesforce
import requests
import numpy as np
import warnings
import urllib
import requests
import msal
import atexit
import os.path
import urllib
import pandas as pd


def api_ImportRow(row):
  try:
      sf.TR1__Job__c.create(
      {
          'Client_Req_Number__c': row[1]['Client Req Number'],
          'OwnerId': row[1]['Record Owner ID'],                     # Recruiter Name
          'Name':row[1]['Job Name'],                                # Job Name
          'Department_Number__c':row[1]['Department Number'],
          'FLSA__c': row[1]["FLSA"],
          'TR1__Contact__c':row[1]["Contact_id"],                   # Contact Name / Hiring Manager
          'Account_Manager__c': '0051G000007n2FzQAI',               # Acccount Manager
          'Customer_Agreement__c': 'a021G00001jladOQAQ',            # Agreement Number
          'TR1__State_Area__c':row[1]["State/Area"],
          'TR1__Regional_Area__c':row[1]["Regional Area"],
          'TR1__Account__c': '0011G00000fZdySQAS',                  # Company Name
          'TR1__Salary_High__c':row[1]["Salary High"],
          'RecordTypeId': '0126R000001NlRCQA0',                     # Record Type like HRPO , RPO
          'Coach__c': '00537000000xKeoAAE',
          'Paygrade__c':row[1]["Pay Grade"],
          'Job_Family__c': row[1]["Job Family"],
          'Primary_Secondary__c':"Primary",


          # Unused for NGHS imports:

            #'Position_Number__c':'AWIMPORTTEST_1',
            #'Department_Name__c':'WOO Nursing Ops Leadership',
            #'Shift_Information__c':'Full-Time',
            #'Client_Req_Number__c':'999999999',
            #'Project__c': 'Austin',
            #'TR1__Salary_Low__c':'10000',

      }
      )
      return print("Successfully inserted job", row[1]["Job Name"])

  except Exception as ex:
      print(ex)
      return "FAILED TO INSERT JOB"

def api_findContact(contactname):

  def api_searchForContact(contactname):
    print("trying contact... " + contactname)
    query = f'''SELECT KX_Full_Name_c__c, Id FROM Contact WHERE Name = '{contactname}' LIMIT 1'''
    #generator on the results page
    fetch_results = sf.bulk.Contact.query_all(query, lazy_operation=True)

    all_results = []
    for list_results in fetch_results:
      all_results.extend(list_results)
    a = pd.DataFrame(all_results)
    a = a.drop(columns = ['attributes'],axis = 1)
    return a["Id"]
  
  
  try:
    id_to_return = api_searchForContact(contactname)
    print("Contact found in TR, returning ID")
  
  except Exception as ex:
    print(ex)
    print("Contact is likely not in TR yet, trying to create them now... ")
    
    try:
      sf.Contact.create({
        'FirstName' : contactname.split(" ")[0],
        'LastName' : contactname.split(" ")[1],
      })

      id_to_return = api_searchForContact(contactname)
      print("Creation successful!")

    except Exception as ex:
      print(ex)
      print("Creation of new contact has failed, and no ID will be returned")
      return ""
  
  return id_to_return.values[0]

def api_findUser(username):

  def api_searchForUser(username, last_name_search = False):
    print("trying... " + username)
    if last_name_search == True:
      query = f'''SELECT Name, Id FROM User WHERE Name LIKE '%{username}' LIMIT 5'''
    else:
      query = f'''SELECT Name, Id FROM User WHERE Name = '{username}' LIMIT 1'''

    #generator on the results page
    fetch_results = sf.bulk.User.query_all(query, lazy_operation=True)

    all_results = []
    for list_results in fetch_results:
      all_results.extend(list_results)
    a = pd.DataFrame(all_results)
    a = a.drop(columns = ['attributes'],axis = 1)
    print(a)
    return a["Id"]
  
  
  try:
    id_to_return = api_searchForUser(username,False)
    print("Contact found in TR, returning ID")
  
  except Exception as ex:
    print(ex)
    print("User not found in TR with exact match, searching for user")
    try:
      username_ = username.split(" ")[-1]
      print("Starting Last Name partial matching using...", username_)
      id_to_return = api_searchForUser(username_,True)
    except:
      print(ex)
      print("Partial matching failed!")

  
  return id_to_return.values[0]

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

session = requests.Session()
# Setting up salesforce functionality
sf = Salesforce(password='Kinetix3', username='awhelan@kinetixhr.com', organizationId='00D37000000HXaI',client_id='My App',session = session) 


# Log in to Azure AD
access_token = loginToSharepointViaAzure()
logging.info("Logged in!")

# Get Sharepoint Details for nghs folder download
item_path = 'Daily New Job Opens/NGHS'
drive_id,folder_id = getToSharepointFolderInCoachesSite(item_path)
logging.info("Got details.")

result = requests.get(f'{ENDPOINT}/drives/{drive_id}/items/{folder_id}/children', headers={'Authorization': 'Bearer ' + access_token})
children = result.json()['value']
for item in children:
    if genDate() in item['name']:
        file_to_upload_path = item_path + '/' + item['name']

logging.info(file_to_upload_path)


# Download NGHS Files and read into dataframes!
data_to_upload = downloadFromSharepoint(file_to_upload_path)
logging.info("Loaded in data - done!.")

data_to_upload["Contact_id"] = data_to_upload["Contact"].apply(api_findContact)
data_to_upload['Record Owner ID'] = data_to_upload['Record Owner'].apply(api_findUser)

logging.info("Done with prep for ",data_to_upload.shape[0]," reqs.")
for el in data_to_upload.iterrows():
    api_ImportRow(el)
logging.info("Done with importing new reqs!")

