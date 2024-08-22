
import logging
logging.basicConfig(filename='opensprocessing/TCH_Opens_new/logs/tch_opens_logging.log', level=logging.INFO,format='%(levelname)s %(asctime)s %(message)s')
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
import tch_helpers as tchhelper


def api_ImportRow(row):
  try:
      logging.info("Starting to import data...")
      logging.info(row)
      #logging.info(row[1]["Record Type"])
      if row[1]['Record Type'] == "01237000000RXdjAAG": #PIPELINE REQS ONLY!
          #logging.info("PIPELINE REQ")
          #logging.info(row)
          sf.TR1__Job__c.create(
        {
            'Position_Number__c': row[1]['Position Number'],
            'Client_Req_Number__c': row[1]['Client Req Number'],
            'OwnerId': row[1]['Job Owner'],                      # Recruiter Name
            'Name':row[1]['Job Name'],                              # Job Name
            'Department_Number__c':row[1]['Department Number'],
            'Department_Name__c': row[1]["Department Name"],
            'FTE__c': row[1]["FTE"],
            'Budgeted_Start_Date__c': row[1]["Budgeted Start Date"],
            'Paygrade__c': row[1]["Pay Grade"],
            'Project__c':row[1]["Project"],
            'TR1__Contact__c':row[1]["Contact"],          # Contact Name / Hiring Manager
            'Account_Manager__c': '00537000004wGaNAAU',        # Acccount Manager
            'Customer_Agreement__c': 'a021G00000z2UWRQA2',  # Agreement Number
            'TR1__State_Area__c':row[1]["State/Area"],
            'TR1__Regional_Area__c':row[1]["Regional Area"],
            'TR1__Account__c': '0013700000cBblUAAS',                   # Company Name
            'TR1__Salary_High__c':row[1]["Salary High"],
            'TR1__Salary_Low__c':row[1]["Salary Low"],
            'Coach__c': row[1]["Coach"],
            'Primary_Secondary__c':"Primary",
            'RecordTypeId': row[1]['Record Type'],
        }
          )
          logging.info("Inserted a pipeline job")
      else:
        sf.TR1__Job__c.create(
        {
            'Position_Number__c': row[1]['Position Number'],
            'Client_Req_Number__c': row[1]['Client Req Number'],
            'OwnerId': row[1]['Job Owner'],                      # Recruiter Name
            'Name':row[1]['Job Name'],                              # Job Name
            'Department_Number__c':row[1]['Department Number'],
            'Department_Name__c': row[1]["Department Name"],
            'FLSA__c': str(row[1]["FLSA"]),
            'FTE__c': row[1]["FTE"],
            'Budgeted_Start_Date__c': row[1]["Budgeted Start Date"],
            'Paygrade__c': row[1]["Pay Grade"],
            'Project__c':row[1]["Project"],
            'TR1__Contact__c':row[1]["Contact"],          # Contact Name / Hiring Manager
            'Account_Manager__c': '00537000004wGaNAAU',        # Acccount Manager
            'Customer_Agreement__c': 'a021G00000z2UWRQA2',  # Agreement Number
            'TR1__State_Area__c':row[1]["State/Area"],
            'TR1__Regional_Area__c':row[1]["Regional Area"],
            'TR1__Account__c': '0013700000cBblUAAS',                   # Company Name
            'TR1__Salary_High__c':row[1]["Salary High"],
            'TR1__Salary_Low__c':row[1]["Salary Low"],
            'RecordTypeId': row[1]['Record Type'],                   # Record Type like HRPO , RPO
            'Coach__c': row[1]["Coach"],
            'Primary_Secondary__c':"Primary",
        }
      )
      logging.info("Successfully inserted job", row[1]["Job Name"])
      return print("Successfully inserted job", row[1]["Job Name"])

  except Exception as ex:
      print(ex)
      sendEmail(f"Failed to insert job {row[1]['Job Name']} into TR. Details: {ex}")
      return logging.warning(["FAILED TO INSERT JOB",row[1]['Job Name'],ex])

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
      username_ = username.split(",")[0]
      print("Starting Last Name partial matching using...", username_)
      id_to_return = api_searchForUser(username_,True)
    except:
      print(ex)
      print("Partial matching failed!")
      return username

  
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
    TCH: M.DD.20YY
    '''
    #import datetime
    today = date.today()
    #today = today - datetime.timedelta(1)
    
    day = str(today.day)
    month = str(today.month)
    year = str(today.year)[-2:]
    #print(month+"."+day+"."+year)
    return month+"."+day+"."+year

def sendEmail(text): 
    # Define your email credentials
    sender_email = 'kinetixopensprocessing@gmail.com'
    sender_password = 'ttljtrsnsqlhmnrz'
    receiver_email = ['dart@kinetixhr.com',"kxdart@kinetixhr.com",'DART@kinetixhr.com']
    #receiver_email = ['awhelan@kinetixhr.com']
    subject = 'TCH Job Import to TR: Alert'
    body = text

    # Create the email message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_email)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))


    # Connect to the SMTP server and send the email
    smtp_server = 'smtp.gmail.com'  # Example: Gmail SMTP server
    smtp_port = 587  # Example: Gmail SMTP port
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        logging.info('Email sent successfully.')
        return True
    except Exception as e:
        logging.warning('Failed to send email. Error:', e)
        return False


fail_flag = 0


try:
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
  item_path = '2024 New Jobs - TCH'
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
  print(data_to_upload["Job Owner"].value_counts())
  logging.info("Loaded in data - done!")
  logging.info(data_to_upload)
  logging.info(data_to_upload["Budgeted Start Date"])

  # Code to import reqs to TR
  #data_to_upload["Salary High"] = data_to_upload["Salary High"].apply(lambda x: x.replace(",",""))
  #logging.info("Salary Changed!")


  data_to_upload["Budgeted Start Date"] =data_to_upload["Budgeted Start Date"].fillna('01-01-1901 0:00')
  logging.info(data_to_upload["Budgeted Start Date"])
  data_to_upload["Budgeted Start Date"] = pd.to_datetime(data_to_upload["Budgeted Start Date"],format = 'mixed').dt.strftime("%Y-%m-%d")
  logging.info("Date Changed")



  data_to_upload["FLSA"] = data_to_upload["FLSA"].fillna("Non-Exempt")
  logging.info("FLSA Changed!")
  logging.info(data_to_upload["FLSA"].value_counts())

  data_to_upload = data_to_upload.fillna()
  
  data_to_upload["Position Number"] = data_to_upload["Position Number"].astype(int)
  data_to_upload["Salary High"] = pd.to_numeric(data_to_upload["Salary High"],errors = 'coerce')
  data_to_upload["Salary High"] = data_to_upload["Salary High"].astype(int)
  logging.info("Salary Changed!")
  data_to_upload["Contact"] = data_to_upload["Hiring Manager"].apply(api_findContact)
  logging.info("Contact Changed!")
  data_to_upload['Job Owner'] = data_to_upload['Job Owner'].apply(api_findUser)
  logging.info("Owner Changed!")
  data_to_upload["Coach"] = data_to_upload["Coach"].apply(api_findUser)
  logging.info("Coach Changed!")
  data_to_upload["Project"] = data_to_upload["Project"].fillna(" ")  
  logging.info("Filled NA's in project")

  

  logging.info("Done with prep for :")
  logging.info(data_to_upload.shape[0])
  logging.info("reqs")

  
  for i,el in data_to_upload.iterrows():
      logging.info(f"Importing row {i} of {data_to_upload.shape[0]}")
      logging.info(el)

      a = el["Record Type"]
      if a == 'RPO':
        logging.info("CHANGING RPO")
        logging.info(data_to_upload.at[i,'Record Type'])
        data_to_upload.at[i,"Record Type"] = "01237000000RWvbAAG"
        logging.info("DONE CHANGING RPO")
      if a == 'Pipeline':
        logging.info("CHANGING PIPELINE")
        data_to_upload.at[i,'Record Type'] = "01237000000RXdjAAG"
        logging.info("DONE CHANGING PIPELINE")
  
  logging.info(data_to_upload)
  
  for el in data_to_upload.iterrows():
    try:
      api_ImportRow(el)
    except Exception as ex:
      fail_flag = 1
      logging.info(ex)
      break
  logging.info("Done attempting to import new reqs...")
  if fail_flag == 0:
      logging.info('Req imported sucessfully')
      #sendEmail("TCH Reqs imported into TR successfully!")
except Exception as e:
  logging.warning("TCH Reqs failed import into TR")
  logging.warning(e)
  #sendEmail(f"TCH Reqs failed import into TR. Details: {e}")

if fail_flag == 1:
  logging.warning("TCH Reqs failed import into TR")
  logging.warning(ex)
  #sendEmail(f"TCH Reqs failed import into TR. Details: {e}")
