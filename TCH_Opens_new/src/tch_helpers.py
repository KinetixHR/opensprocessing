"""Module for TCH Helper Functions, used in TCH_*.py files"""

import os
import urllib
import logging
from datetime import date
import atexit
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import requests
import msal
import pandas as pd
from simple_salesforce import Salesforce


def gen_date():
    """Generates today's date in a format needed for file names"""
    today = date.today()
    day = str(today.day)
    month = str(today.month)
    year = str(today.year)
    if len(month) == 1:
        month = "0"+month
    if len(day) == 1:
        day = "0"+day
    return year+month+day

def check_for_reqs(req_number,dframe):
    """Checking function to see if a req number exists in a list. Helpful for debugging."""
    try:
        if req_number in dframe['Requisition Number'].values:
            print(f"Found {req_number}!")
        else:
            print(f"{req_number} not found")
    except Exception as err:
        if req_number in dframe['Client Req Number'].values:
            print(f"Found {req_number}!")
            print(err)
        else:
            print(f"{req_number} not found")
            print(err)

def tch_name_transformer(name):
    """Many Job Titles at TCH look like Registered Nurse - 1st Year Inpatient Dialysis, which then need to be transformed into their base name, Registered Nurse . This function accomplishes that, while handling some edge cases."""
    name = str(name)
    if "-" in name:
        fixed_name = name.split("-",maxsplit=1)[0]
    else:
        fixed_name = name
    if fixed_name[-1] == " ":
        fixed_name = fixed_name[:-1]
    
    return fixed_name

def login_to_sharepoint_via_azure(client_id,authority,scopes,endpoint):
    """Function to log into Azure via the token_cache.bin file/method. This is almost entirely boilerplate code."""
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

    app = msal.PublicClientApplication(client_id, authority=authority, token_cache=cache)

    accounts = app.get_accounts()
    result = None
    if len(accounts) > 0:
        result = app.acquire_token_silent(scopes, account=accounts[0])
    if len(accounts) == 0:
        logging.warning("Something went wrong with login - likely a token issue")
    #print(result)
    logging.info(result)
    access_token = result['access_token']
    if result is None:
        flow = app.initiate_device_flow(scopes=scopes)
        if 'user_code' not in flow:
            raise Exception('Failed to create device flow')

        print(flow['message'])

        result = app.acquire_token_by_device_flow(flow)

    if 'access_token' in result:
        result = requests.get(f'{endpoint}/me', headers={'Authorization': 'Bearer ' + result['access_token']})
        result.raise_for_status()
        print(result.json())

    else:
        logging.warning("No Access Token in Result.")
        raise Exception('no access token in result')
    
    return access_token

def get_to_sharepoint_folder_in_coaches_site(item_path_,sharepoint_host_name,site_name,access_token,endpoint):
    """Navigates to the coaches sharepoint site - implementation is not general and should only be used to get to coaches site. Returns data to progress the Sharepoint login/data get process."""
    result = requests.get(f'{endpoint}/sites/{sharepoint_host_name}:/sites/{site_name}', headers={'Authorization': 'Bearer ' + access_token},timeout=20)
    site_info = result.json()
    site_id = site_info['id']
    #print(site_id)
    result = requests.get(f'{endpoint}/sites/{site_id}/drive', headers={'Authorization': 'Bearer ' + access_token},timeout=20)
    drive_info = result.json()
    drive_id =  drive_info['id']
    print(drive_info)

    item_path = item_path_
    item_url = urllib.parse.quote(item_path)
    result = requests.get(f'{endpoint}/drives/{drive_id}/root:/{item_url}', headers={'Authorization': 'Bearer ' + access_token},timeout=20)
    item_info = result.json()
    folder_id = item_info['id']
    return [drive_id, folder_id]

def download_from_sharepoint(file_path_,endpoint,drive_id,access_token):
    """Downloads the file from Sharepoint, by dynamically finding the file in a folder and then returning a df"""
    # Dynamically get file path and id for download
    file_path = file_path_
    file_url = urllib.parse.quote(file_path)
    result = requests.get(f'{endpoint}/drives/{drive_id}/root:/{file_url}', headers={'Authorization': 'Bearer ' + access_token},timeout=20)
    file_info = result.json()
    file_id = file_info['id']

    # Download file locally
    result = requests.get(f'{endpoint}/drives/{drive_id}/items/{file_id}/content', headers={'Authorization': 'Bearer ' + access_token},timeout=20)
    open(file_info['name'], 'wb').write(result.content)
    

    try:
        sharepoint_data = pd.read_csv(file_info['name'], encoding = "ISO-8859-1")
    except Exception as e:
        print(e)
        logging.info(e)
        sharepoint_data = pd.read_excel(file_info['name'],engine = 'openpyxl')
    
    return sharepoint_data

def send_email(test_receiver_email_address = "",test_receiver_email_flag = 0,error = 0,error_text = ""):
    """
    send an email, using a flag (default 0) to turn on test email functionality (provide your email!)
    """


    # Define your email credentials
    sender_email = 'kinetixopensprocessing@gmail.com'
    sender_password = 'ttljtrsnsqlhmnrz'

    if (test_receiver_email_flag == 0) and (error == 0):
        receiver_email = ['DART@kinetixhr.com','CFisher@kinetixhr.com']
        subject = f'TCH Opens for {gen_date()}'
        body = 'Hey CJ! Please find the TCH Opens for today. Please note that this has been generated by the new TCH Opens scripting, and may have errors.' 

    if (test_receiver_email_flag != 0) and (error == 0):
        receiver_email = [test_receiver_email_address]
        subject = '!TCH OPENS TESTING EMAIL!'
        body = 'TESTING THE NEW TECH OPENS'
    
    if error != 0:
        receiver_email = ['DART@kinetixhr.com','CFisher@kinetixhr.com']
        subject = 'FAILURE in TCH Opens Processing'
        body = f'There has been a failure in the TCH Opens processing. Failure point is here: {error_text}'        


    # Create the email message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_email)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))


    filename = f'TCH {gen_date()}.xlsx'  # Replace with the name of your file
    with open(filename, 'rb') as attachment:
        file = MIMEBase('application', 'octate-stream')
        file.set_payload(attachment.read())
        encoders.encode_base64(file)
        file.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(file)

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
    except Exception as e:
        logging.warning(f'Failed to send email. Error: {e}')

def remove_files():
    """
    Removes files that were needed to run the opens
    """
    logging.info("Removing files from local disk...")
    for el in os.listdir():
        logging.info(el)
    if "TCH 2" in el:
        os.remove(el)
        logging.info(f"Removed file! {el}")
    if "report1" in el:
        os.remove(el)
        logging.info(f"Removed file! {el}")
    if "Output1" in el:
        os.remove(el)
        logging.info(f"Removed file! {el}")

def get_coach_for_recruiter(recruiter_name,recruiter_first_name,deep_search = 0):
    """
    Search for Recruiter Name in Salesofrce, adn return both name and Coach
    """

    session = requests.Session()
    sf = Salesforce(password='Kinetix2Password', username='salesforceapps@kinetixhr.com', organizationId='00D37000000HXaI',client_id='My App',session = session)
    if deep_search == 0:
        query_statement = f"""SELECT Name, Coach__c FROM User WHERE LastName LIKE '{recruiter_name}' AND IsActive = True"""
        print("Searching with this query... ",query_statement)
    if deep_search == 1:
        query_statement = f"""SELECT Name, Coach__c FROM User WHERE LastName LIKE '{recruiter_name}' AND FirstName LIKE '{recruiter_first_name}' AND IsActive = True"""
        print("Searching with this query... ",query_statement)
 

    fetch_results = sf.bulk.User.query_all(query_statement, lazy_operation=True)
    
    all_results = []
    for list_results in fetch_results:
        all_results.extend(list_results)
        df = pd.DataFrame(all_results)
    try:
        df = df.drop(columns=['attributes'])
    except Exception as e:
        #print("Atribute not in API result.")
        pass
    #print(df)

    return df

def genDate2():
    '''
    Generate date in a format that is ready to be included in a filename, like
    NGHS 20YYMMDD
    TCH: M.DD.20YY
    '''
    today = date.today()
    day = str(today.day)
    month = str(today.month)
    year = str(today.year)[-2:]
    return month+"."+day+"."+year

recruiters = [
    'Amber Gurley',
    'Brett Meador T',
    #'Kara Robinson L',
    #'Jacklyn Hester',
    #'Sheree Herrmann R',
    #'Brandi McDaniel L',
    #'Sylvia Cheung K',
    #'Matt Havins K',
    #'Se Young Han',
    #'Matthew Bing J',
    #'Shelly Hitchens',
    'Margaret Eagan V',
    #'Stephanie Coelho A',
    #'Haley Baldwin A',
    'Erica Siri C',
    'Gabriel Konigsberg A',
    #'Malcolm Mayfield J',
    #'Tiffani Wiesenfeld D',
    #'Samantha Bevis A',
    #'Tiffany Jones J',
    #'Joseph Babcock',
    'Jordan Davison R',
    #'Joshua Hutchins',
    'Sreemayee Roy',
    'Ellen Bookout D',
    'McKenzie Cunningham K',
    'Yanela Sanchez',
    'Nadeja Lewis',
    'Tiffany Davis S',
    'America Espinosa J',
    'Amanda Gates',
    'Maria Camarena G',
    'Tashona Domeaux R',
    'Khalilah Jones D',
    'Amanda Gates',
    'Claudia Scott ',
    "Nadeja Lewis",
    'Karina Rauda',
    'Crystal Williams C', 
    #"Clarence Fisher V",
    "Tashona Domeaux",
    "Ellen Bookout",
    "Khalilah Jones",
    "Domeaux, Tashona R",
    'Bookout, Ellen D',
    'Jones, Khalilah D',
    #'Brodeur, Elizabeth'
    #'Jackson, Mikale',
    'Eagan, Margaret',
    'Margaret Eagan',
]