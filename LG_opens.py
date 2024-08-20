import logging
logging.basicConfig(filename='lg_opens_logging.log', level=logging.DEBUG,format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")

# Import needed functionality
from logging import exception
#from types import NoneType
import pandas as pd
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

def genDate():
    today = date.today()
    day = str(today.day)
    month = str(today.month)
    year = str(today.year)
    if len(month) == 1:
        month = "0"+month
    if len(day) == 1:
        day = "0"+day
    return year+month+day

try:
    os.chdir('opensprocessing')
    logging.info(os.getcwd())
except Exception as e:
    logging.warning(e)
# Find files that we need in current directory / folder
list_files = os.listdir()
for el in list_files:
    if "AW LG Jobs" in el:
        tr_file = el
    if "report1" in el:
        tr_file = el
    if "RequisitionsReport-Component" in el:
        lg_file = el

# Read in data and find list of reqs that TR has not seen 
df_tr = pd.read_csv(tr_file,encoding = 'iso-8859-1')
df_lg = pd.read_csv(lg_file, skiprows=2)
#df_lg = pd.read_excel(lg_file, skiprows=2)

logging.info("Done reading in files!")

list_of_tr_reqs = list(df_tr["Client Req Number"])
list_of_lg_reqs = list(df_lg["Job Req ID"])
fix_list = []

logging.info("Searching for non-numeric reqs...")
print(list_of_tr_reqs)
for el in list_of_tr_reqs:
    print(el)
    m = re.search('(\D)',el)
    #print(el,m)
    if m != None:
        logging.info(f"\tfound: \t{el}")
        #print(f"\tfound: \t{el}")
    
    else:
        fix_list.append(int(el))

logging.info("Number of reqs from TR:",len(list_of_tr_reqs))
logging.info("Number of reqs from TR (minus those that are not numeric):",len(fix_list))
list_of_tr_reqs = fix_list
unseen_reqs = list(set(list_of_lg_reqs) - set(list_of_tr_reqs))

# Go row by row in LG data and build TR-compliant dataframe from the data
df_output = pd.DataFrame(columns = ["Company", "Department Number","Client Req Number",	"Job Name","Record Owner","Contact","Account Manager","Customer Agreement","Regional Area","State/Area","FLSA","Pay Grade","Salary High","Hiring Manager","Req Status"])
for row in df_lg.iterrows():
    
    company = "Landis+Gyr"
    dept_number = row[1]["Region"]
    client_req_num = row[1]["Job Req ID"]
    job_name = row[1]["External Title"]
    contact = row[1]["Hiring Manager Comment"]
    account_manager = "Elise Warren"
    customer_agmt = "AGM-06162022-236"
    
        # Finding Region from Location data
    regional_area = row[1]["Location"]
    l = re.findall(r'\w+', regional_area)
    regional_area = l[0]

        # Finding State from Location data
    state = l[1]
    if state == "USA_REM":
        state = "REMOTE"
    flsa = "Exempt"
    pay_grade = row[1]["Local Pay Grade"]


    salary_high = row[1]["Salary Range"]
    m = re.findall(r'(\d+,\d+)',salary_high)
    try:
        logging.info([(int(m[0].replace(',',"")) + int(m[1].replace(',',"")))/2,m])
        salary_high = (int(m[0].replace(',',"")) + int(m[1].replace(',',"")))/2
        #logging.info("INSERTED UPDATED SALARY HIGH")
    except:
        n = re.findall(r'(\d+)',salary_high)
        logging.info("BAD SALARY INFO")
        logging.info([(int(n[0])+int(n[1])) / 2,n])
        salary_high = (int(n[0])+int(n[1])) / 2


    hiring_manager = row[1]["Hiring Manager"]
    req_status = row[1]["Requisition Status"]
    job_n = f"{job_name} - {client_req_num}"
    
    if row[1]["Recruiter"].__contains__("Raper"):
      record_owner = "Michael Raper"
    elif row[1]["Recruiter"].__contains__("Patterson"):
      record_owner = "Chris Patterson"
    else:
      record_owner = row[1]["Recruiter"]

    dict = {
        "Company": [company],
        "Department Number": [dept_number],
        "Client Req Number": [client_req_num],
        "Job Name": [job_n],
        "Record Owner": [record_owner],
        "Contact": [contact],
        "Account Manager": [account_manager],
        "Customer Agreement": [customer_agmt],
        "Regional Area": [regional_area],
        "State/Area": [state],
        "FLSA": [flsa],
        "Pay Grade": [pay_grade],
        "Salary High": [salary_high],
        "Hiring Manager": [hiring_manager],
        "Req Status": [req_status],
        "Record Type": "0126R000001NlRCQA0"
    }


    df_working = pd.DataFrame(dict)
    df_output = pd.concat([df_output,df_working])

# Finally, export dataframe to csv after finding the open unseen reqs

#   Code to short-circut and print just one req:
#shortcircut = [14986]
#all_new_reqs = df_output[df_output["Client Req Number"].isin(shortcircut)]

all_new_reqs = df_output[df_output["Client Req Number"].isin(unseen_reqs)]
new_reqs_for_tr = all_new_reqs[all_new_reqs["Req Status"] == "Open"]
new_reqs_for_tr["Coach"] = "Elise Warren"

warning_message = ""
for row in new_reqs_for_tr.iterrows():
	if (row [1]["Salary High"] < 60000) or (row[1]["Salary High"] > 300000):
		warning_message = "Req found with salary outside of salary range, please review"

logging.info(f"Working on file with { new_reqs_for_tr.drop('Req Status',axis = 1).shape[0] } rows...")
#   Code to exclude certain reqs
exclusion_list = [17935,14701,18139]
logging.info("Excluding these reqs from the export: ",exclusion_list)
new_reqs_for_tr = new_reqs_for_tr[~new_reqs_for_tr["Client Req Number"].isin(exclusion_list)]

logging.info(f"Generating file with { new_reqs_for_tr.drop('Req Status',axis = 1).shape[0] } rows...")



# Define your email credentials
sender_email = 'kinetixopensprocessing@gmail.com'
sender_password = 'ttljtrsnsqlhmnrz'
receiver_email = ['DART@kinetixhr.com','kinetixopensprocessing@gmail.com','awhelan@kinetixhr.com', 'ewarren@kinetixhr.com', 'bgauthier@kinetixhr.com','jhutchins@kinetixhr.com']
#receiver_email = ['awhelan@kinetixhr.com']
subject = f'New LG Opens for {genDate()}'
body = f'Good Morning folks. {warning_message} Jobs have been loaded into the following Sharepoint Folder for approval before automatic loading later today (6PM Eastern). Please review and approve before then. https://kinetixhr.sharepoint.com/:f:/r/sites/KinetixCoaches/Shared%20Documents/Daily%20New%20Job%20Opens/L+G?csf=1&web=1&e=QTyoXR'

# Create the email message
msg = MIMEMultipart()
msg['From'] = sender_email
msg['To'] = ", ".join(receiver_email)
msg['Subject'] = subject
msg.attach(MIMEText(body, 'plain'))




# Connect to the SMTP server and send the email
smtp_server = 'smtp.gmail.com'      # Example: Gmail SMTP server
smtp_port = 587                     # Example: Gmail SMTP port



if new_reqs_for_tr.drop('Req Status',axis = 1).shape[0] == 0:
    print("No new reqs today.")
    body = 'Good Morning folks. No new L+G Reqs have been found today.'
    
    # Create the email message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_email)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        logging.info('Email sent successfully.')
    except Exception as e:
        logging.warning('Failed to send email. Error:', e)

else:
    new_reqs_for_tr.drop("Req Status",axis = 1).to_csv(f"LG {genDate()}.csv",index = False)
    filename = f'LG {genDate()}.csv'     # Replace with the name of your file
    with open(filename, 'rb') as attachment:
        file = MIMEBase('application', 'octate-stream')
        file.set_payload(attachment.read())
        encoders.encode_base64(file)
        file.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(file)

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        logging.info('Email sent successfully.')
    except Exception as e:
        logging.warning('Failed to send email. Error:', e)
