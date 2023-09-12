import logging
logging.basicConfig(filename='tch_opens_logging.log', level=logging.DEBUG,format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")

import pandas as pd
import numpy as np
import datetime as dt
from datetime import date 
import os
from zipfile import ZipFile
import shutil
import sqlite3
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


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

def searchEMPdb(df_names):
    conn = sqlite3.connect('EID_numbers.db')
    
    el = df_names

    search_for = el.split()[1]
    search_for_first = el.split()[0]

    df_searched = pd.read_sql_query(f"SELECT * FROM mytable WHERE LastName LIKE '%{search_for}%'", conn)    

    if df_searched.shape[0] != 1:
        df_searched = pd.read_sql_query(f"SELECT * FROM mytable WHERE LastName LIKE '%{search_for}%' AND FirstName LIKE '%{search_for_first}%'", conn)    
    
    try:
        to_ret = df_searched["PaychexEmpID"].values[0]
    except:
        to_ret = None

    conn.close()
    return to_ret

def checkForReqs(reqNumber,dframe):
    try:
        if reqNumber in dframe['Requisition Number'].values:
            print(f"Found {reqNumber}!")
        else:
            print(f"{reqNumber} not found")
    except:
        if reqNumber in dframe['Client Req Number'].values:
            print(f"Found {reqNumber}!")
        else:
            print(f"{reqNumber} not found")


def tch_name_transformer(name):
    '''Many Job Titles at TCH look like Registered Nurse - 1st Year Inpatient Dialysis, which then need to be transformed into their base name, Registered Nurse . This function accomplishes that, while handling some edge cases.'''
    name = str(name)
    if "-" in name:
        fixed_name = name.split("-")[0] 
    else:
        fixed_name = name
    if fixed_name[-1] == " ":
        fixed_name = fixed_name[:-1]
    
    return fixed_name



    
find_last_week = date.today() - dt.timedelta(days=100)

# Find files that we need in current directory / folder
try:
    os.chdir('opensprocessing')
    logging.info(os.getcwd())
except Exception as e:
    logging.warning(e)
list_files = os.listdir()
for el in list_files:
    if "TCH Jobs activity" in el:
        logging.info(el)
        tr_file = el
    if "report1" in el:
        logging.info(el)
        tr_file = el
    if "EID" in el:
        logging.info(el)

# Code to spin up a DB at runtime. 
df = pd.read_excel('Employee ID Numbers from Paychex.xlsx')
conn = sqlite3.connect('EID_numbers.db')
df.to_sql('mytable', conn, if_exists='replace', index=False)
conn.commit()
conn.close()

# I use excel to make this sheet - combining the Austin and Houston Salary Data. The Austin Salary Grades should be amended to
# Look like 12Austin instead of just 12 for Houston.
salary_data = pd.read_excel("AustinAndHoustonCombinedSalaryGradeInfo.xlsx",sheet_name='Combined')
salary_data = salary_data[['SG', 'MIN', 'MAX','MID']]


approved_statuses = ['Open - Unposted','Open - Posted','Open - Not Posted','Job formatting - In Progress']


# MAKE THIS STUPID LIST GO AWAY SOMEHOW IN THE FUTURE.

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
    #'Jackson, Mikale'

]

df_tr = pd.read_csv(tr_file,encoding = "ISO-8859-1")
#df_tch = pd.read_excel("Output1.xls",skiprows = 2)
df_tch = pd.read_excel("Output1.xlsx")
df_tch.columns = ['Requisition Title', 'Requisition Number', 'Job Requisition ID',
       'Pipeline Requisition ID', 'Pipeline Requisition Number',
       'Recruiting Type', 'Requisition Type', 'Recruiter',
       'Hiring Manager First Name', 'Hiring Manager Last Name',
       'Requisition Creation Date', 'Creation Date',
       'Job Requisition Open Date', 'Last Update Date', 'Filled Date',
       'Current Phase', 'Current State', 'Budgeted Start Month',
       'Classification', 'Full-Time/Part-Time', 'Initiative Program',
       'Initiative Program (Description)', 'Job Shift', 'Job Type',
       'OPEN_DATE', 'PER_DIEM', 'Regular or Temporary', 'SHIFT_TIMES',
       'WEEKLY_HOURS', 'Job Shift.1', 'Position ID', 'Position Number',
       'Business Unit', 'Department Name', 'Job Code', 'Job Description',
       'Job Family', 'Management Level', 'Number of Job Requisitions',
       'Grade Code', 'Salary Grade', 'Department Id', 'Legal Employer Name',
       'Grade (Description)']
#df_tch = pd.read_excel("TA Master REQS.xlsx")
#checkForReqs("403502", df_tch)

df_tch["Hiring Manager"] = df_tch["Hiring Manager First Name"] + " " + df_tch["Hiring Manager Last Name"]
df_tch["Requisition Status"] = df_tch["Current Phase"] + " - " + df_tch["Current State"]

df_tch["Requisition Number"] = pd.to_numeric(df_tch["Requisition Number"],errors='coerce')
df_tr["Client Req Number"] = pd.to_numeric(df_tr["Client Req Number"],errors='coerce')
df_tch["Creation Date"] = pd.to_datetime(df_tch["Creation Date"])
df_tch = df_tch[df_tch["Requisition Status"].isin(approved_statuses)]
tr_reqs = list(set(df_tr["Client Req Number"]))
tch_reqs = list(set(df_tch['Requisition Number']))
diff = list(set(tch_reqs) - set(tr_reqs))           # Should find new reqs
print(diff)

opens = df_tch[df_tch['Requisition Number'].isin(diff)]
opens = opens[opens["Creation Date"].dt.date >= find_last_week]

opens = opens[~opens["Recruiter"].isin(recruiters)]


'''
# Need these columns:
-----------------------------------------------------------------------
Requisition Number          (Called Requisition Number)
Company	                    (Literal)
Creation Date               (Called Creation Date)
Client Req Number           (Called Requisition Number) (DUPLICATE)
Position Number             (Called Position Number) 
Recruiter	                (Called Recruiter)
Job Name	                (Called Requisition Title)
Contact	                    (Called Hiring Manager)
Department Number	        (Called Department Id)
Department Name	            (Called Department Name)
Pay Grade	                (Called Pay Grade)
Budgeted Start Date	        (Called Budgeted Start Month)
Salary Low	                (Called Salary Low)
Salary High	                (Called Salary High)
Project	                    (Literal)
Shift Information		    (Called Full Time/Part Time)
Customer Agreement	        (Literal)
Account Manager	            (Literal)
FLSA	                    (Called Classification)
State/Area	                (Literal)
Regional Area               (Literal)
'''

opens = opens[[
    'Requisition Number',
    'Position Number',
    "Recruiter",
    "Requisition Title",
    "Department Id",
    "Department Name",
    'Classification',
    'Full-Time/Part-Time',
    "Creation Date",
    "Budgeted Start Month",
    "Hiring Manager",
    "Salary Grade",
    "Business Unit",
]]

opens["Client Req Number"] = opens["Requisition Number"]
opens["Account Manager"] = '00537000004wGaNAAU'
opens["Customer Agreement"] = 'AGM-06212019-143'
opens["Project"] = ""
opens["State/Area"] = 'Texas'
opens['Regional Area'] = 'Houston Metro Area'
opens["Company"] = 'Texas Childrens Hospital'



opens["Pay Grade"] = opens["Salary Grade"].apply(lambda x: str(x).split(' ')[-1] if len(str(x)) > 0 else "NO DATA")

for el in opens.iterrows():
    # Set Austin-related Jobs
    if el[1]["Business Unit"] == "TCH Austin":
        opens.at[el[0],"Project"] = "Austin"
        opens.at[el[0],"Regional Area"] = "Austin-Round Rock-San Marcos Metro Area"
        opens.at[el[0],"Pay Grade"] = el[1]["Pay Grade"] + "Austin"
        print(el[1]["Pay Grade"])

    # Begin weird hacky shift info stuff. 
    if el[1]["Full-Time/Part-Time"].__contains__("ll"):
        opens.at[el[0],'Shift Information'] = "FT"
    if el[1]["Full-Time/Part-Time"].__contains__("Part"):
        opens.at[el[0],'Shift Information'] = "PT"
    else:
        opens.at[el[0],'Shift Information'] = ""
    

    # Get Better Names - by using EMP ID
    
    name = el[1]["Recruiter"]

    if name.__contains__("Hitchens"):
        name = "Shelly Letner"
        opens.at[el[0],"Recruiter"] = name
    opens.at[el[0],"Emp ID"] = searchEMPdb(name)

    if name.__contains__("Bevis"):
        name = "Samantha Coston"
        opens.at[el[0],"Recruiter"] = name
    opens.at[el[0],"Emp ID"] = searchEMPdb(name)

    if el[1]["Recruiter"].__contains__("Fisher"):
        opens.at[el[0],"Recruiter"] = None
        opens.at[el[0],"Emp ID"] = None


    # Fix FLSA
    if el[1]["Classification"] == "Exempt":
        opens.at[el[0],"Classification"] = "Exempt"
    if el[1]["Classification"] == "Nonexempt":
        opens.at[el[0],"Classification"] = "Non-Exempt"
    
    # Look to see if names are too long
    if len(str(el[1]["Requisition Title"])) > 70:
        opens.at[el[0],"Requisition Title"] = el[1]["Requisition Title"].replace(" ","")
        opens.at[el[0],"Requisition Title"] = el[1]["Requisition Title"].replace(",","")
    
    # Fix Hiring Managers
    hm_conv = el[1]["Hiring Manager"].split(" ")
    if len(hm_conv) > 1:
        opens.at[el[0],"Hiring Manager"] = f"{hm_conv[0]} {hm_conv[1]}"
    
    # Get rid of middle initial in Recruiter field
    rec_working = el[1]["Recruiter"].split(" ")
    if len(rec_working) > 2:
        rec_working = rec_working[0:2]
        opens.at[el[0],"Recruiter"] = " ".join(rec_working)


# Thank you Copilot for writing this DF merge!
# Merges on Salary Data sheet. 
opens = pd.merge(opens,salary_data,left_on = "Pay Grade",right_on = "SG",how = "left")

opens["Salary Low"] = opens["MIN"].apply(lambda x: round(x,0))
opens["Salary High"] = opens["MID"].apply(lambda x: round(x,0))
opens["Requisition Title"] = opens["Requisition Title"].apply(lambda x: tch_name_transformer(x))
opens['Requisition Title'] = opens['Requisition Title'].astype("str") + " - " + opens["Requisition Number"].astype('int').astype("str")
opens = opens.drop(['Business Unit', "Salary Grade","SG","MAX", "MIN",'MID'],axis=1)
opens = opens.drop_duplicates()

coaches = pd.read_excel('Coach Mapping.xlsx')
opens = pd.merge(opens,coaches,left_on = "Recruiter",right_on = "TCH Name",how = "left")
print(opens.columns)

opens.columns = ['Requisition Number', 'Position Number', 'Job Owner',
       'Job Name', 'Department Number', 'Department Name',
       'FLSA', 'Shift Information', 'Creation Date',
       'Budgeted Start Date', 'Hiring Manager', 'Client Req Number',
       'Account Manager', 'Customer Agreement', 'Project','State/Area',
       'Regional Area', 'Company','Pay Grade','Shift Information1',"Emp ID", 'Salary Low', 'Salary High',"Full Name","TCH Name","Coach"]

print(opens["Pay Grade"].value_counts())
opens = opens.drop(['Full Name','TCH Name'],axis=1)
# And end wacky hacky shift info stuff. 

opens.drop(['Shift Information1'],axis=1,inplace = True)

opens["Record Type"] = "RPO"

logging.info(f"Done processing, generating file with {opens.shape[0]} rows")

if opens.shape[0] > 1000:
    logging.info("Too many reqs in file! Exiting!")
    quit()

opens.to_csv(f"TCH {genDate()}.csv",index = False)






####################
# EMAIL CODE BELOW #
####################


# Define your email credentials
sender_email = 'kinetixopensprocessing@gmail.com'
sender_password = 'ttljtrsnsqlhmnrz'
#receiver_email = ['kasokan@kinetixhr.com']
receiver_email = ['kinetixopensprocessing@gmail.com','awhelan@kinetixhr.com', 'cfisher@kinetixhr.com', 'pvelusamy@kinetixhr.com','skenney@kinetixhr.com']
subject = 'New TCH Opens'
body = 'Hey CJ, please find todays opens for TCH. NOTE: This is an automated email sent the script that runs the opens processing.'

# Create the email message
msg = MIMEMultipart()
msg['From'] = sender_email
msg['To'] = ", ".join(receiver_email)
msg['Subject'] = subject
msg.attach(MIMEText(body, 'plain'))


filename = f'TCH {genDate()}.csv'  # Replace with the name of your file
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
    logging.warning('Failed to send email. Error:', e)



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
