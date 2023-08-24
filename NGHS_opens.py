import logging
logging.basicConfig(filename='nghs_opens_logging.log', level=logging.DEBUG,format='%(levelname)s %(asctime)s %(message)s')
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
import numpy as np
import warnings

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

def req_finder(req_num):
    '''
    This function takes a req number, and searches for it in each dataset. It then returns whether it was found in one or both datasets.
    
    I cannot use my secret magic set(set(A) - set(B)) here because reqs are named slightly differently in each dataset. 
    However, this code can be improved, I'm sure - though I am capturing all reqs with this (somehow!!)
    '''
    #print("FINDING: ",req_num, type(req_num))
    res = []
    for el in tr_opens_fix:
        if el == req_num:
            res.append(["TR",el])
    for el in wd_opens:
            if el.__contains__(req_num) == True:
                res.append(["WD",el])
    return res

def nghs_name_transformer(name):
    '''
    Many Job Titles at NGHS look like JR_102714 RN Transfer Specialist - PRN (Filled), 
    which then need to be transformed into their base name, RN Transfer Specialist. 
    This function accomplishes that, while handling some edge cases.
    '''
    first_part = name.split("-")[0:2]
    first_part = " ".join(first_part)
    listed_name = first_part.split(" ")[1:]
    fixed_name = " ".join(listed_name)
    #print(name,"||",listed_name,"\n")
    if "(" in fixed_name:
        fixed_name = fixed_name.split("(")[0]
    if fixed_name[-1] == " ":
        fixed_name = fixed_name[:-1]
    return fixed_name



# This is who the email will be sent to. Putting it up here to quickly debug the script without spamming folks. 
receiver_email = ['kinetixopensprocessing@gmail.com', 'awhelan@kinetixhr.com','pvelusamy@kinetixhr.com', 'bgauthier@kinetixhr.com','sschmitt@kinetixhr.com','jhutchins@kinetixhr.com']#, 'ehenschel@kinetixhr.com']
#receiver_email = ['awhelan@kinetixhr.com']



kinetix_recruiters = [
    #'Brandi McDaniel (56837)[C]',
    #'Chloe Gaines (57912)[C]',
    #'Dara Lora (56851)[C]',
    #'Elizabeth Brodeur (56838)[C]',
    #'Francis Williams (56852)[C]',
    #'Justine Clubb (57028)[C]',
    #'Kristin Schmaltz (56847)[C]',
    #'Mikale Jackson (57335)[C]',
    #'Natascha Carpenter (58448)[C]',
    'Vanessa Robinson (56857)[C]',
    'Emily Henschel (56842)[C]',
    ]

tina_walden_hms = [
    'Heather Mitchum',
    'Matthew McKinney',
    'Lucia Gristina',
    'Kaleigh Martin',
    'Katie Collins',
    'Tina Walden',
]
try:
    os.chdir('opensprocessing')
    logging.info(os.getcwd())
except Exception as e:
    logging.warning(e)
list_files = os.listdir()
logging.info(list_files)
for el in list_files:
    if "NGHS Jobs activity" in el:
        tr_file = el
    if "report1" in el:
        tr_file = el
    if 'REC - Job Requisition & Job Application'in el:
        wd_file = el



with warnings.catch_warnings(record=True):
    warnings.simplefilter("always")  # Get rid of annoying warnings in output (so that others can use this script without worrying about warnings)

    excluded_reqs =  ['R1123', 'R797', 'R1387', 'R1348', 'R1326', 'R1506', 'R2111', 'R169']

    df_workday = pd.read_excel(wd_file,skiprows=4)
    df_workday.columns = ['Job Requisition ID', 'Job Requisition', 'Recruiting Start Date','Job Code', 'Job Code.1',
        'NGHS Job Class', 'Recruiter', 'Hiring Manager',
        'Supervisory Organization', 'Compensation Grade',
        'All Candidates for Job Requisition', 'Candidate ID', 'Candidate',
        'Job Application Source', 'Job Application', 'Application Date',
        'Hire Date', 'Job Requisition Status', 'Offer Event',
        'Offer Accepted Date', 'Candidate Stage', 'Internal/External',
        'Offer Declined Date', 'Disposition']
    
    print("Done loading in files!")

    df_workday = df_workday[df_workday["Job Requisition"].str.contains("(Open)")]
    df_workday = df_workday[df_workday["Recruiter"].isin(kinetix_recruiters)]
    df_workday["Job Class ID"] = df_workday["NGHS Job Class"].str.split(" - ").str[0]
    df_tr = pd.read_csv(tr_file,encoding = "ISO-8859-1")

    tr_opens = list(df_tr["Client Req Number"].values)
    tr_opens_fix = []
    wd_opens = list(set((df_workday['Job Requisition ID'].values)))
    #print(tr_opens)

    for el in tr_opens:
        if str(el).__contains__("R") != True and len(str(el)) > 4:
            tr_opens_fix.append("JR_"+el)
        else:
            tr_opens_fix.append(el)


    # Start Code to find reqs that are not in TR yet.
    in_common = 0
    not_in_common = 0
    nic_list = [] # nic = not in common
    
    # using the list of workday opens here is important, it saves us from having to do extra work looking at TCH reqs.
    for el in wd_opens: 
        # if req_finder gives two results, then the req is in both datasets (we don't care about it)
        if len(req_finder(el)) > 1:
            in_common += 1
        else:
            # We care about these reqs, as they are only in Workday, and so need to be loaded into TR. 
            not_in_common +=1
            nic_list.append(el)
    # End code to find reqs that are not in TR yet

    # If excluded reqs need to be taken into account, this code deals with them. 
    pre_exclude = pd.DataFrame(nic_list, columns = ["Req ID"])
    req_list = pre_exclude[~pre_exclude["Req ID"].isin(excluded_reqs)]["Req ID"].values
    
        
    # Start to assemble output dataframe
    df_results = df_workday[df_workday["Job Requisition ID"].isin(req_list)]

    df_results = df_results[["Job Requisition ID","Job Requisition","Recruiter","Hiring Manager","Compensation Grade","NGHS Job Class","Supervisory Organization","Job Class ID"]]
    df_results["Account Manager"] = 'Lisa Cimorelli'
    df_results["Customer Agreement"] = 'AGM-03182022-233'
    df_results["Company"] = 'Northeast Georgia Health System'
    df_results["Regional Area"] = 'Gainesville'
    df_results["State/Area"] = 'GA'

    df_salary = pd.read_csv('salaryhigh2023_7.csv')
    df_salary = df_salary[['Compensation Grade','Job Class ID','MIN','MID','MAX','FLSA']]
    inner_join = pd.merge(df_results, 
                        df_salary, 
                        on =['Compensation Grade',"Job Class ID"], 
                        how ='left')


    inner_join.columns = ['Client Req Number', 'Job Name', 'Record Owner', 'Contact',
       'Pay Grade', 'Job Family', 'Department Number', "Job Class ID",    
       'Account Manager', 'Customer Agreement', 'Company', 'Regional Area',   
       'State/Area', 'Salary Min',"Salary Mid","Salary High", 'FLSA']
    

    # Go row-by-row and transform data
    inner_join = inner_join.drop_duplicates()
    for el in inner_join.iterrows():
        txt3_1 = nghs_name_transformer(el[1]["Job Name"])
        # Remove numbers from Recruiter names 
        txt = el[1]["Record Owner"]
        x = re.split("[(\d)]", txt)
        # Remove numbers from Hiring Manager names
        txt2 = el[1]["Contact"]
        x2 = re.split("[(\d)]", txt2)
        
        #Cut down on Job Title with this regex: /(JR_\d*)|(R\d*)/m
        # Append Client Req Number to Job Title Field
        txt3_1 = nghs_name_transformer(el[1]["Job Name"])
        txt3 = el[1]["Job Name"].replace(str(el[1]["Client Req Number"]),"")
        txt3 = txt3_1 + " - " + str(el[1]["Client Req Number"])
        
        # Set Names
        inner_join.at[el[0],"Record Owner"] = x[0][:-1]
        inner_join.at[el[0],"Contact"] = x2[0][:-1]
        inner_join.at[el[0],"Job Name"] = txt3

        # This Job Class needs to be forced.
        if el[1]["Job Class ID"] == "R06" or el[1]["Job Class ID"] == "P13":
            inner_join.at[el[0],"FLSA"] = "NON-EXEMPT"

        # Hard code two names as they exist differently in TR   
        if el[1]["Record Owner"] == "Elizabeth Brodeur (56838)[C]":
            inner_join.at[el[0],"Record Owner"] ="Beth Brodeur"
        
        if el[1]["Record Owner"] == "Justine Clubb (57028)[C]":
            inner_join.at[el[0],"Record Owner"] ="Paige Clubb"

        # Set FLSA to NON-EXEMPT if not blank
        try:
            if len(el[1]["FLSA"]) < 1:
                continue
        except:
            inner_join.at[el[0],"FLSA"] = "NON-EXEMPT"

        # !IMPORTANT! Setting Salary midpoint in salary high field
        try: 
            if len(el[1]["Salary Mid"]) < 1:
                inner_join.at[el[0],"Salary Mid"] = inner_join.at[el[0],"Salary Mid"] * 2080
        except:
            # I know that the intended path is nestled in the except block and that is BAD, but that's how we're doing it here. 
            inner_join.at[el[0],"Salary Mid"] = df_salary[df_salary["Compensation Grade"] == el[1]["Pay Grade"]]["MID"].values[0] * 2080
        
        
    # Trying to get the Tina Walden HM's working here, not sure if it is... And don't think it's needed anymore!
    inner_join["Hiring Manager"] = inner_join["Contact"].apply(lambda x: x == "Tina Walden" if x in tina_walden_hms else "")
    # Forcing Record Type to ID
    inner_join["Record Type"] = "0126R000001NlRCQA0"
    # Forcing Salary High to equal what is in Salary Mid
    inner_join["Salary High"] = inner_join["Salary Mid"]

# Deal with any duplicates
for el_ in inner_join.iterrows():
    if inner_join[inner_join["Client Req Number"] == el[1]["Client Req Number"]].shape[0] > 1:
        if el[1]["FLSA"] == "EXEMPT":
            inner_join.drop(el[0], inplace=True)


# Dropping columns and exporting dataframe to CSV
inner_join = inner_join.drop(["Salary Min","Salary Mid"],axis = 1)
inner_join["Salary High"] = inner_join["Salary High"].apply(lambda x: float(x[1:]) * 2080)
inner_join["Coach"] = "Sharon Schmitt"
inner_join.drop_duplicates().to_csv(f"NGHS {genDate()}.csv",index = False)
logging.info(f"Done! Exported csv file with {inner_join.drop_duplicates().shape[0]} rows.")

if inner_join.shape[0] > 0:

    #print(f"Done! Exported csv file with {inner_join.drop_duplicates().shape[0]} rows.")

    if inner_join["Client Req Number"].unique().shape[0] != inner_join.drop_duplicates().shape[0]:
        print("Duplicates were detected.")
        dupe_sent = "NOTE: There may be some duplicates in this file! Please check!"
    else:
        dupe_sent = ""

    ####################
    # EMAIL CODE BELOW #
    ####################

    # Define your email credentials
    sender_email = 'kinetixopensprocessing@gmail.com'
    sender_password = 'ttljtrsnsqlhmnrz'
    subject = f'New NGHS Opens for {genDate()}'
    body = f'Good Morning folks. Jobs have been loaded into the follwing Sharepoint Folder for approval before automatic loading later today (6PM Eastern). Please review and approve before then. {dupe_sent}. https://kinetixhr.sharepoint.com/:f:/r/sites/KinetixCoaches/Shared%20Documents/Daily%20New%20Job%20Opens/NGHS?csf=1&web=1&e=r3xCNz'

    # Create the email message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_email)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))


    filename = f'NGHS {genDate()}.csv'  # Replace with the name of your file
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



else:
    logging.info("No new reqs to export.")

    sender_email = 'kinetixopensprocessing@gmail.com'
    sender_password = 'ttljtrsnsqlhmnrz'
    subject = f'No New NGHS Opens for {genDate()}'
    body = f'Good Morning folks. There are no new reqs for NGHS today.'

    # Create the email message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_email)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))


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

