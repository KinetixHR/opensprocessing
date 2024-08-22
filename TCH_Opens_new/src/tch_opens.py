'''TCH Opens Functionality'''

import logging
import datetime as dt
from datetime import date
import os
import pandas as pd
import tch_helpers as tchhelper

logging.basicConfig(filename='opensprocessing/TCH_Opens_new/logs/tch_opens_logging.log', level=logging.INFO,format='%(levelname)s %(asctime)s %(message)s')
logging.info("Starting Script.")

# Find offset date to limit search
find_offset_date = date.today() - dt.timedelta(days=100)

# Find files that we need in current directory / folder
try:
    logging.info(os.getcwd())
    os.chdir('opensprocessing')
    os.chdir('TCH_Opens_new')
    #os.chdir('working_files')
    logging.info(os.getcwd())
except Exception as e:
    logging.warning(e)
    tchhelper.send_email("awhelan@kinetixhr.com",0,1,str(e))
try:
    os.chdir("./working_files")
    logging.info(os.listdir())
except Exception as e:
    logging.warning(e)
    tchhelper.send_email("awhelan@kinetixhr.com",0,1,str(e))
list_files = os.listdir()
logging.info(list_files)
for el in list_files:
    if "report" in el:
        tr_file = el
        df_tr = pd.read_csv(tr_file, encoding = "ISO-8859-1")
    if "Output1" in el:
        tch_file = el
        df_tch = pd.read_excel(tch_file)

# I use excel to make this sheet - combining the Austin and Houston Salary Data. The Austin Salary Grades should be amended to
# Look like 12Austin instead of just 12 for Houston.
try:
    os.chdir("..")
    os.chdir("""./client_info""")
except Exception as e:
    print(e)
    tchhelper.send_email("awhelan@kinetixhr.com",0,1,str(e))
try:
    print(os.getcwd())
    print(os.listdir())
    salary_data = pd.read_excel("AustinAndHoustonCombinedSalaryGradeInfo.xlsx",sheet_name='Houston')
    salary_data = salary_data[['SG', 'MIN', 'MAX','MID']]
    #salary_data["MIN"] = salary_data["MIN"].astype('str')
    #salary_data["MAX"] = salary_data["MAX"].astype('str')
    #salary_data["MID"] = salary_data["MID"].astype('str')

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

    df_tch["Hiring Manager"] = df_tch["Hiring Manager First Name"] + " " + df_tch["Hiring Manager Last Name"]
    df_tch["Requisition Status"] = df_tch["Current Phase"] + " - " + df_tch["Current State"]
    df_tch["Requisition Number"] = pd.to_numeric(df_tch["Requisition Number"],errors='coerce')
    df_tr["Client Req Number"] = pd.to_numeric(df_tr["Client Req Number"],errors='coerce')
    df_tch["Creation Date"] = pd.to_datetime(df_tch["Creation Date"])
    

    approved_statuses = ['Open - Unposted','Open - Posted','Open - Not Posted','Job formatting - In Progress']

    recr_list = list(df_tch["Recruiter"])

    our_recr_list =[]
    for rec in set(recr_list):

        if "," in rec:
            try:
                name = rec.split(",")[0]
                first_name = rec.split(",")[1]
                full_name = first_name +" "+ name
                if " " in first_name:
                    first_name = first_name.split()[0]

                result_obj = tchhelper.get_coach_for_recruiter(name,first_name,0)
                
                if result_obj.shape[0] > 1:
                    result_obj = tchhelper.get_coach_for_recruiter(name,first_name,1)

                name = result_obj["Name"].values[0]
                coach = result_obj["Coach__c"].values[0]

                our_recr_list.append(rec)
            
            except Exception as e:
                print(f"Cannot find info for {rec}",e)
                if ("Meador" in rec) or ("Eagan" in rec) or ("Siri" in rec):
                    print("Adding non kinetix recruiter: ",rec," to list.")
                    logging.info("Adding non kinetix recruiter: ",rec," to list.")
                    our_recr_list.append(rec)
                if ("Clubb" in rec) or ('Bevis' in rec) or ('Gray' in rec):
                    print("Adding Kinetix Recruiter with changed name: ",full_name, " to list.")
                    logging.info([rec, full_name])
                    our_recr_list.append(rec)
                else:
                    continue

    recruiters = our_recr_list
    logging.info(recruiters)

    df_tch = df_tch[df_tch["Requisition Status"].isin(approved_statuses)]
    tr_reqs = list(set(df_tr["Client Req Number"]))
    tch_reqs = list(set(df_tch['Requisition Number']))
    # Finds new reqs
    diff = list(set(tch_reqs) - set(tr_reqs))

    logging.info("New Reqs Found:")
    logging.info(diff)

    opens = df_tch[df_tch['Requisition Number'].isin(diff)]
    opens = opens[opens["Creation Date"].dt.date >= find_offset_date]
    opens = opens[opens["Recruiter"].isin(recruiters)]
    logging.info(opens["Recruiter"].value_counts())


    '''
    Written in spring 2023: 
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
        "WEEKLY_HOURS",
        "PER_DIEM",
        "Grade Code",
        'Requisition Type'
    ]]

    opens["Client Req Number"] = opens["Requisition Number"]
    opens["Account Manager"] = '00537000004wGaNAAU'
    opens["Customer Agreement"] = 'AGM-06212019-143'
    opens["Project"] = ""
    opens["State/Area"] = 'Texas'
    opens['Regional Area'] = 'Houston Metro Area'
    opens["Company"] = 'Texas Childrens Hospital'
    opens["Pay Grade"] = opens["Grade Code"].astype('str')
    opens["Record Type"] = "RPO"

    opens["FTE"] = ""
    for el in opens.iterrows():
        if el[1]["Requisition Type"] == "Pipeline":
            opens.at[el[0],"Record Type"] = "Pipeline"
        # Set Austin-related Jobs
        # THis works!
        if el[1]["Business Unit"] == "TCH Austin":
            opens.at[el[0],"Project"] = "Austin"
            opens.at[el[0],"Regional Area"] = "Austin-Round Rock-San Marcos Metro Area"
            
            if el[1]["Pay Grade"] == "11":
                opens.at[el[0],"Pay Grade"] = "11E"
                logging.info(f"Changed req number: {el[1]['Client Req Number']} to 11E")        
            
            opens.at[el[0],"Pay Grade"] = el[1]["Pay Grade"] + "Austin"  

        # Format Shift Information column
        if el[1]["Full-Time/Part-Time"].__contains__("Full"):
            opens.at[el[0],'Shift Information'] = "FT"
            opens.at[el[0],"FTE"] = "Full Time"
        elif el[1]["Full-Time/Part-Time"].__contains__("Part"):
            opens.at[el[0],'Shift Information'] = "PT"
            opens.at[el[0],"FTE"] = "Part Time"
        else:
            opens.at[el[0],'Shift Information'] = ""
            
        # Deal with FTE data
        #print("LOOKING AT ",el[1]["Client Req Number"])
        #print(el[1]["WEEKLY_HOURS"])
        #print(el[1]["PER_DIEM"])
        #print(el[1]["Requisition Title"])
        #if (el[1]["WEEKLY_HOURS"] == 40) or (el[1]["WEEKLY_HOURS"] == "40") or (el[1]["Full-Time/Part-time"] == "Full time"):
        #    #print(el[0],"FOUND FULL TIME")
        #    opens.at[el[0],"FTE"] = "Full Time"
        if el[1]["PER_DIEM"] == "Yes":
            opens.at[el[0],"FTE"] = "Per Diem"
            print(el[0],"FOUND PD")
        if "PRN" in el[1]["Requisition Title"]:
            opens.at[el[0],"FTE"] = "PRN"
            print(el[0],"FOUND PRN")
        #elif "Part time" in el[1]["Full-Time/Part-time"]:
        #    opens.at[el[0],"FTE"] = "Part Time"
        #else:
        #    opens.at[el[0],"FTE"] = "Part Time"

        # WRITE CODE TO FIND BETTER EXEMPT/NONEXEMPT 


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
    opens = opens.drop(["Requisition Type"],axis = 1)
    opens = pd.merge(opens,salary_data,left_on = "Pay Grade",right_on = "SG",how = "left")

    #opens["MIN"] = opens["MIN"].replace(['NaN',None,"None"],0).astype('int')
    #opens["MAX"] = opens["MAX"].replace(['NaN',None,"None"],0).astype('int')
    #opens["MID"] = opens["MID"].replace(['NaN',None,"None"],0).astype('int')
    logging.info(opens.columns)
    #opens["Department Id"] = opens["Department Id"].astype('str')
    logging.info(opens["Department Id"])
    opens["Salary Low"] = opens["MIN"].apply(lambda x: round(x,0))
    opens["Salary High"] = opens["MID"].apply(lambda x: round(x,0))
    opens["Requisition Number"] = opens["Requisition Number"].apply(lambda x: round(x,0))
    opens["Requisition Number"] = opens["Requisition Number"].astype('int')
    opens["Requisition Title"] = opens["Requisition Title"].apply(lambda x: tchhelper.tch_name_transformer(x))
    opens['Requisition Title'] = opens['Requisition Title'].astype("str") + " - " + opens["Requisition Number"].astype("str")
    opens = opens.drop(['Business Unit', "Grade Code","SG","MAX", "MIN",'MID'],axis=1)
    opens = opens.drop_duplicates()

    # Loop to assign names and coaches. 
    for el in opens.iterrows():
        name = el[1]["Recruiter"]
        name = str(name)
        #logging.info(name)
        if "," in name:
            # CJ needs to be filtered out of these jobs and his rows blanked, so we handle that first. 
            if "Fisher" not in name:

                if "Bevis" in name:
                    logging.info("Found Samantha Bevis")
                    name = rec.split(",")[0]
                    first_name = rec.split(",")[1]
                    full_name = first_name +" "+ name
                    name_to_use = name
                    backup_name_if_more_info_is_needed = first_name
                    logging.info(full_name,name_to_use,backup_name_if_more_info_is_needed)

                else:
                    name_to_use = name.split(",")[0]
                    backup_name_if_more_info_is_needed = name.split(",")[1].strip()

                try:
                    # search in users table using last name, and find the Recruiter's name and their Coach's Name
                    good_name = tchhelper.get_coach_for_recruiter(name_to_use,backup_name_if_more_info_is_needed,0)
                    if good_name.shape[0] == 1:
                        recruiter_name = good_name["Name"].values[0]
                        coach_name = good_name["Coach__c"].values[0]
                    
                    # If there are multiple results in the users object search, try again with first name
                    elif good_name.shape[0] > 1:
                        good_name = tchhelper.get_coach_for_recruiter(name_to_use,backup_name_if_more_info_is_needed,1)
                        if good_name.shape[0] == 1:
                            recruiter_name = good_name["Name"].values[0]
                            coach_name = good_name["Coach__c"].values[0]

                    else:
                        continue
                    
                    opens.at[el[0],"Recruiter"] = recruiter_name
                    opens.at[el[0],"Coach"] = coach_name

                except ValueError as e:
                    logging.info("Likely found a TCH Recruiter or Hiring Manager %s",e)
                    continue

        if "Fisher" in name:
            opens.at[el[0],"Recruiter"] = " "
            opens.at[el[0],"Coach"] = " "

    logging.info(opens["Recruiter"].value_counts())
    logging.info(opens["Coach"].value_counts())

    for el in opens.iterrows():
        mid = el[1]["Salary High"]
        s_grade = el[1]["Pay Grade"]
        print(mid,s_grade)
        if (len(str(mid)) == 3) or (str(s_grade) == "10"):
            print("No Salary Mid")
            if (s_grade == 10) or (s_grade == "10"):
                print("FOUND SGRADE 10!!")
                sal_low_data = salary_data[salary_data["SG"] == int(s_grade)]["MIN"].values[0]
                sal_high_data = salary_data[salary_data["SG"] == int(s_grade)]["MID"].values[0]
                print(sal_low_data)
                print(sal_high_data)
                opens.at[el[0], "Salary Low"] = sal_low_data
                opens.at[el[0], "Salary High"] = sal_high_data
            if str(s_grade) != 'nan':
                try:
                    sal_low_data = salary_data[salary_data["SG"] == int(s_grade)]["MIN"].values[0]
                except:
                    sal_low_data = None
                    opens.at[el[0], "Salary Low"] = sal_low_data
                try:
                    sal_high_data = salary_data[salary_data["SG"] == int(s_grade)]["MID"].values[0]
                except:
                    sal_high_data = None
                    opens.at[el[0], "Salary High"] = sal_high_data
                print(sal_low_data)
                print(sal_high_data)
                #opens.at[el[0], "Salary Low"] = sal_low_data
                #opens.at[el[0], "Salary High"] = sal_high_data
            else:
                opens.at[el[0], "Salary Low"] = None
                opens.at[el[0], "Salary High"] = None
                opens.at[el[0], "Pay Grade"] = None


    logging.info(opens.columns)
    opens.to_excel("opensextract.xlsx")
    opens.columns = ['Requisition Number', 'Position Number', 'Job Owner',
        'Job Name', 'Department Number', 'Department Name',
        'FLSA', 'Shift Information', 'Creation Date',
        'Budgeted Start Date', 'Hiring Manager',"Salary Grade","Weekly Hours","Per Diem", 'Client Req Number',
        'Account Manager', 'Customer Agreement', 'Project','State/Area',
        'Regional Area', 'Company','Pay Grade',"Record Type","FTE",'Shift Information1', 
        'Salary Low', 'Salary High',"Coach"]

    opens = opens.drop(["Weekly Hours","Per Diem"],axis=1)
    # And end wacky hacky shift info stuff. 

    opens.drop(['Shift Information1'], axis=1, inplace = True)

    logging.info(f"Done processing, generating file with {opens.shape[0]} rows")
   
    if opens.shape[0] > 500:
        logging.fatal("Too many reqs in file! Exiting!")
        quit()
  
    opens.to_excel(f"TCH New Jobs - {tchhelper.genDate2()}.xlsx",index = False)
   
    #tchhelper.send_email("awhelan@kinetixhr.com",1,0,"")
    

except Exception as e:
        tchhelper.send_email("awhelan@kinetixhr.com",1,1,str(e))
         
