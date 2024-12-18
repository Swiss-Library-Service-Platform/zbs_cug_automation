"""
ZBS CUG Automation

Description:
This script takes false age-CUGs of ZBS through an Alma analytics report and distributes them correctly to the users.
Afterwards a mail with the updated user count is sent to receivers.

* Author: Rouven Schabinger (rouven.schabinger@slsp.ch)
* Date: 2024-12-16

"""

# Import libraries
from almapiwrapper.users import User, NewUser, fetch_users, fetch_user_in_all_iz, Fee, Loan, Request, check_synchro, force_synchro
from almapiwrapper.inventory import IzBib, NzBib, Holding, Item, Collection
from almapiwrapper.config import RecSet, ItemizedSet, LogicalSet, NewLogicalSet, NewItemizedSet, Job, Library, Location, Desk, fetch_libraries
from almapiwrapper.config import Reminder, fetch_reminders, Library, fetch_libraries, Location, OpenHours
from almapiwrapper.record import JsonData, XmlData
from almapiwrapper.analytics import AnalyticsReport
from almapiwrapper.acquisitions import POLine, Vendor, Invoice, fetch_invoices
from almapiwrapper.configlog import config_log
from almapiwrapper import ApiKeys


import pandas as pd
from datetime import datetime, timedelta

from sendmail import sendmail

# Config logs
config_log()

# Path to Alma Analytics report
path = '/shared/ZB Solothurn 41SLSP_ZBS/Reports/SLSP_ZBS_reports_on_request/SUPPORT-32972_CUG/'

# Define CUG codes
labels = ['ZBS_Group-1', 'ZBS_Group-2', 'ZBS_Group-3', 'ZBS_Group-4']


# Create instances of AnalyticsReport 
report_get = AnalyticsReport(f'{path}SUPPORT-32972_ANOTHER_ANALYSIS', 'ZBS')

# Extract data
df = report_get.data

# Clean df
df.rename(columns={" FLOOR(( CAST ( CURRENT_DATE  AS DATE ) -  CAST (Birth Date AS DATE )) / 365.25)": "Age"}, inplace=True)
df.drop(columns=[" MAX( RCOUNT(1))"], inplace=True)


# modify users
def update_users(df):
    """
    Update the user group of the users

    Parameters
    ----------
    df: DataFrame
        DataFrame containing user data with 'Primary Identifier', and 'Age' columns
    """
    
    # Convert the 'Age' column to integers
    df['Age'] = pd.to_numeric(df['Age'], errors='coerce')
    
    for index, row in df.iterrows():
        primary_id = row['Primary Identifier']
        age = row['Age']

        if primary_id:
            # Fetch user data
            u = User(primary_id, 'ZBS')

            # Define user group based on age
            if age >= 0 and age < 12:
                u.data['user_group']['value'] = labels[0]
            elif age >= 12 and age < 15:
                u.data['user_group']['value'] = labels[1]
            elif age >= 15 and age < 26:
                u.data['user_group']['value'] = labels[2]
            elif age >= 26 and age < 99:
                u.data['user_group']['value'] = labels[3]        
                

            # Update user, override is required to update user group if there is already a user group change on the account
            u.update(override=['user_group'])
            
update_users(df)            
            
# Get the current date in the format YYYY-MM-DD
current_date = datetime.now().strftime('%Y-%m-%d')

# Initialize a counter
count = 0

# Open the log file and read line by line
with open('log/log.txt', 'r') as file:
    for line in file:
        # Check if the line contains the current date and "user updated"
        if current_date in line and "user updated" in line:
            count += 1

# Prepare the email content
subject = f"CUG Updated Report for {current_date}"
message = f"Number of CUG Updated messages for {current_date}: {count}"

# List of report receivers
report_receivers = ["rouven.schabinger@slsp.ch"]

# Send the email to each receiver
for report_receiver in report_receivers:
    sendmail(report_receiver, subject, message)
    