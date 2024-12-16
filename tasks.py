# Path to Analytics reports
path = '/shared/ZB Solothurn 41SLSP_ZBS/Reports/SLSP_ZBS_reports_on_request/SUPPORT-32972_CUG/'

# Define CUG code
labels = ['ZBS_Group-1', 'ZBS_Group-2', 'ZBS_Group-3', 'ZBS_Group-4']

# Define CUG descriptions
cug_descriptions = {
    'ZBS_Group-1': 'ZBS MM-Group 0-12',
    'ZBS_Group-2': 'ZBS MM-Group 12-15',
    'ZBS_Group-3': 'ZBS MM-Group 15-26',
    'ZBS_Group-4': 'ZBS MM-Group 26-99'
}

# Create instances of AnalyticsReport
report_get_sum = AnalyticsReport(f'{path}SUPPORT-32972_0_99', 'ZBS')


# Extract data for each instance
data_get_0_99 = report_get_0_99.data

def update_users(df):
    """
    Update the user group of the users

    Parameters
    ----------
    df: DataFrame
        DataFrame containing user data with 'Primary Identifier', and 'Age' columns
    """
    
    # Convert the 'Age' column to integers
    df.loc[:, 'Age'] = pd.to_numeric(df['Age'], errors='coerce')
    
    for index, row in df.iterrows():
        primary_id = row['Primary Identifier']
        age = row['Age']

        if primary_id:
            # Fetch user data
            u = User(primary_id, 'ZBS')

            # Define user group based on age
            if age >= 0 and age <= 12:
                u.data['user_group']['value'] = labels[0]
                u.data['user_group']['desc'] = cug_descriptions[labels[0]]
            elif age > 12 and age <= 15:
                u.data['user_group']['value'] = labels[1]
                u.data['user_group']['desc'] = cug_descriptions[labels[1]]
            elif age > 15 and age <= 26:
                u.data['user_group']['value'] = labels[2]
                u.data['user_group']['desc'] = cug_descriptions[labels[2]]
            elif age > 26 and age <= 99:
                u.data['user_group']['value'] = labels[3]
                u.data['user_group']['desc'] = cug_descriptions[labels[3]]

            # Update user, override is required to update user group if there is already a user group change on the account
            u.update(override=['user_group'])
            
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