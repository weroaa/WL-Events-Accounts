import snowflake.connector
from fuzzywuzzy import fuzz
import os
import re
import csv

# Function to normalize company names
def normalize_company_name(name):
    name = re.sub(r'[^\w\s]', '', name).lower()  # Remove non-alphanumeric characters and convert to lower case
    name = re.sub(r'\b(inc|llc|pllc|llp)\b', '', name)    # Remove 'inc' and 'llc'
    return ' '.join(name.split())                # Remove extra spaces

# Function to compare names
def compare_names(name1, name2):
    return fuzz.token_sort_ratio(name1, name2)

# Paths for input and output files
desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
input_file_path = os.path.join(desktop_path, 'WL_Event2.csv')
output_file_path = os.path.join(desktop_path, 'WL_Event2_results.csv')

# Connect to Snowflake
with snowflake.connector.connect(
   user='ANALYST_USER',
    password='Select*FromDWH123',
    account='ffa91112.us-east-1',
    warehouse='PROD_WH',
    database='PROD_DWH',
    schema='DWH',
    role='ANALYST'
) as conn:
    # Fetch account data
    with conn.cursor() as cursor:
        cursor.execute("SELECT ACCOUNT_ID, ACCOUNT_NAME,SALES_REPRESENTATIVE_NAME FROM DIM_SF_ACCOUNT")
        accounts = cursor.fetchall()

# Open the input CSV file and read its content
with open(input_file_path, 'r', newline='', encoding='utf-8') as infile, \
     open(output_file_path, 'w', newline='', encoding='utf-8') as outfile:
    
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    
    # Read header and extend with new columns
    header = next(reader)
    header.extend(['duplicate found', 'duplicate account_id','SALES_REPRESENTATIVE_NAME'])
    writer.writerow(header)

    # Normalize account names once to avoid redundant calculations
    normalized_accounts = [(account_id, normalize_company_name(account_name), sales_rep) for account_id, account_name, sales_rep in accounts]

    # Process each row in the input CSV
    for row in reader:
        company_name_csv = row[0]
        normalized_csv_name = normalize_company_name(company_name_csv)
        duplicate_account_ids = []
        sales_representative_names = []
        
        for account_id, normalized_account_name, sales_rep in normalized_accounts:
            if compare_names(normalized_csv_name, normalized_account_name) > 95:
                duplicate_account_ids.append(str(account_id))
                sales_representative_names.append(sales_rep)

        if len(duplicate_account_ids) >= 1:
            row.append('Y')
            row.append(', '.join(duplicate_account_ids))
            row.append(', '.join(sales_representative_names))
        else:
            row.append('N')
            row.append('')
            row.append('')

        writer.writerow(row)

print('Process completed successfully.')