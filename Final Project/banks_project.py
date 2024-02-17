import requests
import sqlite3
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import datetime

db_name = "coursera/Final Project/datafiles/Banks.db"
db_table = "Largest_banks"
log_file = "code_log.txt"

conn = sqlite3.connect(db_name)




url1 = "https://web.archive.org/web/20230908091635" 
url2 = "https://en.wikipedia.org/wiki/List_of_largest_banks"


# Code for ETL operations on Country-GDP data

# Importing the required libraries



def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("coursera/Final Project/datafiles/code_log.txt","a") as f: 
        f.write(timestamp + ',' + message + '\n') 


def extract(url=url2):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    html_page = requests.get(url).text
    soup = BeautifulSoup(html_page, 'html.parser')
    bank_name = []
    market_cap = []

    tbody = soup.find_all("tbody")
    tbody_tr = tbody[0].find_all("tr")

    for rows in tbody_tr:
        col = rows.find_all("td")
        if len(col) != 0:
            bank_name.append(col[1].text.strip())
            market_cap.append(col[2].text.strip())
            data_dict = {
                "Name": bank_name,
                "MC_USD_Billion": market_cap
            }
            df = pd.DataFrame(data_dict)

    return df


def transform(df):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    #convert market cap datatype to float
    df["MC_USD_Billion"] = df["MC_USD_Billion"].astype("float")

    #add exchange rate columns and round to 2 decimal places
    df["MC_GBP_Billion"] = round(df["MC_USD_Billion"] * 0.8, 2)
    df["MC_EUR_Billion"] = round(df["MC_USD_Billion"] * 0.93, 2)
    df["MC_INR_Billion"] = round(df["MC_USD_Billion"] * 82.95, 2)

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    output_path = "coursera/Final Project/datafiles/largest_banks.csv"

    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    # conn = sqlite3.connect(db_name)
    df.to_sql(db_table, conn, if_exists='replace')
    conn.close()

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    cur = sql_connection.cursor()  # Create a cursor object
    cur.execute(query_statement)  # Execute the SQL query
    result = cur.fetchall()  # Fetch all rows of the query result
    cur.close()  # Close the cursor
    return result

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''


target_file = "./Largest_banks.csv"

# Log the initialization of the ETL process 
log_progress("ETL Job Started...")
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started")

log_progress("Loading file to csv")
load_to_csv(transformed_data, target_file)

log_progress("Loading file to database")
load_to_db(transformed_data, conn, db_table)
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 


conn = sqlite3.connect(db_name)
query = "SELECT * FROM Largest_banks"
query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
query3 = "SELECT Name from Largest_banks LIMIT 5"

# Execute the query and fetch the results
query_results = run_query(query3, conn)

# Print the results
for row in query_results:
    print(row)

# Close the database connection
conn.close()

