import requests
import sqlite3
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import datetime
from datetime import timedelta
import os
from prefect import task, flow, get_run_logger
from prefect.tasks import task_input_hash
import logging


directory = "./datafiles"
if not os.path.exists(directory):
    os.makedirs(directory)


db_name = "datafiles/Banks.db" # Database name
db_table = "Largest_banks" #table name
log_file = "datafiles/code_log.txt" #log gile

url = "https://en.wikipedia.org/wiki/List_of_largest_banks"




@task(log_prints = True, retries = 3, retry_delay_seconds = 5, 
      cache_key_fn=task_input_hash, 
      cache_expiration = timedelta(days=1))
def extract(url: str=url):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    logger = get_run_logger()
    logger.info("Extract phase Started")

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

    logger.info("Extract phase Ended")

    return df



@task(log_prints = True)
def transform(df):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    logger = get_run_logger()
    logger.info("Transform phase Started")

    #convert market cap datatype to float
    df["MC_USD_Billion"] = df["MC_USD_Billion"].astype("float")

    #add exchange rate columns and round to 2 decimal places
    df["MC_GBP_Billion"] = round(df["MC_USD_Billion"] * 0.8, 2)
    df["MC_EUR_Billion"] = round(df["MC_USD_Billion"] * 0.93, 2)
    df["MC_INR_Billion"] = round(df["MC_USD_Billion"] * 82.95, 2)

    logger.info("Transform phase Ended")

    return df





@task(log_prints=True)
def load_to_csv(df, output_path: str="datafiles/largest_banks.csv"):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    logger = get_run_logger()
    logger.info("Loading file into csv")


    df.to_csv(output_path, index=False)
    logger.info("File successfully saved into csv")



@task(log_prints=True)
def load_to_db(df, sql_connection, table_name: str= db_table):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    logger = get_run_logger()
    logger.info("Loading file into database ")


    with sqlite3.connect(db_name) as conn:
        df.to_sql(db_table, conn, if_exists='replace', index=False)


    logger.info("Data successfully loaded into database")



@flow(name="Largest Banks ETL Pipeline")
def main_flow(url: str= url, conn: str= db_name, db_table: str= db_table):
    """
    Orchestrates the ETL pipeline: extracting data from the provided URL, transforming it, and then loading the results into a CSV file and SQLite database.

    Args:
        url (str): The URL to scrape data from.
        conn (str): The path to the SQLite database file.
        db_table (str): The name of the table within the database.
    """
    # Open a database connection
    extracted_df = extract()
    transformed_df = transform(extracted_df)
    load_to_csv(transformed_df)
    load_to_db(transformed_df, conn, db_table)


if __name__ == "__main__":
    main_flow()
