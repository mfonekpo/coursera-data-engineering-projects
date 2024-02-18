# Introduction

For learning purposes, I modified the Final Project which I was given on Data Engineering Project with Python on Coursera. I built an Orchestration feature using `Prefect` for the ETL pipeline. Working on this project has expanded my knowledge on Data flow Orchestration, the different types of tools we have for Pipe line orchestration aside `Prefect`

Here is the project case study:

# Project Scenario

A multi-national firm has hired you as a data engineer. Your job is to access and process data as per requirements.

Your boss asked you to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, you need to transform the data and store it in USD, GBP, EUR, and INR per the exchange rate information made available to you as a CSV file. You should save the processed information table locally in a CSV format and as a database table. Managers from different countries will query the database table to extract the list and note the market capitalization value in their own currency.

# Directions

1. Write a function to extract the tabular information from the given URL under the heading By Market Capitalization, and save it to a data frame.
2. Write a function to transform the data frame by adding columns for Market Capitalization in GBP, EUR, and INR, rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.
3. Write a function to load the transformed data frame to an output CSV file.
4. Write a function to load the transformed data frame to an SQL database server as a table.
5. Write a function to run queries on the database table.
   

### Run the following queries on the database table:
- Extract the information for the London office, that is Name and MC_GBP_Billion
  
- Extract the information for the Berlin office, that is Name and MC_EUR_Billion
  
- Extract the information for New Delhi office, that is Name and MC_INR_Billion

6. Write a function to log the progress of the code.
While executing the data initialization commands and function calls, maintain appropriate log entries.


# Largest Banks ETL Pipeline

This script defines an ETL (Extract, Transform, Load) pipeline that extracts data about the largest banks from Wikipedia, transforms the data by converting market capitalization figures to different currencies, and loads the transformed data into a SQLite database and a CSV file.

# Requirements:
- Python 3.x
- Prefect: For orchestrating the ETL tasks and flows.
- Requests: For making HTTP requests to retrieve the web page.
- BeautifulSoup4: For parsing the HTML content of the web page.
- Pandas: For data manipulation and analysis.
- SQLite3: For storing the transformed data in a database.
- NumPy: For numerical operations (if required).

# Structure:
- The pipeline is divided into tasks using Prefect's `@task` decorator, making it modular and easy to manage.
- The `extract` task fetches and parses the webpage to extract relevant data about the banks.
- The `transform` task processes this data, converting market capitalization figures from USD to GBP, EUR, and INR.
- The `load_to_csv` task writes the transformed data to a CSV file.
- The `load_to_db` task inserts the data into a SQLite database.
- The `main_flow` function orchestrates these tasks into a coherent flow that represents the entire ETL process.

# Usage:
- Ensure all dependencies are installed.
- Run the script directly, and it will execute the ETL pipeline using Prefect: `python banks_project_pipeline.py`
- The results will be saved in the specified database and CSV file within the `datafiles` directory.

# Customization:
- The URL, database name, table name, and output CSV path can be customized as per requirements.
- The exchange rates used in the transformation can be updated based on the latest rates.

# Authors
Mfon Nsikak Ekpo @mfonekpo


# Version History
0.1
Various bug fixes and optimizations
See commit change or release history
