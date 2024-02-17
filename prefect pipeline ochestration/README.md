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