import banks_project


target_file = "./Largest_banks.csv"

# Log the initialization of the ETL process 
banks_project.log_progress("ETL Job Started...")
 
# Log the beginning of the Extraction process 
banks_project.log_progress("Extract phase Started") 
extracted_data = banks_project.extract() 
 
# Log the completion of the Extraction process 
banks_project.log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
banks_project.log_progress("Transform phase Started") 
transformed_data = banks_project.transform(extracted_data) 
print("Transformed Data")
print(transformed_data) 
 
# Log the completion of the Transformation process 
banks_project.log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
banks_project.log_progress("Load phase Started") 
banks_project.load_data(transformed_data, target_file)
 
# Log the completion of the Loading process 
banks_project.log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
banks_project.log_progress("ETL Job Ended") 


conn = sqlite3.connect(db_name)
query = "SELECT * FROM Largest_banks"

print(banks_project.run_query(query, conn))