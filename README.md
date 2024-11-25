The first project: DAG-ETL_creation_with_ApacheAirflow&Kafka
Extract, Transform and Load (ETL) operations are of extreme importance in the role of a Data engineer. A data engineer extracts data from multiple sources and different file formats, transforms the extracted data to predefined settings and then loads the data to a database for further processing. 

Generally, tasks performed was to achieve the result include extracting data from different file formats, collecting data through APIs and web scraping and finally transforming the collected data into a ready-to-load format. to extract, transform and load real-world data about the world's largest banks into a database for further processing and querying.

It is to create a code that can be used to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, the data needs to be transformed and stored in GBP, EUR and INR as well, in accordance with the exchange rate information that has been made available as a CSV file. The processed information table is to be saved locally in a CSV format and as a database table.
Finally it is to create an automated system to generate this information so that the same can be executed in every financial quarter to prepare a report.

Project tasks
Task 1:
Write a function log_progress() to log the progress of the code at different stages in a file code_log.txt. Use the list of log points provided to create log entries as every stage of the code.
Task 2:
Extract the tabular information from the given URL under the heading 'By market capitalization' and save it to a dataframe.
a. Inspect the webpage and identify the position and pattern of the tabular information in the HTML code
b. Write the code for a function extract() to perform the required data extraction.
c. Execute a function call to extract() to verify the output.
Task 3:
Transform the dataframe by adding columns for Market Capitalization in GBP, EUR and INR, rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.
a. Write the code for a function transform() to perform the said task.
b. Execute a function call to transform() and verify the output.
Task 4:
Load the transformed dataframe to an output CSV file. Write a function load_to_csv(), execute a function call and verify the output.
Task 5:
Load the transformed dataframe to an SQL database server as a table. Write a function load_to_db(), execute a function call and verify the output.
Task 6:
Run queries on the database table. Write a function load_to_db(), execute a given set of queries and verify the output.
Task 7:
Verify that the log entries have been completed at all stages by checking the contents of the file code_log.txt.




The second project: ETL_project_with_python.txt
This project is “Creating ETL Data Pipelines with BashOperator using Apache Airflow” and “Creating Streaming Data Pipelines using Kafka”. 
To achieve the result.

As a Data Engineer at a data analytics consulting company and been assigned a project to decongest the national highways by analyzing the road traffic data from different toll plazas. Each highway is operated by a different toll operator with a different IT setup that uses different file formats. The task is to collect data available in different formats and consolidate it into a single file.

A DAG was created which comprised of the following processes:

•	Extract data from CSV, TSV, and fixed width files
•	Transform extracted data
•	Load transformed data into the staging area.
•	Submit, unpause, and monitor a DAG.
•	Create a topic in Kafka
•	Download and customize a streaming data consumer
•	Verify that streaming data has been collected in a database table
