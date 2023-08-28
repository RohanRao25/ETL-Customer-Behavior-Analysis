# ETL-Customer-Behavior-Analysis
About
---------------------------------------------------------------------------------------------------------------------------------------------------
 Repository for an ETL application that analyses customer behavior on a fictional shopping website. It takes the data from the CSV file, processess it and gives the output in the form of another csv file that contains the details of the which product was popular in which month.
 It can also be termed as batch processing.

 dataset :- https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

 Project Structure :-

 root/
  |
  |-- dependencies/
  |     |
  |     |-- __init__.py
  |     |-- helpers.py
  |     |-- logger.py
  |     |-- spark.py
  |
  |-- constants.py
  |
  |-- etl_job.py
  |
  |-- log4j.properties
  |
  |-- spark.conf


Structure of the ETL job :-
In order to facilitate easy debugging , it is recommended that the 'Transformation' step be isolated from the 'Extract' and 'Load' steps, into its own function - taking input data arguments in the form of DataFrames and returning the transformed data as a single DataFrame. Then, the code that surrounds the use of the transformation function in the main() job function, is concerned with Extracting the data, passing it to the transformation function and then Loading (or writing) the results to their ultimate destination.

