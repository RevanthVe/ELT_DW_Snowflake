## ELT_DW_Snowflake

# Establishing S3 Connection
Process begins by creating an external stage that points to an S3 bucket containing the source data files. 

S3 connection has been established in Snowflake by creating a storage integration object called 'sst_object'. This object uses the 'storage_aws_role_arn' parameter to assume an IAM role in AWS, which has the necessary permissions to access the S3 bucket. The IAM role and policy were created in the AWS S3 console. The storage integration object also specifies the S3 bucket location using the 'storage_allowed_locations' parameter.

# Creating Staging layer
A file format is then defined to specify the structure of the source data files. The stage is then set to use the storage integration and file format that were just created.

Next, the code creates a set of tables in Snowflake to store the source data. These tables include dimdates, customers, products, sales, and stores.

The code then creates a stored procedure called load_latest_data that loads the latest data from the S3 bucket into the Snowflake tables. This stored procedure is scheduled to run daily using a task called load_latest_data_task.

•	Creating Bronze layer ( De-duplication, Data cleaning)
A separate schema (bronze) is created to hold the transformed data. The populate_bronze_layer stored procedure is responsible for creating tables in the bronze schema and populating them with the latest data from the public tables (Bronze layer). This procedure applies de-duplication by removing any duplicates in all tables. Also, in this layer, we only keep the transactions in Sales table where customer records are present in customers table.

•	Creating Silver layer ( Enforcing star schema, Implementing SCD Type 2)
The code creates the SCD (Slowly Changing Dimension) tables in the silver schema. These tables include additional columns (END_TIME and CURRENT_FLAG) to handle historical data changes using the SCD Type 2 approach.

Creating Streams and Views: Streams are created on the Bronze layer tables to capture data changes (INSERT, UPDATE, DELETE). Views are also created on the Bronze layer tables with change tracking enabled. These views will be used in the MERGE statement to load data into the Silver layer SCD tables.

Implementing the MERGE Statement: The load_silver_scd stored procedure is written in Python and uses the Snowpark library. This procedure defines two helper functions:
get_column_info: Retrieves the column names of a given table from the INFORMATION_SCHEMA.COLUMNS view.
generate_merge_statement: Constructs the MERGE statement based on the common columns between the Bronze layer view and the Silver layer SCD table.
The generate_merge_statement function constructs the MERGE statement in the following way:
It identifies the columns that end with "ID" as the key columns for the ON condition.
It handles the UPDATE case when a row is matched and the action is DELETE, setting the END_TIME and CURRENT_FLAG columns accordingly.
It handles the INSERT case when a row is not matched and the action is INSERT, inserting the new row into the SCD table with the appropriate column values.
The load_silver_scd procedure iterates over the Bronze layer views, Silver layer SCD tables, and streams, retrieving the common columns and calling generate_merge_statement to execute the MERGE statement for each SCD table.
In summary, the code sets up an ELT pipeline that loads data from an S3 bucket into the Bronze layer tables, transforms and populates the data in the Bronze schema, and then uses the MERGE statement to load the data into the Silver layer SCD tables, handling historical changes using the SCD Type 2 approach.

# Scheduling Tasks
Several tasks are created to automate the ELT process:
o	load_latest_data_task: This task runs daily at 6:00 AM (America/Chicago time) and calls the load_latest_data stored procedure to load the latest data from the S3 bucket into the Staging layer tables.
o	delete_oldest_data_task: This task runs after load_latest_data_task and calls the delete_oldest_data stored procedure to delete data older than 7 days from the Staging Layer tables.
o	populate_bronze_layer_task: This task runs after load_latest_data_task and calls the populate_bronze_layer stored procedure to transform and load the latest data into the Bronze layer tables.
o	load_silver_scd_tables: This task runs after populate_bronze_layer_task and calls the load_silver_scd stored procedure to load data from the Bronze layer into the Silver layer SCD tables using the MERGE INTO statement.

In summary, the code sets up an ELT pipeline that loads data from an S3 bucket into the Staging layer tables, transforms and populates the data in the Bronze schema, and then uses the MERGE INTO statement to load the data into the Silver layer SCD tables, handling historical changes using the SCD Type 2 approach.
