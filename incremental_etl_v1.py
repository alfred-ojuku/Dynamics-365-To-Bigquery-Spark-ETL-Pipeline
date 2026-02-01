# -*- coding: utf-8 -*-
"""
Created on Fri Jul 26 10:30:53 2024

@author: Alfred Ojuku
"""

#import necessary libraries
import sys
import os
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, lit, col
from datetime import timezone
from helper_functions import transform_column_name, find_duplicate_columns, convert_date_columns_to_string, convert_timestamp_columns_to_string, convert_to_filename, update_latest_csv, create_new_table

# Create a SparkSession
spark = SparkSession.builder.appName("Incremental_ETL").getOrCreate()

# variables
bc2adls_bucket = os.getenv("bc2adls") 
gcs_path= os.getenv("path")
project_id = os.getenv("project_id")
dataset_id = os.getenv("dataset_id")
temp_bucket = os.getenv("temp_bucket")
bucket_name = os.getenv("bucket_name")
latest_csv_path = os.getenv("latest_csv_path")
len_sys_args = len(sys.argv) 

# Initialize a client
client = storage.Client()

# Get the list of blobs (objects) in the specified folder
blobs = client.list_blobs(bc2adls_bucket)

#loops through the blobs and gets the folder names in bc2adls storage object
if len_sys_args > 1:
    folders = set()
    for i in range (1, len_sys_args):
            folders.add(sys.argv[i])
else:
    folders = set()
    for blob in blobs:
        folder = blob.name.split('/')[0]
        if folder:
            folders.add(folder)

latest_df = spark.read.option("header", True).option("inferSchema", True).option("multiline", True).option("quote", "\"").csv(latest_csv_path)
rows_list = {row['latest_blob_name']: row['max_created_at'] for row in latest_df.collect()}

# Get the bucket
bucket = client.get_bucket(bc2adls_bucket)

#Loops through all the folders in the bc2adls storage bucket
#reads all the csv's in those folders into a dataframe
# GCS bucket and temp bucket variables
for folder in folders:
    if folder in rows_list.keys():
        
        max_datetime = rows_list[folder].replace(tzinfo=timezone.utc)
        
        try:
            #creates the gcs folder path
            folder_path = "gs://{}/{}".format(bc2adls_bucket, folder)
            
            #extracts the table name from the folder path
            table_name = convert_to_filename(folder_path)

            #for debugging incase the pipeline errors.
            print(folder_path)

            # List all blobs in the folder and filter by creation time
            folder_blobs = bucket.list_blobs(prefix=folder)
            filtered_blobs = [blob for blob in folder_blobs if blob.time_created > max_datetime]

            # Get the paths of the filtered files
            filtered_paths = [f"gs://{bucket_name}/{blob.name}" for blob in filtered_blobs]

            # Check if there are any files to read
            if filtered_paths:
                # Read the filtered CSV files into a DataFrame
                df_change = spark.read.option("header", True).option("inferSchema", True).option("multiline", True).option("quote", "\"").csv(filtered_paths)
                
                # Read data from BigQuery into a Spark DataFrame
                df_bigquery = spark.read.format("bigquery").option("table", f"{project_id}.{dataset_id}.{table_name}").load()
                
                # Get the column names
                columns = df_change.columns

                # Apply the transformation to each column name
                transformed_columns = [transform_column_name(col) for col in columns]

                # Zip the original and transformed column names into a dictionary
                column_mapping = dict(zip(columns, transformed_columns))

                # Rename the columns in the DataFrame
                for old_col, new_col in column_mapping.items():
                    df_change = df_change.withColumnRenamed(old_col, new_col)     

                column_name = df_change.columns

                duplicates = find_duplicate_columns(column_name)
                indexes_to_exclude = [index for indexes in duplicates.values() for index in indexes]
                to_select = [col_name for index, col_name in enumerate(column_name) if index not in indexes_to_exclude]

                # Select these columns from the DataFrame
                df_change = df_change.select(to_select)

                #converts all the timestamp and date columns to string
                df_change = convert_timestamp_columns_to_string(df_change)
                df_change = convert_date_columns_to_string(df_change)

                # Rename the company column
                df_change = df_change.withColumnRenamed("$company", "_company")

                #Adding extracted at to the dataframe
                df_change = df_change.withColumn("extracted_at", from_utc_timestamp(current_timestamp(), "Africa/Nairobi"))

                # Find all column names
                all_columns = list(set(df_bigquery.columns).union(set(df_change.columns)))

                # Add missing columns to df_bigquery
                for column in all_columns:
                    if column not in df_bigquery.columns:
                        df_bigquery = df_bigquery.withColumn(column, lit(None))

                # Add missing columns to df_change
                for column in all_columns:
                    if column not in df_change.columns:
                        df_change = df_change.withColumn(column, lit(None))
                        
                final_column_order = [col for col in column_name if col in all_columns] + \
                                     [col for col in all_columns if col not in column_name]

                # Reorder columns to match in both DataFrames
                df_bigquery = df_bigquery.select(final_column_order)
                df_change = df_change.select(final_column_order)

                bq_schema = {field.name: field.dataType for field in df_bigquery.schema}

                for column, datatype in bq_schema.items():
                    if column in df_change.columns:
                        df_change = df_change.withColumn(column, col(column).cast(datatype))

                df_union = df_bigquery.unionAll(df_change)

                # Register the DataFrame as a temporary SQL table
                df_union.createOrReplaceTempView("test_table")

                # Use Spark SQL to filter the DataFrame
                #Basically filters out the deleted rows, identified by a unique systemid column
                #A deleted row satisfies this condition, the systemcreatedat2000000001 and systemmodifiedat2000000003 are null,
                #systemcreatedby2000000002 and {00000000-0000-0000-0000-000000000000} are {00000000-0000-0000-0000-000000000000}
                filtered_df_union = spark.sql("""
                    SELECT *
                    FROM test_table
                    WHERE `systemid` NOT IN ( SELECT `systemid`
                                            FROM test_table
                                            WHERE `systemcreatedat` IS NULL AND `systemcreatedby` = '{00000000-0000-0000-0000-000000000000}' AND `systemmodifiedat` IS NULL AND `systemmodifiedby` = '{00000000-0000-0000-0000-000000000000}')
                    """)

                # Register the DataFrame as a temporary SQL table
                filtered_df_union.createOrReplaceTempView("table_view")

                # Use Spark SQL to filter the DataFrame
                df_union = spark.sql("""
                    WITH ranked_data AS (
                    SELECT 
                        table_view.*,
                    ROW_NUMBER() OVER (PARTITION BY `systemid` ORDER BY `systemmodifiedat` DESC) AS rn
                    FROM table_view
                    )
                    SELECT *
                    FROM ranked_data
                    WHERE rn = 1; """)

                # Get all column names except 'rn'
                columns_to_select = [col for col in df_union.columns if col != 'rn']

                # Select these columns from the DataFrame
                df_union = df_union.select(*columns_to_select)

                #writes the dataframe to a bigquery table
                df_union.write.format("bigquery").option("temporaryGcsBucket", temp_bucket).option("project", project_id) \
                    .option("dataset", dataset_id) \
                    .option("table", table_name) \
                    .mode("overwrite") \
                    .save()
                print(f"Success {table_name}")    
            else:
                print("No CSV files found with the specified creation time criteria.")
                
        except Exception as e:
            print(f"Error processing {folder}: {e}")
            continue
            
    else:
         create_new_table(spark, folder, bc2adls_bucket)

#update latest.csv
if len_sys_args <= 1:
    update_latest_csv(client, spark, folders, bc2adls_bucket)
    
spark.stop()
