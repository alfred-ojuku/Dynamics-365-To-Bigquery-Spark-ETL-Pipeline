#import necessary libraries
import re
from collections import Counter
from google.cloud import storage
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, date_format, current_timestamp, from_utc_timestamp

# Define a function to transform column names
def transform_column_name(column_name):
    # Remove numerical characters
    column_name = re.sub(r'-\d+', '', column_name).lower()

    return column_name

#renames columns with similar names
def find_duplicate_columns(column_names):
    column_counter = Counter(column_names)
    duplicates = {column: [] for column, count in column_counter.items() if count > 1}
    for i, column_name in enumerate(column_names):
        if column_name in duplicates:
            duplicates[column_name].append(i)

    return duplicates

# The reason for this is because some tables have weird dates which throws errors when the code runs.
def convert_date_columns_to_string(df):
    # Get all column names
    column_names = df.columns

    # Filter date columns
    date_columns = [col_name for col_name in column_names if df.schema[col_name].dataType.typeName() == "date"]

    # Convert date columns to strings
    converted_df = df
    for col_name in date_columns:
        converted_df = converted_df.withColumn(col_name, date_format(col(col_name), "yyyy-MM-dd HH:mm:ss"))

    return converted_df

def convert_timestamp_columns_to_string(df):
    # Get all column names
    column_names = df.columns

    # Filter timestamp columns
    timestamp_columns = [col_name for col_name in column_names if df.schema[col_name].dataType.typeName() == "timestamp"]

    # Convert timestamp columns to strings with the desired format
    converted_df = df
    for col_name in timestamp_columns:
        converted_df = converted_df.withColumn(col_name, date_format(col(col_name), "yyyy-MM-dd HH:mm:ss"))

    return converted_df

# A function that makes the bigquery table name i.e the name of the table which the dataframe will be written into
def convert_to_filename(path):
    # Split the path by "/"
    parts = path.split("/")
    # Take the last part which is the filename
    filename = parts[-1]
    # Remove non-alphanumeric characters and hyphens, and convert to lowercase
    filename = ''.join(c for c in filename if c.isalnum()).lower()

    return filename

# update the latest.csv file with latest csv timestamps
def update_latest_csv(client, spark, folders, bucket, path):
    # Get the bucket
    bucket = client.get_bucket(bucket)

    schema = StructType([
        StructField("latest_blob_name", StringType(), True),
        StructField("max_created_at", TimestampType(), True)
    ])

    # Create an empty DataFrame with the defined schema
    df_results = spark.createDataFrame([], schema)

    for folder in folders:
        # List all blobs in the folder
        blobs = bucket.list_blobs(prefix=folder)

        # Collect blob names and creation times in a list
        blob_info = []

        # Loop over each blob in the folder
        for blob in blobs:
            # Get the creation time of the object
            created_at = blob.time_created

            blob_info.append((blob.name, created_at))  # Store blob name and creation time

        # Extract max_created_at
        max_created_at = max(blob_info, key=lambda x: x[1])[1]

        # Find the latest blob name
        latest_blob_name = next(blob[0] for blob in blob_info if blob[1] == max_created_at)

        # Create a DataFrame with the new row
        new_row = spark.createDataFrame([(latest_blob_name.split('/')[0], max_created_at)], schema)

        # Append the new row to the existing DataFrame
        df_results = df_results.union(new_row)

    gcs_path = f"gs://{path}/latest.csv"
    df_results.coalesce(1).write.csv(gcs_path, header=True, mode="overwrite")

def create_new_table(spark, folder, bucket, temp_bucket, project, dataset):    
    #creates the gcs folder path
    folder_path = "gs://{}/{}".format(bucket, folder)

    #for debugging incase the pipeline errors.
    print(folder_path)

    # Read data from GCS into a DataFrame
    df = spark.read.option("header", True).option("inferSchema", True).option("multiline", True).option("quote", "\"").csv(folder_path)

    # Get the column names
    columns = df.columns

    # Apply the transformation to each column name
    transformed_columns = [transform_column_name(col) for col in columns]

    # Zip the original and transformed column names into a dictionary
    column_mapping = dict(zip(columns, transformed_columns))

    # Rename the columns in the DataFrame
    for old_col, new_col in column_mapping.items():
        df = df.withColumnRenamed(old_col, new_col)     

    column_name = df.columns

    duplicates = find_duplicate_columns(column_name)
    indexes_to_exclude = [index for indexes in duplicates.values() for index in indexes]
    to_select = [col_name for index, col_name in enumerate(column_name) if index not in indexes_to_exclude]

    # Select these columns from the DataFrame
    df = df.select(to_select)

    # Register the DataFrame as a temporary SQL table
    df.createOrReplaceTempView("test_table")

    # Use Spark SQL to filter the DataFrame
    #Basically filters out the deleted rows, identified by a unique systemid column
    #A deleted row satisfies this condition, the systemcreatedat2000000001 and systemmodifiedat2000000003 are null,
    #systemcreatedby2000000002 and {00000000-0000-0000-0000-000000000000} are {00000000-0000-0000-0000-000000000000}
    filtered_df = spark.sql("""
        SELECT *
        FROM test_table
        WHERE `systemid` NOT IN ( SELECT `systemid`
                                FROM test_table
                                WHERE `systemcreatedat` IS NULL AND `systemcreatedby` = '{00000000-0000-0000-0000-000000000000}' AND `systemmodifiedat` IS NULL AND `systemmodifiedby` = '{00000000-0000-0000-0000-000000000000}')
        """)

    # Register the DataFrame as a temporary SQL table
    filtered_df.createOrReplaceTempView("table_view")

    # Use Spark SQL to filter the DataFrame
    df = spark.sql("""
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
    columns_to_select = [col for col in df.columns if col != 'rn']

    # Select these columns from the DataFrame
    df = df.select(*columns_to_select)

    #extracts the table name from the folder path
    table_name = convert_to_filename(folder_path)

    #converts all the timestamp and date columns to string
    df = convert_timestamp_columns_to_string(df)
    df = convert_date_columns_to_string(df)

    # Rename the company column
    df = df.withColumnRenamed("$company", "_company")

    #Adding extracted at to the dataframe
    df = df.withColumn("extracted_at", current_timestamp())

    # Convert timestamp to Nairobi timezone (UTC+3)
    df = df.withColumn("extracted_at", from_utc_timestamp(df["extracted_at"], "Africa/Nairobi"))

    #writes the dataframe to a bigquery table
    df.write.format("bigquery").option("temporaryGcsBucket", temp_bucket).option("project", project) \
        .option("dataset", dataset) \
        .option("table", table_name) \
        .mode("overwrite") \
        .save()
    
    print(f"Success {table_name}")
