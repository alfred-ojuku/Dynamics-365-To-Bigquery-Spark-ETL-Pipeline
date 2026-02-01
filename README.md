# Dynamics-To-Bigquery-ETL-Spark-Pipeline
This spark ETL code reads exported data from dynamics 365 in google cloud storage, processes it and loads it into bigquery for analytics

# Initial set up
Set up [bc2adls](https://github.com/microsoft/bc2adls) as explained in the link. For my use case, I used Google cloud platform as our cloud provider, the processing was done mainly in dataproc. We had to figure out a way of processing the delta files from dynamics 365 without using synapse hence this spark pipeline. Because of this we setup the export functionality of bc2adls to azure data lake, transfered the csv's into google cloud storage and did our own processing with spark from there onwards.

Disclaimer: bc2adls as a tool can do the export and data processing writing the results into a azure data lake storage, please read the documentation linked above.

# Getting started
You need to have Java, Python  installed, please look at Apache spark dependencies and also how to set up and install Apache Spark. The code was written intentinally to run on dataproc but this is something you can change and make it run anywhere, provided all the dependencies are OK.

# Usage
The code can run on a cluster or locally, but you need to first edit source and sik dependencies; google cloud storage and google bigquery. The ETL pipeline in our case works like this
- Create a dataproc cluster using the terraform template. (We have not switched to dataproc serverless because of costs incurred in serverless dataproc.) See the command in the run_etl.sh.
- Submit the pyspark ETL pipeline with the corresponding helper function.
- After the pipeline runs and finishes, destroy the cluster. All these are in the run_etl.sh script and sits in a linux vm in google cloud. The run_etl.sh script is scheduled using a cron job to run every midnight.
