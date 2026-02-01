# Dynamics-To-Bigquery-ETL-Spark-Pipeline
This spark ETL code reads exported data from dynamics 365 in google cloud storage, processes it and load it into bigquery for Analytics

#Initial set up
Set up [bc2adls](https://github.com/microsoft/bc2adls) as explain in the link. For my use case, we used Google cloud platform as our cloud provider, the processing was done mainly in dataproc. We had to figure out a way of processing the delta files from dynamics 365 without using synapse hence this spark pipeline. Because of this we setup the export functionality of bc2adls to azure data lake, transfered the csv's into google cloud storage and did our own processing with spark from there onwards.

Disclaimer: bc2adls as a tool can do the export and data processing writing the results into a azure data lake storage, please read the documentation linked above.

# Getting started
You need to have Java, Python  installed, please look at Apache spark dependencies and also how to set up and install Apache Spark. The code was written intentinally to run on dataproc but this is something you can change and make it run anywhere, provided all the dependencies are OK.




