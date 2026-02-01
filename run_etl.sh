#!/bin/bash

cd $TERRAFORM_PATH && terraform apply -auto-approve

gcloud dataproc jobs submit pyspark --cluster=$CLUSTER_NAME  --region=$REGION gs://$PATH/incremental_etl_v1.py  --py-files=gs://$PATH/helper_functions.py

gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet
