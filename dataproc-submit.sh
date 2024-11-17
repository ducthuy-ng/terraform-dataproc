#!/bin/bash

gcloud storage cp ingest-data.py gs://spark-artifacts/ingest-data.py

gcloud dataproc jobs submit pyspark \
    --project modified-glyph-438213-k8 \
    --cluster=dataproc-cluster \
    --region=asia-southeast1 \
    --properties=spark.driver.memory=8g \
    gs://spark-artifacts/ingest-data.py