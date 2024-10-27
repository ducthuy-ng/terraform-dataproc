#!/bin/bash

gcloud storage cp spark-pi.py gs://spark-artifacts/spark-pi.py

gcloud dataproc jobs submit pyspark \
    --project modified-glyph-438213-k8 \
    --cluster=dataproc-cluster \
    --region=asia-southeast1 \
    gs://spark-artifacts/spark-pi.py