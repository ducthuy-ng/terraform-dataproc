# Data Engineering - Spark ETL

## BigQuery scripts

### Data ingestion

These are scripts that our team has developed during our experiment with BigQuery.
Inside [./bigquery](./bigquery) directory, there are scripts number from 01, marking the order of execution:

- [bigquery/01_CREATE_SCHEMA.bq.sql](bigquery/01_CREATE_SCHEMA.bq.sql): Create the tables
- [bigquery/02_ingest_data.sh](bigquery/02_ingest_data.sh): Using BigQuery CLI to import data from file to staging table
- [bigquery/03_CUSTOMER_DATA_CLEANING.bq.sql](bigquery/03_CUSTOMER_DATA_CLEANING.bq.sql): Data preprocessing
- [bigquery/04_NULL_PROCESSING.bq.sql](bigquery/04_NULL_PROCESSING.bq.sql): Cleanup NULL values

### PPR generation

A native BigQuery SQL script was written to calculate the ranking of product per customer.
It first joins 2 tables `saleheader_maintable` and `saleline_maintable`. Then, it orders the product,
based on the number of transactions that a person purchase for a specific product code.

- [bigquery/05_PPR.bq.sql](bigquery/05_PPR.bq.sql): Generating PPR as data product
- [bigquery/06_PREVIEW_PPR.bq.sql](bigquery/06_PREVIEW_PPR.bq.sql): Generating PPR as data product

### Rendering Total Sale per day

We go to **Data canvases** and create a new canvas. Then, we execute the script in [bigquery/07_TOTAL_SALE_PER_DAY.bq.sql](bigquery/07_TOTAL_SALE_PER_DAY.bq.sql)

## How to use this repo for development

First, please install:

- [Terraform](https://developer.hashicorp.com/terraform/install)
- [`gcloud` CLI](https://cloud.google.com/sdk/docs/install)

### Login to `gcloud`

Then, login to gcloud:

```bash
gcloud auth login
```

A web browser will open. Continue Google login flow. An example of a success login would be:

```
You are now logged in as [htom58361@gmail.com].
Your current project is [None].  You can change this setting by running:
  $ gcloud config set project PROJECT_ID
```

Then, set your gcloud to use our project

```bash
gcloud config set project modified-glyph-438213-k8
```

### Start infrastructure

Use Terraform to quickly spawn up our infrastructure

```bash
terraform apply
```

Terraform will output:

```
Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value:
```

Type `yes` for Terraform to apply modification

### Submit Spark

I have attached `spark-pi.py`, a simple script to test submitting PySpark to Dataproc.
To run it, execute `dataproc-submit.sh`

In the file, there are 2 commands. The first one upload our `.py` file to Cloud Storage, at bucket `spark-artifacts`.
Then, the next command submit our uploaded script to Dataproc

```bash
gcloud storage cp spark-pi.py gs://spark-artifacts/spark-pi.py

gcloud dataproc jobs submit pyspark \
    --project modified-glyph-438213-k8 \
    --cluster=dataproc-cluster \
    --region=asia-southeast1 \
    gs://spark-artifacts/spark-pi.py
```

### Stop infrastructure

To stop infrastructure, run below command:

```bash
terraform destroy
```

It may request for your confirmation:

```
Do you really want to destroy all resources?
  Terraform will destroy all your managed infrastructure, as shown above.
  There is no undo. Only 'yes' will be accepted to confirm.

  Enter a value: yes
```
