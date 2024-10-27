# Data Engineering - Spark ETL

## How to use this repo for development
First, please install:
- [Terraform](https://developer.hashicorp.com/terraform/install)
- [`gcloud` CLI](https://cloud.google.com/sdk/docs/install)

## Login to `gcloud`
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

## Start infrastructure
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


## Submit Spark
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

## Stop infrastructure
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