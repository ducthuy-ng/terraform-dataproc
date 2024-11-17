#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from pyspark.sql import SparkSession
from pyspark import SparkConf


def process_customer_data(spark: SparkSession, input_file, output_table):
    # Define schema for the Customers data as a string array
    schema = [
        "RESIDENCE_TYPE STRING",
        "RESIDENCE_OWNERSHIP STRING",
        "CUSTOMER_LOCATION STRING",
        "RESIDENT_STATUS STRING",
        "LOYALTY_STATUS STRING",
        "INDUSTRY STRING",
        "LIFE_STYLE STRING",
        "CUSTOMER_TIER STRING",
        "NATIONALITY STRING",
        "CUSTOMER_DOOR_NO STRING",
        "CUSTOMER_STREET STRING",
        "CUSTOMER_ZIP_CODE STRING",
        "CUSTOMER_GEO_LEVEL1_CODE STRING",
        "CUSTOMER_GEO_LEVEL1_DESC STRING",
        "CUSTOMER_GEO_LEVEL2_CODE STRING",
        "CUSTOMER_GEO_LEVEL2_DESC STRING",
        "CUSTOMER_GEO_LEVEL3_CODE STRING",
        "CUSTOMER_GEO_LEVEL3_DESC STRING",
        "CUSTOMER_GEO_LEVEL4_CODE STRING",
        "CUSTOMER_GEO_LEVEL4_DESC STRING",
        "RESIDENCE_ADDRESS STRING",
        "EMAIL_ADDRESS STRING",
        "PHONE_NUMBER STRING",
        "MOBILE_NUMBER STRING",
        "FAX_NUMBER STRING",
        "SOCIAL_MEDIA_ADDRESS STRING",
        "COMMS_BY_POST_FLG STRING",
        "COMMS_BY_SMS_FLG STRING",
        "COMMS_BY_EMAIL_FLG STRING",
        "COMMS_BY_SOCIAL_MEDIA_FLG STRING",
        "COMMS_BY_MOBILE_APP_FLG STRING",
        "COMM_LANGUAGE STRING",
        "OFFICE_ADDRESS STRING",
        "COMPANY_NAME STRING",
        "PROOF_OF_IDENTITY_TYPE STRING",
        "PROOF_OF_IDENTITY STRING",
        "HOME_STORE_CODE INTEGER",
        "HOME_STORE STRING",
        "HOUSEHOLD STRING",
        "LOYALTY_MEMBERSHIP_FLG STRING",
        "CUST_ATTRIBUTE1 STRING",
        "CUST_ATTRIBUTE2 STRING",
        "CUST_ATTRIBUTE3 STRING",
        "CUST_ATTRIBUTE4 STRING",
        "CUST_ATTRIBUTE5 STRING",
        "CUST_ATTRIBUTE6 STRING",
        "CUST_ATTRIBUTE7 STRING",
        "CUST_ATTRIBUTE8 STRING",
        "CUST_ATTRIBUTE9 STRING",
        "CUST_ATTRIBUTE10 STRING",
        "CUST_ATTRIBUTE11 STRING",
        "CUST_ATTRIBUTE12 STRING",
        "CUST_ATTRIBUTE13 STRING",
        "CUST_ATTRIBUTE14 STRING",
        "CUST_ATTRIBUTE15 STRING",
        "CUST_ATTRIBUTE16 STRING",
        "CUST_ATTRIBUTE17 STRING",
        "CUST_ATTRIBUTE18 STRING",
        "CUST_ATTRIBUTE19 STRING",
        "CUST_ATTRIBUTE20 STRING",
        "CUST_ATTRIBUTE21 STRING",
        "CUST_ATTRIBUTE22 STRING",
        "CUST_ATTRIBUTE23 STRING",
        "CUST_ATTRIBUTE24 STRING",
        "CUST_ATTRIBUTE25 STRING",
        "OCCUPATION STRING",
        "ACTIVE STRING",
        "HOUSEHOLD_CODE STRING",
        "HOUSEHOLD_HEAD_FLG STRING",
        "PREF_COMM_CHANNEL_CODE STRING",
        "LAST_UPDATED_DATE DATE",
        "LOYALTY_JOINING_DATE DATE",
    ]

    # Join the schema array into a single string
    schema_str = ", ".join(schema)

    df = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .option("encoding", "ISO-8859-1")
        .option("header", True)
        .schema(schema_str)
        .load(input_file)
        .coalesce(8)
    )

    df.write.format("bigquery").mode("append").option("table", output_table).save()


if __name__ == "__main__":
    conf = (
        SparkConf()
        .set("temporaryGcsBucket", "spark-artifacts")
        .set("spark.spark.network.timeout", "800s")
        .set("spark.executor.heartbeatInterval", "60s")
    )
    spark: SparkSession = SparkSession.builder.config(conf=conf).appName(
        "SalesHeaderProcessor"
    ).getOrCreate()

    input_file = "gs://retailer-raw-ingest-data/CustomerDIM_full_20240927.txt"
    output_table = "modified-glyph-438213-k8.spark_retailer_dataset.customers"

    process_customer_data(spark, input_file, output_table)