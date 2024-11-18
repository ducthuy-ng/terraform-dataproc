from pyspark import SparkConf
from pyspark.sql import SparkSession


def append_customers(spark: SparkSession, input_file, output_table):
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
    )
    df.write.format("bigquery").mode("append").option("table", output_table).save()


def append_products(spark: SparkSession, input_file, output_table):
    schema = [
        "INVOICE_DATE DATE",
        "STORE_CODE INTEGER",
        "TILL_NO INTEGER",
        "SALE_INVC_TYPE INTEGER",
        "INVOICE_NO INTEGER",
        "SALE_LINE_NO INTEGER",
        "SALE_LINE_TYPE STRING",
        "PRODUCT_CODE STRING",
        "SALE_TOT_QTY FLOAT",
        "SALE_NET_VAL FLOAT",
        "SALE_TOT_TAX_VAL FLOAT",
        "ACTUAL_PRODUCT_PRICE FLOAT",
        "PRODUCT_PRICE FLOAT",
        "SALE_VAL_AT_COST FLOAT",
        "SALE_TOT_DISC_VAL FLOAT",
        "RETURN_REASON_CODE STRING",
        "PROMOTION_CODE STRING",
        "PROMO_OFFER_PRICE FLOAT",
        "DELIVERY_STORE_CODE INTEGER",
        "SHIPMENT_METHOD_CODE STRING",
        "SHIPMENT_CHANNEL_CODE STRING",
        "SESSION_ID INTEGER",
        "ORDER_DATE DATE",
        "ORDER_ID INTEGER",
        "ORDER_LINE_NO INTEGER",
        "OFFER_CODE STRING",
        "REEDEMED_COUPON_CODE STRING",
    ]
    schema_str = ", ".join(schema)

    df = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .option("encoding", "ISO-8859-1")
        .option("header", True)
        .schema(schema_str)
        .load(input_file)
    )
    df.write.format("bigquery").mode("append").option("table", output_table).save()


def append_sale_headers(spark: SparkSession, input_file, output_table):
    # Define schema for the SalesHeader data as a string array
    schema = [
        "INVOICE_DATE DATE",
        "STORE_CODE INTEGER",
        "TILL_NO INTEGER",
        "SALE_INVC_TYPE INTEGER",
        "INVOICE_NO INTEGER",
        "SALE_INVC_START_TIME TIMESTAMP",
        "SALE_INVC_END_TIME TIMESTAMP",
        "CUSTOMER_CODE STRING",
        "SESSION_ID INTEGER",
        "ORDER_ID INTEGER",
        "ORDER_DATE DATE",
        "SALE_NET_VAL FLOAT",
        "SALE_TOT_QTY FLOAT",
        "LOYALTY_CARD_NO STRING",
        "LOYALTY_POINTS_ISSUED FLOAT",
        "LOYALTY_POINTS_REDEEMED FLOAT",
        "CARD_TYPE_CODE STRING",
        "LOYALTY_SCHEME_ID STRING",
        "LOYALTY_POOL_ID STRING",
        "CUSTOMER_IDENTITY_TYPE STRING",
        "CUSTOMER_IDENTITY_VAL STRING",
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
    )
    df.write.format("bigquery").mode("append").option("table", output_table).save()


def append_sale_lines(spark: SparkSession, input_file, output_table):
    schema = [
        "INVOICE_DATE DATE",
        "STORE_CODE INTEGER",
        "TILL_NO INTEGER",
        "SALE_INVC_TYPE INTEGER",
        "INVOICE_NO INTEGER",
        "SALE_LINE_NO INTEGER",
        "SALE_LINE_TYPE STRING",
        "PRODUCT_CODE STRING",
        "SALE_TOT_QTY FLOAT",
        "SALE_NET_VAL FLOAT",
        "SALE_TOT_TAX_VAL FLOAT",
        "ACTUAL_PRODUCT_PRICE FLOAT",
        "PRODUCT_PRICE FLOAT",
        "SALE_VAL_AT_COST FLOAT",
        "SALE_TOT_DISC_VAL FLOAT",
        "RETURN_REASON_CODE STRING",
        "PROMOTION_CODE STRING",
        "PROMO_OFFER_PRICE FLOAT",
        "DELIVERY_STORE_CODE INTEGER",
        "SHIPMENT_METHOD_CODE STRING",
        "SHIPMENT_CHANNEL_CODE STRING",
        "SESSION_ID INTEGER",
        "ORDER_DATE DATE",
        "ORDER_ID INTEGER",
        "ORDER_LINE_NO INTEGER",
        "OFFER_CODE STRING",
        "REEDEMED_COUPON_CODE STRING",
    ]
    schema_str = ", ".join(schema)

    df = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .option("encoding", "ISO-8859-1")
        .option("header", True)
        .schema(schema_str)
        .load(input_file)
    )
    df.write.format("bigquery").mode("append").option("table", output_table).save()


if __name__ == "__main__":
    conf = (
        SparkConf()
        .set("temporaryGcsBucket", "spark-artifacts")
        .set("spark.spark.network.timeout", "800s")
        .set("spark.executor.heartbeatInterval", "60s")
    )
    spark: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()  # type: ignore

    append_customers(
        spark,
        "gs://retailer-raw-ingest-data/CustomerDIM_full_20240927.txt",
        "modified-glyph-438213-k8.spark_retailer_dataset.customers",
    )

    append_sale_headers(
        spark,
        "gs://retailer-raw-ingest-data/SalesHeaderFCT_inc_20240920.txt",
        "modified-glyph-438213-k8.spark_retailer_dataset.sales_headers",
    )

    append_products(
        spark,
        "gs://retailer-raw-ingest-data/SalesLineFCT_inc_20240919.txt",
        "modified-glyph-438213-k8.spark_retailer_dataset.sales_lines",
    )

    append_sale_lines(
        spark,
        "gs://retailer-raw-ingest-data/SalesLineFCT_inc_20240919.txt",
        "modified-glyph-438213-k8.spark_retailer_dataset.sales_lines",
    )
