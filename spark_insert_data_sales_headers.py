from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def process_sales_data(spark: SparkSession, input_file, output_table):
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
        "CUSTOMER_IDENTITY_VAL STRING"
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

    print(df.printSchema())

    df.write.format("bigquery").mode("append").option("table", output_table).save()

if __name__ == "__main__":
    spark: SparkSession = SparkSession.builder.appName(
        "SalesHeaderProcessor"
    ).getOrCreate()

    spark.conf.set("temporaryGcsBucket", "spark-artifacts")

    input_file = "gs://retailer-raw-ingest-data/SalesHeaderFCT_inc_20240919.txt"
    output_table = "modified-glyph-438213-k8.spark_retailer_dataset.sales_headers"

    process_sales_data(spark, input_file, output_table)

    df = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .option("encoding", "ISO-8859-1")
        .option("header", True)
        .load(input_file)
    )

    print(df.printSchema())

    df.write.format("bigquery").mode("append").option("table", output_table).save()


if __name__ == "__main__":
    spark: SparkSession = SparkSession.builder.appName(
        "SalesHeaderProcessor"
    ).getOrCreate()

    spark.conf.set("temporaryGcsBucket", "spark-artifacts")

    input_file = "gs://retailer-raw-ingest-data/SalesHeaderFCT_inc_20240920.txt"
    output_table = "modified-glyph-438213-k8.spark_retailer_dataset.sales_headers"

    process_sales_data(spark, input_file, output_table)