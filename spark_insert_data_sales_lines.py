from pyspark.sql import SparkSession


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
    spark: SparkSession = SparkSession.builder.getOrCreate()  # type: ignore

    spark.conf.set("temporaryGcsBucket", "spark-artifacts")

    input_file = "gs://retailer-raw-ingest-data/SalesLineFCT_inc_20240919.txt"
    output_table = "modified-glyph-438213-k8.spark_retailer_dataset.sales_lines"

    append_sale_lines(spark, input_file, output_table)
