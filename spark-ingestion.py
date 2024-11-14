from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def process_sales_data(spark: SparkSession, input_file, output_table):
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

    input_file = "gs://retailer-raw-ingest-data/SalesHeaderFCT_inc_20240919.txt"
    output_table = "modified-glyph-438213-k8.spark_retailer_dataset.sales_headers"

    process_sales_data(spark, input_file, output_table)
