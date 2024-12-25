from pyspark.sql import SparkSession
import os
from SparkExceptionLogger import SparkExceptionLogger

process_name = "sales"
sub_process = "quaterly_sales"

spark = (
    SparkSession.builder.master("spark://spark-master:7077")
    .appName("spark-minio")
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT"))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_SECRET_KEY"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .getOrCreate()
)


@SparkExceptionLogger(
    spark, process_name, __file__, sub_process=sub_process,
    service_name="local", log_to_path=True,
    log_path="s3a://warehouse/logging/",
)
def main():
    # Create a dataframe of sample data to write to the Iceberg table
    df = spark.createDataFrame(
        [
            (1, "foo"),
            (2, "bar"),
            (3, "baz"),
        ],
        ["id", "value"],
    )

    # Writing into a non-existent table for failing it explicitly.
    # To generate a failure log.
    df.coalesce(1).write.insertInto("db.non_existing_table")


if __name__ == "__main__":
    main()
