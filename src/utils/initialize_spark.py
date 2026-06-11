import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()


def initialize_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("Lab02BatchProcess")
        .master("local[*]")
        .config("spark.driver.host", "localhost")
        .config(
            "spark.jars.packages",
            (
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.datastax.spark:"
                "spark-cassandra-connector_2.12:3.4.1"
            ),
        )
        .getOrCreate()
    )

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    hadoop_conf.set(
        "fs.s3a.endpoint",
        f"s3.{os.getenv('AWS_REGION')}.amazonaws.com",
    )
    hadoop_conf.set(
        "fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )
    hadoop_conf.set("fs.s3a.fast.upload", "true")
    hadoop_conf.set("fs.s3a.fast.upload.buffer", "bytebuffer")

    return spark


if __name__ == "__main__":
    initialize_spark()
    print("Spark Session iniciada com sucesso para o Lab 01.")
