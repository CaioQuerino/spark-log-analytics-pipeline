import os
import sys

from dotenv import load_dotenv
from pyspark.sql.functions import col, lower, when
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.initialize_spark import initialize_spark  # noqa: E402

load_dotenv()


def run_lab_02():
    spark = initialize_spark()
    print("Spark Session iniciada com sucesso para o Lab 02.")

    schema = StructType(
        [
            StructField("log_id", StringType(), False),
            StructField("timestamp", TimestampType(), True),
            StructField("user_id", StringType(), True),
            StructField("action", StringType(), True),
            StructField("status_code", IntegerType(), True),
        ]
    )

    input_path = f"s3a://{os.getenv('S3_BUCKET_NAME')}/raw/logs/*.csv"

    print(f"Lendo dados de: {input_path}")

    df = spark.read.csv(input_path, schema=schema, header=True)

    processed_df = (
        df.withColumn("action", lower(col("action")))
        .withColumn(
            "is_error",
            when(col("status_code") >= 400, True).otherwise(False),
        )
        .filter(col("user_id").isNotNull())
    )

    output_path = (
        f"s3a://{os.getenv('S3_BUCKET_NAME')}/processed/logs_parquet/"
    )

    print(f"Escrevendo dados particionados em: {output_path}")

    processed_df.write.mode("overwrite").partitionBy(
        "status_code"
    ).parquet(output_path)

    processed_df.show()


if __name__ == "__main__":
    run_lab_02()
