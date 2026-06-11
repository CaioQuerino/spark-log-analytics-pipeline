import os
import sys

# Adiciona o diretório src ao path para permitir a importação de utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, when, lower
from utils.initialize_spark import initialize_spark
from dotenv import load_dotenv

load_dotenv()

def run_lab_02():
    spark = initialize_spark()
    print("Spark Session iniciada com sucesso para o Lab 02.")

    # 1. Definição de Schema (Boa prática de Engenharia de Dados)
    schema = StructType([
        StructField("log_id", StringType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("user_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("status_code", IntegerType(), True)
    ])

    # 2. Leitura de CSV (S3)
    # Substitua pelo seu path real ou um arquivo local de teste
    input_path = f"s3a://{os.getenv('S3_BUCKET_NAME')}/raw/logs/*.csv"
    
    print(f"Lendo dados de: {input_path}")
    
    # Lendo dados do S3 usando o schema definido e tratando o cabeçalho
    df = spark.read.csv(input_path, schema=schema, header=True)

    # 3. Transformações (Data Cleaning)
    # - Padronizar ações para minúsculo
    # - Criar flag de erro para status_code >= 400
    processed_df = df.withColumn("action", lower(col("action"))) \
                     .withColumn("is_error", when(col("status_code") >= 400, True).otherwise(False)) \
                     .filter(col("user_id").isNotNull())

    # 4. Escrita em Parquet (Particionado por status para performance)
    output_path = f"s3a://{os.getenv('S3_BUCKET_NAME')}/processed/logs_parquet/"
    
    print(f"Escrevendo dados particionados em: {output_path}")
    # Salvando efetivamente no S3
    processed_df.write.mode("overwrite").partitionBy("status_code").parquet(output_path)
    processed_df.show()

if __name__ == "__main__":
    run_lab_02()
