import os
import sys
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit, rand
from pyspark.sql.types import StructType

# Adiciona o diretório src ao path para importar utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from utils.initialize_spark import initialize_spark
from utils.get_s3_client import get_s3_client

load_dotenv()

def load_from_s3(bucket_name: str, prefix: str) -> DataFrame:
    """
    Implemente uma função que use Boto3 para listar todos os arquivos Parquet
    em um bucket e retorne um DataFrame PySpark unificado.
    Utilizar Boto3 aqui permite uma filtragem prévia de metadados antes
    de carregar o Spark.
    """
    s3 = get_s3_client()
    spark = initialize_spark()

    # Limpando o prefixo para o Boto3 listar o diretório corretamente
    clean_prefix = prefix.replace("*.parquet", "")
    s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=clean_prefix)

    if 'Contents' in s3_objects:
        # Constrói caminhos s3a:// completos para que o Spark localize os dados
        parquet_files = [
            f"s3a://{bucket_name}/{obj['Key']}"
            for obj in s3_objects['Contents']
            if obj['Key'].endswith('.parquet')
        ]
        if parquet_files:
            return spark.read.parquet(*parquet_files)

    # Retorna um DataFrame vazio tipado caso não encontre arquivos
    return spark.createDataFrame([], StructType([]))


def optimize_shuffles(df: DataFrame) -> DataFrame:
    """
    Implementação de técnicas de redução de shuffle.
    O Coalesce reduz partições evitando um full shuffle (movimentação mínima).
    """
    print(f"📊 Partições originais: {df.rdd.getNumPartitions()}")

    # Reduzindo para 1 partição (comum após filtros pesados ou para gerar arquivo único)
    df_optimized = df.coalesce(1)
    print(f"✅ Partições após coalesce: {df_optimized.rdd.getNumPartitions()}")

    return df_optimized


def handle_skewed_data(df: DataFrame, column: str) -> DataFrame:
    """
    Trata desbalanceamento de dados (Skewness) usando Salting.
    Adiciona um sufixo aleatório à chave para melhor distribuição em Joins.
    """
    print(f"🧂 Aplicando Salting na coluna: {column}")
    salt_bins = 5
    return df.withColumn(
        f"{column}_salted",
        concat(col(column), lit("_"), (rand() * salt_bins).cast("int"))
    )


if __name__ == "__main__":
    bucket_name = os.getenv("S3_BUCKET_NAME")
    # Prefixo onde os dados processados pelo Lab 02 foram salvos
    raw_prefix = "processed/logs_parquet/"

    df_raw = load_from_s3(bucket_name, raw_prefix)

    if df_raw.count() > 0:
        df_optimized = optimize_shuffles(df_raw)
        df_salted = handle_skewed_data(df_optimized, "user_id")

        print("🧐 Visualizando dados com Salting:")
        df_salted.select("user_id", "user_id_salted").show(5)
        print("🚀 Lab 04: Performance e Otimização concluído.")
    else:
        print("⚠️ Nenhum dado encontrado em processed/logs_parquet/ para otimizar.")
