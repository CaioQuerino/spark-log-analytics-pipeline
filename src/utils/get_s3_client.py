import boto3
import os

from dotenv import load_dotenv

load_dotenv()

def get_s3_client():
    """Inicializa o cliente S3 usando as credenciais do .env"""
    return boto3.client (
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

if __name__ == "__main__":
    get_s3_client()
    print("Cliente S3 inicializado com sucesso.")