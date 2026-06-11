import os
import sys
import glob
from datetime import datetime
from dotenv import load_dotenv
from utils.get_s3_client import get_s3_client

# Garante que o diretório atual está no path para importações locais
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

load_dotenv()

def save_new_data(data_pattern: str):
    """
    Lê arquivos locais e faz o upload para o S3 com timestamp no nome.
    Nota: Usamos boto3 diretamente para upload de arquivos locais.
    """
    s3 = get_s3_client()
    bucket_name = os.getenv('S3_BUCKET_NAME')
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Busca todos os arquivos que casam com o padrão (ex: src/data/*.csv)
    files = glob.glob(data_pattern)
    
    if not files:
        print(f"Nenhum arquivo encontrado em: {data_pattern}")
        return

    for file_path in files:
        file_name = os.path.basename(file_path)

        name_part, ext_part = os.path.splitext(file_name)
        new_file_name = f"{name_part}_{date_str}{ext_part}"
        
        s3_key = f"raw/logs/{new_file_name}"
        
        print(f"Subindo {file_path} para s3://{bucket_name}/{s3_key}")
        s3.upload_file(file_path, bucket_name, s3_key)

if __name__ == "__main__":
    data_path = os.path.join("src", "data", "*.csv")
    save_new_data(data_path)