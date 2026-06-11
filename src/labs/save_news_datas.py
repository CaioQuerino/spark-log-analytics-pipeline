import os
import sys
import glob
from datetime import datetime
from dotenv import load_dotenv

# Adiciona o diretório src ao path para importar utils e outros labs
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.get_s3_client import get_s3_client  # noqa: E402


load_dotenv()


def save_new_data(data_pattern: str):
    """
    Lê arquivos locais e faz o upload para o S3 com timestamp no nome.
    Nota: Usamos boto3 diretamente para upload de arquivos locais.
    """
    s3 = get_s3_client()
    default_bucket = 'data-lake-logs-integrations-analytics'
    bucket_name = os.getenv('S3_BUCKET_NAME', default_bucket)
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Busca todos os arquivos que casam com o padrão (ex: src/data/*.csv)
    files = glob.glob(data_pattern)

    if not files:
        print(f"⚠️ Nenhum arquivo encontrado em: {data_pattern}")
        return

    for file_path in files:
        try:
            file_name = os.path.basename(file_path)
            name_part, ext_part = os.path.splitext(file_name)

            # Gerando a chave S3 organizada por prefixo
            new_file_name = f"{name_part}_{date_str}{ext_part}"
            s3_key = f"raw/logs/{new_file_name}"

            print(f"🚀 Subindo {file_path} para s3://{bucket_name}/{s3_key}")
            s3.upload_file(file_path, bucket_name, s3_key)

        except Exception as e:
            print(f"❌ Erro ao subir {file_path}: {str(e)}")


if __name__ == "__main__":
    # Ajustado para o path correto do projeto
    data_path = os.path.join("synapos", "data", "*.csv")
    save_new_data(data_path)