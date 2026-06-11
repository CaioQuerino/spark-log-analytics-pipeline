import os
import sys

# Adiciona o diretório src ao path para importar utils e outros labs
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from utils.get_s3_client import get_s3_client
from lab02_batch_process import run_lab_02

load_dotenv()

def check_for_new_data(bucket_name, prefix):
    """Verifica se existem arquivos CSV na pasta raw"""
    s3 = get_s3_client()
    print(f"🔍 Verificando novos dados em s3://{bucket_name}/{prefix}")
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' in response:
        # Filtra apenas arquivos .csv que não sejam a própria pasta
        files = [
            obj['Key'] for obj in response['Contents']
            if obj['Key'].endswith('.csv')
        ]
        return files
    return []

def move_file_to_archive(bucket_name, file_key):
    """Move o arquivo processado para evitar re-processamento"""
    s3 = get_s3_client()
    copy_source = {'Bucket': bucket_name, 'Key': file_key}
    new_key = file_key.replace("raw/", "archive/")
    
    print(f"📦 Movendo {file_key} para {new_key}...")
    
    # Copia o objeto para o novo destino
    s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=new_key)
    # Deleta o original
    s3.delete_object(Bucket=bucket_name, Key=file_key)

def run_orchestration():
    bucket_name = os.getenv("S3_BUCKET_NAME")
    raw_prefix = "raw/logs/"
    
    # 1. Verificar se há trabalho a ser feito
    new_files = check_for_new_data(bucket_name, raw_prefix)
    
    if not new_files:
        print("✨ Nada novo para processar. Encerrando.")
        return

    msg = f"🚀 {len(new_files)} novos arquivos detectados. Iniciando Spark..."
    print(msg)

    try:
        # 2. Chamar o Job do Spark (Lab 02)
        # O Lab 02 processará os arquivos CSV listados no prefixo
        run_lab_02()
        
        print("✅ Spark Job concluído com sucesso.")
        
        # 3. Pós-processamento com Boto3: Limpeza do Landing Zone
        for file_key in new_files:
            move_file_to_archive(bucket_name, file_key)
            
        print("🏁 Orquestração finalizada com sucesso.")
        
    except Exception as e:
        print(f"❌ Erro durante a orquestração: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_orchestration()