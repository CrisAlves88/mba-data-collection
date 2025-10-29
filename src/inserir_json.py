import json
import io
import os
import datetime
import time # Adicionado para garantir um timestamp único
from minio import Minio
from minio.error import S3Error

# --- 1. CONFIGURAÇÃO DO MINIO ---
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE = False  # Use True se estiver usando HTTPS/SSL

# --- 2. DADOS DO BUCKET E DO DIRETÓRIO ---
LOCAL_FOLDER = "/workspace/json"  # Onde estão os arquivos JSON
BUCKET_NAME = "bronze"
PREFIXO_MINIO = "json/"  # Prefixo/pasta dentro do bucket
CONTENT_TYPE = "application/json"

def get_unique_timestamp():
    """Gera um timestamp único e preciso (dia, mês, ano, hora, minuto, segundo, microssegundo)"""
    # Usamos time.time() para microssegundos, o que é mais seguro contra colisões rápidas.
    agora = datetime.datetime.now()
    # Formato completo: DDMMYY_HHMMSS
    return agora.strftime('%Y%m%d_%H%M%S')

def upload_json_files_to_minio_unique():
    """
    Conecta ao MinIO e faz o upload de todos os arquivos JSON do diretório local,
    garantindo que o nome do objeto seja único e não haja sobrescrita.
    """
    
    if not os.path.isdir(LOCAL_FOLDER):
        print(f"ERRO: O diretório '{LOCAL_FOLDER}' não existe ou não é acessível.")
        return

    try:
        # Inicializa o cliente MinIO
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )

        # --- 3. VERIFICA E CRIA O BUCKET SE NÃO EXISTIR ---
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' criado com sucesso.")
        else:
            print(f"Bucket '{BUCKET_NAME}' já existe.")

        # --- 4. Itera sobre os arquivos no diretório local ---
        uploaded_count = 0
        
        for filename in os.listdir(LOCAL_FOLDER):
            if filename.endswith(".json"):
                local_file_path = os.path.join(LOCAL_FOLDER, filename)
                
                # --- GERAÇÃO DO NOME DO OBJETO ÚNICO ---
                # 1. Obtém a parte principal do nome do arquivo (sem a extensão .json)
                base_name = os.path.splitext(filename)[0]
                # 2. Gera o timestamp único
                timestamp_unico = get_unique_timestamp()
                
                # 3. Monta o novo nome do objeto no MinIO: PREFIXO/base_name_TIMESTAMP.json
                # Exemplo: json_vendas/dados_extrato_20251027_221005123456.json
                object_name = f"{PREFIXO_MINIO}{base_name}_{timestamp_unico}.json"
                # ----------------------------------------

                print(f"\nIniciando processamento do arquivo: {filename}")
                
                # --- 5. LÊ O CONTEÚDO DO ARQUIVO LOCAL ---
                try:
                    with open(local_file_path, 'rb') as file_data:
                        file_bytes = file_data.read()
                        data_length = len(file_bytes)
                        json_stream = io.BytesIO(file_bytes)
                    
                except FileNotFoundError:
                    print(f"AVISO: Arquivo {filename} não encontrado. Pulando.")
                    continue
                
                # --- 6. FAZ O UPLOAD DO OBJETO ---
                print(f"Upload: '{object_name}' (Tamanho: {data_length} bytes)")

                result = client.put_object(
                    bucket_name=BUCKET_NAME,
                    object_name=object_name,
                    data=json_stream,
                    length=data_length,
                    content_type=CONTENT_TYPE
                )
                
                uploaded_count += 1
                print(f"UPLOAD OK! Objeto: {result.object_name}")

        print(f"\n--- RESUMO: {uploaded_count} ARQUIVO(S) JSON ENVIADO(S) COM NOMES ÚNICOS ---")

    except S3Error as err:
        print(f"ERRO S3 ao interagir com o MinIO: {err}")
    except Exception as e:
        print(f"OCORREU UM ERRO INESPERADO: {e}")

if __name__ == "__main__":
    upload_json_files_to_minio_unique()