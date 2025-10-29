import json
import io
import datetime
import time
import requests # <- Adicionamos a biblioteca requests para chamadas HTTP
from minio import Minio
from minio.error import S3Error

# --- 1. CONFIGURAÇÃO DO MINIO ---
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE = False

# --- 2. CONFIGURAÇÃO DA API E DESTINO ---
API_URL = "https://brasilapi.com.br/api/ibge/uf/v1"

# Destino no MinIO
BUCKET_NAME = "bronze"
PREFIXO_MINIO = "api/ibge_uf/" # Nova pasta para dados de API
BASE_OBJECT_NAME = "ibge_uf_data" # Nome base do arquivo antes do timestamp
CONTENT_TYPE = "application/json"

def get_unique_timestamp():
    """Gera um timestamp único e preciso (ano, mês, dia, hora, minuto, segundo, microssegundo)."""
    agora = datetime.datetime.now()
    # Formato: YYYYMMDD_HHMMSS_microseconds
    return agora.strftime('%Y%m%d_%H%M%S') #+ str(int(time.time() * 1000) % 1000000)

def fetch_and_upload_api_data():
    """
    Busca dados de uma API, gera um nome de objeto único com timestamp
    e faz o upload do JSON para o MinIO.
    """
    try:
        # --- 3. REQUISIÇÃO À API ---
        print(f"Buscando dados da API: {API_URL}")
        
        # Faz a requisição GET
        response = requests.get(API_URL)
        response.raise_for_status() # Lança exceção para códigos de erro HTTP (4xx ou 5xx)
        
        # Converte a resposta (JSON) em um objeto Python
        api_data = response.json()
        print("Dados da API recebidos com sucesso.")
        
        # --- 4. CONFIGURAÇÃO MINIO E BUCKET ---
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )

        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' criado com sucesso.")
        else:
            print(f"Bucket '{BUCKET_NAME}' já existe.")

        # --- 5. PREPARAÇÃO DO OBJETO ÚNICO ---
        timestamp_unico = get_unique_timestamp()
        
        # Monta o nome final do objeto: api/ibge_uf/ibge_uf_data_TIMESTAMP.json
        object_name = f"{PREFIXO_MINIO}{BASE_OBJECT_NAME}_{timestamp_unico}.json"

        # Serializa o objeto Python (lista/dict) para uma string JSON e codifica em bytes
        json_string = json.dumps(api_data, indent=2, ensure_ascii=False)
        json_bytes = json_string.encode('utf-8')
        
        # Cria o stream de bytes
        json_stream = io.BytesIO(json_bytes)
        data_length = len(json_bytes)

        # --- 6. FAZ O UPLOAD ---
        print(f"Iniciando upload do objeto único '{object_name}'...")

        result = client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=object_name,
            data=json_stream,
            length=data_length,
            content_type=CONTENT_TYPE
        )

        print("\n--- INGESTÃO DE API CONCLUÍDA ---")
        print(f"Objeto Salvo: {result.object_name}")
        print(f"Localização: {BUCKET_NAME}/{object_name}")

    except requests.exceptions.HTTPError as errh:
        print(f"Erro HTTP ao acessar a API: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Erro de Conexão: {errc}")
    except S3Error as err_minio:
        print(f"Erro ao interagir com o MinIO: {err_minio}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")

if __name__ == "__main__":
    fetch_and_upload_api_data()