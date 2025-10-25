import json
import io
import datetime
from minio import Minio
from minio.error import S3Error

# --- 1. CONFIGURAÇÃO DO MINIO ---
# ATENÇÃO: Substitua estes valores pelos seus dados de conexão!
MINIO_ENDPOINT = "minio:9000"  # Exemplo: "192.168.1.10:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE = False  # Use True se estiver usando HTTPS/SSL
LOCAL_FOLDER = "/workspace/json"  # caminho real no teu Codespace

# --- 2. DADOS DO BUCKET E DO OBJETO JSON ---
agora = datetime.datetime.now()
timestamp_formatado = agora.strftime('%d%m%y%H%M')

BUCKET_NAME = "raw"
OBJECT_NAME = "json/dados_extrato_" + timestamp_formatado + "_.json"  # O nome que o arquivo terá no MinIO
CONTENT_TYPE = "application/json"
DATA_JSON =  "/workspace/json"

def upload_json_to_minio():
    """Conecta ao MinIO, garante que o bucket exista e faz o upload do objeto JSON."""
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

        # --- 4. CONVERTE O OBJETO JSON EM BYTES ---
        # Serializa o objeto Python (dict) para uma string JSON formatada (indent=2)
        json_string = json.dumps(DATA_JSON, indent=2)
        # Codifica a string JSON em bytes (necessário para o put_object)
        json_bytes = json_string.encode('utf-8')
        # Cria um stream de bytes (BytesIO)
        json_stream = io.BytesIO(json_bytes)
        # Obtém o tamanho dos dados
        data_length = len(json_bytes)

        # --- 5. FAZ O UPLOAD DO OBJETO ---
        print(f"Iniciando upload do objeto '{OBJECT_NAME}' para o bucket '{BUCKET_NAME}'...")

        result = client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            data=json_stream,
            length=data_length,
            content_type=CONTENT_TYPE
        )

        print("\n--- UPLOAD CONCLUÍDO ---")
        print(f"Objeto: {result.object_name}")
        print(f"ETag: {result.etag}")
        print(f"Localização: {BUCKET_NAME}/{OBJECT_NAME}")

    except S3Error as err:
        print(f"Erro ao interagir com o MinIO: {err}")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")

if __name__ == "__main__":
    upload_json_to_minio()