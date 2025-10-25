# -- coding: utf-8 --
"""
Ingestão da API BrasilAPI (IBGE UF) para a camada Bronze no MinIO.
"""

import requests
from datetime import datetime
from minio import Minio
from io import BytesIO
import json

# === CONFIGURAÇÕES ===
API_URL = "https://brasilapi.com.br/api/ibge/uf/v1"
BUCKET_NAME = "raw"
BRONZE_PREFIX = "api/ibge_uf"

# Conexão MinIO
MINIO_ENDPOINT = "minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
SECURE = False

# === CONECTAR AO MINIO ===
client = Minio(
    MINIO_ENDPOINT,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=SECURE
)

if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)
    print(f"Bucket '{BUCKET_NAME}' criado.")
else:
    print(f"Bucket '{BUCKET_NAME}' já existe.")

# === FAZER REQUISIÇÃO HTTP ===
print(f"Requisitando dados da API: {API_URL}")
response = requests.get(API_URL)

if response.status_code != 200:
    raise Exception(f"Erro ao acessar API: {response.status_code}")

data = response.json()
print(f"{len(data)} registros recebidos da API BrasilAPI (IBGE UF).")

# === SALVAR NO MINIO ===
date_str = datetime.now().strftime("%Y-%m-%d")
base_path = f"{BRONZE_PREFIX}/data_ingestao={date_str}/"
object_name = f"{base_path}uf.json"

json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")

client.put_object(
    BUCKET_NAME,
    object_name,
    BytesIO(json_bytes),
    length=len(json_bytes),
    content_type="application/json"
)

print(f"Arquivo enviado para o MinIO -> {object_name}")
print("Ingestão da API concluída com sucesso!")
