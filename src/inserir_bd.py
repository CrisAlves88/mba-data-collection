from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import datetime
import time
from minio import Minio
from minio.error import S3Error

# --- 1. CONFIGURAÇÃO DA CONEXÃO SQL (AJUSTE OS VALORES!) ---
JDBC_HOSTNAME = "db"
JDBC_PORT = 5432
JDBC_DATABASE = "mydb"
JDBC_USER = "myuser"
JDBC_PASSWORD = "mypassword"

JDBC_DRIVER = "org.postgresql.Driver" 
JDBC_URL = f"jdbc:postgresql://{JDBC_HOSTNAME}:{JDBC_PORT}/{JDBC_DATABASE}" 

CONNECTION_PROPERTIES = {
    "user": JDBC_USER,
    "password": JDBC_PASSWORD,
    "driver": JDBC_DRIVER
}

# --- 2. CONFIGURAÇÃO DO MINIO (DESTINO) ---
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE = False
BUCKET_NAME = "bronze2"  # <- Variável global definida aqui!

# --- 3. TABELAS A SEREM INGERIDAS ---
DB_SCHEMA = "db_loja"
TABELAS_PARA_INGESTAO = [
    "categorias_produto",
    "cliente",
    "pedido_cabecalho",
    "pedido_itens",
    "produto"
    
]

# --- 4. FUNÇÕES DE SUPORTE ---
def get_unique_timestamp():
    """Gera um timestamp único (YYYYMMDD_HHMMSS_micros) para o nome da pasta."""
    agora = datetime.datetime.now()
    return agora.strftime('%Y%m%d_%H%M%S') #+ str(int(time.time() * 1000) % 1000000)

def ingest_full_load(spark_session: SparkSession, table_name: str, target_base_path: str):
    """
    Realiza a ingestão completa (Full Load) de uma tabela do banco de dados 
    e salva no MinIO no formato Parquet.
    """
    # A variável BUCKET_NAME é acessada aqui porque é global.
    #global BUCKET_NAME 
    
    print(f"\n--- INICIANDO INGESTÃO FULL LOAD: {table_name} ---")

    db_table_name = f"{DB_SCHEMA}.{table_name}"
    
    # 1. Carrega o DataFrame do SQL
    df = spark_session.read.jdbc(
        url=JDBC_URL,
        table=db_table_name,
        properties=CONNECTION_PROPERTIES
    )
    
    print(f"DataFrame '{table_name}' carregado. Total de registros: {df.count()}")
    
    # 2. Define o caminho de destino no MinIO
    target_path = f"{target_base_path}/{table_name}"
    
    # 3. Salva no MinIO (camada Bronze)
    df.write \
      .mode("overwrite") \
      .parquet(f"s3a://{BUCKET_NAME}/{target_path}")

    print(f"INGESTÃO CONCLUÍDA: {table_name} salva em: {BUCKET_NAME}/{target_path}")

def ingest_incremental_load(spark_session: SparkSession, table_name: str, target_base_path: str, update_column: str):
    # Simplesmente chama o Full Load com o path único para fins de Bronze
    ingest_full_load(spark_session, table_name, target_base_path)


# --- 5. LÓGICA PRINCIPAL (EXECUÇÃO) ---
if __name__ == "__main__":


    # Inicializa a SparkSession
    spark = SparkSession.builder \
        .appName("DBLojaIngestionToBronze") \
        .getOrCreate()
        
    # Gera o timestamp único para a pasta de execução (run-id)
    timestamp_run = get_unique_timestamp()
    
    # Define o caminho base de destino no MinIO/S3
    TARGET_BASE_PATH = f"db_loja/data/{timestamp_run}"
    
    print(f"TARGET BUCKET: {BUCKET_NAME}")
    print(f"TARGET PATH BASE PARA ESTA EXECUÇÃO: {TARGET_BASE_PATH}")
    
    # Garante que o cliente Minio esteja configurado para verificar o bucket
    try:
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
        
        # AQUI ESTAVA O PROBLEMA: Agora BUCKET_NAME é global e acessível.
        if not minio_client.bucket_exists(BUCKET_NAME): 
            minio_client.make_bucket(BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' criado com sucesso.")
        else:
            print(f"Bucket '{BUCKET_NAME}' já existe.")
            
    except Exception as e:
        print(f"ERRO DE CONEXÃO/CRIAÇÃO DO BUCKET NO MINIO. Verifique as credenciais: {e}")
        spark.stop()
        exit()
        

    # Itera sobre a lista de tabelas para ingestão
    for tabela in TABELAS_PARA_INGESTAO:
        if tabela == "produto":
            ingest_incremental_load(
                spark, 
                table_name=tabela, 
                target_base_path=TARGET_BASE_PATH, 
                update_column="data_atualizacao"
            )
        else:
            ingest_full_load(
                spark, 
                table_name=tabela, 
                target_base_path=TARGET_BASE_PATH
            )

    print("\n\n*** PROCESSO DE INGESTÃO DO DB_LOJA CONCLUÍDO ***")
    spark.stop()