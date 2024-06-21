from datetime import datetime, timedelta
import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pyspark.sql.functions as f



# Lista dos argumentos esperados
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'process_name'])

# Inicializando o Spark e Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)

# Inicializando a SparkSession a partir do GlueContext
spark = glueContext.spark_session

# Inicializando o Job do Glue
job = Job(glueContext)
job.init(job_name=args['JOB_NAME'])

# Acessando os parâmetros passados para o job
process_name = args['process_name']
data_atual = datetime.now()
ano = data_atual.year
mes = data_atual.month
dia = data_atual.day
mes = f"{mes:02d}"
dia = f"{dia:02d}"

file_name ='vigent_contracts.csv'

# Constrói o caminho do arquivo com a data de ontem
caminho_arquivo = f"s3://raw-zone-echo/{process_name}/vigent_contracts/{ano}/{mes}/{dia}/*/*/*/{file_name}"

# Script modificado para ler o arquivo com a data de ontem
df = spark.read.csv(
    path=caminho_arquivo,
    sep=";",               # Define o separador como ponto e vírgula
    header=True,           # Usa a primeira linha como cabeçalho
    quote='"',             # Define o caractere de aspas
    encoding='UTF-8',      # Especificando o encoding do arquivo
    inferSchema=True       # Infere o esquema dos dados
)


df = df.withColumn("data_hora_processamento", f.from_utc_timestamp(f.current_timestamp(), "GMT-3").cast(TimestampType()))

# Modificado para criar o Parquet na nova estrutura de pasta
caminho_parquet = f"s3://trusted-zone-echo/{process_name}/vigent_contracts/"


df.write.option("compression", "snappy") \
    .mode("append") \
    .parquet(caminho_parquet)

job.commit()