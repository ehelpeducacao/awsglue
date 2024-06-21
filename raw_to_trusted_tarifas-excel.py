import datetime
import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from com.crealytics.spark.excel import *


# Lista dos argumentos esperados
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'process_name', 'ano', 'mes', 'dia'])

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
ano = args['ano']
mes = args['mes']
dia = args['dia']
file_name = 'db-tarifas-trsolucoes.xlsx'

# Constrói o caminho do arquivo com a data de ontem
caminho_arquivo = f"s3://raw-zone-echo/tarifas/{ano}/{mes}/{dia}/*/*/*/{file_name}"

# Leitura do arquivo Excel
df = spark.read.format("com.crealytics.spark.excel") \
    .option("useHeader", "true") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("inferSchema", "true") \
    .load(caminho_arquivo)

# Modificado para criar o Parquet na nova estrutura de pasta
caminho_parquet = f"s3://trusted-zone-echo/tarifas/{process_name}"

# Ajuste na partição, assumindo que você queria usar as variáveis ano, mes, dia
# Se 'data' é uma coluna no seu DataFrame, substitua ['ano', 'mes', 'dia'] por ['data']
df.write.partitionBy("Data") \
    .option("compression", "snappy") \
    .mode("overwrite") \
    .parquet(caminho_parquet)

job.commit()