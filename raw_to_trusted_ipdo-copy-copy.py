import datetime
import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# Lista dos argumentos esperados
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'process_name', 'ano', 'mes', 'dia', 'subproc'])

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
subproc = args['subproc']
file_name = f'db-ipdo-{subproc}.csv'

# Constrói o caminho do arquivo com a data de ontem
caminho_arquivo = f"s3://raw-zone-echo/{process_name}/{ano}/{mes}/{dia}/*/*/*/{file_name}"

# Script modificado para ler o arquivo com a data de ontem
df = spark.read.csv(
    path=caminho_arquivo,
    sep=";",               # Define o separador como ponto e vírgula
    header=True,           # Usa a primeira linha como cabeçalho
    quote='"',             # Define o caractere de aspas
    inferSchema=True       # Infere o esquema dos dados
)

# Modificado para criar o Parquet na nova estrutura de pasta
caminho_parquet = f"s3://trusted-zone-echo/ipdo/{subproc}"

# Ajuste na partição, assumindo que você queria usar as variáveis ano, mes, dia
# Se 'data' é uma coluna no seu DataFrame, substitua ['ano', 'mes', 'dia'] por ['data']
df.write.partitionBy("data") \
    .option("compression", "snappy") \
    .mode("overwrite") \
    .parquet(caminho_parquet)

job.commit()