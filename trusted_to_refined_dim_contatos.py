import sys
import pyspark.sql.functions as f
import boto3
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3 = boto3.client('s3')
bucket_name = 'trusted-zone-echo'
prefix = 'sales_force/contatos/'

# Função para listar todos os arquivos no diretório e subdiretórios
def list_files_in_s3(bucket_name, prefix):
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                files.append(obj['Key'])
    return files

files = list_files_in_s3(bucket_name, prefix)

print("Arquivos listados no S3:")
for file in files:
    print(file)

s3_prefix = "s3a://trusted-zone-echo/"

# Filtrar apenas os arquivos (sem diretórios)
data_files = [file for file in files if not file.endswith('/')]

# Ler e unir os arquivos Parquet de cada caminho
dfs = []
for file in data_files:
    path = s3_prefix + file
    try:
        df = spark.read.parquet(path)
        dfs.append(df)
        print(f"Lido com sucesso: {path}")
    except Exception as e:
        print(f"Erro ao ler {path}: {e}")

# Verificar se existem DataFrames lidos com sucesso
if dfs:
    df_trusted = dfs[0]
    for df in dfs[1:]:
        df_trusted = df_trusted.union(df)

    # Definir a hora atual ajustada
    now = (datetime.now() - timedelta(hours=3)).strftime("%m/%d/%Y %H:%M:%S")

    # Definir chave e ordem para a janela
    key = ['id']
    order = 'lastmodifieddate'

    # Definir janela de partição
    w = Window.partitionBy(key).orderBy(f.col(order).desc())

    # Adicionar colunas rank e ts_refined
    df_trusted = df_trusted.withColumn("rank", f.row_number().over(w)) \
        .withColumn("ts_refined", f.to_timestamp(f.lit(now), 'MM/dd/yyyy HH:mm:ss'))
else:
    print("Nenhum DataFrame foi lido com sucesso.")
    
#filtrar apenas os registros com a maior data de modificação
df_trusted = df_trusted.where("rank = 1")

# Montar o dataframe limpo e ajustado da dimensão para ser salvo
df = df_trusted.select(
    f.col("id").alias("idContato"),
    f.col("accountid").alias("idConta"),
    f.coalesce(f.col("title"), f.lit("N/A")).alias("titulo"),
    f.coalesce(f.col("salutation"), f.lit("N/A")).alias("saudacao"),
    f.col("name").alias("nomeCompleto"),
    f.coalesce(f.col("firstname"), f.lit("N/A")).alias("primeiroNome"),
    f.coalesce(f.col("lastname"), f.lit("N/A")).alias("ultimoNome"),
    f.coalesce(f.col("email"), f.lit("N/A")).alias("email"),
    f.coalesce(f.col("mobilephone"), f.col("phone"), f.lit("N/A")).alias("telefone"),
    f.coalesce(f.col("phone"), f.lit("N/A")).alias("outroTelefone"),
    f.coalesce(f.col("mailingstreet"), f.lit("N/A")).alias("endereco"),
    f.coalesce(f.col("mailingcity"), f.lit("N/A")).alias("cidade"),
    f.coalesce(f.col("mailingstate"), f.lit("N/A")).alias("estado"),
    f.coalesce(f.col("mailingpostalcode"), f.lit("N/A")).alias("cep"),
    f.coalesce(f.col("mailingcountry"), f.lit("N/A")).alias("pais"),
    f.coalesce(f.col("carteira__c"), f.lit("N/A")).alias("carteira"),
    f.coalesce(f.col("regional__c"), f.lit("N/A")).alias("regional"),
    f.coalesce(f.col("department"), f.lit("N/A")).alias("departamento"),
    f.coalesce(f.col("lastactivitydate"), f.lit("N/A")).alias("ultimaAtividade"),
    f.when(f.col("isdeleted") == "true", "SIM") \
        .when(f.col("isdeleted") == "false", "NAO") \
        .otherwise("N/A") \
        .alias("foiDeletado"),
    f.col("lastmodifieddate").alias("dataUltimaModificacao"),
    f.col("ts_refined").alias("datahoraProcessamento")
)

output_path = "s3a://refined-zone-echo/dim_contato/"
df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(output_path)

job.commit()