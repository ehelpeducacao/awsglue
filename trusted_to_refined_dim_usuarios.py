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
prefix = 'sales_force/usuarios/'

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

schema = StructType([
    StructField("id", StringType(), True),
    StructField("contactid", StringType(), True),
    StructField("accountid", StringType(), True),
    StructField("aboutme", StringType(), True),
    StructField("name", StringType(), True),
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("regional__c", StringType(), True),
    StructField("state", StringType(), True),
    StructField("street", StringType(), True),
    StructField("uf__c", StringType(), True),
    StructField("country", StringType(), True),
    StructField("createdbyid", StringType(), True),
    StructField("createddate", StringType(), True),
    StructField("department", StringType(), True),
    StructField("division", StringType(), True),
    StructField("email", StringType(), True),
    StructField("title", StringType(), True),
    StructField("employeenumber", StringType(), True),
    StructField("gerencia__c", StringType(), True),
    StructField("isactive", StringType(), True),
    StructField("lastlogindate", StringType(), True),
    StructField("lastmodifieddate", StringType(), True),
    StructField("lastreferenceddate", StringType(), True),
    StructField("lastvieweddate", StringType(), True),
    StructField("managerid", StringType(), True),
    StructField("matricula__c", StringType(), True),
    StructField("mobilephone", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("profileid", StringType(), True),
    StructField("usertype", StringType(), True)
])

s3_prefix = "s3a://trusted-zone-echo/"

# Filtrar apenas os arquivos (sem diretórios)
data_files = [file for file in files if not file.endswith('/')]

# Ler e unir os arquivos Parquet de cada caminho
dfs = []
for file in data_files:
    path = s3_prefix + file
    try:
        df = spark.read.schema(schema).parquet(path)
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
df_trusted = df_trusted.where("rank = 1")

df = df_trusted.select(
    f.col("id").alias("idUsuario"),
    f.coalesce(f.col("contactId"), f.lit("N/A")).alias("idContato"),
    f.col("name").alias("nomeCompleto"),
    f.col("firstname").alias("primeiroNome"),
    f.col("lastname").alias("ultimoNome"),
    f.coalesce(f.col("aboutme"), f.lit("N/A")).alias("sobre"),
    f.coalesce(f.col("email"), f.lit("N/A")).alias("ëmail"),
    f.coalesce(f.col("mobilephone"), f.col("phone"), f.lit("N/A")).alias("telefone"),
    f.coalesce(f.col("phone"), f.lit("N/A")).alias("outroTelefone"),
    f.col("usertype").alias("tipoDeUsuario"),
    f.coalesce(f.col("department"), f.lit("N/A")).alias("departamento"),
    f.coalesce(f.col("division"), f.lit("N/A")).alias("divisao"),
    f.coalesce(f.col("gerencia__c"), f.lit("N/A")).alias("gerencia"),
    f.coalesce(f.col("matricula__c"), f.lit("N/A")).alias("matricula"),
    f.when(f.col("isactive") == "true", "Ativo")\
         .when(f.col("isactive") == "false", "Inativo")\
         .otherwise("N/A")\
         .alias("ativoInativo"),
    f.col("lastlogindate").alias("ultimoLogin"),
    f.col("lastmodifieddate").alias("dataUltimaModificacao"),
    f.col("ts_refined").alias("datahoraProcessamento")
)
output_path = "s3a://refined-zone-echo/dim_usuario/"
df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(output_path)

job.commit()