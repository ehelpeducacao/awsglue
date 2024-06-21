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
prefix = 'sales_force/carteiras/'

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
    
# filtrar apenas os registros com a maior data de modificação
df_trusted = df_trusted.where("rank = 1")

# Montar dataframe com nome da regional com idRegional e nomeRegional e juntar com dados de df_trusted
path_regional = "s3://refined-zone-echo/dim_regional/"
df_regional = spark.read.parquet(path_regional)
df_regional = df_regional.select(
    f.col("idRegional"),
    f.col("nomeRegional")
    )

path_usuario = "s3://refined-zone-echo/dim_usuario/"
df_usuario = spark.read.parquet(path_usuario)
df_usuario = df_usuario.select(
    f.col("idusuario"),
    f.col("nomeCompleto")
    )

path_distribuidora = "s3://refined-zone-echo/dim_distribuidora/"
df_distrib = spark.read.parquet(path_distribuidora)
df_distrib = df_distrib.select(
    f.col("idDistribuidora"),
    f.col("nomeDistribuidora")
    )
 
df_join = df_trusted.join(df_regional, f.col("regional__c") == f.col("idRegional"), "left")\
    .join(df_usuario, f.col("representante_de_venda__c") == f.col("idUsuario"), "left")\
    .join(df_distrib, f.col("distribuidora__c")==f.col("idDistribuidora"), "left")

# Montar o dataframe limpo e ajustado da dimensão para ser salvo
df = df_join.select(
    f.col("id").alias("idCarteira"),
    f.col("name").alias("nomeCarteira"),
    f.coalesce(f.col("idUsuario"),f.lit("N/A")).alias("idrepresentanteDeVenda"),
    f.coalesce(f.col("nomeCompleto"), f.lit("N/A")).alias("representanteDeVenda"),
    f.coalesce(f.col("idRegional"),f.lit("N/A")).alias("idRegional"),
    f.coalesce(f.col("nomeRegional"), f.lit("N/A")).alias("regional"),
    f.coalesce(f.col("idDistribuidora"),f.lit("N/A")).alias("idDistribuidora"),
    f.coalesce(f.col("nomeDistribuidora"), f.lit("N/A")).alias("distribuidora"),
    f.coalesce(f.col("lastactivitydate"), f.lit("N/A")).alias("ultimaAtividade"),
    f.when(f.col("isdeleted") == "true", "SIM") \
        .when(f.col("isdeleted") == "false", "NAO") \
        .otherwise("N/A") \
        .alias("foiDeletado"),
    f.col("lastmodifieddate").alias("dataUltimaModificacao"),
    f.col("ts_refined").alias("datahoraProcessamento")
)

output_path = "s3a://refined-zone-echo/dim_carteira/"
df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(output_path)

job.commit()