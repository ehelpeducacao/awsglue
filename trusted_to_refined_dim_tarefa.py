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
prefix = 'sales_force/tarefas/'

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

# ajustar campos de datas 
df_trusted = df_trusted.withColumn('createddate',
                                   f.to_utc_timestamp(
                                       f.concat_ws(' ', f.expr("substring(createddate, 1, 10)"), 
                                                       f.expr("substring(createddate, 12, 8)")), 
                                       "UTC"))


df_trusted = df_trusted.withColumn('reminderdatetime',
                                   f.to_utc_timestamp(
                                       f.concat_ws(' ', f.expr("substring(reminderdatetime, 1, 10)"), 
                                                       f.expr("substring(reminderdatetime, 12, 8)")), 
                                       "UTC"))

df_trusted = df_trusted.withColumn('activitydate',
                                   f.to_utc_timestamp(
                                       f.concat_ws(' ', f.expr("substring(activitydate, 1, 10)"), 
                                                       f.expr("substring(activitydate, 12, 8)")), 
                                       "UTC"))

df_trusted = df_trusted.withColumn('completeddatetime',
                                   f.to_utc_timestamp(
                                       f.concat_ws(' ', f.expr("substring(completeddatetime, 1, 10)"), 
                                                       f.expr("substring(completeddatetime, 12, 8)")), 
                                       "UTC"))

df_trusted = df_trusted.withColumn('lastmodifieddate',
                                   f.to_utc_timestamp(
                                       f.concat_ws(' ', f.expr("substring(lastmodifieddate, 1, 10)"), 
                                                       f.expr("substring(lastmodifieddate, 12, 8)")), 
                                       "UTC"))
# fim da tratativa das datas

df_usuarios = spark.read.parquet("s3://refined-zone-echo/dim_usuario/")
df_owner = df_usuarios.select(
    f.col("idusuario").alias("idproprietario"),
    f.col("nomeCompleto").alias("nomeproprietario")
    )
df_created = df_usuarios.select(
    f.col("idusuario").alias("idcriacao"),
    f.col("nomeCompleto").alias("criadopor")
    )

# Montar o dataframe df_Compromisso
df = df_trusted.select(
    f.coalesce(f.col("id"), f.lit("N/A")).alias("idTarefa"),
    f.date_format(f.col("createddate"), "dd/MM/yyyy HH:mm").alias("dataCriacao"),
    f.date_format(f.col("activitydate"), "dd/MM/yyyy").alias("dataTarefa"),
    f.coalesce(f.col("subject"), f.lit("N/A")).alias("assunto"),
    f.coalesce(f.col("description"), f.lit("N/A")).alias("descricao"),
    f.coalesce(f.col("accountid"), f.lit("N/A")).alias("idconta"),
    f.coalesce(f.col("fase_atual__c"), f.lit("N/A")).alias("faseAtual"),
    f.coalesce(f.col("status"), f.lit("N/A")).alias("statusTarefa"),
    f.coalesce(f.col("status__c"), f.lit("N/A")).alias("statusAgendamento"),
    f.coalesce(f.col("motivo_nao_realizado__c"), f.lit("N/A")).alias("motivoNaoRealizado"),
    f.coalesce(f.col("priority"), f.lit("N/A")).alias("prioridade"),
        f.when(f.col("ishighpriority") == "true", "SIM") \
        .when(f.col("ishighpriority") == "false", "NAO") \
        .otherwise("N/A") \
        .alias("altaPrioridade"),
    f.coalesce(f.col("observasao_de_conclusao__c"), f.lit("N/A")).alias("observacao"),
    f.coalesce(f.col("type"), f.lit("N/A")).alias("tipo"),
    f.coalesce(f.col("tasksubtype"), f.lit("N/A")).alias("subtipo"),
    f.coalesce(f.col("submercado__c"), f.lit("N/A")).alias("submercado"),
    f.when(f.col("isreminderset") == "true", "SIM") \
        .when(f.col("isreminderset") == "false", "NAO") \
        .otherwise("N/A") \
        .alias("lembrete"),
    f.date_format(f.col("reminderdatetime"), "dd/MM/yyyy HH:mm").alias("datahoraLembrete"),
    f.when(f.col("isclosed") == "true", "SIM") \
        .when(f.col("isclosed") == "false", "NAO") \
        .otherwise("N/A") \
        .alias("tarefaFechada"),   
    f.date_format(f.col("completeddatetime"), "dd/MM/yyyy HH:mm").alias("datahoraCompletado"),
    f.when(f.col("isdeleted") == "true", "SIM") \
        .when(f.col("isdeleted") == "false", "NAO") \
        .otherwise("N/A") \
        .alias("foiDeletado"),
    f.date_format(f.col("lastmodifieddate"), "dd/MM/yyyy HH:mm").alias("dataUltimaModificacao"),
    f.coalesce(f.col("ownerid") ,f.lit("N/A")).alias("proprietario"),
    f.coalesce(f.col("createdbyid"),f.lit("N/A")).alias("criado_por"),
    f.col("ts_refined").alias("datahoraProcessamento")
)

df = df.join(df_owner, \
    f.col("proprietario") == f.col("idproprietario"),\
    "left").join(df_created, \
        f.col("criado_por") == f.col("idcriacao"),\
        "left").drop("proprietario").drop("criado_por").drop("idcriacao").drop("idproprietario")

output_path = "s3a://refined-zone-echo/dim_tarefa/"
df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(output_path)

job.commit()