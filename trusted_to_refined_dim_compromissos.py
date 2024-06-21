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
prefix = 'sales_force/compromissos/'

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


# Montar o dataframe df_Compromisso
df = df_trusted.select(
    f.col("id").alias("idCompromisso"),
    f.coalesce(f.col("subject"), f.lit("N/A")).alias("assunto"),
    f.coalesce(f.col("description"), f.lit("N/A")).alias("descricao"),
    f.coalesce(f.col("status__c"), f.lit("N/A")).alias("statuscompromisso"), 
    f.coalesce(f.col("observasao_de_conclusao__c"), f.lit("N/A")).alias("obervacaoConclusao"),     
    f.coalesce(f.col("eventsubtype"), f.lit("N/A")).alias("subTipoDeEvento"),    
    f.coalesce(f.col("groupeventtype"), f.lit("N/A")).alias("tipoGrupoDeEvento"),    
    f.coalesce(f.col("type"), f.lit("N/A")).alias("tipo"),
    f.coalesce(f.col("submercado__c"), f.lit("N/A")).alias("submercado"),    
    f.coalesce(f.col("createddate"), f.lit(None)).alias("dataCriacaoCompromisso"), 
    f.coalesce(f.col("activitydate"), f.lit(None)).alias("dataCompromisso"),  
    f.coalesce(f.col("activitydatetime"), f.lit(None)).alias("dataHoraCompromisso"),
    f.coalesce(f.col("startdatetime"), f.lit(None)).alias("dataHoraInicioCompromisso"),
    f.coalesce(f.col("enddate"), f.lit(None)).alias("dataFimCompromisso"),  
    f.coalesce(f.col("enddatetime"), f.lit(None)).alias("dataHoraFimCompromisso"),
    f.coalesce(f.col("durationinminutes"), f.lit(None)).cast("integer").alias("duracaoEmMinutos"),
    f.coalesce(f.col("whoid"), f.lit("N/A")).alias("contato"),
    f.coalesce(f.col("whatid"), f.lit("N/A")).alias("oportunidade"),
    f.coalesce(f.col("motivo_nao_realizado__c"), f.lit("N/A")).alias("motivoNaoRealizado"),
    f.when(f.col("ischild") == "true", "SIM") \
        .when(f.col("ischild") == "false", "NAO") \
        .otherwise("N/A") \
        .alias("visaoParticipante"),
    f.coalesce(f.col("showas"), f.lit("N/A")).alias("mostrarComo"),
    f.coalesce(f.col("whatcount"), f.lit("N/A")).alias("qtdOportunidades"),
    f.coalesce(f.col("whocount"), f.lit("N/A")).alias("qtdContatos"),    
    f.when(f.col("isdeleted") == "true", "SIM") \
        .when(f.col("isdeleted") == "false", "NAO") \
        .otherwise("N/A") \
        .alias("foiDeletado"),
    f.col("lastmodifieddate").alias("dataUltimaModificacao"),
    f.col("ts_refined").alias("datahoraProcessamento")
)

output_path = "s3a://refined-zone-echo/dim_compromisso/"
df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(output_path)

job.commit()