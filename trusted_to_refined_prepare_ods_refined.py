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
directories = ['sales_force/oportunidades/', 'sales_force/cotacoes/', 'sales_force/casos/', 'sales_force/migracoes/']  # Adicione aqui os diretórios desejados

# Função para listar todos os arquivos no diretório e subdiretórios
def list_files_in_s3(bucket_name, prefix):
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                files.append(obj['Key'])
    return files

# Iterar sobre os diretórios
for directory in directories:
    prefix = directory
    files = list_files_in_s3(bucket_name, prefix)

    print(f"Arquivos listados no S3 para o diretório {prefix}:")
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
            .withColumn("datahoraPreProcessamento", f.to_timestamp(f.lit(now), 'MM/dd/yyyy HH:mm:ss'))

        # Filtrar apenas os registros com a maior data de modificação
        df_trusted = df_trusted.where("rank = 1")

        # Montar o dataframe limpo e ajustado da dimensão para ser salvo
        df = df_trusted

        output_path = f"s3a://trusted-zone-echo/ods/sf/{directory.split('/')[-2]}/"
        df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
    else:
        print(f"Nenhum DataFrame foi lido com sucesso para o diretório {prefix}.")

job.commit()
