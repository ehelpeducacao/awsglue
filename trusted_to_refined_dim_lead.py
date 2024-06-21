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
prefix = 'sales_force/lead/'

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
    f.col("id").alias("idLead"),
    f.col("name").alias("nomeLead"),
    f.coalesce(f.col("leadsource"), f.lit("N/A")).alias("origemLead"), 
    f.coalesce(f.col("title"), f.lit("N/A")).alias("funcao"), 
    f.coalesce(f.col("company"), f.lit("N/A")).alias("razaoSocial"),
    f.coalesce(f.col("nome_fantasia__c"), f.lit("N/A")).alias("nomeFantasia"),
    f.coalesce(f.col("numberofemployees"), f.lit(None)).cast("integer").alias("quantFuncionarios"),
    f.coalesce(f.col("cnpj__c"), f.lit("N/A")).alias("cnpj"),
    f.coalesce(f.col("inscricao_estadual__c"), f.lit("N/A")).alias("inscricaoEstadual"),
    f.coalesce(f.col("cpf__c"), f.lit("N/A")).alias("cpf"),
    f.coalesce(f.col("email"), f.lit("N/A")).alias("email"),
    f.coalesce(f.col("phone"),f.col("mobilephone"),f.lit("N/A")).alias("telefone"),
    f.coalesce(f.col("mobilephone"),f.col("phone"),f.lit("N/A")).alias("telefoneMovel"),
    f.coalesce(f.col("street"), f.lit("N/A")).alias("endereco"),
    f.coalesce(f.col("city"), f.lit("N/A")).alias("cidade"),
    f.coalesce(f.col("state"), f.lit("N/A")).alias("estado"),
    f.coalesce(f.col("country"), f.lit("N/A")).alias("pais"),
    f.coalesce(f.col("postalcode"), f.lit("N/A")).alias("cep"),
    f.coalesce(f.col("website"), f.lit("N/A")).alias("website"),    
    f.coalesce(f.col("industry"), f.lit("N/A")).alias("industria"),    
    f.coalesce(f.col("status"), f.lit("N/A")).alias("status"),  
    f.when(f.col("isconverted") == "true", "SIM") \
        .when(f.col("isconverted") == "false", "NAO") \
        .otherwise("N/A") \
        .alias("convertido"),
    f.coalesce(f.col("converteddate"), f.lit("N/A")).alias("dataConversao"),
    f.when(f.col("qualificado__c") == "true", "SIM") \
        .when(f.col("qualificado__c") == "false", "NAO") \
        .otherwise("N/A") \
        .alias("qualificado"),
    f.coalesce(f.col("tipo_cliente__c"), f.lit("N/A")).alias("tipoCliente"),
    f.coalesce(f.col("conta_contrato__c"), f.lit("N/A")).alias("contaContrato"),
    f.coalesce(f.col("data_de_vigencia_contrato__c"), f.lit("N/A")).alias("dataVigenciaContrato"),
    f.coalesce(f.col("numero_de_ucs__c"), f.lit(None)).cast("integer").alias("numerodeUcs"),
    f.coalesce(f.col("distribuidora__c"), f.lit("N/A")).alias("distribuidora"),
    f.coalesce(f.col("fornecedor_de_energia__c"), f.lit("N/A")).alias("fornecedorEnergia"),    
    f.coalesce(f.col("classe_de_tensao__c"), f.lit("N/A")).alias("classeTensao"),
    f.coalesce(f.col("modalidade_tarifaria__c"), f.lit("N/A")).alias("modalidadeTarifaria"),
    f.coalesce(f.col("volume_mwm__c"), f.lit(None)).cast("float").alias("volumeMwm"),
    f.coalesce(f.col("consumo_total_kwh__c"), f.lit(None)).cast("float").alias("consumoTotalKwh"),
    f.coalesce(f.col("demanda_total_kw__c"), f.lit(None)).cast("float").alias("demandaTotalKw"),
    f.coalesce(f.col("consumo_fora_ponta_kwh__c"), f.lit(None)).cast("float").alias("consumoForaPontaKwh"),
    f.coalesce(f.col("demanda_fora_ponta_kw__c"), f.lit(None)).cast("float").alias("demandaForaPontaKw"),
    f.coalesce(f.col("consumo_ponta_kwh__c"), f.lit(None)).cast("float").alias("consumoPontaKwh"),
    f.coalesce(f.col("demanda_ponta__c"), f.lit(None)).cast("float").alias("demandaPonta"),
    f.coalesce(f.col("consumo_de_energia__c"), f.lit(None)).cast("float").alias("consumoEnergia"),
    f.coalesce(f.col("consumo_em_reais__c"), f.lit(None)).cast("float").alias("consumoemReais"),
    f.coalesce(f.col("motivo__c"), f.lit("N/A")).alias("motivo"),
    f.coalesce(f.col("utiliza_gerador__c"), f.lit("N/A")).alias("utilizaGerador"),
    f.coalesce(f.col("observacoes_gerais__c"), f.lit("N/A")).alias("oberservacoes"),
    f.when(f.col("isdeleted") == "true", "SIM") \
        .when(f.col("isdeleted") == "false", "NAO") \
        .otherwise("N/A") \
        .alias("foiDeletado"),
    f.col("lastmodifieddate").alias("dataUltimaModificacao"),
    f.col("ts_refined").alias("datahoraProcessamento")
)

output_path = "s3a://refined-zone-echo/dim_lead/"
df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(output_path)

job.commit()