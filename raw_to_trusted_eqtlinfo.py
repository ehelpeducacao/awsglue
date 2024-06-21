import sys
import pandas as pd
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import lit

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# schema = StructType([
#     StructField("CONTA_CONTRATO_C", StringType(), True),
#     StructField("DATA_ATE_C", StringType(), True),
#     StructField("DENUNCIA", StringType(), True),
#     StructField("CARTA_RESPOSTA", StringType(), True),
#     StructField("DTA_PREV_MIGRACAO", StringType(), True),
#     StructField("INSTALACAO_C", StringType(), True),
#     StructField("MES_COMPETENCIA_F", StringType(), True),
#     StructField("DISTRIBUIDORA", StringType(), True),
#     StructField("GRUPO_TENSAO_C", StringType(), True),
#     StructField("PERFIL", StringType(), True),
#     StructField("DEMANDA_CONTRATADO_P", FloatType(), True),
#     StructField("DEMANDA_CONTRATADO_FP", FloatType(), True),
#     StructField("DEMANDA_REGISTRADA_P", FloatType(), True),
#     StructField("DEMANDA_REGISTRADA_FP", FloatType(), True),
#     StructField("CONTA_COVID", StringType(), True),
#     StructField("CONTA_EH", StringType(), True),
#     StructField("ICMS_F", FloatType(), True),
#     StructField("PIS_F", FloatType(), True),
#     StructField("COFINS_F", FloatType(), True),
#     StructField("DESC_APLICADO_TUSD", FloatType(), True),
#     StructField("CONSUMO_P", FloatType(), True),
#     StructField("CONSUMO_FP", FloatType(), True),
#     StructField("CONSUMO_CONTRATADO_P", FloatType(), True),
#     StructField("CONSUMO_CONTRATADO_FP", FloatType(), True),
#     StructField("OUTROS_CUSTOS_CATIVO", FloatType(), True),
#     StructField("OUTROS_CUSTOS_LIVRE", FloatType(), True),
#     StructField("MATRIZ", StringType(), True),
#     StructField("NOME_C", StringType(), True),
#     StructField("CNPJ_C", StringType(), True),
#     StructField("CPF_C", StringType(), True),
#     StructField("UF", StringType(), True),
#     StructField("MUNICIPIO_C", StringType(), True),
#     StructField("COMPLEMENTO_C", StringType(), True),
#     StructField("REGIONAL_C", StringType(), True),
#     StructField("ENDERECO_C", StringType(), True),
#     StructField("NUMERO_C", StringType(), True),
#     StructField("COMPLEMENTO2_C", StringType(), True),
#     StructField("PONTO_REFERENCIA_C", StringType(), True),
#     StructField("BAIRRO_C", StringType(), True),
#     StructField("CEP_C", StringType(), True),
#     StructField("LATITUDE_C", StringType(), True),
#     StructField("LONGITUDE_C", StringType(), True),
#     StructField("CLASSE_PRINCIPAL_C", StringType(), True),
#     StructField("SUBCLASSE_C", StringType(), True),
#     StructField("PARCEIRO_NEGOCIO_C", StringType(), True),
#     StructField("CARTEIRA_CLIENTE", StringType(), True),
#     StructField("NIVEL_TENSAO_C", StringType(), True),
#     StructField("CATEGORIA_TARIFA_F", StringType(), True),
#     StructField("CONSUMO_ATIVO_PONTA", FloatType(), True),
#     StructField("CONSUMO_ATIVO_FORA_PONTA", FloatType(), True),
#     StructField("CONSUMO_ATIVO", FloatType(), True),
#     StructField("CONSUMO_REATIVO_PONTA", FloatType(), True),
#     StructField("CONSUMO_REATIVO_FORA_PONTA", FloatType(), True),
#     StructField("CONSUMO_REATIVO", FloatType(), True),
#     StructField("DEMANDA_ATIVA_PONTA", FloatType(), True),
#     StructField("DEMANDA_ATIVA_FORA_PONTA", FloatType(), True),
#     StructField("DEMANDA_ATIVA", FloatType(), True),
#     StructField("DEMANDA_CONTRATADA_PONTA", FloatType(), True),
#     StructField("DEMANDA_CONTRATADA_FORA_PONTA", FloatType(), True),
#     StructField("DEMANDA_CONTRATADA", FloatType(), True),
#     StructField("IAR", StringType(), True),
#     StructField("CONSUMO_FATURADO_F", FloatType(), True),
#     StructField("TEL_FIXO_C", StringType(), True),
#     StructField("TEL_MOVEL_C", StringType(), True),
#     StructField("EMAIL_C", StringType(), True),
#     StructField("STATUS_INSTALACAO_C", StringType(), True),
#     StructField("ENERGIA_INJETADA", FloatType(), True),
#     StructField("INF_GERADOR", StringType(), True),
#     StructField("INF_GD", StringType(), True),
#     StructField("CLIENTE_LIVRE_C", StringType(), True),
#     StructField("DATA_LEITURA_REAL_L", StringType(), True),
#     StructField("VALOR_FATURA_F", FloatType(), True),
#     StructField("OUTROS_PROD_SERV", StringType(), True)
# ])

ano = datetime.now().strftime('%Y')
mes = datetime.now().strftime('%m')
dia = datetime.now().strftime('%d')
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

csv_path = "s3://raw-zone-echo/eqtlinfo/*/*/*/*/"
df = spark.read.csv(csv_path, header=True, sep=';', quote='"', encoding='ISO-8859-1')
df = df.withColumn("data_hora_processamento", lit(current_datetime))

parquet_path = f"s3://trusted-zone-echo/eqtlinfo/{ano}/{mes}/{dia}/" 
df.write.parquet(parquet_path, mode='append', compression='snappy')

job.commit()