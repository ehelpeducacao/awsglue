from datetime import datetime, timedelta
import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType, LongType, TimestampType
import pyspark.sql.functions as f



# Lista dos argumentos esperados
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'process_name'])

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
data_atual = datetime.now()
ano = data_atual.year
mes = data_atual.month
dia = data_atual.day
mes = f"{mes:02d}"
dia = f"{dia:02d}"

file_name ='daily_report.parquet'

# Constrói o caminho do arquivo com a data de ontem
caminho_arquivo = f"s3://raw-zone-echo/{process_name}/daily_report/{ano}/{mes}/{dia}/*/*/*/{file_name}"
schema = StructType([
    StructField("Negócio", StringType(), True),
    StructField("Entrega", LongType(), True),
    StructField("Unidade de negócio", StringType(), True),
    StructField("Tipo de operação", StringType(), True),
    StructField("Natureza de operação", StringType(), True),
    StructField("Empresa Responsável", StringType(), True),
    StructField("Negociante", StringType(), True),
    StructField("Operador", StringType(), True),
    StructField("Origem da operação", StringType(), True),
    StructField("Data de criação", TimestampType(), True),
    StructField("Data do fornecimento", TimestampType(), True),
    StructField("Contrato CCEE", DoubleType(), True),
    StructField("Início do fornecimento", TimestampType(), True),
    StructField("Fim do fornecimento", TimestampType(), True),
    StructField("Volume previsto (MWm)", DoubleType(), True),
    StructField("Volume previsto (MWh)", DoubleType(), True),
    StructField("Volume ajustado (MWm)", DoubleType(), True),
    StructField("Volume ajustado (MWh)", DoubleType(), True),
    StructField("Possui ajuste de volume", StringType(), True),
    StructField("Submercado", StringType(), True),
    StructField("Possui ajuste de submercado", StringType(), True),
    StructField("Fonte de energia", StringType(), True),
    StructField("RETUSD", DoubleType(), True),
    StructField("Classificação", StringType(), True),
    StructField("Tipo de modulação", StringType(), True),
    StructField("Flexibilidade mensal", StringType(), True),
    StructField("Flexibilidade superior", DoubleType(), True),
    StructField("Flexibilidade inferior", DoubleType(), True),
    StructField("Volume diferenciado por período", StringType(), True),
    StructField("Condição de pagamento", StringType(), True),
    StructField("Observação Financeiro", StringType(), True),
    StructField("Preço diferenciado por período", StringType(), True),
    StructField("Preço previsto", DoubleType(), True),
    StructField("Preço ajustado", DoubleType(), True),
    StructField("Possui ajuste de preço", StringType(), True),
    StructField("Flexibilidade de preço", StringType(), True),
    StructField("Tipo de Spread", StringType(), True),
    StructField("Índice", StringType(), True),
    StructField("Spread", DoubleType(), True),
    StructField("Piso da flexibilidade de preço", DoubleType(), True),
    StructField("Teto da flexibilidade de preço", DoubleType(), True),
    StructField("Índice de reajuste", StringType(), True),
    StructField("Data base", TimestampType(), True),
    StructField("Data do 1º reajuste", TimestampType(), True),
    StructField("Valor NF", DoubleType(), True),
    StructField("Observações", StringType(), True),
    StructField("CNPJ da parte", StringType(), True),
    StructField("CNPJ da contraparte", StringType(), True),
    StructField("PLD", DoubleType(), True),
    StructField("Ágio", DoubleType(), True),
    StructField("Código Integração", DoubleType(), True),
    StructField("Parte - Código Perfil", StringType(), True),
    StructField("Parte - Perfil", DoubleType(), True),
    StructField("Contraparte - Código Perfil", StringType(), True),
    StructField("Contraparte - perfil", DoubleType(), True)
])


df = spark.read.schema(schema).parquet(caminho_arquivo)


df = df.withColumnRenamed("Negócio", "Negocio") \
       .withColumnRenamed("Unidade de negócio", "Unidade_de_negocio") \
       .withColumnRenamed("Tipo de operação", "Tipo_de_operacao") \
       .withColumnRenamed("Natureza de operação", "Natureza_de_operacao") \
       .withColumnRenamed("Empresa Responsável", "Empresa_Responsavel") \
       .withColumnRenamed("Origem da operação", "Origem_da_operacao") \
       .withColumnRenamed("Data de criação", "Data_de_criacao") \
       .withColumnRenamed("Data do fornecimento", "Data_do_fornecimento") \
       .withColumnRenamed("Início do fornecimento", "Inicio_do_fornecimento") \
       .withColumnRenamed("Fim do fornecimento", "Fim_do_fornecimento") \
       .withColumnRenamed("Volume previsto (MWm)", "Volume_previsto_MWm") \
       .withColumnRenamed("Volume previsto (MWh)", "Volume_previsto_MWh") \
       .withColumnRenamed("Volume ajustado (MWm)", "Volume_ajustado_MWm") \
       .withColumnRenamed("Volume ajustado (MWh)", "Volume_ajustado_MWh") \
       .withColumnRenamed("Possui ajuste de volume", "Possui_ajuste_de_volume") \
       .withColumnRenamed("Possui ajuste de submercado", "Possui_ajuste_de_submercado") \
       .withColumnRenamed("Fonte de energia", "Fonte_de_energia") \
       .withColumnRenamed("Classificação", "Classificacao") \
       .withColumnRenamed("Tipo de modulação", "Tipo_de_modulacao") \
       .withColumnRenamed("Flexibilidade mensal", "Flexibilidade_mensal") \
       .withColumnRenamed("Flexibilidade superior", "Flexibilidade_superior") \
       .withColumnRenamed("Flexibilidade inferior", "Flexibilidade_inferior") \
       .withColumnRenamed("Volume diferenciado por período", "Volume_diferenciado_por_periodo") \
       .withColumnRenamed("Condição de pagamento", "Condicao_de_pagamento") \
       .withColumnRenamed("Observação Financeiro", "Observacao_Financeiro") \
       .withColumnRenamed("Preço diferenciado por período", "Preco_diferenciado_por_periodo") \
       .withColumnRenamed("Preço previsto", "Preco_previsto") \
       .withColumnRenamed("Preço ajustado", "Preco_ajustado") \
       .withColumnRenamed("Possui ajuste de preço", "Possui_ajuste_de_preco") \
       .withColumnRenamed("Flexibilidade de preço", "Flexibilidade_de_preco") \
       .withColumnRenamed("Tipo de Spread", "Tipo_de_Spread") \
       .withColumnRenamed("Índice", "Indice") \
       .withColumnRenamed("Piso da flexibilidade de preço", "Piso_da_flexibilidade_de_preco") \
       .withColumnRenamed("Teto da flexibilidade de preço", "Teto_da_flexibilidade_de_preco") \
       .withColumnRenamed("Índice de reajuste", "Indice_de_reajuste") \
       .withColumnRenamed("Data base", "Data_base") \
       .withColumnRenamed("Data do 1º reajuste", "Data_do_1o_reajuste") \
       .withColumnRenamed("Valor NF", "Valor_NF") \
       .withColumnRenamed("Observações", "Observacoes") \
       .withColumnRenamed("CNPJ da parte", "CNPJ_da_parte") \
       .withColumnRenamed("CNPJ da contraparte", "CNPJ_da_contraparte") \
       .withColumnRenamed("Ágio", "Agio") \
       .withColumnRenamed("Código Integração", "Codigo_Integracao") \
       .withColumnRenamed("Parte - Código Perfil", "Parte_Codigo_Perfil") \
       .withColumnRenamed("Parte - Perfil", "Parte_Perfil") \
       .withColumnRenamed("Contraparte - Código Perfil", "Contraparte_Codigo_Perfil") \
       .withColumnRenamed("Contraparte - perfil", "Contraparte_perfil") \
       .withColumn("data_hora_processamento", f.from_utc_timestamp(f.current_timestamp(), "GMT-3").cast(TimestampType()))

# Modificado para criar o Parquet na nova estrutura de pasta
caminho_parquet = f"s3://trusted-zone-echo/{process_name}/daily_report/"


df.write.option("compression", "snappy") \
    .mode("append") \
    .parquet(caminho_parquet)

job.commit()