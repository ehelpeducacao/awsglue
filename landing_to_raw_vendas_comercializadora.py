import sys
import subprocess
import pandas as pd
from datetime import datetime
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType, LongType, FloatType, TimestampType


# Instalar openpyxl e pyarrow para suporte de leitura de Excel e escrita de Parquet, respectivamente
subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl", "pyarrow"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

data_atual = datetime.now()
ano = data_atual.year
mes = data_atual.month
dia = data_atual.day
mes = f"{mes:02d}"
dia = f"{dia:02d}"


s3_excel_path = 's3://landing-zone-echo/IM - Vendas/vendas_comercializadora.xlsx'
column_names = [
    'Controle', 'Mês', 'Representante', 'UF', 'EQTL/Outras', 'Canal de Venda', 'Comissionado', 'Submercado', 
    'Código Pipefy', 'pipefy', 'Cliente', 'CNPJ', 'Ucs', 'Produto', 'Migração', 'Gestão Helius', 'Fonte', 
    'Prazo Inicio ', 'Prazo Final', 'Tempo de Amortização ( MESES)', 'Volume (MWm)', 'Preço Médio', 'Desconto %', 
    'Custo Adeq', 'Custo total de adeq', 'CAMPANHA DO MÊS', 'Cash Back (Campanha desconto Imediato)', 'Multa', 
    'Comissão', 'Data Base', 'Data Base FWD', 'Custo Adequação', 'Preço Real', 'Horas', 'Faturamento', 'Boleta', 
    'Data Assinatura Proposta', 'Contrato Assinado', 'data_mes', 'Checagem PIPEFY ', 'Checagem THUNDERS', 
    'Faturamento de Serviços?', 'Column1'
]
df = pd.read_excel(s3_excel_path, sheet_name='2024', usecols=column_names)


schema = StructType([
    StructField("Controle", StringType(), True),
    StructField("Mês", StringType(), True),
    StructField("Representante", StringType(), True),
    StructField("UF", StringType(), True),
    StructField("EQTL/Outras", StringType(), True),
    StructField("Canal de Venda", StringType(), True),
    StructField("Comissionado", StringType(), True),
    StructField("Submercado", StringType(), True),
    StructField("Código Pipefy", StringType(), True),
    StructField("pipefy", StringType(), True),
    StructField("Cliente", StringType(), True),
    StructField("CNPJ", StringType(), True),
    StructField("Ucs", FloatType(), True),
    StructField("Produto", StringType(), True),
    StructField("Migração", StringType(), True),
    StructField("Gestão Helius", StringType(), True),
    StructField("Fonte", StringType(), True),
    StructField("Prazo Inicio", DateType(), True),
    StructField("Prazo Final", StringType(), True),
    StructField("Tempo de Amortização ( MESES)", FloatType(), True),
    StructField("Volume (MWm)", DoubleType(), True),
    StructField("Preço Médio", DoubleType(), True), 
    StructField("Desconto %", StringType(), True), 
    StructField("Custo Adeq", StringType(), True),
    StructField("Custo total de adeq", DoubleType(), True),
    StructField("CAMPANHA DO MÊS", StringType(), True),
    StructField("Cash Back (Campanha desconto Imediato)", DoubleType(), True),
    StructField("Multa", DoubleType(), True),
    StructField("Comissão", DoubleType(), True),
    StructField("Data Base", DateType(), True),
    StructField("Data Base FWD", DateType(), True),
    StructField("Custo Adequação", DoubleType(), True),
    StructField("Preço Real", DoubleType(), True),
    StructField("Horas", FloatType(), True),
    StructField("Faturamento", DoubleType(), True),
    StructField("Boleta", StringType(), True),
    StructField("Data Assinatura Proposta", DateType(), True),
    StructField("Contrato Assinado", StringType(), True),
    StructField("data_mes", StringType(), True),
    StructField("Checagem PIPEFY ", StringType(), True),
    StructField("Checagem THUNDERS", StringType(), True),
    StructField("Faturamento de Serviços?", StringType(), True),
    StructField("Column1", StringType(), True)
])

df_spark = spark.createDataFrame(df, schema=schema)

caminho_parquet = f"s3://raw-zone-echo/vendas_Comercializadora/{ano}/"

df_spark.write.option("compression", "snappy") \
    .mode("overwrite") \
    .parquet(caminho_parquet)