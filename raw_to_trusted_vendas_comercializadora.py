from datetime import datetime, timedelta
import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import pyspark.sql.functions as f
from pyspark.sql.types import TimestampType



# Lista dos argumentos esperados
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Inicializando o Spark e Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)

# Inicializando a SparkSession a partir do GlueContext
spark = glueContext.spark_session

# Inicializando o Job do Glue
job = Job(glueContext)
job.init(job_name=args['JOB_NAME'])

# Acessando os parâmetros passados para o job
data_atual = datetime.now()
ano = data_atual.year



# Constrói o caminho do arquivo com a data de ontem
caminho_arquivo = f"s3://raw-zone-echo/vendas_Comercializadora/{ano}/"

df = spark.read.parquet(caminho_arquivo)


df = df.withColumnRenamed("Controle", "Controle") \
       .withColumnRenamed("Mês", "Mes") \
       .withColumnRenamed("Representante", "Representante") \
       .withColumnRenamed("UF", "UF") \
       .withColumnRenamed("EQTL/Outras", "EQTL_Outras") \
       .withColumnRenamed("Canal de Venda", "Canal_de_Venda") \
       .withColumnRenamed("Comissionado", "Comissionado") \
       .withColumnRenamed("Submercado", "Submercado") \
       .withColumnRenamed("Código Pipefy", "Codigo_Pipefy") \
       .withColumnRenamed("pipefy", "pipefy") \
       .withColumnRenamed("Cliente", "Cliente") \
       .withColumnRenamed("CNPJ", "CNPJ") \
       .withColumnRenamed("Ucs", "Ucs") \
       .withColumnRenamed("Produto", "Produto") \
       .withColumnRenamed("Migração", "Migracao") \
       .withColumnRenamed("Gestão Helius", "Gestao_Helius") \
       .withColumnRenamed("Fonte", "Fonte") \
       .withColumnRenamed("Prazo Inicio", "Prazo_Inicio") \
       .withColumnRenamed("Prazo Final", "Prazo_Final") \
       .withColumnRenamed("Tempo de Amortização ( MESES)", "Tempo_de_Amortizacao_MESES") \
       .withColumnRenamed("Volume (MWm)", "Volume_MWm") \
       .withColumnRenamed("Preço Médio", "Preco_Medio") \
       .withColumnRenamed("Desconto %", "Desconto_Perc") \
       .withColumnRenamed("Custo Adeq", "Custo_Adeq") \
       .withColumnRenamed("Custo total de adeq", "Custo_total_de_adeq") \
       .withColumnRenamed("CAMPANHA DO MÊS", "CAMPANHA_DO_MES") \
       .withColumnRenamed("Cash Back (Campanha desconto Imediato)", "Cash_Back_Campanha_desconto_Imediato") \
       .withColumnRenamed("Multa", "Multa") \
       .withColumnRenamed("Comissão", "Comissao") \
       .withColumnRenamed("Data Base", "Data_Base") \
       .withColumnRenamed("Data Base FWD", "Data_Base_FWD") \
       .withColumnRenamed("Custo Adequação", "Custo_Adequacao") \
       .withColumnRenamed("Preço Real", "Preco_Real") \
       .withColumnRenamed("Horas", "Horas") \
       .withColumnRenamed("Faturamento", "Faturamento") \
       .withColumnRenamed("Boleta", "Boleta") \
       .withColumnRenamed("Data Assinatura Proposta", "Data_Assinatura_Proposta") \
       .withColumnRenamed("Contrato Assinado", "Contrato_Assinado") \
       .withColumnRenamed("data_mes", "data_mes") \
       .withColumnRenamed("Checagem PIPEFY ", "Checagem_PIPEFY") \
       .withColumnRenamed("Checagem THUNDERS", "Checagem_THUNDERS") \
       .withColumnRenamed("Faturamento de Serviços?", "Faturamento_de_Servicos") \
       .withColumnRenamed("Column1", "Column1") \
       .withColumn("data_hora_processamento", f.from_utc_timestamp(f.current_timestamp(), "GMT-3").cast(TimestampType()))

# Modificado para criar o Parquet na nova estrutura de pasta
caminho_parquet = f"s3://trusted-zone-echo/vendas_Comercializadora/{ano}/"


df.write.option("compression", "snappy") \
    .mode("overwrite") \
    .parquet(caminho_parquet)

job.commit()