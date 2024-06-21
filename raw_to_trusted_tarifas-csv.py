from datetime import datetime, timedelta
import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import pyspark.sql.functions as f
from pyspark.sql.types import DoubleType, TimestampType, DateType


# Lista dos argumentos esperados
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'process_name', 'subproc'])

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
subproc = args['subproc']
file_name = f'db-tarifas-{subproc}.csv'

# Constrói o caminho do arquivo com a data de ontem
caminho_arquivo = f"s3://raw-zone-echo/{process_name}/{ano}/{mes}/{dia}/*/*/*/{file_name}"

# Script modificado para ler o arquivo com a data de ontem
df = spark.read.csv(
    path=caminho_arquivo,
    sep=";",                # Define o separador como ponto e vírgula
    header=True,            # Usa a primeira linha como cabeçalho
    quote='"',              # Define o caractere de aspas
    inferSchema=True,       # Infere o esquema dos dados
    encoding="utf-8"
)

# Modificado para criar o Parquet na nova estrutura de pasta
caminho_parquet = f"s3://trusted-zone-echo/tarifas/{subproc}"
elseon = False

if subproc == 'echoenergia':
    df = (df
          .withColumnRenamed('Data Aniversario', 'Data')
          .withColumnRenamed('Ano', 'Ano')
          .withColumnRenamed('Dias Anuais Anteriores', 'Dias_Anuais_Anteriores')
          .withColumnRenamed('Dias Anuais Posteriores', 'Dias_Anuais_Posteriores')
          .withColumnRenamed('Distribuidora', 'Distribuidora')
          .withColumnRenamed('Origem', 'Origem')
          .withColumnRenamed('Cenario', 'Cenario')
          .withColumnRenamed('Grupo', 'Grupo')
          .withColumnRenamed('Subgrupo', 'Subgrupo')
          .withColumnRenamed('Modalidade', 'Modalidade')
          .withColumnRenamed('Mercado', 'Mercado')
          .withColumnRenamed('Unidade', 'Unidade')
          .withColumnRenamed('Posto', 'Posto')
          .withColumnRenamed('TUSD_APLICACAO', 'TUSD_Aplicacao')
          .withColumnRenamed('TE_APLICACAO', 'TE_Aplicacao')
          .withColumnRenamed('TOTAL_APLICACAO', 'Total_Aplicacao')
          .withColumn("TUSD_Aplicacao", f.col("TUSD_Aplicacao").cast(DoubleType()))
          .withColumn("TE_Aplicacao", f.col("TE_Aplicacao").cast(DoubleType()))
          .withColumn("Total_Aplicacao", f.col("Total_Aplicacao").cast(DoubleType()))
          .withColumn("data_hora_processamento", f.from_utc_timestamp(f.current_timestamp(), "GMT-3").cast(TimestampType()))
         )

elif subproc == 'aneel':
    df = (df
          .withColumnRenamed('Data Aniversario', 'Data')
          .withColumnRenamed('Distribuidora', 'Distribuidora')
          .withColumnRenamed('Origem', 'Origem')
          .withColumnRenamed('Cenario', 'Cenario')
          .withColumnRenamed('Grupo', 'Grupo')
          .withColumnRenamed('Subgrupo', 'Subgrupo')
          .withColumnRenamed('Modalidade', 'Modalidade')
          .withColumnRenamed('Mercado', 'Mercado')
          .withColumnRenamed('Unidade', 'Unidade')
          .withColumnRenamed('Posto', 'Posto')
          .withColumnRenamed('TUSD_APLICACAO', 'TUSD_Aplicacao')
          .withColumnRenamed('TE_APLICACAO', 'TE_Aplicacao')
          .withColumnRenamed('TOTAL_APLICACAO', 'Total_Aplicacao')
          .withColumn("TUSD_Aplicacao", f.col("TUSD_Aplicacao").cast(DoubleType()))
          .withColumn("TE_Aplicacao", f.col("TE_Aplicacao").cast(DoubleType()))
          .withColumn("Total_Aplicacao", f.col("Total_Aplicacao").cast(DoubleType()))
          .withColumn("data_hora_processamento", f.from_utc_timestamp(f.current_timestamp(), "GMT-3").cast(TimestampType()))
         )

elif subproc == "trsolucoes":
     df = (df
          .withColumnRenamed('Data Aniversario', 'Data')
          .withColumnRenamed('Distribuidora', 'Distribuidora')
          .withColumnRenamed('Origem', 'Origem')
          .withColumnRenamed('Cenario', 'Cenario')
          .withColumnRenamed('Grupo', 'Grupo')
          .withColumnRenamed('Subgrupo', 'Subgrupo')
          .withColumnRenamed('Modalidade', 'Modalidade')
          .withColumnRenamed('Mercado', 'Mercado')
          .withColumnRenamed('Unidade', 'Unidade')
          .withColumnRenamed('Posto', 'Posto')
          .withColumnRenamed('TUSD_APLICACAO', 'TUSD_Aplicacao')
          .withColumnRenamed('TE_APLICACAO', 'TE_Aplicacao')
          .withColumnRenamed('TOTAL_APLICACAO', 'Total_Aplicacao')
          .withColumn("TUSD_Aplicacao", f.col("TUSD_Aplicacao").cast(DoubleType()))
          .withColumn("TE_Aplicacao", f.col("TE_Aplicacao").cast(DoubleType()))
          .withColumn("Total_Aplicacao", f.col("Total_Aplicacao").cast(DoubleType()))
          .withColumn("data_hora_processamento", f.from_utc_timestamp(f.current_timestamp(), "GMT-3").cast(TimestampType()))
         )

else:
    print("Nenhum subproc foi definido")
    elseon = True

# Fim das transformações
if not elseon:
    df.write.option("compression", "snappy") \
    .mode("overwrite") \
    .parquet(caminho_parquet)

job.commit()