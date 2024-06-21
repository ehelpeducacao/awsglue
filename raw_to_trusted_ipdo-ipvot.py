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
file_name = f'db-ipdo-{subproc}.csv'

# Início Leitura do dado de raw-zone-echo
caminho_arquivo = f"s3://raw-zone-echo/{process_name}/{ano}/{mes}/{dia}/*/*/*/{file_name}"

df = spark.read.csv(
    path=caminho_arquivo,
    sep=";",  # Define o separador como ponto e vírgula
    header=True,  # Usa a primeira linha como cabeçalho
    quote='"',  # Define o caractere de aspas
    inferSchema=True  # Infere o esquema dos dados
)

# Fim da leitura Padronizada
# Início da transformação
caminho_parquet = f"s3://trusted-zone-echo/ipdo/{subproc}"
elseon = False

if subproc == 'ipvot':
    df = (df
          .withColumnRenamed('Data', 'Data')
          .withColumnRenamed('Submercado', 'Submercado')
          .withColumnRenamed('Hidreletrica', 'Hidreletrica')
          .withColumnRenamed('Termeletrica Convencional', 'Termeletrica_Convencional')
          .withColumnRenamed('Termeletrica Nuclear', 'Termeletrica_Nuclear')
          .withColumnRenamed('Eolica', 'Eolica')
          .withColumnRenamed('Solar', 'Solar')
          .withColumnRenamed('Total Carga', 'Total_Carga')
          .withColumnRenamed('Total Geracao', 'Total_Geracao')
          .withColumnRenamed('Itaipu', 'Itaipu')
          .withColumnRenamed('Intercambio Nacional', 'Intercambio_Nacional')
          .withColumnRenamed('Intercambio Internacional', 'Intercambio_Internacional')
          .withColumnRenamed('Exportacao', 'Exportacao')
          .withColumnRenamed('Importacao', 'Importacao')
          .withColumnRenamed('ENA Armazenada (%)', 'ENA_Armazenada_Perc')
          .withColumnRenamed('ENA Bruta (%)', 'ENA_Bruta_Perc')
          .withColumnRenamed('ENA Mwmed', 'ENA_Mwmed')
          .withColumnRenamed('EAR', 'EAR')
          .withColumnRenamed('EAR %', 'EAR_Perc')
          .withColumnRenamed('EAR Desvio', 'EAR_Desvio')
          .withColumnRenamed('Variacao %', 'Variacao_Perc')
          .withColumnRenamed('Variacao', 'Variacao')
          .withColumn("Hidreletrica", f.col("Hidreletrica").cast(DoubleType()))
          .withColumn("Termeletrica_Convencional", f.col("Termeletrica_Convencional").cast(DoubleType()))
          .withColumn("Termeletrica_Nuclear", f.col("Termeletrica_Nuclear").cast(DoubleType()))
          .withColumn("Eolica", f.col("Eolica").cast(DoubleType()))
          .withColumn("Solar", f.col("Solar").cast(DoubleType()))
          .withColumn("Total_Carga", f.col("Total_Carga").cast(DoubleType()))
          .withColumn("Total_Geracao", f.col("Total_Geracao").cast(DoubleType()))
          .withColumn("Itaipu", f.col("Itaipu").cast(DoubleType()))
          .withColumn("Intercambio_Nacional", f.col("Intercambio_Nacional").cast(DoubleType()))
          .withColumn("Intercambio_Internacional", f.col("Intercambio_Internacional").cast(DoubleType()))
          .withColumn("Exportacao", f.col("Exportacao").cast(DoubleType()))
          .withColumn("Importacao", f.col("Importacao").cast(DoubleType()))
          .withColumn("ENA_Armazenada_Perc", f.col("ENA_Armazenada_Perc").cast(DoubleType()))
          .withColumn("ENA_Bruta_Perc", f.col("ENA_Bruta_Perc").cast(DoubleType()))
          .withColumn("ENA_Mwmed", f.col("ENA_Mwmed").cast(DoubleType()))
          .withColumn("EAR", f.col("EAR").cast(DoubleType()))
          .withColumn("EAR_Perc", f.col("EAR_Perc").cast(DoubleType()))
          .withColumn("EAR_Desvio", f.col("EAR_Desvio").cast(DoubleType()))
          .withColumn("Variacao_Perc", f.col("Variacao_Perc").cast(DoubleType()))
          .withColumn("Variacao", f.col("Variacao").cast(DoubleType()))
          .withColumn("data_hora_processamento", f.from_utc_timestamp(f.current_timestamp(), "GMT-3").cast(TimestampType()))
         )

elif subproc == 'thermal':
    df = (df
          .withColumnRenamed("Data", "Data")
          .withColumnRenamed("Tipo", "Tipo")
          .withColumnRenamed("Submercado", "Submercado")
          .withColumnRenamed("Usinas", "Usinas")
          .withColumnRenamed("Razao do Despacho", "Razao_do_despacho")
          .withColumnRenamed("Capacidade Instalada", "Capacidade_instalada")
          .withColumnRenamed("Capacidade Disponivel", "Capacidade_disponivel")
          .withColumnRenamed("Media Diaria Programada", "Media_diaria_programada")
          .withColumnRenamed("Media Diaria Verificada", "Media_diaria_verificada")
          .withColumnRenamed("Media Diaria Diferenca", "Media_diaria_diferenca")
          .withColumnRenamed("Media Diaria Variacao %", "Media_diaria_variacao_perc")
          .withColumn("Capacidade_instalada", f.col("Capacidade_instalada").cast(DoubleType()))
          .withColumn("Capacidade_disponivel", f.col("Capacidade_disponivel").cast(DoubleType()))
          .withColumn("Media_diaria_programada", f.col("Media_diaria_programada").cast(DoubleType()))
          .withColumn("Media_diaria_verificada", f.col("Media_diaria_verificada").cast(DoubleType()))
          .withColumn("Media_diaria_diferenca", f.col("Media_diaria_diferenca").cast(DoubleType()))
          .withColumn("Media_diaria_variacao_perc", f.col("Media_diaria_variacao_perc").cast(DoubleType()))
          .withColumn("data_hora_processamento", f.from_utc_timestamp(f.current_timestamp(), "GMT-3").cast(TimestampType()))
          )

elif subproc == "main":
    df = (df
          .withColumn("Valor", f.col("Valor").cast(DoubleType()))
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