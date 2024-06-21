
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# Caminho do arquivo no S3
s3_path = "s3://trusted-zone-echo/eqtlinfo/2024/06/14/"

# Ler o arquivo Parquet do S3
df = spark.read.parquet(s3_path, sep=";", mode="overwrite")
import pyspark.sql.functions as f
# Filtrar registros onde a coluna distribuidora não é 'CELG'
df_filtrado = df.filter(f.col('DISTRIBUIDORA') != 'CELG')

# Obter a lista de todas as colunas no DataFrame
todas_colunas = df_filtrado.columns

# Substituir valores 0.00 por NULL em todas as colunas
df_modificado = df_filtrado.select(
    *[f.when(f.col(c) == 0.00, None).otherwise(f.col(c)).alias(c) for c in todas_colunas]
)

# Mostrar o resultado para verificar
df_modificado.show()

##df_modificado.coalesce(1).write.csv("s3://sandbox-zone-echo/analises_vitor_sanches/analises-mapapial/", header= True, sep=";", quote="'", mode="overwrite")



# Calcular a porcentagem de valores nulos por coluna
total_linhas = df_modificado.count()
percentual_nulos = df_modificado.select(
    [(f.count(f.when(f.col(c).isNull(), c)) / total_linhas * 100).alias(c) for c in todas_colunas]
)

# Mostrar o resultado da porcentagem de nulos
percentual_nulos.show()
df_modificado.coalesce(1).write.csv("s3://sandbox-zone-echo/analises_vitor_sanches/analises-mapapial/", header= True, sep=";", quote="'", mode="overwrite")
from pyspark.sql.functions import col, when, isnan, count

# Filtrar registros onde a coluna distribuidora não é 'CELG'
df_filtrado = df.filter(f.col('distribuidora') != 'CELG')
import pyspark.sql.functions as f


# Ler o arquivo Parquet do S3
df = spark.read.parquet(s3_path)


# Filtrar registros onde a coluna distribuidora não é 'CELG'
df_filtrado = df.filter(col('DISTRIBUIDORA') != 'CELG')




df_modificado = df_filtrado.select(
    *[when(col(c) == 0.00, None).otherwise(col(c)).alias(c) for c in colunas_a_filtrar]
)

# Mostrar o resultado para verificar
df_modificado.show()
# Calcular a porcentagem de valores nulos por coluna
total_linhas = df_modificado.count()
percentual_nulos = df_modificado.select(
    [(count(when(col(c).isNull(), c)) / total_linhas * 100).alias(c + '_PERCENTUAL_NULOS') for c in colunas_a_filtrar]
)

# Mostrar o resultado da porcentagem de nulos
percentual_nulos.show()

null_percentage_df.coalesce(1).write.csv("s3://sandbox-zone-echo/analises_vitor_sanches/analises-mapapial/", header= True, sep=";", quote="'", mode="overwrite")
colunas = df.columns

# Iterar sobre as colunas e obter os valores distintos
for coluna in colunas:
    valores_distintos = df.select(col(coluna)).distinct().collect()
    print(f"Valores distintos na coluna '{coluna}':")
    for valor in valores_distintos:
        print(valor[0])
df_1 = df.groupBy("anoMes").agg(f.avg("valor_fatura_f").cast("float").alias("TotalMensal"))
df_filter = df_1.where("anoMes in ('202402','202403','202404')")
df_filter = df_filter.withColumn("teste", f.format_string("%.2f", f.col("TotalMensal")))
df_filter.show()
df_filter.write.csv("s3a://sandbox-zone-echo/analises_vitor_sanches/media_faturas_mensal/", header=True, sep=";", mode="overwrite")
job.commit()