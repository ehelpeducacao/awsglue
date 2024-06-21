import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Carregar os dados das tabelas do Catálogo
cotacoes_node = glueContext.create_dynamic_frame.from_catalog(database="trusted-zone", table_name="cotacoes")
contas_node = glueContext.create_dynamic_frame.from_catalog(database="trusted-zone", table_name="contas")

# Converter DynamicFrames para DataFrames
cotacoes_df = cotacoes_node.toDF()
contas_df = contas_node.toDF()

# Escrever os dados nas tabelas com overwrite
cotacoes_df.write.mode("overwrite").format("parquet").save("s3://trusted-zone-echo/sales_force/cotacoes/")
contas_df.write.mode("overwrite").format("parquet").partitionBy("createddate").save("s3://trusted-zone-echo/sales_force/contas/")

# Atualizar o catálogo Glue
AWSGlueDataCatalog_node_cotacoes = glueContext.write_dynamic_frame.from_catalog(
    frame=cotacoes_node,
    database="trusted-zone",
    table_name="evt_sf_cotacoes",
    additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"},
    transformation_ctx="AWSGlueDataCatalog_node_cotacoes"
)

AWSGlueDataCatalog_node_contas = glueContext.write_dynamic_frame.from_catalog(
    frame=contas_node,
    database="trusted-zone",
    table_name="evt_sf_contas",
    additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE", "partitionKeys": ["createddate"]},
    transformation_ctx="AWSGlueDataCatalog_node_contas"
)

job.commit()
