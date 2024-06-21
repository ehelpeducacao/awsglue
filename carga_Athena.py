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

# Script generated for node cotacoes
cotacoes_node1716303305043 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://trusted-zone-echo/sales_force/cotacoes/schemaVersion_1/"], "recurse": True}, transformation_ctx="cotacoes_node1716303305043")

# Script generated for node contas
contas_node1716301152541 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://trusted-zone-echo/sales_force/contas/schemaVersion_1/"], "recurse": True}, transformation_ctx="contas_node1716301152541")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1716303437497 = glueContext.write_dynamic_frame.from_catalog(frame=cotacoes_node1716303305043, database="trusted-zone", table_name="evt_sf_cotacoes", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1716303437497")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1716301303415 = glueContext.write_dynamic_frame.from_catalog(frame=contas_node1716301152541, database="trusted-zone", table_name="evt_sf_contas", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE", "partitionKeys": ["createddate"]}, transformation_ctx="AWSGlueDataCatalog_node1716301303415")

job.commit()