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

# Script generated for node trusted_contas
trusted_contas_node1715795518660 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://trusted-zone-echo/sales_force/contas/schemaVersion_1/"], "recurse": True}, transformation_ctx="trusted_contas_node1715795518660")

# Script generated for node sandbox_contas
sandbox_contas_node1715795586277 = glueContext.getSink(path="s3://sandbox-zone-echo", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="sandbox_contas_node1715795586277")
sandbox_contas_node1715795586277.setCatalogInfo(catalogDatabase="sandbox",catalogTableName="sales_force_contas")
sandbox_contas_node1715795586277.setFormat("glueparquet", compression="snappy")
sandbox_contas_node1715795586277.writeFrame(trusted_contas_node1715795518660)
job.commit()