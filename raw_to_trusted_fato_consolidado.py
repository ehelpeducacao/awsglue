import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_now

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node LerRaw
LerRaw_node1715629085163 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://raw-zone-echo/consolidado_snapshot/2024/"], "recurse": True}, transformation_ctx="LerRaw_node1715629085163")

# Script generated for node data_hora_processamento
data_hora_processamento_node1715629311636 = LerRaw_node1715629085163.gs_now(colName="data_hora_processamento")

# Script generated for node Sink_trusted
Sink_trusted_node1715629354799 = glueContext.getSink(path="s3://trusted-zone-echo/fato_consolidado/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Sink_trusted_node1715629354799")
Sink_trusted_node1715629354799.setCatalogInfo(catalogDatabase="trusted-zone",catalogTableName="evt_consolidado")
Sink_trusted_node1715629354799.setFormat("glueparquet", compression="snappy")
Sink_trusted_node1715629354799.writeFrame(data_hora_processamento_node1715629311636)
job.commit()