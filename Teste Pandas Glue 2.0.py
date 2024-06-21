import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import subprocess
import pandas as pd

# Instalar openpyxl
subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl"])

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

excel_path = r"s3://raw-zone-echo/thunders/daily_report/2024/05/09/08/00/43/daily_report.xlsx"
# Usar openpyxl como engine para ler o arquivo xlsx
df_xl_op = pd.read_excel(excel_path, sheet_name="Relatório diário", engine='openpyxl')
df = df_xl_op.applymap(str)
input_df = spark.createDataFrame(df)
input_df.printSchema()
output_path = "s3://raw-zone-echo/thunders/daily_report/2024/05/09/08/00/43/"
input_df.write.parquet(output_path)

job.commit()