import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
import boto3
import requests

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Inicializando o Spark e Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)

# Inicializando a SparkSession a partir do GlueContext
spark = glueContext.spark_session

# Inicializando o Job do Glue
job = Job(glueContext)
job.init(job_name=args['JOB_NAME'])

date_today = datetime.now()
year, month, day = date_today.strftime('%Y'), date_today.strftime('%m'), date_today.strftime('%d')
base_url = "https://download.in.gov.br/sgpub/do/secao"
sections = ['1', '2', '3']
bucket_name = 'raw-zone-echo' 
s3 = boto3.client('s3')

# Processamento para cada seção
for section in sections:
    file_url = f"{base_url}{section}/{date_today}_ASSINADO_do{section}.pdf"
    response = requests.get(file_url)
    response.raise_for_status() 
    s3_key = f"dou/{year}/{month}/{day}/{year}_{month}_{day}_ASSINADO_do{section}.pdf"
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=response.content)
    print(f"Arquivo da seção {section} foi baixado e carregado com sucesso.")

job.commit()