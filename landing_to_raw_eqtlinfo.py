import subprocess
import sys
# Instalar biblioteca py7zr
subprocess.check_call([sys.executable, "-m", "pip", "install", "py7zr"])

import boto3
import py7zr
import os
from datetime import datetime, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3 = boto3.client('s3')

def process_files():
    now = datetime.now()  # Linha corrigida para inicializar a data corretamente
    year = now.year
    month = now.month
    day = now.day
    destination_prefix = f'eqtlinfo/{year}/{month}/{day}/'
    
    # Listar arquivos na pasta eqtlinfo-landing-zone
    bucket_name = 'eqtlinfo-landing-zone'
    response = s3.list_objects_v2(Bucket=bucket_name)
    
    if 'Contents' not in response:
        print('No files found in the bucket')
        return
    
    for item in response['Contents']:
        object_key = item['Key']
        
        if object_key.endswith('.7z'):
            # Baixar o arquivo .7z
            download_path = f'/tmp/{os.path.basename(object_key)}'
            s3.download_file(bucket_name, object_key, download_path)
            
            # Descompactar o arquivo .7z
            with py7zr.SevenZipFile(download_path, mode='r') as archive:
                archive.extractall(path='/tmp/unzipped')
            
            # Enviar arquivos descompactados de volta ao S3
            for root, dirs, files in os.walk('/tmp/unzipped'):
                for file in files:
                    file_path = os.path.join(root, file)
                    s3.upload_file(file_path, 'raw-zone-echo', destination_prefix + file)
            
            # Limpeza
            os.remove(download_path)
            for root, dirs, files in os.walk('/tmp/unzipped'):
                for file in files:
                    os.remove(os.path.join(root, file))
            for dir in dirs:
                try:
                    os.rmdir(os.path.join(root, dir))
                except OSError as e:
                    print(f"Erro ao remover o diretório: {e}")
            
            # Apagar o arquivo .7z do S3
            s3.delete_object(Bucket=bucket_name, Key=object_key)
            print(f"Deleted {object_key} from {bucket_name}")

process_files()
job.commit()
