import boto3
from datetime import datetime
import requests
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

# Definir o nome do bucket e o endpoint
BUCKET_NAME = 'raw-zone-echo'
ENDPOINT_URL = 'https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-40438d40aba2/resource/b1bd71e7-d0ad-4214-9053-cbd58e9564a7/download/empreendimento-geracao-distribuida.csv'

def main():
    # Inicializa o contexto do Spark e GlueContext
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    
    # Gerar o caminho de destino no S3 com base na data e hora atual
    now = datetime.now()
    s3_path = f'aneel/{now.strftime("%Y/%m/%d/%H/%M/%S")}/empreendimento-geracao-distribuida.csv'
    
    # Fazer a requisição para o endpoint
    response = requests.get(ENDPOINT_URL, stream=True)

    if response.status_code == 200:
        # Inicializar o cliente S3
        s3 = boto3.client('s3')
        
        try:
            # Fazer o upload do conteúdo diretamente para o S3
            s3.put_object(Bucket=BUCKET_NAME, Key=s3_path, Body=response.content)
            print(f'Arquivo salvo com sucesso em {BUCKET_NAME}/{s3_path}')
        except Exception as e:
            print(e)
            print('Erro ao salvar o arquivo no S3')
    else:
        print(f'Erro ao acessar o endpoint: {response.status_code}')

if __name__ == "__main__":
    main()
