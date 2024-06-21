import boto3
import sys
import requests
import subprocess
import pandas as pd
from datetime import datetime
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from io import BytesIO

# Instalar openpyxl e pyarrow para suporte de leitura de Excel e escrita de Parquet, respectivamente
subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl", "pyarrow"])

BUCKET_NAME = 'raw-zone-echo'
ENDPOINT_URL = 'https://api.novo.thunders.com.br'
ENDPOINT_GET = f'{ENDPOINT_URL}/v2/token'
ENDPOINT_POST = f'{ENDPOINT_URL}/connect/token'

def main():
    # Inicializa o contexto do Spark e GlueContext
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # Gerar o caminho de destino no S3 com base na data e hora atual
    now = datetime.now()
    s3_path = f'thunders/vigent_contracts/{now.strftime("%Y/%m/%d/%H/%M/%S")}/vigent_contract.parquet'
    
    token = requests.post(
        url=ENDPOINT_POST,
        data={
            'client_id': 'echoenergia_integr',
            'client_secret': '6dRV3acGWLpKB019',
            'grant_type': 'client_credentials'
        }
    ).json()['access_token']

    #print(token)

    response = requests.get(
        url=f'{ENDPOINT_URL}/gw/operations/api/operationsExport/Vigency',
        headers={
            'Accept': 'application/octet-stream',  # Atualizado para receber binário
            'Authorization': f'Bearer {token}',
            'X-Thundersaccess-Token': f'{token}'
        },
        params={
            'ScenarioId': '7'
        }
    )

    if response.status_code == 200:
        # Ler o conteúdo do Excel em um DataFrame
        excel_data = BytesIO(response.content)
        df = pd.read_excel(excel_data)

        # Converter DataFrame para Parquet
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False)

        # Inicializar o cliente S3
        s3 = boto3.client('s3')

        try:
            # Fazer o upload do Parquet para o S3
            s3.put_object(Bucket=BUCKET_NAME, Key=s3_path, Body=parquet_buffer.getvalue())
            print(f'Arquivo Parquet salvo com sucesso em {BUCKET_NAME}/{s3_parquet_path}')
        except Exception as e:
            print(e)
            print('Erro ao salvar o arquivo Parquet no S3')
    else:
        print(f'Erro ao acessar o endpoint: {response.status_code}')
if __name__ == "__main__":
    main()