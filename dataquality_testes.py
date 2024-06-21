
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
import pandas as pd
colunas = [
    "CONTA_CONTRATO_C",
    "DATA_ATE_C",
    "DENUNCIA",
    "CARTA_RESPOSTA",
    "DTA_PREV_MIGRACAO",
    "INSTALACAO_C",
    "MES_COMPETENCIA_F",
    "DISTRIBUIDORA",
    "GRUPO_TENSAO_C",
    "PERFIL",
    "DEMANDA_CONTRATADO_P",
    "DEMANDA_CONTRATADO_FP",
    "DEMANDA_REGISTRADA_P",
    "DEMANDA_REGISTRADA_FP",
    "CONTA_COVID",
    "CONTA_EH",
    "ICMS_F",
    "PIS_F",
    "COFINS_F",
    "DESC_APLICADO_TUSD",
    "CONSUMO_P",
    "CONSUMO_FP",
    "CONSUMO_CONTRATADO_P",
    "CONSUMO_CONTRATADO_FP",
    "OUTROS_CUSTOS_CATIVO",
    "OUTROS_CUSTOS_LIVRE",
    "MATRIZ",
    "NOME_C",
    "CNPJ_C",
    "CPF_C",
    "UF",
    "MUNICIPIO_C",
    "COMPLEMENTO_C",
    "REGIONAL_C",
    "ENDERECO_C",
    "NUMERO_C",
    "COMPLEMENTO2_C",
    "PONTO_REFERENCIA_C",
    "BAIRRO_C",
    "CEP_C",
    "LATITUDE_C",
    "LONGITUDE_C",
    "CLASSE_PRINCIPAL_C",
    "SUBCLASSE_C",
    "PARCEIRO_NEGOCIO_C",
    "CARTEIRA_CLIENTE",
    "NIVEL_TENSAO_C",
    "CATEGORIA_TARIFA_F",
    "CONSUMO_ATIVO_PONTA",
    "CONSUMO_ATIVO_FORA_PONTA",
    "CONSUMO_ATIVO",
    "CONSUMO_REATIVO_PONTA",
    "CONSUMO_REATIVO_FORA_PONTA",
    "CONSUMO_REATIVO",
    "DEMANDA_ATIVA_PONTA",
    "DEMANDA_ATIVA_FORA_PONTA",
    "DEMANDA_ATIVA",
    "DEMANDA_CONTRATADA_PONTA",
    "DEMANDA_CONTRATADA_FORA_PONTA",
    "DEMANDA_CONTRATADA",
    "IAR",
    "CONSUMO_FATURADO_F",
    "TEL_FIXO_C",
    "TEL_MOVEL_C",
    "EMAIL_C",
    "STATUS_INSTALACAO_C",
    "ENERGIA_INJETADA",
    "INF_GERADOR",
    "INF_GD",
    "CLIENTE_LIVRE_C",
    "DATA_LEITURA_REAL_L",
    "VALOR_FATURA_F",
    "OUTROS_PROD_SERV",
]
# Caminho para o arquivo CSV
csv_path = "s3://raw-zone-echo/eqtlinfo/CELG/2024/05/31/CELG202301.csv"

# Lê o arquivo CSV sem cabeçalho
df = spark.read.csv(csv_path, header=False, inferSchema=True, sep=';', encoding='ISO-8859-1')

# Adiciona os cabeçalhos ao DataFrame
df = df.toDF(*colunas)

# Exibe o DataFrame com os cabeçalhos adicionados
df.show()
import pandas as pd 

# Caminho para o arquivo CSV
csv_path = "s3://raw-zone-echo/eqtlinfo/2024/5/24/CELG202301.csv"
try:
    df_pandas = pd.read_csv(csv_path, encoding='ISO-8859-1', sep=',', error_bad_lines=False)
    print(f"Número de colunas (pandas): {df_pandas.shape[1]}")
    print(df_pandas.head())
except pd.errors.ParserError as e:
    print(f"Erro ao ler o CSV: {e}")

# Inspecione as linhas para encontrar o número de colunas em cada linha
line_counts = []

with open(csv_path, 'r', encoding='ISO-8859-1') as file:
    for line_num, line in enumerate(file, start=1):
        line_counts.append((line_num, len(line.split(','))))

# Exiba linhas que não têm o número esperado de colunas
expected_columns = 73  # Altere conforme necessário
problematic_lines = [line for line in line_counts if line[1] != expected_columns]

print("Linhas problemáticas:")
for line in problematic_lines:
    print(f"Linha {line[0]}: {line[1]} colunas")
import requests
import datetime as dt
import json

basic_url = 'https://api.pluvia.app'
verify_certificate = True

# Dados do usuário para autenticação
user = 'aws.comercializadora' 
pwd = '!Pluv14!' 

# Autenticação e obtenção do token
response = requests.post(
    url=basic_url + '/v2/token',
    headers={
        'content-type': 'application/json',
        'accept': '*/*'
    },
    json={
        'username': user,
        'password': pwd
    },
    verify=verify_certificate
)

token = ''
if response.status_code == 200:
    response_json = response.json()
    token = response_json['access_token']

# Exemplo de como usar o token para pegar informações
api_function = '/v2/valoresParametros/modos'
responsemodos = requests.get(
    url=basic_url + api_function,
    headers={
        'Authorization': 'Bearer ' + token,
        "Content-Type": "application/json"
    },
    verify=verify_certificate
)

if responsemodos.status_code == 200:
    modes_info = responsemodos.json()
    print(modes_info)
import boto3
import requests
from datetime import datetime
from awsglue.context import GlueContext
from pyspark.context import SparkContext

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
    s3_path = f'thunders/vigent_contracts/{now.strftime("%Y/%m/%d/%H/%M/%S")}/vigent_contract.xlsx'
    
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
        # Inicializar o cliente S3
        s3 = boto3.client('s3')

        try:
            # Fazer o upload do conteúdo diretamente para o S3
            s3.put_object(Bucket=BUCKET_NAME, Key=s3_path, Body=response.content)
            print(f'Arquivo Excel salvo com sucesso em {BUCKET_NAME}/{s3_path}')
        except Exception as e:
            print(e)
            print('Erro ao salvar o arquivo Excel no S3')
    else:
        print(f'Erro ao acessar o endpoint: {response.status_code}')

if __name__ == "__main__":
    main()
main()
job.commit()