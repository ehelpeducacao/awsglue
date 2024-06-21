import sys
import pyspark.sql.functions as f
import boto3
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
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
bucket_name = 'trusted-zone-echo'
prefix = 'sales_force/contas/'

# Função para listar todos os arquivos no diretório e subdiretórios
def list_files_in_s3(bucket_name, prefix):
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                files.append(obj['Key'])
    return files

files = list_files_in_s3(bucket_name, prefix)

print("Arquivos listados no S3:")
for file in files:
    print(file)

s3_prefix = "s3a://trusted-zone-echo/"

# Filtrar apenas os arquivos (sem diretórios)
data_files = [file for file in files if not file.endswith('/')]

# Ler e unir os arquivos Parquet de cada caminho
dfs = []
for file in data_files:
    path = s3_prefix + file
    try:
        df = spark.read.parquet(path)
        dfs.append(df)
        print(f"Lido com sucesso: {path}")
    except Exception as e:
        print(f"Erro ao ler {path}: {e}")

# Verificar se existem DataFrames lidos com sucesso
if dfs:
    df_trusted = dfs[0]
    for df in dfs[1:]:
        df_trusted = df_trusted.union(df)

    # Definir a hora atual ajustada
    now = (datetime.now() - timedelta(hours=3)).strftime("%m/%d/%Y %H:%M:%S")

    # Definir chave e ordem para a janela
    key = ['id']
    order = 'lastmodifieddate'

    # Definir janela de partição
    w = Window.partitionBy(key).orderBy(f.col(order).desc())

    # Adicionar colunas rank e ts_refined
    df_trusted = df_trusted.withColumn("rank", f.row_number().over(w)) \
        .withColumn("ts_refined", f.to_timestamp(f.lit(now), 'MM/dd/yyyy HH:mm:ss'))
else:
    print("Nenhum DataFrame foi lido com sucesso.")
    
#filtrar apenas os registros com a maior data de modificação
df_trusted = df_trusted.where("rank = 1")

#df conta pai 
df_conta_pai = df_trusted.select(
    f.col("id").alias("idContaPai"),
    f.col("name").alias("nomeContaPai"),
    f.col("cnpj__c").alias("cnpjContaPai")
)

#df regionais
df_regional= spark.read.parquet("s3://refined-zone-echo/dim_regional/")
df_regional = df_regional.select(
    f.col("idRegional"),
    f.col("nomeRegional")
)

#df contato_representante
df_contato = spark.read.parquet("s3://refined-zone-echo/dim_contato")
df_contato = df_contato.select(
    f.col("idcontato"),
    f.col("nomeCompleto")
)

#df tipo de registro
df_tipo_reg = spark.read.parquet("s3://refined-zone-echo/dim_tipo_de_registro")
df_tipo_reg = df_tipo_reg.select(
    f.col("idtipoderegistro"),
    f.col("nometipoderegistro")
)

# Montar o dataframe df_Conta
df_conta = df_trusted.select(
    f.col("id").alias("idConta"),
    f.col("name").alias("nomeConta"),
    f.coalesce(f.col("conta_contrato__c"), f.lit("N/A")).alias("contaContrato"), 
    f.coalesce(f.col("natureza_da_unidade_consumidora__c"), f.lit("N/A")).alias("naturezaDaUc"), 
    f.coalesce(f.col("accountsource"), f.lit("N/A")).alias("origemConta"),
    f.coalesce(f.col("status__c"), f.lit("N/A")).alias("statusConta"),
    f.coalesce(f.col("cnpj__c"), f.lit("N/A")).alias("cnpj"),
    f.coalesce(f.col("razao_social__c"), f.lit("N/A")).alias("razaoSocial"),
    f.coalesce(f.col("nome_fantasia__c"), f.lit("N/A")).alias("nomeFantasia"),
    f.coalesce(f.col("cpf__c"), f.lit("N/A")).alias("cpf"),  
    f.coalesce(f.col("inscricao_estadual__c"), f.lit("N/A")).alias("inscricaoEstadual"),
    f.coalesce(f.col("billingstreet"), f.lit("N/A")).alias("enderecoCobranca"),
    f.coalesce(f.col("billingcity"), f.lit("N/A")).alias("cidadeCobranca"),    
    f.coalesce(f.col("billingstate"), f.lit("N/A")).alias("ufCobranca"),
    f.coalesce(f.col("billingcountry"), f.lit("N/A")).alias("paisCobranca"),
    f.coalesce(f.col("billingpostalcode"), f.lit("N/A")).alias("cepCobranca"),
    f.coalesce(f.col("shippingstreet"), f.lit("N/A")).alias("enderecoFornecimento"),
    f.coalesce(f.col("shippingcity"), f.lit("N/A")).alias("cidadeFornecimento"),    
    f.coalesce(f.col("shippingstate"), f.lit("N/A")).alias("ufFornecimento"),
    f.coalesce(f.col("shippingcountry"), f.lit("N/A")).alias("paisFornecimento"),
    f.coalesce(f.col("shippingpostalcode"), f.lit("N/A")).alias("cepFornecimento"),
    f.coalesce(f.col("email__c"), f.lit("N/A")).alias("email"),
    f.coalesce(f.col("phone"),f.lit("N/A")).alias("telefone"),
    f.coalesce(f.col("numero_de_ucs__c"), f.lit(None)).cast("integer").alias("numeroUcs"),
    f.coalesce(f.col("numberofemployees"), f.lit(None)).cast("integer").alias("quantFuncionarios"),
    f.coalesce(f.col("website"), f.lit("N/A")).alias("website"),    
    f.coalesce(f.col("industry"), f.lit("N/A")).alias("industria"),    
    f.when(f.col("ispartner") == "true", "SIM") \
        .when(f.col("ispartner") == "false", "NAO") \
        .otherwise("N/A") \
        .alias("parceiro"),    
    f.when(f.col("distribuidora_preenchida__c") == "true", "SIM") \
        .when(f.col("distribuidora_preenchida__c") == "false", "NAO") \
        .otherwise("N/A") \
        .alias("distribuidoraPreenchida"),
    f.when(f.col("cliente_vip__c") == "true", "SIM") \
        .when(f.col("cliente_vip__c") == "false", "NAO") \
        .otherwise("N/A") \
        .alias("clienteVip"),
    f.coalesce(f.col("classe_de_tensao__c"), f.lit("N/A")).alias("classeTensao"),   
    f.coalesce(f.col("cliente_irrigante__c"), f.lit("N/A")).alias("clienteIrrigante"),
    f.coalesce(f.col("cliente_rural__c"), f.lit("N/A")).alias("clienteRural"),    
    f.coalesce(f.col("cliente_desliga_na_ponta__c"), f.lit("N/A")).alias("clienteDesligaNaPonta"),   
    f.coalesce(f.col("modalidade_tarifaria__c"), f.lit("N/A")).alias("modalidadeTarifaria"),    
    f.coalesce(f.col("fornecedor_de_energia__c"), f.lit("N/A")).alias("fornecedorDeEnergia"),    
    f.coalesce(f.col("submercado__c"), f.lit("N/A")).alias("submercado"),   
    f.coalesce(f.col("utiliza_gerador__c"), f.lit("N/A")).alias("utilizaGerador"),  
    f.coalesce(f.col("tem_gd__c"), f.lit("N/A")).alias("temGd"),
    f.coalesce(f.col("ja_e_livre__c"), f.lit("N/A")).alias("jaELivre"),   
    f.coalesce(f.col("statuscarga__c"), f.lit("N/A")).alias("statusCarga"),   
    f.coalesce(f.col("classificacao_do_produto_potencial__c"), f.lit("N/A")).alias("classificacaoProdPotencial"), 
    f.when(f.col("fatura_de_energia__c") == "true", "SIM") \
        .when(f.col("fatura_de_energia__c") == "false", "NAO") \
        .otherwise("N/A") \
        .alias("faturaDeEnergia"),
    f.coalesce(f.col("data_de_vigencia_contrato__c"), f.lit("N/A")).alias("dataVigenciaContrato"),
    f.coalesce(f.col("unidadeconsumidora__c"), f.lit("N/A")).alias("unidadeConsumidora"),
    f.coalesce(f.col("iar__c"), f.lit(None)).cast("float").alias("iar"), 
    f.coalesce(f.col("volume_mwm__c"), f.lit(None)).cast("float").alias("volumeMwm"), 
    f.coalesce(f.col("demanda_fora_ponta_kw__c"), f.lit(None)).cast("float").alias("demandaForaPontaKw"), 
    f.coalesce(f.col("consumo_fora_ponta_kwh__c"), f.lit(None)).cast("float").alias("consumoForaPontaKwh"), 
    f.coalesce(f.col("demanda_ponta_kw__c"), f.lit(None)).cast("float").alias("demandaPontaKw"), 
    f.coalesce(f.col("consumo_ponta_kwh__c"), f.lit(None)).cast("float").alias("consumoPontaKwh"), 
    f.coalesce(f.col("demanda_total_kw__c"), f.lit(None)).cast("float").alias("demandaTotalKw"),
    f.coalesce(f.col("consumo_total_kwh__c"), f.lit(None)).cast("float").alias("consumoTotalKwh"),
    f.coalesce(f.col("motivoperdacarga__c"), f.lit("N/A")).alias("motivoPerdaCarga"),
    f.coalesce(f.col("oportunidadeganhadistribuidora__c"), f.lit(None)).cast("integer").alias("oportunidadeGanhaDistr"),
    f.coalesce(f.col("totaloportunidadesganhas__c"), f.lit(None)).cast("integer").alias("totalOportunidadesGanhas"),  
    f.coalesce(f.col("observacoes_gerais__c"), f.lit("N/A")).alias("observacoes"),
    f.col("parentid"), 
    f.col("recordtypeid"),
    f.col("contato_representante__c"),
    f.col("regionalpesquisa__c"),
    f.when(f.col("isdeleted") == "true", "SIM") \
        .when(f.col("isdeleted") == "false", "NAO") \
        .otherwise("N/A") \
        .alias("foiDeletado"),
    f.col("lastmodifieddate").alias("dataUltimaModificacao"),
    f.col("ts_refined").alias("datahoraProcessamento")
)

df = df_conta.join(df_conta_pai, f.col("parentid") == f.col("idContaPai"), "left")\
    .join(df_regional, f.col("regionalpesquisa__c") == f.col("idRegional"), "left")\
    .join(df_contato, f.col("contato_representante__c") == f.col("idcontato"), "left")\
    .join(df_tipo_reg, f.col("recordtypeid") == f.col("idtipoderegistro"), "left")\
    .drop("regionalpesquisa__c")\
    .drop("parentid")\
    .drop("contato_representante__c")

df = df.withColumnRenamed("nometipoderegistro", "tipoDeRegistro")\
    .withColumnRenamed("nomeCompleto", "nomeRepresentante")\
    .withColumnRenamed("nomeRegional", "regionalPesquisa")\
    .where("recordtypeid <> '012Hs000001R8DqIAK'")\
    .drop("recordtypeid").drop("idcontato").drop("idtipoderegistro").drop("idRegional")

output_path = "s3a://refined-zone-echo/dim_conta/"
df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(output_path)

job.commit()