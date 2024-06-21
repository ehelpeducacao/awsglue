import sys
from datetime import datetime, timedelta
import pyspark.sql.functions as f
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print("INCIANDO O SPARK")
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("Iniciando Fato Atividades / ")
print("E construindo os data frames de origem: Contas, Tarefas e compromissos")

path_df = {
  'contas': "s3://refined-zone-echo/dim_conta/",
  'tarefas': "s3://refined-zone-echo/dim_tarefa/",
  'compromissos': "s3://refined-zone-echo/dim_compromisso"
}

print("carregando o dataframe contas ")
df_contas = spark.read.parquet(path_df['contas'])
df_contas = df_contas.select(
     f.col("idconta"),
    f.col("nomeconta"),
    f.col("origemconta"),
    f.col("contaContrato"),
    f.col("statusConta"),
    f.col("cnpj"),
    f.col("cpf"),
    f.col("razaosocial"),
    f.col("nomerepresentante"),
    )
    
print("carregando o dataframe tarefas ")
df_tarefas = spark.read.parquet(path_df['tarefas'])
df_tarefas = df_tarefas.select(
    f.col("idtarefa").alias("idatividade"),
    f.col("idconta").alias("idconta_atividade"),
    f.col("datatarefa").alias("dataAtividade"),
    f.col("assunto"),
    f.col("descricao"),
    f.col("datacriacao").alias("dataCriacao"),
    f.col("criadopor"),
    f.col("nomeproprietario").alias("proprietario"),
    f.col("faseatual"),
    f.col("motivoNaoRealizado"),
    f.col("observacao").alias("observacao"),
    f.col("statusAgendamento").alias("status"),
    f.col("submercado"),
    f.col("tipo"),
    f.col("statusTarefa").alias("statusGeral"),
    f.col("prioridade").alias("prioridade"),
    f.col("datahoraCompletado").alias("dataEncerramento"),
    f.col("subtipo").alias("subtipo"),
    f.lit("N/A").alias("local"),
    f.lit("N/A").alias("mostrarComo"),
    f.col("dataUltimaModificacao"),
    f.lit("Tarefa").alias("origem")
    )
    
print("carregando o dataframe compromissos ")
df_compromissos = spark.read.parquet(path_df['compromissos'])
df_compromissos = df_compromissos.select(
    f.col("idcompromisso").alias("idatividade"),
    f.col("idconta").alias("idconta_atividade"),
    f.col("datacompromisso").alias("dataAtividade"),
    f.col("assunto"),
    f.col("descricao"),
    f.col("datacriacaocompromisso").alias("dataCriacao"),
    f.col("criadopor"),
    f.col("nomeproprietario").alias("proprietario"),
    f.col("faseatual"),
    f.col("motivoNaoRealizado"),
    f.col("observacaoConclusao").alias("observacao"),
    f.col("statusCompromisso").alias("status"),
    f.col("submercado"),
    f.col("tipo"),
    f.lit("N/A").alias("statusGeral"),
    f.lit("N/A").alias("prioridade"),
    f.col("datahoraFimCompromisso").alias("dataEncerramento"),
    f.col("subtipoDeEvento").alias("subtipo"),
    f.col("local"),
    f.col("mostrarComo"),
    f.col("dataUltimaModificacao"),
    f.lit("Compromisso").alias("origem")
    )
    
print("Realizando a uniao dos dataframes de tarefas e compromissos")
df_union = df_tarefas.union(df_compromissos)
print("fim do union dos dataframes / ")
print("Iniciando a juncao dos dataframes")
df_join = df_contas.join(df_union,\
    f.col("idconta") == f.col("idconta_atividade"),\
    "inner")
print("fim da juncao / Inicializando select em ordem da tabela fato")

now = (datetime.now() - timedelta(hours=3)).strftime("%m/%d/%Y %H:%M:%S")
df = df_join.select(
    f.col("idconta"),
    f.col("nomeconta"),
    f.col("origemconta"),
    f.col("contaContrato"),
    f.col("statusConta"),
    f.col("cnpj"),
    f.col("cpf"),
    f.col("razaosocial"),
    f.col("nomerepresentante"),
    f.col("idatividade"),
    f.col("origem"),
    f.col("proprietario"),
    f.col("dataAtividade"),
    f.col("dataCriacao"),
    f.col("criadopor"),
    f.col("assunto"),
    f.col("descricao"),
    f.col("local"),
    f.col("tipo"),
    f.col("subtipo"),
    f.col("status"),
    f.col("statusGeral"),
    f.col("faseatual"),
    f.col("dataEncerramento"),
    f.col("prioridade"),
    f.col("submercado"),
    f.col("mostrarComo"),
    f.col("motivoNaoRealizado"),
    f.col("observacao"),
    f.col("dataUltimaModificacao")
    ).withColumn("datahoraPreProcessamento", f.to_timestamp(f.lit(now), 'MM/dd/yyyy HH:mm:ss'))
    
print("encerramento o dataframe final e iniciando gravacao do parquet")

output_path = "s3://refined-zone-echo/fato_atividade/"
df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(output_path)
print("fim da geracao do parquet / fim do script")
job.commit()