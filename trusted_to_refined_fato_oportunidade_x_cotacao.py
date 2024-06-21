import sys
import pyspark.sql.functions as f
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


    

print("Iniciando dataframe cotacao")
#ler dados de cotacao 
#cotacao é a menor granularidade na fato, uma oportunidade tem varias cotacoes. 
path_cotacao = 's3://trusted-zone-echo/ods/sf/cotacoes/'
df_cotacao = spark.read.parquet(path_cotacao)
df_cotacao = df_cotacao.select(
    f.coalesce(f.col("accountid"), f.lit(None)).alias("idconta_cotacao"),
    f.coalesce(f.col("additionaladdress"), f.lit(None)).alias("enderecofulladicional_cotacao"),
    f.coalesce(f.col("additionalcity"), f.lit(None)).alias("cidadeadicional_cotacao"),
    f.coalesce(f.col("additionalcountry"), f.lit(None)).alias("paisadicional_cotacao"),
    f.coalesce(f.col("additionalgeocodeaccuracy"), f.lit(None)).alias("geocodeadicional_cotacao"),
    f.coalesce(f.col("additionallatitude"), f.lit(None)).alias("latitudeadicional_cotacao"),
    f.coalesce(f.col("additionallongitude"), f.lit(None)).alias("longitudeadicional_cotacao"),
    f.coalesce(f.col("additionalname"), f.lit(None)).alias("nomeadicional_cotacao"),
    f.coalesce(f.col("additionalpostalcode"), f.lit(None)).alias("cepadicional_cotacao"),
    f.coalesce(f.col("additionalstate"), f.lit(None)).alias("estadoadicional_cotacao"),
    f.coalesce(f.col("additionalstreet"), f.lit(None)).alias("enderecoadicional_cotacao"),
    f.coalesce(f.col("alcada_de_aprovacao__c"), f.lit(None)).alias("alcadadeaprovacao_cotacao"),
    f.coalesce(f.col("billingaddress"), f.lit(None)).alias("enderecofullcobranca_cotacao"),
    f.coalesce(f.col("billingcity"), f.lit(None)).alias("cidadecobranca_cotacao"),
    f.coalesce(f.col("billingcountry"), f.lit(None)).alias("paiscobranca_cotacao"),
    f.coalesce(f.col("billinggeocodeaccuracy"), f.lit(None)).alias("geocodecobranca_cotacao"),
    f.coalesce(f.col("billinglatitude"), f.lit(None)).alias("latitudecobranca_cotacao"),
    f.coalesce(f.col("billinglongitude"), f.lit(None)).alias("longitudecobranca_cotacao"),
    f.coalesce(f.col("billingname"), f.lit(None)).alias("nomecobranca_cotacao"),
    f.coalesce(f.col("billingpostalcode"), f.lit(None)).alias("cepcobranca_cotacao"),
    f.coalesce(f.col("billingstate"), f.lit(None)).alias("estadocobranca_cotacao"),
    f.coalesce(f.col("billingstreet"), f.lit(None)).alias("enderecocobranca_cotacao"),
    f.coalesce(f.col("cancreatequotelineitems"), f.lit(None)).alias("podecriarlinhasdecotacao_cotacao"),
    f.coalesce(f.col("cliente_vip__c"), f.lit(None)).alias("clientevip_cotacao"),
    f.coalesce(f.col("consumo_fora_ponta_kwh__c"), f.lit(None)).alias("consumoforapontakwh_cotacao"),
    f.coalesce(f.col("consumo_ponta_kwh__c"), f.lit(None)).alias("consumopontakwh_cotacao"),
    f.coalesce(f.col("contactid"), f.lit(None)).alias("idcontato_cotacao"),
    f.coalesce(f.col("contractid"), f.lit(None)).alias("idcontrato_cotacao"),
    f.coalesce(f.col("createdbyid"), f.lit(None)).alias("criadopor_cotacao"),
    f.coalesce(f.col("createddate"), f.lit(None)).alias("datacriacao_cotacao"),
    f.coalesce(f.col("custo_adequacao__c"), f.lit(None)).alias("custoadequacao_cotacao"),
    f.coalesce(f.col("de_desconto_ofertado__c"), f.lit(None)).alias("decontoofertado_cotacao"),
    f.coalesce(f.col("demanda_fora_ponta_kw__c"), f.lit(None)).alias("demandaforapontakw_cotacao"),
    f.coalesce(f.col("demanda_ponta_kw__c"), f.lit(None)).alias("demandapontakw_cotacao"),
    f.coalesce(f.col("desconto_maximo_comercial__c"), f.lit(None)).alias("descontomaxcomercial_cotacao"),
    f.coalesce(f.col("desconto_maximo_devido_ao_risco__c"), f.lit(None)).alias("descontomaxdevidorisco_cotacao"),
    f.coalesce(f.col("description"), f.lit(None)).alias("descricao_cotacao"),
    f.coalesce(f.col("discount"), f.lit(None)).alias("desconto_cotacao"),
    f.coalesce(f.col("email"), f.lit(None)).alias("email_cotacao"),
    f.coalesce(f.col("emailcontato__c"), f.lit(None)).alias("emailcontato_cotacao"),
    f.coalesce(f.col("expirationdate"), f.lit(None)).alias("dataValidade_cotacao"),
    f.coalesce(f.col("fax"), f.lit(None)).alias("fax_cotacao"),
    f.coalesce(f.col("flex__c"), f.lit(None)).alias("flex_cotacao"),
    f.coalesce(f.col("flex_porcentagem__c"), f.lit(None)).alias("flexporcentagem_cotacao"),
    f.coalesce(f.col("garantia_do_comprador__c"), f.lit(None)).alias("garantiadocomprador_cotacao"),
    f.coalesce(f.col("grandtotal"), f.lit(None)).alias("totalGeral_cotacao"),
    f.coalesce(f.col("id"), f.lit(None)).alias("idcotacao_cotacao"),
    f.coalesce(f.col("indice_de_reajuste_data_base__c"), f.lit(None)).alias("indicereajustedatabase_cotacao"),
    f.coalesce(f.col("isdeleted"), f.lit(None)).alias("foideletado_cotacao"),
    f.coalesce(f.col("issyncing"), f.lit(None)).alias("foisincronizado_cotacao"),
    f.coalesce(f.col("lastmodifiedbyid"), f.lit(None)).alias("ultimamodificacaopor_cotacao"),
    f.coalesce(f.col("lastmodifieddate"), f.lit(None)).alias("dataultimamodificacao_cotacao"),
    f.coalesce(f.col("lastreferenceddate"), f.lit(None)).alias("dataultimareferencia_cotacao"),
    f.coalesce(f.col("lastvieweddate"), f.lit(None)).alias("dataultimavizualizacao_cotacao"),
    f.coalesce(f.col("lineitemcount"), f.lit(None)).alias("countagemdeitens_cotacao"),
    f.coalesce(f.col("modalidade_de_negocio__c"), f.lit(None)).alias("modalidadenegocio_cotacao"),
    f.coalesce(f.col("modulacao__c"), f.lit(None)).alias("modulacao_cotacao"),
    f.coalesce(f.col("multa_antecipacao__c"), f.lit(None)).alias("multaantecipacao_cotacao"),
    f.coalesce(f.col("name"), f.lit(None)).alias("nomecotacao_cotacao"),
    f.coalesce(f.col("oferta_preco_fixo_aplicavel__c"), f.lit(None)).alias("ofertaprecofixoaplicavel_cotacao"),
    f.coalesce(f.col("opportunityid"), f.lit(None)).alias("idoportunidade_cotacao"),
    f.coalesce(f.col("ownerid"), f.lit(None)).alias("proprietario_cotacao"),
    f.coalesce(f.col("phone"), f.lit(None)).alias("telefone_cotacao"),
    f.coalesce(f.col("preco_medio_venda__c"), f.lit(None)).alias("precomediovenda_cotacao"),
    f.coalesce(f.col("preco_minimo_energia__c"), f.lit(None)).alias("precominimoenergia_cotacao"),
    f.coalesce(f.col("precoponderado__c"), f.lit(None)).alias("precoponderado_cotacao"),
    f.coalesce(f.col("pricebook2id"), f.lit(None)).alias("livrodepreco_cotacao"),
    f.coalesce(f.col("produtopotencial__c"), f.lit(None)).alias("produtopontencial_cotacao"),
    f.coalesce(f.col("quotenumber"), f.lit(None)).alias("numerocotacao_cotacao"),
    f.coalesce(f.col("quotetoaddress"), f.lit(None)).alias("enderecocotacao_cotacao"),
    f.coalesce(f.col("quotetocity"), f.lit(None)).alias("cidadecotacao_cotacao"),
    f.coalesce(f.col("quotetocountry"), f.lit(None)).alias("paiscotacao_cotacao"),
    f.coalesce(f.col("quotetogeocodeaccuracy"), f.lit(None)).alias("geocodecotacao_cotacao"),
    f.coalesce(f.col("quotetolatitude"), f.lit(None)).alias("latitudecotacao_cotacao"),
    f.coalesce(f.col("quotetolongitude"), f.lit(None)).alias("longitudecotacao_cotacao"),
    f.coalesce(f.col("quotetoname"), f.lit(None)).alias("nomeparacotacao_cotacao"),
    f.coalesce(f.col("quotetopostalcode"), f.lit(None)).alias("cepcotacao_cotacao"),
    f.coalesce(f.col("quotetostate"), f.lit(None)).alias("estadocotacao_cotacao"),
    f.coalesce(f.col("quotetostreet"), f.lit(None)).alias("enderecoparacotacao_cotacao"),
    f.coalesce(f.col("recordtypeid"), f.lit(None)).alias("idTipoRegistro_cotacao"),
    f.coalesce(f.col("sazonalidade__c"), f.lit(None)).alias("sazonalidade_cotacao"),
    f.coalesce(f.col("sazonalidade_porcentagem__c"), f.lit(None)).alias("sazonalidadeporcentagem_cotacao"),
    f.coalesce(f.col("shippingaddress"), f.lit(None)).alias("enderecofullfornecimento_cotacao"),
    f.coalesce(f.col("shippingcity"), f.lit(None)).alias("cidadefornecimento_cotacao"),
    f.coalesce(f.col("shippingcountry"), f.lit(None)).alias("paisfornecimento_cotacao"),
    f.coalesce(f.col("shippinggeocodeaccuracy"), f.lit(None)).alias("geocodefornecimento_cotacao"),
    f.coalesce(f.col("shippinghandling"), f.lit(None)).alias("manuseiofornecimento_cotacao"),
    f.coalesce(f.col("shippinglatitude"), f.lit(None)).alias("latitudefornecimento_cotacao"),
    f.coalesce(f.col("shippinglongitude"), f.lit(None)).alias("longitudefornecimento_cotacao"),
    f.coalesce(f.col("shippingname"), f.lit(None)).alias("nomeparafornecimento_cotacao"),
    f.coalesce(f.col("shippingpostalcode"), f.lit(None)).alias("cepfornecimento_cotacao"),
    f.coalesce(f.col("shippingstate"), f.lit(None)).alias("estadoforncecimento_cotacao"),
    f.coalesce(f.col("shippingstreet"), f.lit(None)).alias("enderecofornecimento_cotacao"),
    f.coalesce(f.col("sinalizador__c"), f.lit(None)).alias("sinalizador_cotacao"),
    f.coalesce(f.col("situacao__c"), f.lit(None)).alias("situacao_cotacao"),
    f.coalesce(f.col("status"), f.lit(None)).alias("statuscotacao_cotacao"),
    f.coalesce(f.col("submercado__c"), f.lit(None)).alias("submercado_cotacao"),
    f.coalesce(f.col("subtotal"), f.lit(None)).alias("subtotal_cotacao"),
    f.coalesce(f.col("suprimento__c"), f.lit(None)).alias("suprimento_cotacao"),
    f.coalesce(f.col("systemmodstamp"), f.lit(None)).alias("systemmodstamp_cotacao"),
    f.coalesce(f.col("tax"), f.lit(None)).alias("imposto_cotacao"),
    f.coalesce(f.col("telefonecontato__c"), f.lit(None)).alias("telefonecontato_cotacao"),
    f.coalesce(f.col("tipo_cliente__c"), f.lit(None)).alias("tipocliente_cotacao"),
    f.coalesce(f.col("tipo_cotacao__c"), f.lit(None)).alias("tipocotacao_cotacao"),
    f.coalesce(f.col("tipo_de_proposta__c"), f.lit(None)).alias("tipoproposta_cotacao"),
    f.coalesce(f.col("totalprice"), f.lit(None)).alias("precototal_cotacao"),
    f.coalesce(f.col("usina_a_ser_ofertada__c"), f.lit(None)).alias("usinaaserofertada_cotacao"),
    f.coalesce(f.col("volume_mwm_de_energia__c"), f.lit(None)).alias("volumemwmdeenergia_cotacao")
)
print("Finalizado Dataframe Cotacao")

print("Iniciando Dataframe oportunidade")
# montando dataframe de oportunidade
path_oportunidade = 's3://trusted-zone-echo/ods/sf/oportunidades/'
df_oportunidade = spark.read.parquet(path_oportunidade)
df_oportunidade = df_oportunidade.select(
    f.coalesce(f.col("accountid"), f.lit(None)).alias("idconta_oportunidade"),
    f.coalesce(f.col("ageindays"), f.lit(None)).alias("idadeEmDias_oportunidade"),
    f.coalesce(f.col("amount"), f.lit(None)).alias("valor_oportunidade"),
    f.coalesce(f.col("boleta__c"), f.lit(None)).alias("boleta_oportunidade"),
    f.coalesce(f.col("budget_confirmed__c"), f.lit(None)).alias("orcamentoConfirmado_oportunidade"),
    f.coalesce(f.col("campaignid"), f.lit(None)).alias("idcampanha_oportunidade"),
    f.coalesce(f.col("classificacao__c"), f.lit(None)).alias("classificacao_oportunidade"),
    f.coalesce(f.col("classificacao_do_produto_potencial__c"), f.lit(None)).alias("classificacaodoprodutopotencial_oportunidade"),
    f.coalesce(f.col("closedate"), f.lit(None)).alias("datafechamento_oportunidade"),
    f.coalesce(f.col("cnpj__c"), f.lit(None)).alias("cnpj_oportunidade"),
    f.coalesce(f.col("consumo_fora_ponta_kwh__c"), f.lit(None)).alias("consumoforapontakwh_oportunidade"),
    f.coalesce(f.col("consumo_mwm__c"), f.lit(None)).alias("consumomwm_oportunidade"),
    f.coalesce(f.col("consumo_ponta_kwh__c"), f.lit(None)).alias("consumopontakwh_oportunidade"),
    f.coalesce(f.col("consumo_total_kwh__c"), f.lit(None)).alias("consumototalkwh_oportunidade"),
    f.coalesce(f.col("contactid"), f.lit(None)).alias("idcontato_oportunidade"),
    f.coalesce(f.col("contractid"), f.lit(None)).alias("idcontrato_oportunidade"),
    f.coalesce(f.col("contrato_em_anos__c"), f.lit(None)).alias("anosContrato_oportunidade"),
    f.coalesce(f.col("contrato_em_anos_formula__c"), f.lit(None)).alias("formulaAnosContrato_oportunidade"),
    f.coalesce(f.col("cor_restricao__c"), f.lit(None)).alias("restricaoCor_oportunidade"),
    f.coalesce(f.col("corscore__c"), f.lit(None)).alias("corscore_oportunidade"),
    f.coalesce(f.col("cotacao_inicial_criada__c"), f.lit(None)).alias("cotacaoInicialCriada_oportunidade"),
    f.coalesce(f.col("cpf__c"), f.lit(None)).alias("cpf_oportunidade"),
    f.coalesce(f.col("createdbyid"), f.lit(None)).alias("criadopor_oportunidade"),
    f.coalesce(f.col("createddate"), f.lit(None)).alias("datacriacao_oportunidade"),
    f.coalesce(f.col("data_de_vigencia_contrato__c"), f.lit(None)).alias("datavigenciaContrato_oportunidade"),
    f.coalesce(f.col("dataassinaturacontrato__c"), f.lit(None)).alias("dataAssinaturaContrato_oportunidade"),
    f.coalesce(f.col("datacriacaodaproposta__c"), f.lit(None)).alias("dataCriacaoProposta_oportunidade"),
    f.coalesce(f.col("datafechadoeganho__c"), f.lit(None)).alias("dataFechadoeGanho_oportunidade"),
    f.coalesce(f.col("datafinalizacaocriacaodaproposta__c"), f.lit(None)).alias("dataFinalizacaoCriacaoProposta_oportunidade"),
    f.coalesce(f.col("dataultimaintegracaorisk3__c"), f.lit(None)).alias("dataultimaIntegracaoRisco_oportunidade"),
    f.coalesce(f.col("demanda_fora_ponta_kw__c"), f.lit(None)).alias("demandaforapontaKw_oportunidade"),
    f.coalesce(f.col("demanda_ponta__c"), f.lit(None)).alias("demandaPonta_oportunidade"),
    f.coalesce(f.col("demanda_total_kw__c"), f.lit(None)).alias("demandaTotal_oportunidade"),
    f.coalesce(f.col("description"), f.lit(None)).alias("descricao_oportunidade"),
    f.coalesce(f.col("discovery_completed__c"), f.lit(None)).alias("completouDiscovey_oportunidade"),
    f.coalesce(f.col("distribuidora__c"), f.lit(None)).alias("distribuidora_oportunidade"),
    f.coalesce(f.col("duracaocriacaodapropostadias__c"), f.lit(None)).alias("duracaocriacaoPropostaemDias_oportunidade"),
    f.coalesce(f.col("elegivel_motor_de_preco__c"), f.lit(None)).alias("elegivelMotordePreco_oportunidade"),
    f.coalesce(f.col("etapa__c"), f.lit(None)).alias("etapa_oportunidade"),
    f.coalesce(f.col("fatura_de_energia_anexada__c"), f.lit(None)).alias("faturaEnergiaAnexada_oportunidade"),
    f.coalesce(f.col("fim_de_suprimento__c"), f.lit(None)).alias("fimdeSuprimento_oportunidade"),
    f.coalesce(f.col("fiscal"), f.lit(None)).alias("fiscal_oportunidade"),
    f.coalesce(f.col("fiscalquarter"), f.lit(None)).alias("fiscalTrimestre_oportunidade"),
    f.coalesce(f.col("fiscalyear"), f.lit(None)).alias("fiscalAno_oportunidade"),
    f.coalesce(f.col("forecastcategory"), f.lit(None)).alias("categoriaPrevisao_oportunidade"),
    f.coalesce(f.col("forecastcategoryname"), f.lit(None)).alias("nomeCategoriaPrevisao_oportunidade"),
    f.coalesce(f.col("hasopenactivity"), f.lit(None)).alias("tematividadeAberta_oportunidade"),
    f.coalesce(f.col("hasopportunitylineitem"), f.lit(None)).alias("temLinhadeOportunidade_oportunidade"),
    f.coalesce(f.col("hasoverduetask"), f.lit(None)).alias("temTarefaVencida_oportunidade"),
    f.coalesce(f.col("iar__c"), f.lit(None)).alias("iar_oportunidade"),
    f.coalesce(f.col("id"), f.lit(None)).alias("idOportunidade_oportunidade"),
    f.coalesce(f.col("inicio_de_suprimento__c"), f.lit(None)).alias("inicioSuprimento_oportunidade"),
    f.coalesce(f.col("isclosed"), f.lit(None)).alias("foiFechado_oportunidade"),
    f.coalesce(f.col("isdeleted"), f.lit(None)).alias("foiDeletado_oportunidade"),
    f.coalesce(f.col("ispriorityrecord"), f.lit(None)).alias("registroPrioritario_oportunidade"),
    f.coalesce(f.col("iswon"), f.lit(None)).alias("foiGanho_oportunidade"),
    f.coalesce(f.col("lastactivitydate"), f.lit(None)).alias("dataultimaAtividade_oportunidade"),
    f.coalesce(f.col("lastactivityindays"), f.lit(None)).alias("ultimaAtividadeEmDias_oportunidade"),
    f.coalesce(f.col("lastamountchangedhistoryid"), f.lit(None)).alias("UltimaAlteracaoValorHistoricoID_oportunidade"),
    f.coalesce(f.col("lastclosedatechangedhistoryid"), f.lit(None)).alias("ulimtaDatafechamentoAlteracaohistoricoID_oportunidade"),
    f.coalesce(f.col("lastmodifiedbyid"), f.lit(None)).alias("ultimamodificacaopor_oportunidade"),
    f.coalesce(f.col("lastmodifieddate"), f.lit(None)).alias("dataultimamodificacao_oportunidade"),
    f.coalesce(f.col("lastreferenceddate"), f.lit(None)).alias("datadaultimareferencia_oportunidade"),
    f.coalesce(f.col("laststagechangedate"), f.lit(None)).alias("datadoultimoestagiodealteracao_oportunidade"),
    f.coalesce(f.col("laststagechangeindays"), f.lit(None)).alias("ultimaestagiodealteracaoemdias_oportunidade"),
    f.coalesce(f.col("lastvieweddate"), f.lit(None)).alias("dataultimavizualizacao_oportunidade"),
    f.coalesce(f.col("leadsource"), f.lit(None)).alias("origemdolead_oportunidade"),
    f.coalesce(f.col("loss_reason__c"), f.lit(None)).alias("motivoDaPerda_oportunidade"),
    f.coalesce(f.col("maiordatadeentradanafase__c"), f.lit(None)).alias("maiorDatadeEntradanaFase_oportunidade"),
    f.coalesce(f.col("menordatadeentradanafase__c"), f.lit(None)).alias("menorDatadeEntradanaFase_oportunidade"),
    f.coalesce(f.col("motivo_de_perda__c"), f.lit(None)).alias("motivoDePerda_oportunidade"),
    f.coalesce(f.col("name"), f.lit(None)).alias("nomeOportunidade_oportunidade"),
    f.coalesce(f.col("nextstep"), f.lit(None)).alias("proximoPasso_oportunidade"),
    f.coalesce(f.col("nivel_de_desconto_maximo__c"), f.lit(None)).alias("nivelDescontoMaximo_oportunidade"),
    f.coalesce(f.col("numero_restricao__c"), f.lit(None)).alias("numeroRestricao_oportunidade"),
    f.coalesce(f.col("numeroproposta__c"), f.lit(None)).alias("numeroProposta_oportunidade"),
    f.coalesce(f.col("observacoes_gerais__c"), f.lit(None)).alias("observacoes_oportunidade"),
    f.coalesce(f.col("ownerid"), f.lit(None)).alias("proprietario_oportunidade"),
    f.coalesce(f.col("parceiro__c"), f.lit(None)).alias("parceiro_oportunidade"),
    f.coalesce(f.col("porcentagem_score__c"), f.lit(None)).alias("porcentagemScore_oportunidade"),
    f.coalesce(f.col("previsao_de_ganho__c"), f.lit(None)).alias("previsaodeGanho_oportunidade"),
    f.coalesce(f.col("pricebook2id"), f.lit(None)).alias("idLivropreco_oportunidade"),
    f.coalesce(f.col("probability"), f.lit(None)).alias("probabilidade_oportunidade"),
    f.coalesce(f.col("pushcount"), f.lit(None)).alias("qntadiamentodeOprotunidade_oportunidade"),
    f.coalesce(f.col("qualificacao__c"), f.lit(None)).alias("qualificacao_oportunidade"),
    f.coalesce(f.col("quantidade_ucs_proposta__c"), f.lit(None)).alias("quantidadeUcsProposta_oportunidade"),
    f.coalesce(f.col("rating_de_risco__c"), f.lit(None)).alias("ratingDeRisco_oportunidade"),
    f.coalesce(f.col("recordtypeid"), f.lit(None)).alias("idTipoRegistro_oportunidade"),
    f.coalesce(f.col("regional__c"), f.lit(None)).alias("regional_oportunidade"),
    f.coalesce(f.col("roi_analysis_completed__c"), f.lit(None)).alias("completouAnaliseROI_oportunidade"),
    f.coalesce(f.col("situacaomigracao__c"), f.lit(None)).alias("situacaomigracao_oportunidade"),
    f.coalesce(f.col("sla__c"), f.lit(None)).alias("sla_oportunidade"),
    f.coalesce(f.col("stagename"), f.lit(None)).alias("nomeEstagio_oportunidade"),
    f.coalesce(f.col("submercado__c"), f.lit(None)).alias("submercado_oportunidade"),
    f.coalesce(f.col("syncedquoteid"), f.lit(None)).alias("idCotacaoDesde_oportunidade"),
    f.coalesce(f.col("tempoelaboracaodaproposta__c"), f.lit(None)).alias("tempoElaboracaodaProposta_oportunidade"),
    f.coalesce(f.col("tempopropostaassinada__c"), f.lit(None)).alias("tempoPropostaAssinada_oportunidade"),
    f.coalesce(f.col("tipo_cliente__c"), f.lit(None)).alias("tipoCliente_oportunidade"),
    f.coalesce(f.col("totaldeoportunidades__c"), f.lit(None)).alias("totaldeOportunidades_oportunidade"),
    f.coalesce(f.col("totaldeoppganhas__c"), f.lit(None)).alias("totaldeOportunidadesGanhas_oportunidade"),
    f.coalesce(f.col("type"), f.lit(None)).alias("Tipooportunidade_oportunidade"),
    f.coalesce(f.col("volume_mwm__c"), f.lit(None)).alias("volumeMwm_oportunidade"),
    f.coalesce(f.col("volume_total_mwm__c"), f.lit(None)).alias("volumeTotalMwm_oportunidade"),
    f.col("datahoraPreProcessamento").alias("datahoraProcessamento")
    )
print("Finalizado Dataframe oportunidade")

print("Executando juncao dos df Cotacao e oportunidade")
# join dos dataframes de origem 
df_join = df_oportunidade.join(
    df_cotacao, \
    f.col("idOportunidade_cotacao") == f.col("idOportunidade_oportunidade"), \
    "fullouter"
    )
print("Finalizado juncao dos DFs")

print("Iniciando a limpeza do Dataframe final")
df = df_join.select(
    f.concat(\
            f.coalesce(f.col("idcotacao_cotacao"),f.lit("semCotacao")),\
            f.lit("_"), \
            f.coalesce(f.col("idOportunidade_oportunidade"),f.lit("semOportunidade"))\
            )\
            .alias("idFatoOportunidadeXCotacao"),
    f.coalesce(f.col("idconta_oportunidade"), f.lit("N/A")).alias("idconta_oportunidade"),
    f.coalesce(f.col("idadeEmDias_oportunidade"), f.lit(None)).cast("integer").alias("idadeEmDias_oportunidade"),
    f.coalesce(f.col("valor_oportunidade"), f.lit(None)).cast("float").alias("valor_oportunidade"),
    f.coalesce(f.col("boleta_oportunidade"), f.lit("N/A")).alias("boleta_oportunidade"),
    f.when(f.col("orcamentoConfirmado_oportunidade") == "true", "SIM")
     .when(f.col("orcamentoConfirmado_oportunidade") == "false", "NÃO")
     .otherwise("N/A").alias("orcamentoConfirmado_oportunidade"),
    f.coalesce(f.col("idcampanha_oportunidade"), f.lit("N/A")).alias("idcampanha_oportunidade"),
    f.coalesce(f.col("classificacao_oportunidade"), f.lit("N/A")).alias("classificacao_oportunidade"),
    f.coalesce(f.col("classificacaodoprodutopotencial_oportunidade"), f.lit("N/A")).alias("classificacaodoprodutopotencial_oportunidade"),
    f.coalesce(f.col("datafechamento_oportunidade"), f.lit("N/A")).alias("datafechamento_oportunidade"),
    f.coalesce(f.col("cnpj_oportunidade"), f.lit("N/A")).alias("cnpj_oportunidade"),
    f.coalesce(f.col("consumoforapontakwh_oportunidade"), f.lit(None)).cast("float").alias("consumoforapontakwh_oportunidade"),
    f.coalesce(f.col("consumomwm_oportunidade"), f.lit(None)).cast("float").alias("consumomwm_oportunidade"),
    f.coalesce(f.col("consumopontakwh_oportunidade"), f.lit(None)).cast("float").alias("consumopontakwh_oportunidade"),
    f.coalesce(f.col("consumototalkwh_oportunidade"), f.lit(None)).cast("float").alias("consumototalkwh_oportunidade"),
    f.coalesce(f.col("idcontato_oportunidade"), f.lit("N/A")).alias("idcontato_oportunidade"),
    f.coalesce(f.col("idcontrato_oportunidade"), f.lit("N/A")).alias("idcontrato_oportunidade"),
    f.coalesce(f.col("anosContrato_oportunidade"), f.lit(None)).alias("anosContrato_oportunidade"),
    f.coalesce(f.col("formulaAnosContrato_oportunidade"), f.lit(None)).alias("formulaAnosContrato_oportunidade"),
    f.coalesce(f.col("restricaoCor_oportunidade"), f.lit("N/A")).alias("restricaoCor_oportunidade"),
    f.coalesce(f.col("corscore_oportunidade"), f.lit("N/A")).alias("corscore_oportunidade"),
    f.when(f.col("cotacaoInicialCriada_oportunidade") == "true", "SIM")
     .when(f.col("cotacaoInicialCriada_oportunidade") == "false", "NÃO")
     .otherwise("N/A").alias("cotacaoInicialCriada_oportunidade"),
    f.coalesce(f.col("cpf_oportunidade"), f.lit("N/A")).alias("cpf_oportunidade"),
    f.coalesce(f.col("datacriacao_oportunidade"), f.lit("N/A")).alias("datacriacao_oportunidade"),
    f.coalesce(f.col("datavigenciaContrato_oportunidade"), f.lit("N/A")).alias("datavigenciaContrato_oportunidade"),
    f.coalesce(f.col("dataAssinaturaContrato_oportunidade"), f.lit("N/A")).alias("dataAssinaturaContrato_oportunidade"),
    f.coalesce(f.col("dataCriacaoProposta_oportunidade"), f.lit("N/A")).alias("dataCriacaoProposta_oportunidade"),
    f.coalesce(f.col("dataFechadoeGanho_oportunidade"), f.lit("N/A")).alias("dataFechadoeGanho_oportunidade"),
    f.coalesce(f.col("dataFinalizacaoCriacaoProposta_oportunidade"), f.lit("N/A")).alias("dataFinalizacaoCriacaoProposta_oportunidade"),
    f.coalesce(f.col("dataultimaIntegracaoRisco_oportunidade"), f.lit("N/A")).alias("dataultimaIntegracaoRisco_oportunidade"),
    f.coalesce(f.col("demandaforapontaKw_oportunidade"), f.lit(None)).cast("float").alias("demandaforapontaKw_oportunidade"),
    f.coalesce(f.col("demandaPonta_oportunidade"), f.lit(None)).cast("float").alias("demandaPonta_oportunidade"),
    f.coalesce(f.col("demandaTotal_oportunidade"), f.lit(None)).cast("float").alias("demandaTotal_oportunidade"),
    f.coalesce(f.col("descricao_oportunidade"), f.lit("N/A")).alias("descricao_oportunidade"),
    f.when(f.col("completouDiscovey_oportunidade") == "true", "SIM")
     .when(f.col("completouDiscovey_oportunidade") == "false", "NÃO")
     .otherwise("N/A").alias("completouDiscovey_oportunidade"),
    f.coalesce(f.col("distribuidora_oportunidade"), f.lit("N/A")).alias("iddistribuidora_oportunidade"),
    f.coalesce(f.col("duracaocriacaoPropostaemDias_oportunidade"), f.lit(None)).cast("float").alias("duracaocriacaoPropostaemDias_oportunidade"),
    f.when(f.col("elegivelMotordePreco_oportunidade") == "true", "SIM")
     .when(f.col("elegivelMotordePreco_oportunidade") == "false", "NÃO")
     .otherwise("N/A").alias("elegivelMotordePreco_oportunidade"),
    f.coalesce(f.col("etapa_oportunidade"), f.lit("N/A")).alias("etapa_oportunidade"),
    f.when(f.col("faturaEnergiaAnexada_oportunidade") == "true", "SIM")
     .when(f.col("faturaEnergiaAnexada_oportunidade") == "false", "NÃO")
     .otherwise("N/A").alias("faturaEnergiaAnexada_oportunidade"),
    f.coalesce(f.col("fimdeSuprimento_oportunidade"), f.lit("N/A")).alias("fimdeSuprimento_oportunidade"),
    f.coalesce(f.col("fiscal_oportunidade"), f.lit("N/A")).alias("datafiscal_oportunidade"),
    f.coalesce(f.col("fiscalTrimestre_oportunidade"), f.lit(None)).cast("integer").alias("fiscalTrimestre_oportunidade"),
    f.coalesce(f.col("fiscalAno_oportunidade"), f.lit(None)).cast("integer").alias("fiscalAno_oportunidade"),
    f.coalesce(f.col("categoriaPrevisao_oportunidade"), f.lit("N/A")).alias("categoriaPrevisao_oportunidade"),
    f.coalesce(f.col("nomeCategoriaPrevisao_oportunidade"), f.lit("N/A")).alias("nomeCategoriaPrevisao_oportunidade"),
    f.when(f.col("tematividadeAberta_oportunidade") == "true", "SIM")
     .when(f.col("tematividadeAberta_oportunidade") == "false", "NÃO")
     .otherwise("N/A").alias("tematividadeAberta_oportunidade"),
    f.when(f.col("temLinhadeOportunidade_oportunidade") == "true", "SIM")
     .when(f.col("temLinhadeOportunidade_oportunidade") == "false", "NÃO")
     .otherwise("N/A").alias("temLinhadeOportunidade_oportunidade"),
    f.when(f.col("temTarefaVencida_oportunidade") == "true", "SIM")
     .when(f.col("temTarefaVencida_oportunidade") == "false", "NÃO")
     .otherwise("N/A").alias("temTarefaVencida_oportunidade"),
    f.coalesce(f.col("iar_oportunidade"), f.lit(None)).alias("iar_oportunidade"),
    f.coalesce(f.col("idOportunidade_oportunidade"), f.lit("N/A")).alias("idOportunidade"),
    f.coalesce(f.col("inicioSuprimento_oportunidade"), f.lit("N/A")).alias("inicioSuprimento_oportunidade"),
    f.when(f.col("foiFechado_oportunidade") == "true", "SIM")
     .when(f.col("foiFechado_oportunidade") == "false", "NÃO")
     .otherwise("N/A").alias("foiFechado_oportunidade"),
    f.when(f.col("foiDeletado_oportunidade") == "true", "SIM")
     .when(f.col("foiDeletado_oportunidade") == "false", "NÃO")
     .otherwise("N/A").alias("foiDeletado_oportunidade"),
    f.when(f.col("registroPrioritario_oportunidade") == "true", "SIM")
     .when(f.col("registroPrioritario_oportunidade") == "false", "NÃO")
     .otherwise("N/A").alias("registroPrioritario_oportunidade"),
    f.when(f.col("foiGanho_oportunidade") == "true", "SIM")
     .when(f.col("foiGanho_oportunidade") == "false", "NÃO")
     .otherwise("N/A").alias("foiGanho_oportunidade"),
    f.coalesce(f.col("dataultimaAtividade_oportunidade"), f.lit("N/A")).alias("dataultimaAtividade_oportunidade"),
    f.coalesce(f.col("ultimaAtividadeEmDias_oportunidade"), f.lit(None)).cast("float").alias("ultimaAtividadeEmDias_oportunidade"),
    f.coalesce(f.col("UltimaAlteracaoValorHistoricoID_oportunidade"), f.lit("N/A")).alias("UltimaAlteracaoValorHistoricoID_oportunidade"),
    f.coalesce(f.col("ulimtaDatafechamentoAlteracaohistoricoID_oportunidade"), f.lit("N/A")).alias("ulimtaDatafechamentoAlteracaohistoricoID_oportunidade"),
    f.coalesce(f.col("datadoultimoestagiodealteracao_oportunidade"), f.lit("N/A")).alias("datadoultimoestagiodealteracao_oportunidade"),
    f.coalesce(f.col("ultimaestagiodealteracaoemdias_oportunidade"), f.lit(None)).cast("integer").alias("ultimaestagiodealteracaoemdias_oportunidade"),
    f.coalesce(f.col("origemdolead_oportunidade"), f.lit("N/A")).alias("origemdolead_oportunidade"),
    f.coalesce(f.col("motivoDaPerda_oportunidade"), f.lit("N/A")).alias("motivoDaPerda_oportunidade"),
    f.coalesce(f.col("maiorDatadeEntradanaFase_oportunidade"), f.lit("N/A")).alias("maiorDatadeEntradanaFase_oportunidade"),
    f.coalesce(f.col("menorDatadeEntradanaFase_oportunidade"), f.lit("N/A")).alias("menorDatadeEntradanaFase_oportunidade"),
    f.coalesce(f.col("motivoDePerda_oportunidade"), f.lit("N/A")).alias("motivoDePerda_oportunidade"),
    f.coalesce(f.col("nomeOportunidade_oportunidade"), f.lit("N/A")).alias("nomeOportunidade"),
    f.coalesce(f.col("proximoPasso_oportunidade"), f.lit("N/A")).alias("proximoPasso_oportunidade"),
    f.coalesce(f.col("nivelDescontoMaximo_oportunidade"), f.lit("N/A")).alias("nivelDescontoMaximo_oportunidade"),
    f.coalesce(f.col("numeroRestricao_oportunidade"), f.lit(None)).cast("integer").alias("numeroRestricao_oportunidade"),
    f.coalesce(f.col("numeroProposta_oportunidade"), f.lit("N/A")).alias("numeroProposta_oportunidade"),
    f.coalesce(f.col("observacoes_oportunidade"), f.lit("N/A")).alias("observacoes_oportunidade"),
    f.coalesce(f.col("parceiro_oportunidade"), f.lit("N/A")).alias("parceiro_oportunidade"),
    f.coalesce(f.col("porcentagemScore_oportunidade"), f.lit(None)).cast("float").alias("porcentagemScore_oportunidade"),
    f.coalesce(f.col("previsaodeGanho_oportunidade"), f.lit("N/A")).alias("previsaodeGanho_oportunidade"),
    f.coalesce(f.col("idLivropreco_oportunidade"), f.lit("N/A")).alias("idLivropreco_oportunidade"),
    f.coalesce(f.col("probabilidade_oportunidade"), f.lit(None)).cast("integer").alias("probabilidade_oportunidade"),
    f.coalesce(f.col("qntadiamentodeOprotunidade_oportunidade"), f.lit(None)).cast("integer").alias("qntadiamento_oportunidade"),
    f.coalesce(f.col("qualificacao_oportunidade"), f.lit("N/A")).alias("qualificacao_oportunidade"),
    f.coalesce(f.col("quantidadeUcsProposta_oportunidade"), f.lit(None)).cast("integer").alias("quantidadeUcsProposta_oportunidade"),
    f.coalesce(f.col("ratingDeRisco_oportunidade"), f.lit("N/A")).alias("ratingDeRisco_oportunidade"),
    f.coalesce(f.col("regional_oportunidade"), f.lit("N/A")).alias("regional_oportunidade"),
    f.when(f.col("completouAnaliseROI_oportunidade") == "true", "SIM")
     .when(f.col("completouAnaliseROI_oportunidade") == "false", "NÃO")
     .otherwise("N/A").alias("completouAnaliseROI_oportunidade"),
    f.coalesce(f.col("situacaomigracao_oportunidade"), f.lit("N/A")).alias("situacaomigracao_oportunidade"),
    f.coalesce(f.col("sla_oportunidade"), f.lit(None)).cast("integer").alias("sla_oportunidade"),
    f.coalesce(f.col("nomeEstagio_oportunidade"), f.lit("N/A")).alias("nomeEstagio_oportunidade"),
    f.coalesce(f.col("submercado_oportunidade"), f.lit("N/A")).alias("submercado_oportunidade"),
    f.coalesce(f.col("tempoElaboracaodaProposta_oportunidade"), f.lit(None)).cast("float").alias("tempoElaboracaodaProposta_oportunidade"),
    f.coalesce(f.col("tempoPropostaAssinada_oportunidade"), f.lit(None)).cast("float").alias("tempoPropostaAssinada_oportunidade"),
    f.coalesce(f.col("tipoCliente_oportunidade"), f.lit("N/A")).alias("tipoCliente_oportunidade"),
    f.coalesce(f.col("totaldeOportunidades_oportunidade"), f.lit(None)).cast("float").alias("totaldeOportunidades"),
    f.coalesce(f.col("totaldeOportunidadesGanhas_oportunidade"), f.lit(None)).cast("float").alias("totaldeOportunidadesGanhas"),
    f.coalesce(f.col("Tipooportunidade_oportunidade"), f.lit("N/A")).alias("Tipooportunidade_oportunidade"),
    f.coalesce(f.col("volumeMwm_oportunidade"), f.lit(None)).cast("float").alias("volumeMwm_oportunidade"),
    f.coalesce(f.col("volumeTotalMwm_oportunidade"), f.lit(None)).cast("float").alias("volumeTotalMwm_oportunidade"),
    f.coalesce(f.col("idconta_cotacao"), f.lit("N/A")).alias("idconta_cotacao"),
    f.coalesce(f.col("cidadeadicional_cotacao"), f.lit("N/A")).alias("cidadeadicional_cotacao"),
    f.coalesce(f.col("paisadicional_cotacao"), f.lit("N/A")).alias("paisadicional_cotacao"),
    f.coalesce(f.col("nomeadicional_cotacao"), f.lit("N/A")).alias("nomeadicional_cotacao"),
    f.coalesce(f.col("cepadicional_cotacao"), f.lit("N/A")).alias("cepadicional_cotacao"),
    f.coalesce(f.col("estadoadicional_cotacao"), f.lit("N/A")).alias("estadoadicional_cotacao"),
    f.coalesce(f.col("enderecoadicional_cotacao"), f.lit("N/A")).alias("enderecoadicional_cotacao"),
    f.coalesce(f.col("alcadadeaprovacao_cotacao"), f.lit("N/A")).alias("alcadadeaprovacao_cotacao"),
    f.coalesce(f.col("cidadecobranca_cotacao"), f.lit("N/A")).alias("cidadecobranca_cotacao"),
    f.coalesce(f.col("paiscobranca_cotacao"), f.lit("N/A")).alias("paiscobranca_cotacao"),
    f.coalesce(f.col("nomecobranca_cotacao"), f.lit("N/A")).alias("nomecobranca_cotacao"),
    f.coalesce(f.col("cepcobranca_cotacao"), f.lit("N/A")).alias("cepcobranca_cotacao"),
    f.coalesce(f.col("estadocobranca_cotacao"), f.lit("N/A")).alias("estadocobranca_cotacao"),
    f.coalesce(f.col("enderecocobranca_cotacao"), f.lit("N/A")).alias("enderecocobranca_cotacao"),
    f.when(f.col("clientevip_cotacao") == "true", "SIM")
     .when(f.col("clientevip_cotacao") == "false", "NÃO")
     .otherwise("N/A").alias("clientevip_cotacao"),
    f.coalesce(f.col("consumoforapontakwh_cotacao"), f.lit(None)).cast("float").alias("consumoforapontakwh_cotacao"),
    f.coalesce(f.col("consumopontakwh_cotacao"), f.lit(None)).cast("float").alias("consumopontakwh_cotacao"),
    f.coalesce(f.col("idcontato_cotacao"), f.lit("N/A")).alias("idcontato_cotacao"),
    f.coalesce(f.col("idcontrato_cotacao"), f.lit("N/A")).alias("idcontrato_cotacao"),
    f.coalesce(f.col("datacriacao_cotacao"), f.lit("N/A")).alias("datacriacao_cotacao"),
    f.coalesce(f.col("custoadequacao_cotacao"), f.lit(None)).cast("float").alias("custoadequacao_cotacao"),
    f.coalesce(f.col("decontoofertado_cotacao"), f.lit(None)).cast("float").alias("decontoofertado_cotacao"),
    f.coalesce(f.col("demandaforapontakw_cotacao"), f.lit(None)).cast("float").alias("demandaforapontakw_cotacao"),
    f.coalesce(f.col("demandapontakw_cotacao"), f.lit(None)).cast("float").alias("demandapontakw_cotacao"),
    f.coalesce(f.col("descontomaxcomercial_cotacao"), f.lit(None)).cast("float").alias("descontomaxcomercial_cotacao"),
    f.coalesce(f.col("descontomaxdevidorisco_cotacao"), f.lit(None)).cast("float").alias("descontomaxdevidorisco_cotacao"),
    f.coalesce(f.col("descricao_cotacao"), f.lit("N/A")).alias("descricao_cotacao"),
    f.coalesce(f.col("desconto_cotacao"), f.lit("N/A")).alias("desconto_cotacao"),
    f.coalesce(f.col("email_cotacao"), f.lit("N/A")).alias("email_cotacao"),
    f.coalesce(f.col("emailcontato_cotacao"), f.lit("N/A")).alias("emailcontato_cotacao"),
    f.coalesce(f.col("dataValidade_cotacao"), f.lit("N/A")).alias("dataValidade_cotacao"),
    f.coalesce(f.col("fax_cotacao"), f.lit("N/A")).alias("fax_cotacao"),
    f.coalesce(f.col("flex_cotacao"), f.lit("N/A")).alias("flex_cotacao"),
    f.coalesce(f.col("flexporcentagem_cotacao"), f.lit(None)).cast("float").alias("flexporcentagem_cotacao"),
    f.coalesce(f.col("garantiadocomprador_cotacao"), f.lit("N/A")).alias("garantiadocomprador_cotacao"),
    f.coalesce(f.col("totalGeral_cotacao"), f.lit(None)).cast("float").alias("totalGeral_cotacao"),
    f.coalesce(f.col("idcotacao_cotacao"), f.lit("N/A")).alias("idcotacao_cotacao"),
    f.coalesce(f.col("indicereajustedatabase_cotacao"), f.lit("N/A")).alias("indicereajustedatabase_cotacao"),
    f.when(f.col("foideletado_cotacao") == "true", "SIM")
     .when(f.col("foideletado_cotacao") == "false", "NÃO")
     .otherwise("N/A").alias("foideletado_cotacao"),
    f.when(f.col("foisincronizado_cotacao") == "true", "SIM")
     .when(f.col("foisincronizado_cotacao") == "false", "NÃO")
     .otherwise("N/A").alias("foisincronizado_cotacao"),
    f.coalesce(f.col("countagemdeitens_cotacao"), f.lit(None)).cast("float").alias("countagemdeitens_cotacao"),
    f.coalesce(f.col("modalidadenegocio_cotacao"), f.lit("N/A")).alias("modalidadenegocio_cotacao"),
    f.coalesce(f.col("modulacao_cotacao"), f.lit("N/A")).alias("modulacao_cotacao"),
    f.coalesce(f.col("multaantecipacao_cotacao"), f.lit(None)).cast("float").alias("multaantecipacao_cotacao"),
    f.coalesce(f.col("nomecotacao_cotacao"), f.lit("N/A")).alias("nomecotacao_cotacao"),
    f.coalesce(f.col("ofertaprecofixoaplicavel_cotacao"), f.lit("N/A")).alias("ofertaprecofixoaplicavel_cotacao"),
    f.coalesce(f.col("telefone_cotacao"), f.lit("N/A")).alias("telefone_cotacao"),
    f.coalesce(f.col("precomediovenda_cotacao"), f.lit(None)).cast("float").alias("precomediovenda_cotacao"),
    f.coalesce(f.col("precominimoenergia_cotacao"), f.lit(None)).cast("float").alias("precominimoenergia_cotacao"),
    f.coalesce(f.col("precoponderado_cotacao"), f.lit(None)).cast("float").alias("precoponderado_cotacao"),
    f.coalesce(f.col("livrodepreco_cotacao"), f.lit("N/A")).alias("livrodepreco_cotacao"),
    f.coalesce(f.col("produtopontencial_cotacao"), f.lit("N/A")).alias("produtopontencial_cotacao"),
    f.coalesce(f.col("numerocotacao_cotacao"), f.lit(None)).cast("float").alias("numero_cotacao"),
    f.coalesce(f.col("cidadecotacao_cotacao"), f.lit("N/A")).alias("cidadecotacao_cotacao"),
    f.coalesce(f.col("paiscotacao_cotacao"), f.lit("N/A")).alias("paiscotacao_cotacao"),
    f.coalesce(f.col("nomeparacotacao_cotacao"), f.lit("N/A")).alias("nomeparacotacao"),
    f.coalesce(f.col("cepcotacao_cotacao"), f.lit("N/A")).alias("cepcotacao_cotacao"),
    f.coalesce(f.col("estadocotacao_cotacao"), f.lit("N/A")).alias("estadocotacao_cotacao"),
    f.coalesce(f.col("enderecoparacotacao_cotacao"), f.lit("N/A")).alias("enderecoparacotacao_cotacao"),
    f.coalesce(f.col("idTipoRegistro_cotacao"), f.lit("N/A")).alias("idTipoRegistro_cotacao"),
    f.coalesce(f.col("sazonalidade_cotacao"), f.lit("N/A")).alias("sazonalidade_cotacao"),
    f.coalesce(f.col("sazonalidadeporcentagem_cotacao"), f.lit(None)).alias("sazonalidadeporcentagem_cotacao"),
    f.coalesce(f.col("cidadefornecimento_cotacao"), f.lit("N/A")).alias("cidadefornecimento_cotacao"),
    f.coalesce(f.col("paisfornecimento_cotacao"), f.lit("N/A")).alias("paisfornecimento_cotacao"),
    f.coalesce(f.col("manuseiofornecimento_cotacao"), f.lit("N/A")).alias("manuseiofornecimento_cotacao"),
    f.coalesce(f.col("nomeparafornecimento_cotacao"), f.lit("N/A")).alias("nomeparafornecimento_cotacao"),
    f.coalesce(f.col("cepfornecimento_cotacao"), f.lit("N/A")).alias("cepfornecimento_cotacao"),
    f.coalesce(f.col("estadoforncecimento_cotacao"), f.lit("N/A")).alias("estadoforncecimento_cotacao"),
    f.coalesce(f.col("enderecofornecimento_cotacao"), f.lit("N/A")).alias("enderecofornecimento_cotacao"),
    f.coalesce(f.col("situacao_cotacao"), f.lit("N/A")).alias("situacao_cotacao"),
    f.coalesce(f.col("statuscotacao_cotacao"), f.lit("N/A")).alias("statuscotacao_cotacao"),
    f.coalesce(f.col("submercado_cotacao"), f.lit("N/A")).alias("submercado_cotacao"),
    f.coalesce(f.col("subtotal_cotacao"), f.lit(None)).cast("integer").alias("subtotal_cotacao"),
    f.coalesce(f.col("suprimento_cotacao"), f.lit("N/A")).alias("suprimento_cotacao"),
    f.coalesce(f.col("imposto_cotacao"), f.lit(None)).cast("float").alias("imposto_cotacao"),
    f.coalesce(f.col("telefonecontato_cotacao"), f.lit("N/A")).alias("telefonecontato_cotacao"),
    f.coalesce(f.col("tipocliente_cotacao"), f.lit("N/A")).alias("tipocliente_cotacao"),
    f.coalesce(f.col("tipocotacao_cotacao"), f.lit("N/A")).alias("tipocotacao_cotacao"),
    f.coalesce(f.col("tipoproposta_cotacao"), f.lit("N/A")).alias("tipoproposta_cotacao"),
    f.coalesce(f.col("precototal_cotacao"), f.lit(None)).cast("float").alias("precototal_cotacao"),
    f.coalesce(f.col("usinaaserofertada_cotacao"), f.lit("N/A")).alias("usinaaserofertada_cotacao"),
    f.coalesce(f.col("volumemwmdeenergia_cotacao"), f.lit(None)).cast("float").alias("volumemwmdeenergia_cotacao"),
    f.col("datahoraProcessamento").cast("timestamp").alias("datahoraProcessamento")
)
print("Finalizado padronizacao do DF final")

total_registros_cotacao = df_cotacao.count()
total_registros_oportunidade = df_oportunidade.count()
total_registros_join = df_join.count()
total_registros_final = df.count()

print(f"Total registros em df_cotacao: {total_registros_cotacao}")
print(f"Total registros em df_oportunidade: {total_registros_oportunidade}")
print(f"Total registros após o join: {total_registros_join}")
print(f"Total registros gravados: {total_registros_final}")

print("Inicinando a gravação do Dataframe final no S3")
df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet("s3://refined-zone-echo/fato_oportunidade_x_cotacao/")
print("Fechando a gravação do Dataframe final no S3")

print("Encerrado a geração da tabela Fato")

job.commit()