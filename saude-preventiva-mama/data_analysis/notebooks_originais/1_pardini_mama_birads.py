# Databricks notebook source
# MAGIC %md
# MAGIC # Extração de dado - BI_RADS
# MAGIC - Extrair BI-RADIS 0,1,2,3,4,5,6 e sem birads.
# MAGIC - Lista de ativação considera apenas BI_RADS 1, 2 e 3 e tem com objetivo identificar pacientes com exames em atraso e enviar notificação.
# MAGIC

# COMMAND ----------

# MAGIC %pip install octoops

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import col, year, month, dayofmonth, when, lit, expr, to_timestamp
from pyspark.sql.types import DateType
from pyspark.sql import DataFrame
from pyspark.sql.functions import datediff, to_date
from datetime import datetime
import logging
import sys
import traceback
from octoops import OctoOps
from octoops import Sentinel

# COMMAND ----------

logger = logging.getLogger(__name__)

# COMMAND ----------

table_birads = "refined.saude_preventiva.pardini_laudos_mama_birads"
table_birads_ativacao = "refined.saude_preventiva.pardini_laudos_mama_birads_ativacao"
where_clause = ""
 
if spark.catalog.tableExists(table_birads):
    where_clause = f"""
    WHERE        
        flr._datestamp > (
            SELECT MAX(brd._datestamp)
            FROM {table_birads} brd
        )
        
    """
 
filtro_ativacao = """
    WHERE
        eleg.ficha IS NULL
        AND brd.BIRADS IN (1, 2, 3)
        AND flr.sigla_exame IN ('MAMODI','MAMO')
        AND UPPER(flr.sexo_cliente) = 'F'
        AND (
            idade_cliente >= 40 AND idade_cliente < 76
        )
"""

# COMMAND ----------

query = """
WITH base AS (
    SELECT
        flr.ficha,
        flr.sigla_exame, 
        flr.id_marca,
        flr.sequencial,
        flr.laudo_tratado,
        REGEXP_EXTRACT_ALL(
            REGEXP_EXTRACT(
                REGEXP_REPLACE(UPPER(flr.laudo_tratado), r'[-:®]|\xa0', ''),
                r'(?mi)(AVALIA[CÇ][AÃ]O|CONCLUS[AÃ]O|IMPRESS[AÃ]O|OPINI[AÃ]O)?(.*)', 2
            ),
            r"(?mi)(BIRADS|CATEGORI[AO]|CATEGORA|CATEGORIA R|CAT\W)\s*(\d+\w*|VI|V|IV|III|II|I)\W*\w?(BIRADS|CATEGORI[AO]|CATEGORA|CATEGORIA R)?(\W|$)", 2
        )        
        AS RAW_BIRADS,
        FILTER(
            TRANSFORM(RAW_BIRADS, x ->
                CASE
                    WHEN x = "I" THEN 1
                    WHEN x = "II" THEN 2
                    WHEN x = "III" THEN 3
                    WHEN x = "IV" THEN 4
                    WHEN x = "V" THEN 5
                    WHEN x = "VI" THEN 6
                    WHEN TRY_CAST(x AS INT) > 6 THEN NULL
                    ELSE REGEXP_REPLACE(x, r'[^0-9]', '')
                END
            ), x -> x IS NOT NULL
        ) AS CAT_BIRADS
    FROM refined.saude_preventiva.pardini_laudos flr
    WHERE
        flr.linha_cuidado = 'mama'
        AND UPPER(flr.sexo_cliente) = 'F' 
        AND flr.sigla_exame IN ('MAMO','MAMODI')
),
 
dados_birads AS (
    SELECT
        *,
        ARRAY_MIN(CAT_BIRADS) AS MIN_BIRADS,
        ARRAY_MAX(CAT_BIRADS) AS MAX_BIRADS,
        TRY_ELEMENT_AT(CAST(CAT_BIRADS AS ARRAY<INT>), -1) AS BIRADS
    FROM base
),
 
dados_laudos AS (
    SELECT
        flr.linha_cuidado,
        flr.id_unidade,
        flr.id_ficha,
        flr.id_exame, 
        flr.id_marca,
        flr.sequencial,
        flr.ficha,
        flr.id_cliente,
        flr.pefi_cliente,
        flr.sigla_exame,
        flr.marca,
        flr.laudo_tratado,
        (
          TIMESTAMPDIFF(DAY, flr.dth_nascimento_cliente, CURDATE()) / 365.25
        ) AS idade_cliente,
        flr.sexo_cliente,
        flr.dth_pedido,
        flr._datestamp
    FROM refined.saude_preventiva.pardini_laudos flr
    {where_clause}
)
 
SELECT
    flr.* except(idade_cliente),
    brd.MIN_BIRADS,
    brd.MAX_BIRADS,
    brd.BIRADS,
 
    eleg.dth_pedido        AS dth_pedido_retorno_elegivel,
    eleg.ficha             AS ficha_retorno_elegivel,
    eleg.siglas_ficha      AS siglas_ficha_retorno_elegivel,
    eleg.marca             AS marca_retorno_elegivel,
    eleg.unidade           AS unidade_retorno_elegivel,
    eleg.convenio          AS convenio_retorno_elegivel,
    eleg.valores_exame     AS valores_exame_retorno_elegivel,
    eleg.valores_ficha     AS valores_ficha_retorno_elegivel,
    eleg.qtd_exame         AS qtd_exame_retorno_elegivel,
    eleg.secao             AS secao_retorno_elegivel,
    eleg.dias_entre_ficha  AS dias_entre_ficha_elegivel

FROM dados_laudos flr
INNER JOIN dados_birads brd
    ON flr.ficha = brd.ficha
    AND flr.sigla_exame = brd.sigla_exame    
    AND flr.id_marca = brd.id_marca
    AND flr.sequencial = brd.sequencial    
LEFT JOIN refined.saude_preventiva.pardini_retorno_elegivel_ficha eleg
    ON eleg.ficha_origem = flr.ficha
    AND eleg.id_cliente = flr.id_cliente
    AND eleg.linha_cuidado = flr.linha_cuidado
{filtro_ativacao}
"""
 
df_spk = spark.sql(query.format(
    where_clause = where_clause,
    filtro_ativacao = ""
    )
)
 
df_spk_ativacao = spark.sql(query.format(
    where_clause = "",
    filtro_ativacao = filtro_ativacao
    )
)

# COMMAND ----------

def calcular_data_prevista(df_spk: DataFrame):
    """
    Adiciona uma coluna 'dth_previsao_retorno' ao DataFrame com base na coluna 'BIRADS'.

    - Para BIRADS 1 ou 2, adiciona 360 dias à data da coluna 'dth_pedido'.
    - Para BIRADS 3, adiciona 180 dias à data da coluna 'dth_pedido'.
    - Para outros valores de BIRADS, define 'dth_previsao_retorno' como None.

    Parâmetros:
    df_spk (DataFrame): O DataFrame Spark contendo os dados de entrada.

    Retorna:
    DataFrame: O DataFrame atualizado com a nova coluna 'dth_previsao_retorno'.
    """
    df_spk = df_spk.withColumn(
        'dth_previsao_retorno',
        when(
            col('BIRADS').isin([1, 2]),
            expr("date_add(dth_pedido, 360)")
        ).when(
            col('BIRADS') == 3,
            expr("date_add(dth_pedido, 180)")  
        ).otherwise(None)
    )
    return df_spk

# COMMAND ----------

def transform_fields(df_spk: DataFrame) -> DataFrame:
  """
  Transforma os campos do DataFrame fornecido.

  - Verifica se o DataFrame está vazio. Se estiver, registra um aviso e retorna o DataFrame sem alterações.
  - Adiciona uma coluna 'retorno_cliente' com valores baseados na coluna 'BIRADS':
    - 12 meses para BIRADS 1 ou 2
    - 6 meses para BIRADS 3
    - 0 para outros valores
  - Calcula a data prevista de retorno usando a função 'calcular_data_prevista'.
  - Converte a coluna 'dth_pedido_retorno_elegivel' para o tipo timestamp.
  - Converte a coluna 'dth_previsao_retorno' para o tipo timestamp.
  - Calcula a diferença em dias entre 'dth_previsao_retorno' e 'dth_pedido', armazenando o resultado na coluna 'dias_ate_retorno'.

  Parâmetros:
  df_spk (DataFrame): O DataFrame a ser transformado.

  Retorna:
  DataFrame: O DataFrame transformado com as novas colunas.
  """

  if df_spk.isEmpty():
      logger.warning("No Data Found!")
      return df_spk

  df_spk = df_spk.withColumn(
      'retorno_cliente',
      when(col('BIRADS').isin([1, 2]), 12).when(col('BIRADS') == 3, 6).otherwise(0)
  )

  df_spk = calcular_data_prevista(df_spk)
  df_spk = df_spk.withColumn('dth_pedido_retorno_elegivel', to_timestamp(col('dth_pedido_retorno_elegivel')))
  df_spk = df_spk.withColumn('dth_previsao_retorno', to_timestamp(col('dth_previsao_retorno')))
  df_spk = df_spk.withColumn('dias_ate_retorno', datediff(to_date(col('dth_previsao_retorno')), to_date(col('dth_pedido'))))
  return df_spk


# COMMAND ----------

WEBHOOK_DS_AI_BUSINESS_STG = 'stg'

def error_message(e):
    """
    Envia alerta para o Sentinel e exibe o traceback em caso de erro ao salvar dados.

    Parâmetros:
        e (Exception): Exceção capturada.

    Comportamento:
        - Formata o traceback do erro.
        - Envia alerta para o Sentinel com detalhes do erro.
        - Exibe o traceback no console.
        - Relança a exceção.
    """
    error_message = traceback.format_exc()
    summary_message = f"""Erro ao salvar dados.\n{error_message}"""
    sentinela_ds_ai_business = Sentinel(
        project_name='Monitor_Linhas_Cuidado_Mama',
        env_type=WEBHOOK_DS_AI_BUSINESS_STG,
        task_title='Pardini Mama'
    )
    sentinela_ds_ai_business.alerta_sentinela(
        categoria='Alerta', 
        mensagem=summary_message,
        job_id_descritivo='1_pardini_mama_birads'
    )
    traceback.print_exc()
    raise e

# COMMAND ----------

def insert_data(df_spk: DataFrame, table_name: str):
    """
    Insere os dados do DataFrame na tabela Delta especificada, sobrescrevendo o conteúdo existente.
    
    Parâmetros:
        df_spk (DataFrame): DataFrame a ser salvo.
        table_name (str): Nome da tabela Delta de destino.
    """
    try:
        logger.info(f"Inserting Data: {table_name}")
        (
            df_spk.write
                .mode('overwrite')
                .option('mergeSchema','true')
                .option('overwriteSchema','true')
                .format('delta')
                .partitionBy('_datestamp')
                .saveAsTable(table_name)
        )
    except Exception as e:
        error_message(e)
 
def merge_data(df_spk: DataFrame, table_name: str):
    """
    Realiza merge dos dados do DataFrame na tabela Delta, atualizando registros existentes e inserindo novos.
    
    Parâmetros:
        df_spk (DataFrame): DataFrame com os dados incrementais.
        table_name (str): Nome da tabela Delta de destino.
    """
    try:
        logger.info(f"Merging Data: {table_name}")
        df_spk.createOrReplaceTempView(f"increment_birads")
        merge_query = f"""
            MERGE INTO {table_name} AS target
            USING increment_birads AS source
                ON target.ficha = source.ficha
                AND target.sequencial = source.sequencial
                AND target.sigla_exame = source.sigla_exame
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """
        spark.sql(merge_query)
    except Exception as e:
        error_message(e)
 
def save_data(df_spk: DataFrame, table_name: str):
    """
    Salva os dados do DataFrame na tabela Delta, utilizando merge se a tabela existir ou insert se não existir.
    
    Parâmetros:
        df_spk (DataFrame): DataFrame a ser salvo.
        table_name (str): Nome da tabela Delta de destino.
    """
    if df_spk.isEmpty():
        return None
 
    if spark.catalog.tableExists(table_name):
        merge_data(df_spk, table_name)
    else:
        insert_data(df_spk, table_name)

# COMMAND ----------

df_spk = transform_fields(df_spk)
df_spk_ativacao = transform_fields(df_spk_ativacao)

# COMMAND ----------

# Excluir duplicados para considerar apenas a ficha na ativação e não os exames (itens). Assim vamos enviar apenas 
# 1 push por ficha
df_spk_ativacao = df_spk_ativacao.dropDuplicates(['ficha'])

# COMMAND ----------

print('quantidade de laudos salvos na tabela',df_spk.count())
print('quantidade de laudos salvos na tabela de ativação', df_spk_ativacao.count())

# COMMAND ----------

save_data(df_spk, table_birads)
insert_data(df_spk_ativacao, table_birads_ativacao)
