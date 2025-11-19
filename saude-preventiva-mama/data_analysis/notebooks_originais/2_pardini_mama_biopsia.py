# Databricks notebook source
# MAGIC %md
# MAGIC # Extrair dados dos exames de Biopsia
# MAGIC + Filtrar os laudos que possuem o termo topografia MAMA e desses laudos filtrar aqueles que possuem a palavra carcinoma (extrair toda a frase que está assoaciada ao termo).
# MAGIC

# COMMAND ----------

# %pip install databricks-feature-store -q
%pip install octoops

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
from octoops import FeatureStoreManager
from databricks.feature_store import FeatureStoreClient

# COMMAND ----------

# O código configura um logger para registrar mensagens de log no módulo atual (__name__).
logger = logging.getLogger(__name__)

# COMMAND ----------

# filtros para extração

table_biopsia = "refined.saude_preventiva.pardini_laudos_mama_biopsia"
 
where_clause = f"""
    WHERE
        _datestamp > (
            SELECT MAX(biop._datestamp)
            FROM {table_biopsia} biop
        )
    """

 
filtro_extracao = """
    WHERE
        linha_cuidado   = 'mama'
        AND sigla_exame IN (
            "USPCOB",
            "CBGEST",
            "USPAFI",
            "ESMATO",
            "USMATO",
            "MPMFECG",
            "RMMATO",
            "USPMAF"
        )
        AND UPPER(sexo_cliente) = 'F'
        AND (
            idade_cliente >= 40 AND idade_cliente < 76
        ) 
"""

# COMMAND ----------

# Utiliza REGEXP_EXTRACT para buscar, no texto do campo 'laudo_tratado', 
# a ocorrência da palavra "CARCINOMA" relacionada à "MAMA" (seja antes ou depois de "Topografia").
# O padrão regex considera maiúsculas/minúsculas e múltiplas linhas.
# Se encontrar, extrai "CARCINOMA" para a coluna RAW_CARCINOMA; caso contrário, retorna string vazia.
# O CASE seguinte verifica se RAW_CARCINOMA está vazia:
# - Se estiver vazia, HAS_CARCINOMA recebe FALSE (não há carcinoma relacionado à mama no laudo).
# - Se não estiver vazia, HAS_CARCINOMA recebe TRUE (há carcinoma relacionado à mama no laudo).

query = f"""
WITH base AS (
    SELECT
        flr.id_marca,
        flr.id_unidade,
        flr.id_cliente, 
        flr.id_ficha,
        flr.ficha,
        flr.id_item, 
        flr.id_subitem, 
        flr.sigla_exame, 
        flr.dth_pedido,
        flr.dth_resultado,
        flr.sigla_exame,
        flr.laudo_tratado,
        flr.linha_cuidado,
        flr.sexo_cliente,
        flr.`_datestamp`,
        (
          TIMESTAMPDIFF(DAY, flr.dth_nascimento_cliente, CURDATE()) / 365.25
        ) AS idade_cliente,
        REGEXP_EXTRACT(
            flr.laudo_tratado,
            r'(?mi).*Topografia:.*MAMA.*(CARCINOMA).*|.*(CARCINOMA).*Topografia:.*MAMA.*',
            1
        ) AS RAW_CARCINOMA,
        CASE
            WHEN REGEXP_EXTRACT(
                    flr.laudo_tratado,
                    r'(?mi).*Topografia:.*MAMA.*(CARCINOMA).*|.*(CARCINOMA).*Topografia:.*MAMA.*',
                    1
                 ) = ''
            THEN FALSE
            ELSE TRUE
        END AS HAS_CARCINOMA
    FROM refined.saude_preventiva.pardini_laudos flr
    
)
SELECT *
FROM base
{filtro_extracao}
"""
df_spk_biopsia = spark.sql(query)
display(df_spk_biopsia)

# COMMAND ----------

df_spk.count()

# COMMAND ----------

# Não temos todas as siglas na base de dados
df_spk.select("sigla_exame").distinct()
display(df_spk.select("sigla_exame").distinct())

# COMMAND ----------

# FeatureStore
# fs = FeatureStoreClient()
# fs.create_table(
#     name="refined.saude_preventiva.pardini_laudos_mama_biopsia",
#     primary_keys=["ficha",'id_item','id_subitem'],
#     schema=df_spk.schema,    
#     description="Jornada de Mama - Features extraídas de laudos de biopsia Pardini. Siglas: USPCOB, CBGEST, USPAFI, ESMATO, USMATO, MPMFECG, RMMATO, USPMAF")

# COMMAND ----------

import sys
import traceback
from octoops import Sentinel
from delta.tables import DeltaTable

WEBHOOK_DS_AI_BUSINESS_STG = 'stg'


#OUTPUT_DATA_PATH = dbutils.widgets.get('OUTPUT_DATA_PATH')
OUTPUT_DATA_PATH = 'refined.saude_preventiva.pardini_laudos_mama_biopsia'

# Selecionar colunas de interesse
df_spk = df_spk_biopsia.select("ficha","sequencial","sigla_exame","id_cliente","dth_pedido","dth_resultado", "laudo_tratado","linha_cuidado","_datestamp","RAW_CARCINOMA","HAS_CARCINOMA").display()

# função para salvar dados na tabela
def insert_data(df_spk, output_data_path):  

    # Carrega a tabela Delta existente
    delta_table = DeltaTable.forName(spark, output_data_path)

    # Faz o merge (upsert)
    (delta_table.alias("target")
     .merge(
         df_spk.alias("source"),
         "target.ficha = source.ficha AND target.sequencial = source.sequencial AND target.sigla_exame = source.sigla_exame"
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
            


# salvar dados
try:
    if df_spk.count() > 0:
        
        #1/0 
        insert_data(df_spk, OUTPUT_DATA_PATH)
        # fs.write_table(
    #     name="refined.saude_preventiva.fleury_laudos_mama_biopsia",
    #     df=df_spk,
    #     mode="merge",
        # )
        print(f'Total de registros salvos na tabela: {df_spk.count()}')
            
    else:
        error_message = traceback.format_exc()
        summary_message = f"""Não há laudos para extração.\n{error_message}"""
        sentinela_ds_ai_business = Sentinel(
            project_name='Monitor_Linhas_Cuidado_Torax',
            env_type=WEBHOOK_DS_AI_BUSINESS_STG,
            task_title='Pardini Mama Biopsia'
        )

        sentinela_ds_ai_business.alerta_sentinela(
            categoria='Alerta', 
            mensagem=summary_message,
            job_id_descritivo='2_pardini_mama_biopsia'
        )
except Exception as e:
    traceback.print_exc()        
    raise e
