# Databricks notebook source
# MAGIC %md
# MAGIC # Extração de dado - Imunohistoquimico
# MAGIC **Extrair os seguintes labels estruturados:**
# MAGIC - Receptor de Estrógeno (categórica): POSITIVO ou NEGATIVO 
# MAGIC   - "RECEPTOR DE ESTRÓGENO - POSITIVO “ 
# MAGIC   - "RECEPTOR DE ESTRÓGENO – NEGATIVO” 
# MAGIC
# MAGIC - Receptor de Progesterona (categórica): POSITIVO ou NEGATIVO 
# MAGIC   - "RECEPTOR DE PROGESTERONA - POSITIVO" 
# MAGIC   - "RECEPTOR DE PROGESTERONA - NEGATIVO" 
# MAGIC
# MAGIC - Status do HER-2 (categórica): Negativo, inconclusivo ou positivo 
# MAGIC   - "HER-2 - ESCORE 0” = Negativo 
# MAGIC   - "HER-2 - ESCORE 1+” = Negativo 
# MAGIC   - "HER-2  -  ESCORE  2+” = Inconclusivo 
# MAGIC   - "HER-2  -  ESCORE  3+” = Positivo 
# MAGIC
# MAGIC - Porcentagem do Ki-67 (numérica):  "KI-67 - POSITIVO EM 20% DAS CÉLULAS NEOPLÁSICAS"
# MAGIC   - Deve ser extraído esse número da porcentagem nessa frase 
# MAGIC
# MAGIC - Status CK5/6 (categórica): POSITIVO ou NEGATIVO 
# MAGIC   - "CK5/6 - POSITIVO “ou "CK5/6 - NEGATIVO" 
# MAGIC   
# MAGIC + Para laudos que não possuem carcinoma, ou seja, casos negativos para câncer não devem se processados no LLM.  

# COMMAND ----------

# MAGIC %pip install openai -q
# MAGIC %pip install tqdm -q
# MAGIC %pip install pandarallel -q
# MAGIC %pip install databricks-feature-store -q
# MAGIC %pip install octoops

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import SparkSession
import json
import re
import os
import sys
import mlflow
import time
import warnings
from tqdm import tqdm
import pandas as pd
import numpy as np
from typing import List, Any
import openai
from dateutil.relativedelta import relativedelta
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from databricks.feature_store import FeatureStoreClient


spark = SparkSession.builder.appName("LLM_Extractor").getOrCreate()

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get() if dbutils.notebook.entry_point.getDbutils().notebook().getContext() is not None else None


# COMMAND ----------

mlflow.tracing.disable_notebook_display()

# COMMAND ----------

table_anatom = "refined.saude_preventiva.pardini_laudos_mama_imunohistoquimico" 

where_clause = f"""
WHERE
    _datestamp > (
        SELECT MAX(anatom._datestamp)
        FROM {table_anatom} anatom
    )
"""

 
filtro_extracao = """
    WHERE
        linha_cuidado  = 'mama'
        AND UPPER(flr.sexo_cliente) = 'F'
        AND sigla_exame IN ("IHMAMA")
        AND laudo_tratado RLIKE '(?i)mama' AND laudo_tratado RLIKE '(?i)carcinoma'
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from refined.saude_preventiva.pardini_laudos_mamo_imunohistoquimico

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct (sigla_exame)
# MAGIC from refined.saude_preventiva.pardini_laudos
# MAGIC where linha_cuidado = 'mama'
# MAGIC

# COMMAND ----------

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
        flr.sequencial, 
        flr.id_exame, 
        flr.dth_pedido,
        flr.dth_resultado,
        flr.sigla_exame,
        flr.laudo_tratado,
        flr.linha_cuidado,
        flr.sexo_cliente,
        flr.`_datestamp`
    FROM refined.saude_preventiva.pardini_laudos flr  
    {where_clause}  
    
)
SELECT *
FROM base
{filtro_extracao}
"""
df_spk = spark.sql(query)
display(df_spk)

# COMMAND ----------


# query_append = """
# WITH base AS (
#     SELECT
#         flr.id_unidade,
#         flr.id_cliente, 
#         flr.id_item, 
#         flr.id_subitem, 
#         flr.id_exame, 
#         flr.dth_pedido,
#         flr.laudo_tratado,
#         flr.sigla_exame,
#         flr.linha_cuidado
#         FROM refined.saude_preventiva.pardini_laudos flr
#     WHERE
#         flr.linha_cuidado   = 'mama'
#         flr.sigla_exame IN ("IHMAMA")
# ),
# sem_extracao AS (
#     SELECT
#         b.id_unidade,
#         b.id_cliente,
#         b.id_item,
#         b.id_subitem,
#         b.id_exame,
#         b.dth_pedido,
#         b.sigla_exame,
#         b.laudo_tratado,
#         b.RAW_CARCINOMA,
#         b.HAS_CARCINOMA
#     FROM base b
#     LEFT JOIN refined.saude_preventiva.pardini_laudos_mamo_imunohistoquimico mb
#       ON mb.id_unidade = b.id_unidade
#      AND mb.id_cliente = b.id_cliente
#      AND mb.id_item    = b.id_item
#      AND mb.id_subitem = b.id_subitem
#      AND mb.id_exame   = b.id_exame
#     WHERE mb.id_unidade IS NULL
# )
# SELECT *
# FROM sem_extracao
# """

# query_all_base = """WITH base AS (
#         SELECT
#             flr.id_unidade,
#             flr.id_cliente, 
#             CAST(flr.id_item AS INT) AS id_item, 
#             CAST(flr.id_subitem AS INT) AS id_subitem, 
#             flr.id_exame, 
#             flr.dth_pedido,
#             flr.laudo_tratado,
#             flr.sigla_exame
#         FROM refined.saude_preventiva.pardini_laudos flr
#         WHERE
#         flr.linha_cuidado = 'mama'
#         AND
#         flr.sigla_exame IN ("IHMAMA")
#     )
#     SELECT
#         id_unidade,
#         id_cliente, 
#         id_item, 
#         id_subitem, 
#         id_exame, 
#         dth_pedido,
#         sigla_exame,
#         laudo_tratado
#     FROM base """

# df_imunohistoquimico = spark.sql(query_all_base)
# df_imunohistoquimico = df_imunohistoquimico.filter(F.col("laudo_tratado").rlike("(?i)mama"))
# df_imunohistoquimico = df_imunohistoquimico.withColumn("laudo_tratado", F.lower(df_imunohistoquimico["laudo_tratado"]))
# window = Window.orderBy(F.monotonically_increasing_id())
# df_imunohistoquimico = df_imunohistoquimico.withColumn("index", row_number().over(window) - 1)

# COMMAND ----------

df_spk.count()

# COMMAND ----------

def prompt_laudo(laudo_texto: str) -> str:
    prompt = f"""A seguir está um laudo médico de mamografia, conforme indicado abaixo. Se alguma informação não estiver presente no texto, retorne "NÃO INFORMADO". Sempre retorne apenas o dicionário Python.

    Laudo clínico:
    \"\"\"{laudo_texto}\"\"\"

    ### Critérios de extração:

    - **Receptor de Estrogênio**: retorne se é "POSITIVO", "NEGATIVO" ou "NÃO INFORMADO".

    - **Receptor de Progesterona**: retorne se é "POSITIVO", "NEGATIVO" ou "NÃO INFORMADO".

    - **Status do HER-2**: retorne se o Status do HER-2 é "NEGATIVO", "INCONCLUSIVO", "POSITIVO" ou "NÃO INFORMADO". Com base no score seguindo as regras:
    - "HER-2 - ESCORE 0" ou "1+" → "NEGATIVO"
    - "HER-2 - ESCORE 2+" → "INCONCLUSIVO"
    - "HER-2 - ESCORE 3+" → "POSITIVO"

    - **Ki-67 (%)**: retorne o valor numérico da porcentagem de positividade do KI-67.

    - **Status do CK5/6**: retorne "POSITIVO", "NEGATIVO" ou "NÃO INFORMADO" do Status do CK5/6.

    ### Saída esperada (dicionário Python válido):
    ```python
    {{
    "receptor_estrogeno": "POSITIVO" | "NEGATIVO" |  "NÃO INFORMADO",
    "receptor_progesterona": "POSITIVO" | "NEGATIVO" |  "NÃO INFORMADO",
    "status_her2": "POSITIVO" | "POSITIVO" | "INCONCLUSIVO" |  "NÃO INFORMADO",
    "ki67_percentual": float |  0,
    "status_ck5_6": "POSITIVO" | "NEGATIVO" |  "NÃO INFORMADO"
    }}
    ```
    """
    return prompt.strip()

def generate(descricao_agente:str, laudo:str, llm_client) -> str:
    """
    Gera o resultado da análise de um laudo
    Params:
        descricao_agente: descricao do agente que a LLM representa (primeira mensagem enviada à LLM)
        prompt: prompt base que será utilizado para gerar a análise
        laudo: laudo a ser analisado (incluido dentro do prompt)
        llm_client: cliente da API da LLM
    Return:
        response_message: resposta da LLM
    """
    prompt = prompt_laudo(laudo)
    messages = [
        # Utilizamos o primeiro prompt para contextualizar o llm do que ele deve fazer. 
        # No exemplo utilizamos a abordagem Role, Task, Output, Prompting.
        # Mas sintam-se a vontade para alterar de acordo com a necessidade
        {
            "role": "system",
            "content": descricao_agente
        },
        {
            "role": "user",
            "content": prompt
        }
    ]
    model_params = {
        "model": "databricks-llama-4-maverick",
        "messages": messages,
        "temperature": 0,
        "max_tokens": 4000,
        "top_p": 0.75,
        "frequency_penalty": 0,
        "presence_penalty": 0
    }
    connection_retry = 0
    while connection_retry < 3:
        try:
            response = llm_client.chat.completions.create(**model_params)
            response_message = response.choices[0].message.content
            break
        # TODO: verificar se a excessao é de conexao
        except (ConnectionError, TimeoutError) as e:
            connection_retry += 1
            print("Sem reposta do modelo")
            print(str(e))
            print("Tentando novamente...")
            time.sleep(0.1)
        except Exception as e:
            raise e

    if connection_retry >= 3:
        response_message = ''
    
    return response_message


def batch_generate(descricao_agente, laudos, llm_client, batch_size=25):
    responses = []
    
    llm_client = openai.OpenAI(
        api_key=DATABRICKS_TOKEN,
        base_url="https://dbc-d80f50a9-af23.cloud.databricks.com/serving-endpoints"
    )
    
    # Dividir em lotes
    for i in range(0, len(laudos), batch_size):
        laudos_batch = laudos[i:i+batch_size]
        for laudo in tqdm(laudos_batch, desc=f"Processando lote {i//batch_size + 1}", total=len(laudos_batch)):
            responses.append(generate(descricao_agente, laudo, llm_client))
    
    return responses

def limpar_e_converter(item):
    try:
        item_limpo = re.sub(r"```(?:python)?", "", item).replace("```", "").strip()
        return json.loads(item_limpo)
    except Exception as e:
        print(f"Erro ao converter resposta: {e}")
        return {
            "receptor_estrogeno": "NÃO INFORMADO",
            "receptor_progesterona": "NÃO INFORMADO",
            "status_her2": "NÃO INFORMADO",
            "ki67_percentual": "NÃO INFORMADO",
            "status_ck5_6": "NÃO INFORMADO"
        }

# COMMAND ----------

import ast
#import ace_tools as tools

# ==============================================
# Funções de extração heurística (pseudo-gold) para o novo prompt
# ==============================================

def extrai_receptor_estrogeno(txt: str) -> str:
    """
    Extrai o status de Receptor de Estrogênio: valores possíveis
    "POSITIVO", "NEGATIVO" ou retorna "NÃO INFORMADO".
    """
    txt_lower = txt.lower()
    # Padrões comuns: "receptor de estrogênio: positivo" ou "er+: positivo" etc.
    if re.search(r"receptor\s+de\s+estrog[eê]genio\s*[:\-]?\s*positivo", txt_lower):
        return "POSITIVO"
    if re.search(r"receptor\s+de\s+estrog[eê]genio\s*[:\-]?\s*negativo", txt_lower):
        return "NEGATIVO"
    # Abreviações: "ER+" / "ER-"
    if re.search(r"\ber\s*[\+]\b", txt, flags=re.IGNORECASE):
        return "POSITIVO"
    if re.search(r"\ber\s*[\-]\b", txt, flags=re.IGNORECASE):
        return "NEGATIVO"
    return "NÃO INFORMADO"

def extrai_receptor_progesterona(txt: str) -> str:
    """
    Extrai o status de Receptor de Progesterona: valores possíveis
    "POSITIVO", "NEGATIVO" ou retorna "NÃO INFORMADO".
    """
    txt_lower = txt.lower()
    if re.search(r"receptor\s+de\s+progesterona\s*[:\-]?\s*positivo", txt_lower):
        return "POSITIVO"
    if re.search(r"receptor\s+de\s+progesterona\s*[:\-]?\s*negativo", txt_lower):
        return "NEGATIVO"
    # Abreviações: "PR+" / "PR-"
    if re.search(r"\bpr\s*[\+]\b", txt, flags=re.IGNORECASE):
        return "POSITIVO"
    if re.search(r"\bpr\s*[\-]\b", txt, flags=re.IGNORECASE):
        return "NEGATIVO"
    return "NÃO INFORMADO"

def extrai_status_her2(txt: str) -> str:
    """
    Extrai o Status do HER-2 baseado em termos de score:
    - "HER-2 - ESCORE 0" ou "1+" → "NEGATIVO"
    - "HER-2 - ESCORE 2+" → "INCONCLUSIVO"
    - "HER-2 - ESCORE 3+" → "POSITIVO"
    """
    txt_lower = txt.lower()
    # Primeiro, busca padrão "her-2 ... escore X+"
    m = re.search(r"her[-\s]?2.*?escore\s*[:\-]?\s*([0-3]\+?)", txt_lower, flags=re.IGNORECASE)
    if m:
        score = m.group(1)
        if score.startswith("0") or score == "1+":
            return "NEGATIVO"
        if score == "2+":
            return "INCONCLUSIVO"
        if score == "3+":
            return "POSITIVO"
    # Outra forma: "her2 3+" isolado
    m2 = re.search(r"\bher[-]?2\s*[:\-]?\s*([0-3]\+)\b", txt_lower, flags=re.IGNORECASE)
    if m2:
        score2 = m2.group(1)
        if score2 == "1+":
            return "NEGATIVO"
        if score2 == "2+":
            return "INCONCLUSIVO"
        if score2 == "3+":
            return "POSITIVO"
    return "NÃO INFORMADO"

def extrai_ki67_percentual(txt: str) -> float:
    """
    Extrai o valor de Ki-67 em porcentagem. 
    Exemplo no texto: "Ki-67: 20%", "Ki 67 15 %", etc.
    Se não encontrar, retorna 0.0.
    """
    m = re.search(r"ki[-\s]?67.*?[:\-]?\s*(\d{1,3}(?:\.\d+)?)\s*%", txt, flags=re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return 0.0
    return 0.0

def extrai_status_ck5_6(txt: str) -> str:
    """
    Extrai o status do CK5/6: "POSITIVO", "NEGATIVO" ou retorna "NÃO INFORMADO".
    """
    txt_lower = txt.lower()
    if re.search(r"ck5\s*\/\s*6.*?[:\-]?\s*positivo", txt_lower):
        return "POSITIVO"
    if re.search(r"ck5\s*\/\s*6.*?[:\-]?\s*negativo", txt_lower):
        return "NEGATIVO"
    # Abreviações: "CK5/6+" ou "CK5/6-"
    if re.search(r"\bck5\s*\/\s*6\s*[\+]\b", txt, flags=re.IGNORECASE):
        return "POSITIVO"
    if re.search(r"\bck5\s*\/\s*6\s*[\-]\b", txt, flags=re.IGNORECASE):
        return "NEGATIVO"
    return "NÃO INFORMADO"

# ==============================================
# Função de avaliação sem ground truth completo (novo conjunto de campos)
# ==============================================

def avalia_extracao_sem_ground_truth_novo(laudo_texto: str, json_modelo: dict):
    """
    Gera pseudo-gold (json_heu) para os campos do novo prompt
    e compara com json_modelo (saída da IA).
    Retorna:
      - json_heu: dicionário com valores heurísticos
      - comparacoes: dicionário que, para cada campo, traz:
          * valor_heu
          * valor_mod
          * acertou (boolean)
    """
    # 1. Gera pseudo-gold (json_heu)
    rei = extrai_receptor_estrogeno(laudo_texto)
    rpr = extrai_receptor_progesterona(laudo_texto)
    her = extrai_status_her2(laudo_texto)
    ki6 = extrai_ki67_percentual(laudo_texto)
    ck6 = extrai_status_ck5_6(laudo_texto)

    json_heu = {
        "receptor_estrogeno": rei,
        "receptor_progesterona": rpr,
        "status_her2": her,
        "ki67_percentual": ki6,
        "status_ck5_6": ck6
    }

    # 2. Prepara json_modelo: se não for dict, converte para dict vazio
    if not isinstance(json_modelo, dict):
        json_modelo = {}

    # 3. Comparações campo a campo
    comparacoes = {}

    # Campos categóricos (strings)
    for campo in ["receptor_estrogeno", "receptor_progesterona", "status_her2", "status_ck5_6"]:
        val_heu = json_heu[campo]
        val_mod = json_modelo.get(campo, "NÃO INFORMADO")
        comparacoes[campo] = {
            "valor_heu": val_heu,
            "valor_mod": val_mod,
            "acertou": (val_heu == val_mod)
        }

    # Campo Ki-67 (%)
    val_heu_ki = json_heu["ki67_percentual"]
    try:
        val_mod_ki = float(json_modelo.get("ki67_percentual", 0))
    except (ValueError, TypeError):
        val_mod_ki = 0.0
    comparacoes["ki67_percentual"] = {
        "valor_heu": val_heu_ki,
        "valor_mod": val_mod_ki,
        "acertou": (val_heu_ki == val_mod_ki)
    }

    return json_heu, comparacoes

# COMMAND ----------

def parse_json_string(s):
    if isinstance(s, str):
        try:
            return ast.literal_eval(s)
        except Exception:
            return {}
    return s

# COMMAND ----------

from collections import Counter

def agrega_resultados_dinamico(lista_comparacoes):
    """
    Agraga resultados de uma lista de dicionários de comparações, retornando para cada campo:
      - acertos: número de vezes que 'acertou' == True
      - total: número total de laudos (len(lista_comparacoes))
      - taxa_acerto: acertos / total (ou 0.0 se total == 0)
    
    Suporta qualquer conjunto de chaves em cada dict, desde que cada valor seja outro dict contendo a chave 'acertou'.
    """
    total_laudos = len(lista_comparacoes)
    acertos_por_campo = Counter()

    # Para cada comparação, percorremos todas as chaves e contamos os acertos
    for comp in lista_comparacoes:
        for campo, info in comp.items():
            # Supondo que info seja um dict com a chave "acertou"
            if info.get("acertou", False):
                acertos_por_campo[campo] += 1

    # Monta o dicionário de saída
    resultado = {}
    for campo, acertos in acertos_por_campo.items():
        resultado[campo] = {
            "acertos": acertos,
            "total": total_laudos,
            "taxa_acerto": (acertos / total_laudos) if total_laudos > 0 else 0.0
        }

    # É possível que exista algum campo em alguma comparação que nunca teve 'acertou' == True.
    # Se quisermos incluir também esses campos (com acertos = 0), podemos varrer as chaves da primeira entrada:
    if lista_comparacoes:
        primeira = lista_comparacoes[0]
        for campo in primeira.keys():
            if campo not in resultado:
                resultado[campo] = {
                    "acertos": 0,
                    "total": total_laudos,
                    "taxa_acerto": 0.0
                }

    return resultado


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import StringType
import pandas as pd



if df_spk.count() > 0:
    llm_client = openai.OpenAI(api_key=DATABRICKS_TOKEN,
                           base_url="https://dbc-d80f50a9-af23.cloud.databricks.com/serving-endpoints"
                           )
    descricao_agente = "Atue como um médico oncologista especialista em laudos de mamografia."

    # Coleta os dados 
    df_local = df_spk.select("ficha","id_exame","id_marca","sequencial","laudo_tratado").toPandas()

    # Aplica o LLM 
    respostas = batch_generate(descricao_agente, df_local["laudo_tratado"].tolist(), llm_client, batch_size=100)
    respostas_limpa = [limpar_e_converter(item) for item in respostas]

    # Adiciona as respostas ao DataFrame
    df_local["resposta_llm"] = respostas_limpa

    # Converte de volta para Spark
    df_respostas = spark.createDataFrame(df_local)

    # Faz join com o DataFrame original para manter todas as colunas
    df_final = df_spk.join(df_respostas.select("ficha","id_exame","id_marca","sequencial","resposta_llm"), on=["ficha","id_exame","id_marca","sequencial"], how="inner")

    # Definir estrutura
    schema_resposta = StructType([
    StructField("receptor_estrogeno", StringType(), True),
    StructField("receptor_progesterona", StringType(), True),
    StructField("status_her2", StringType(), True),
    StructField("ki67_percentual", DoubleType(), True),
    StructField("status_ck5_6", DoubleType(), True),
    ])

    df_final_expanded = df_final.withColumn("resposta_struct", col("resposta_llm"))

    # expandir resultado llma para colunas

    df_final_expanded = df_final_expanded.select(
    "*",
    col("resposta_struct.receptor_estrogeno").alias("receptor_estrogeno"),
    col("resposta_struct.receptor_progesterona").alias("receptor_progesterona"),
    col("resposta_struct.status_her2").alias("status_her2"),
    col("resposta_struct.ki67_percentual").alias("ki67_percentual"),
    col("resposta_struct.status_ck5_6").alias("status_ck5_6"),
    ).drop("resposta_struct")

    #display(df_final_expanded)

    # a partir dos dados extraídos definir uma classificação final
    df_final_classif = df_final_expanded.withColumn(
            "categoria_final",
            F.when(
                (
                    ((F.col("receptor_estrogeno") == "POSITIVO") | (F.col("receptor_progesterona") == "POSITIVO")) &
                    (F.col("status_her2") == "NEGATIVO") &
                    (F.col("ki67_percentual") < 14)
                ),
                "Luminal A"
            ).when(
                (
                    ((F.col("receptor_estrogeno") == "POSITIVO") | (F.col("receptor_progesterona") == "POSITIVO")) &
                    (F.col("status_her2") == "NEGATIVO") &
                    (F.col("ki67_percentual") >= 14)
                ),
                "Luminal B"
            ).when(
                (
                    ((F.col("receptor_estrogeno") == "POSITIVO") | (F.col("receptor_progesterona") == "POSITIVO")) &
                    (F.col("status_her2") == "POSITIVO")
                ),
                "Luminal com HER2 Positivo"
            ).when(
                (
                    (F.col("receptor_estrogeno") == "NEGATIVO") &
                    (F.col("receptor_progesterona") == "NEGATIVO") &
                    (F.col("status_her2") == "POSITIVO")
                ),
                "HER-2 Superexpresso"
            ).when(
                (
                    (F.col("receptor_estrogeno") == "NEGATIVO") &
                    (F.col("receptor_progesterona") == "NEGATIVO") &
                    (F.col("status_her2") == "NEGATIVO")
                ),
                "Triplo Negativo"
            ).otherwise("Indefinido")
        )

    display(df_final_classif)








    # ###################### Apenas para testes #################
    # df_imunohistoquimico = df_imunohistoquimico.limit(100)
    # ###########################################################

    # rows = df_imunohistoquimico.select("laudo_tratado").collect()
    # laudos = [row.laudo_tratado for row in rows]

    # respostas = batch_generate(descricao_agente, laudos, llm_client, batch_size=10)

    # lista_dicts = [limpar_e_converter(item) for item in respostas]

    # schema = StructType([
    #     StructField("receptor_estrogeno", StringType(), True),
    #     StructField("receptor_progesterona", StringType(), True),
    #     StructField("status_her2", StringType(), True),
    #     StructField("ki67_percentual", StringType(), True),
    #     StructField("status_ck5_6", StringType(), True),
    # ])

    # df_lista = spark.createDataFrame(lista_dicts, schema=schema)

    # w = Window.orderBy(F.lit(1))

    # df_imuno_indexed = (
    #     df_imunohistoquimico
    #     .withColumn("row_id", F.row_number().over(w) - 1)  # subtrai 1 para ficar zero‐based
    # )

    # df_lista_indexed = (
    #     df_lista
    #     .withColumn("row_id", F.row_number().over(w) - 1)
    # )

    # df_final = df_imuno_indexed.join(df_lista_indexed, on="row_id").drop("row_id")

    # df_final = df_final.withColumn(
    #     "categoria_final",
    #     F.when(
    #         (
    #             ((F.col("receptor_estrogeno") == "POSITIVO") | (F.col("receptor_progesterona") == "POSITIVO")) &
    #             (F.col("status_her2") == "NEGATIVO") &
    #             (F.col("ki67_percentual") < 14)
    #         ),
    #         "Luminal A"
    #     ).when(
    #         (
    #             ((F.col("receptor_estrogeno") == "POSITIVO") | (F.col("receptor_progesterona") == "POSITIVO")) &
    #             (F.col("status_her2") == "NEGATIVO") &
    #             (F.col("ki67_percentual") >= 14)
    #         ),
    #         "Luminal B"
    #     ).when(
    #         (
    #             ((F.col("receptor_estrogeno") == "POSITIVO") | (F.col("receptor_progesterona") == "POSITIVO")) &
    #             (F.col("status_her2") == "POSITIVO")
    #         ),
    #         "Luminal com HER2 Positivo"
    #     ).when(
    #         (
    #             (F.col("receptor_estrogeno") == "NEGATIVO") &
    #             (F.col("receptor_progesterona") == "NEGATIVO") &
    #             (F.col("status_her2") == "POSITIVO")
    #         ),
    #         "HER-2 Superexpresso"
    #     ).when(
    #         (
    #             (F.col("receptor_estrogeno") == "NEGATIVO") &
    #             (F.col("receptor_progesterona") == "NEGATIVO") &
    #             (F.col("status_her2") == "NEGATIVO")
    #         ),
    #         "Triplo Negativo"
    #     ).otherwise("INDEFINIDO")
    # )

    # Base histórica
    #fs = FeatureStoreClient()
    #fs.create_table(
    #    name="refined.saude_preventiva.pardini_laudos_mamo_imunohistoquimico",
    #    primary_keys=["id_unidade", "id_cliente", "id_item", "id_subitem", "id_exame"],
    #    schema=df_final.schema,
    #    description="Features extraídas de laudos de mamografia. Siglas: IH-NEO e IHMAMA"
    #)

    # Append em prd
    #num_linhas = df_final.count()
    #fs = FeatureStoreClient()
    #if num_linhas > 0:
    #    print(f"Há {num_linhas} registros para inserir — executando gravação…")
    #    primary_keys = ["id_unidade", "id_cliente", "id_item", "id_subitem", "id_exame"]
    #    ###### Apenas para testes ##############
    #    df_final = df_final.dropna()
    #    df_final = df_final.dropDuplicates(primary_keys)
    #    ########################################
    #    fs.write_table(
    #        name="refined.saude_preventiva.pardini_laudos_mamo_imunohistoquimico",
    #        df=df_final,
    #        mode="merge",
    #    )
    #else:
    #    print("Nenhum registro encontrado; nada a fazer.")

    df_metrics = pd.DataFrame()
    df_metrics["laudos"] = laudos
    df_metrics["resultados"] = lista_dicts

    df_metrics["resultados"] = df_metrics["resultados"].apply(parse_json_string)

    lista_laudos2 = df_metrics["laudos"].tolist()
    lista_modelo2 = df_metrics["resultados"].tolist()

    lista_pseudo_gold2 = []
    lista_comparacoes2 = []

    for laudo_txt, json_mod in zip(lista_laudos2, lista_modelo2):
        json_heu, comp = avalia_extracao_sem_ground_truth_novo(laudo_txt, json_mod)
        lista_pseudo_gold2.append(json_heu)
        lista_comparacoes2.append(comp)

    df_metrics = pd.DataFrame()
    df_metrics["laudos"] = laudos
    df_metrics["resultados"] = lista_comparacoes2
    resultados_expandidos = pd.json_normalize(df_metrics["resultados"])
    df_metrics = pd.concat(
            [df_metrics.drop(columns=["resultados"]), resultados_expandidos],
            axis=1
        )
    
    # json_metricas = agrega_resultados_dinamico(lista_comparacoes2)

    # mlflow.set_experiment("/Users/aureliano.paiva@grupofleury.com.br/imunohistoquimico_pardini_metricas")

    # threshold = 0.8

    # with mlflow.start_run(run_name="Extracao_Laudos_Run_Threshold"):
    #     for campo, stats in json_metricas.items():
    #         taxa = stats["taxa_acerto"]
            
    #         # Registrar a taxa de acerto
    #         mlflow.log_metric(f"{campo}_taxa_acerto", taxa)
            
    #         # Registrar flag de aprovação no threshold
    #         passou_flag = 1 if taxa >= threshold else 0
    #         mlflow.log_metric(f"{campo}_passou_threshold", passou_flag)
            
    #         # Opcional: registrar acertos e total
    #         mlflow.log_metric(f"{campo}_acertos", stats["acertos"])
    #         mlflow.log_metric(f"{campo}_total", stats["total"])
        
    #     run_id = mlflow.active_run().info.run_id
    #     print(f"Run registrada: {run_id}")

# COMMAND ----------

df_final_classif.count()

# COMMAND ----------

import traceback
from octoops import Sentinel
from delta.tables import DeltaTable

WEBHOOK_DS_AI_BUSINESS_STG = 'stg'

# Iniciar sessão Spark
spark = SparkSession.builder.appName("DfPandasparaSpark").getOrCreate()

#OUTPUT_DATA_PATH = dbutils.widgets.get("OUTPUT_DATA_PATH")
OUTPUT_DATA_PATH = "refined.saude_preventiva.pardini_laudos_mama_imunohistoquimico"

# função para salvar dados na tabela
def insert_data(df_spk, output_data_path):

    # Cria a tabela Delta se não existir
    if not DeltaTable.isDeltaTable(spark, output_data_path):
        df_spk.write.format("delta").saveAsTable(output_data_path)
    else:
        # Carrega a tabela Delta existente
        delta_table = DeltaTable.forPath(spark, output_data_path)

        # Faz o merge (upsert)
        (delta_table.alias("target")
        .merge(
            df_spk.alias("source"),
            "target.ficha = source.ficha AND target.id_exame = source.id_exame AND target.id_marca = source.id_marca AND target.sequencial = source.sequencial"
        )
        .whenMatchedUpdateAll() #atualiza todos os campos se o ID já existir
        .whenNotMatchedInsertAll() #insere se o ID não existir
        .execute())

# salvar dados na tabela
try:
    # 1/0
    if (df_final_classif.count() > 0):        


        # Inserir tabela catalog
        insert_data(df_final_classif, OUTPUT_DATA_PATH)
        print('Total de registros salvos na tabela:', df_final_classif.count())
       

    else: 
        error_message = traceback.format_exc()
        error_message = "Pardini Imunuhistoquimico - Não há laudos para extração."
        sentinela_ds_ai_business = Sentinel(
            project_name='Monitor_Linhas_Cuidado_Mama',
            env_type=WEBHOOK_DS_AI_BUSINESS_STG,
            task_title='Pardini Mama Imunuhistoquimico'
        )

        sentinela_ds_ai_business.alerta_sentinela(
            categoria='Alerta', 
            mensagem=error_message,
            job_id_descritivo='4_Pardini_mama_Imunuhistoquimico'
        )
except Exception as e:
    traceback.print_exc()
    raise e    

# COMMAND ----------

#%pip install openpyxl
#df_pandas = df_final.toPandas()
#df_pandas.to_excel("pardini_laudos_mamo_imunohistoquimico.xlsx", index=False)
