# Databricks notebook source
# MAGIC %md
# MAGIC # Extração de dados - Anatomo Patologico
# MAGIC Serão analisados os descritores de malignidade.
# MAGIC **Descritores de MALIGNIDADE:**
# MAGIC - carcinoma
# MAGIC - invasivo
# MAGIC - invasor
# MAGIC - sarcoma
# MAGIC - metástase
# MAGIC - metastático
# MAGIC - maligno
# MAGIC - maligna
# MAGIC - cdi, cli, cdis

# COMMAND ----------

# MAGIC %md
# MAGIC Outros labels a serem extraídos:
# MAGIC
# MAGIC - **Grau histológico:** será sempre um algarismo 1, 2 ou 3 (apenas três categorias). Para encontrar, basta procurar o primeiro algarismo numérico após o termo **"grau histológico"**.
# MAGIC
# MAGIC - **Grau nuclear:** será sempre um algarismo 1, 2 ou 3 (apenas três categorias). Para encontrar, basta procurar o primeiro algarismo numérico após o termo **"grau nuclear"**.
# MAGIC
# MAGIC - **Formação de túbulos:** será sempre um algarismo 1, 2 ou 3 (apenas três categorias). Para encontrar, basta procurar o primeiro algarismo numérico após o termo **"formação de túbulos"**.
# MAGIC
# MAGIC - **Índice mitótico:** será sempre um algarismo 1, 2 ou 3 (apenas três categorias). Para encontrar, basta procurar o primeiro algarismo numérico após o termo **"mm2"**. Nesse caso, é melhor procurar o termo **"mm2"** ao invés de **"índice mitótico"**.
# MAGIC
# MAGIC - **Labels de tipos histológicos:**
# MAGIC   - Carcinoma de mama ductal invasivo (CDI)/SOE
# MAGIC   - Carcinoma de mama ductal in situ
# MAGIC   - Carcinoma de mama lobular invasivo
# MAGIC   - Carcinoma de mama lobular
# MAGIC   - Carcinoma de mama papilífero
# MAGIC   - Carcinoma de mama metaplásico
# MAGIC   - Carcinoma de mama mucinoso
# MAGIC   - Carcinoma de mama tubular
# MAGIC   - Carcinoma de mama cístico adenoide
# MAGIC   - Carcinoma de mama medular
# MAGIC   - Carcinoma de mama micropapilar
# MAGIC   - Carcinoma de mama misto (ductal e lobular) invasivo

# COMMAND ----------

# MAGIC %pip install openai
# MAGIC # %pip install tqdm -q
# MAGIC # %pip install pandarallel -q
# MAGIC %pip install databricks-feature-store -q
# MAGIC %pip install octoops
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import re
import os
import sys
import json
import time
import warnings
import mlflow
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

mlflow.tracing.disable_notebook_display()

# spark = SparkSession.builder.appName("LLM_Extractor").getOrCreate()



# COMMAND ----------

# filtros de extração
table_anatom = "refined.saude_preventiva.fleury_laudos_mama_anatomia_patologica_v2" 

where_clause = f"""
WHERE
    flr.`_datestamp` >= (
        SELECT MAX(anatom._datestamp)
        FROM {table_anatom} anatom
    )
    """

 
filtro_extracao = """
    WHERE
        linha_cuidado  = 'mama'
        AND UPPER(sexo_cliente) = 'F'
        AND sigla_exame IN ("ANATPATP", "CTPUNC", "FISHHER")
        AND laudo_tratado RLIKE '(?i)Topografia: mama'        
"""

# COMMAND ----------

query = f"""
WITH 
base AS (
    SELECT
        flr.id_marca,
        flr.id_unidade,
        flr.id_cliente, 
        flr.id_ficha,
        flr.ficha,
        flr.id_item, 
        flr.id_subitem, 
        flr.id_exame, 
        flr.dth_pedido,
        flr.dth_resultado,
        flr.sigla_exame,
        flr.laudo_tratado,
        flr.linha_cuidado,
        flr.sexo_cliente,
        flr.`_datestamp`
    FROM refined.saude_preventiva.fleury_laudos flr 
    {where_clause}
 
    
)
SELECT *
FROM base
{filtro_extracao}
"""
df_spk = spark.sql(query)
display(df_spk)

# COMMAND ----------

DATABRICKS_TOKEN = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    if dbutils.notebook.entry_point.getDbutils().notebook().getContext() is not None
    else None
)

# COMMAND ----------

def prompt_laudo(laudo_texto: str) -> str:
    prompt = f"""A seguir está um laudo médico de mamografia. Se alguma informação não estiver presente no texto, retorne "NÃO INFORMADO". Sempre retorne apenas o dicionário Python.

    Laudo clínico:
    \"\"\"{laudo_texto}\"\"\"

    ### Critérios de extração:

    - **Descritores de malignidade**: retorne uma **lista** com os termos de malignidade encontrados no texto (case-insensitive). Se nenhum for encontrado, retorne lista vazia `[]`. Lista de termos: ['carcinoma', "invasivo", "invasor", "sarcoma", "metástase", "metastático", "maligno", "maligna", "cdi", "cli", "cdis"]

    - **Grau histológico**: retorne o valor numérico do grau histológico.

    - **Grau nuclear**: retorne o valor numérico do grau nuclear.

    - **Formação de túbulos**: retorne o valor numérico caso exista formação de túbulos.

    - **Índice mitótico**: retorne o valor numérico do score do índice mitótico que aparece após o mm2.

    - **Tipo histológico**: identifique e retorne a frase correspondente se algum dos seguintes for mencionado (case-insensitive, variações aceitas):
      - Carcinoma de mama ductal invasivo
      - Carcinoma de mama ductal in situ
      - Carcinoma de mama lobular invasivo
      - Carcinoma de mama lobular
      - Carcinoma de mama papilífero
      - Carcinoma de mama metaplásico
      - Carcinoma de mama mucinoso
      - Carcinoma de mama tubular
      - Carcinoma de mama cístico adenoide
      - Carcinoma de mama medular
      - Carcinoma de mama micropapilar
      - Carcinoma de mama misto (ductal e lobular) invasivo

    ### Saída esperada (dicionário Python válido):
    ```python
    {{
      "descritores_malignidade": ["termo1", "termo2", ...],
      "grau_histologico": número | "NÃO INFORMADO",
      "grau_nuclear": número | "NÃO INFORMADO",
      "formacao_tubulos": número | "NÃO INFORMADO",
      "indice_mitotico": número | "NÃO INFORMADO",
      "tipo_histologico": "texto correspondente ou 'NÃO INFORMADO'
    }}
    ```
    """
    return prompt.strip()



# COMMAND ----------


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
        "model": "teste-maverick",
        #"model": "databricks-llama-4-maverick",
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
            # para obter qtde de tokens
            # usage = response.usage        
            # prompt_tokens = usage.prompt_tokens
            # completion_tokens = usage.completion_tokens
            # total_tokens = usage.total_tokens
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
        #usage = ()
        
    
    return response_message #,usage


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


# COMMAND ----------

# # nova implementação
# llm_client = OpenAI(
#     api_key=DATABRICKS_TOKEN,
#     base_url="https://dbc-d80f50a9-af23.cloud.databricks.com/serving-endpoints"
# )

# def generate(descricao_agente: str, laudo: str) -> str:
#     prompt = prompt_laudo(laudo)
#     messages = [
#         {
#             "role": "system",
#             "content": descricao_agente
#         },
#         {
#             "role": "user",
#             "content": prompt
#         }
#     ]
#     connection_retry = 0
#     while connection_retry < 3:
#         try:
#             response = client.chat.completions.create(
#                 model="teste-maverick",
#                 messages=messages,
#                 max_tokens=4000,
#                 temperature=0,
#                 top_p=0.75,
#                 frequency_penalty=0,
#                 presence_penalty=0
#             )
#             response_message = response.choices[0].message.content
#             break
#         except Exception as e:
#             print(f"Erro na requisição: {e}")
#             connection_retry += 1
#             print("Tentando novamente...")
#             time.sleep(0.1)

#     if connection_retry >= 3:
#         response_message = ''

#     return response_message

# COMMAND ----------

# # implementação para o endpoint (Aguarando Ricardo_Arquitetura)

# def generate(descricao_agente: str, laudo: str, llm_client) -> str:
#     """
#     Gera o resultado da análise de um laudo
#     Params:
#         descricao_agente: descricao do agente que a LLM representa (primeira mensagem enviada à LLM)
#         prompt: prompt base que será utilizado para gerar a análise
#         laudo: laudo a ser analisado (incluido dentro do prompt)
#         llm_client: cliente da API da LLM
#     Return:
#         response_message: resposta da LLM
#     """
#     prompt = prompt_laudo(laudo)
#     messages = [
#         {
#             "role": "system",
#             "content": descricao_agente
#         },
#         {
#             "role": "user",
#             "content": prompt
#         }
#     ]
#     payload = {
#         "model": "databricks-llama-4-maverick",
#         "messages": messages,
#         "temperature": 0,
#         "max_tokens": 4000,
#         "top_p": 0.75,
#         "frequency_penalty": 0,
#         "presence_penalty": 0
#     }
#     connection_retry = 0
#     while connection_retry < 3:
#         try:
#             url ="https://dbc-d80f50a9-af23.cloud.databricks.com/serving-endpoints/teste-maverick/invocations"
#             #"https://dbc-d80f50a9-af23.cloud.databricks.com/serving-endpoints/batch-saudepreventiva/invocations"
#             response = requests.post(url, headers=headers, json=payload)
#             response.raise_for_status()            
#             data = response.json()
#             response_message = data.choices[0].message.content 
#             #data["choices"][0]["message"]["content"]                      
#             usage = data["usage"]
#             prompt_tokens = usage["prompt_tokens"]
#             completion_tokens = usage["completion_tokens"]
#             total_tokens = usage["total_tokens"]
#             print("Requisição bem-sucedida!")
#             print(data)
#             break
#         except requests.exceptions.RequestException as e:
#             print(f"Erro na requisição: {e}")
#             if response:
#                 print(f"Status Code: {response.status_code}")
#                 print(f"Resposta do servidor: {response.text}")
#             connection_retry += 1
#             print("Tentando novamente...")
#             time.sleep(0.1)
#         except Exception as e:
#             raise e

#     if connection_retry >= 3:
#         response_message = ''

#     return response_message

# COMMAND ----------

# llm_client = openai.OpenAI(
#     api_key=DATABRICKS_TOKEN,
#     base_url= "https://dbc-d80f50a9-af23.cloud.databricks.com/serving-endpoints/teste-maverick/invocations"
#     #"https://dbc-d80f50a9-af23.cloud.databricks.com/serving-endpoints/batch-saudepreventiva/invocations"
    
# ) 

# headers = {
# "Authorization": f"Bearer {DATABRICKS_TOKEN}",
# "Content-Type": "application/json"
# }


# def batch_generate(descricao_agente, laudos, llm_client, batch_size=25):
#     responses = [] 

#     # Dividir em lotes
#     for i in range(0, len(laudos), batch_size):
#         laudos_batch = laudos[i:i+batch_size]
#         for laudo in tqdm(laudos_batch, desc=f"Processando lote {i//batch_size + 1}", total=len(laudos_batch)):
#             responses.append(generate(descricao_agente, laudo, llm_client))
    
#     return responses



# COMMAND ----------

def limpar_e_converter(item):
    try:
        item_limpo = re.sub(r"```(?:python)?", "", item).replace("```", "").strip()
        return json.loads(item_limpo)
    except Exception as e:
        print(f"Erro ao converter resposta: {e}")
        return {
            'descritores_malignidade': [],
            'grau_histologico': "NÃO INFORMADO",
            'grau_nuclear': "NÃO INFORMADO",
            'formacao_tubulos': "NÃO INFORMADO",
            'indice_mitotico': "NÃO INFORMADO",
            'tipo_histologico': "NÃO INFORMADO"
        }

# COMMAND ----------


TERMS = ['carcinoma', 'invasivo', 'invasor', 'sarcoma', 
         'metástase', 'metastático', 'maligno', 'maligna', 
         'cdi', 'cli', 'cdis']

def extrai_descritores(txt):
    achados = set()
    for termo in TERMS:
        # insensível a maiúsculas e minúsculas, plenos caracteres
        if re.search(rf"\b{re.escape(termo)}\b", txt, flags=re.IGNORECASE):
            achados.add(termo.lower())
    return sorted(achados)  # lista em ordem alfabética

def extrai_grau_histologico(txt):
    # Captura algo como "Grau histológico: 2" ou "grau histológico 2"
    m = re.search(r"grau\s+histol[oó]gico\s*[:\-]?\s*(\d)", txt, flags=re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None

def extrai_grau_nuclear(txt):
    m = re.search(r"grau\s+nuclear\s*[:\-]?\s*(\d)", txt, flags=re.IGNORECASE)
    return int(m.group(1)) if m else None

def extrai_formacao_tubulos(txt):
    m = re.search(r"forma[cç][aã]o\s+de\s+t[uú]bulos\s*[:\-]?\s*(\d)", txt, flags=re.IGNORECASE)
    return int(m.group(1)) if m else None

# def extrai_indice_mitotico(txt):
#     # Ex.: "índice mitótico 3/10 mm2" ou "mitótico: 2 mm2"
#     m = re.search(r"mit[oó]tico\s*[:\-]?\s*(\d+)\s*/?\s*\d*\s*mm2", txt, flags=re.IGNORECASE)
#     if m:
#         return int(m.group(1))
#     return None

def extrai_indice_mitotico(txt):
    # Ex.: "índice mitótico 3/10 mm2", "mitótico: 2 mm2", "Índice mitótico: 1 mitose / mm2", "Índice mitótico: 11 mitoses / mm2"
    m = re.search(r"mit[oó]tico\s*[:\-]?\s*(\d+)(?:\s*/\s*\d+)?\s*(?:mitoses?|mitose)?\s*/?\s*mm2", txt, flags=re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None

TIPOS = [
  "carcinoma de mama ductal invasivo",
  "carcinoma de mama ductal in situ",
  "carcinoma de mama lobular invasivo",
  "carcinoma de mama lobular",
  "carcinoma de mama papilífero",
  "carcinoma de mama metapl[aá]sico",
  "carcinoma de mama mucinoso",
  "carcinoma de mama tubular",
  "carcinoma de mama c[ií]stico adenoide",
  "carcinoma de mama medular",
  "carcinoma de mama micropapilar",
  "carcinoma de mama misto (ductal e lobular) invasivo"
]

def extrai_tipo_histologico(txt):
    txt_lower = txt.lower()
    for tipo in TIPOS:
        # usar comparação simplificada, removendo acentos se quiser
        padrao = tipo.lower()
        if padrao in txt_lower:
            return tipo  # retorna exatamente a frase padronizada
    return None

def avalia_extracao_sem_ground_truth(laudo_texto, json_modelo):
    # 1. Gera pseudo-gold
    descrs_hei = extrai_descritores(laudo_texto)
    gr_hist_hei = extrai_grau_histologico(laudo_texto)
    gr_nuc_hei  = extrai_grau_nuclear(laudo_texto)
    form_tub_hei= extrai_formacao_tubulos(laudo_texto)
    ind_mit_hei = extrai_indice_mitotico(laudo_texto)
    tipo_histo_hei = extrai_tipo_histologico(laudo_texto)

    json_heu = {
        "descritores_malignidade": descrs_hei,
        "grau_histologico": gr_hist_hei if gr_hist_hei is not None else "NÃO INFORMADO",
        "grau_nuclear": gr_nuc_hei if gr_nuc_hei is not None else "NÃO INFORMADO",
        "formacao_tubulos": form_tub_hei if form_tub_hei is not None else "NÃO INFORMADO",
        "indice_mitotico": ind_mit_hei if ind_mit_hei is not None else "NÃO INFORMADO",
        "tipo_histologico": tipo_histo_hei if tipo_histo_hei is not None else "NÃO INFORMADO"
    }

    # 2. Prepara json_modelo – já é recebido do ChatGPT como dicionário Python

    # 3. Comparações campo a campo:
    comparacoes = {}

    # 3.1. Descritores de malignidade: compara igualdade exata (acertos ou não)
    val_heu_desc = set(json_heu["descritores_malignidade"])
    val_mod_desc = set(json_modelo.get("descritores_malignidade", []))
    acertou_desc = (val_heu_desc == val_mod_desc)
    comparacoes["descritores_malignidade"] = {
        "pseudo_gold": json_heu["descritores_malignidade"],
        "IA": json_modelo.get("descritores_malignidade", []),
        "acertou": acertou_desc
    }

    # 3.2. Para cada campo numérico ou de texto, basta verificar igualdade exata
    def compara_campo(nome):
        val_heu = json_heu[nome]
        val_mod = json_modelo.get(nome, "NÃO INFORMADO")
        acertou = (val_heu == val_mod)
        return {
            "pseudo_gold": val_heu,
            "IA": val_mod,
            "acertou": acertou
        }

    for campo in ["grau_histologico", "grau_nuclear", "formacao_tubulos", "indice_mitotico", "tipo_histologico"]:
        comparacoes[campo] = compara_campo(campo)

    return json_heu, comparacoes

# COMMAND ----------

from collections import Counter

def agrega_resultados(lista_comparacoes):
    total_laudos = len(lista_comparacoes)
    
    # Conta quantos acertos em descritores_malignidade
    acertos_descritores = 0
    
    # Conta acertos por campo numérico/textual
    acertos_campos = Counter()
    
    for comp in lista_comparacoes:
        # Para descritores_malignidade, só existe "acertou"
        if comp["descritores_malignidade"]["acertou"]:
            acertos_descritores += 1
        
        # Para cada campo numérico/textual
        for campo in [
            "grau_histologico", 
            "grau_nuclear", 
            "formacao_tubulos", 
            "indice_mitotico", 
            "tipo_histologico"
        ]:
            if comp[campo]["acertou"]:
                acertos_campos[campo] += 1

    resultado = {
        "descritores_malignidade": {
            "acertos": acertos_descritores,
            "total": total_laudos,
            "taxa_acerto": acertos_descritores / total_laudos if total_laudos > 0 else 0.0
        }
    }

    for campo in [
        "grau_histologico", 
        "grau_nuclear", 
        "formacao_tubulos", 
        "indice_mitotico", 
        "tipo_histologico"
    ]:
        acertou = acertos_campos[campo]
        resultado[campo] = {
            "acertos": acertou,
            "total": total_laudos,
            "taxa_acerto": acertou / total_laudos if total_laudos > 0 else 0.0
        }

    return resultado

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
import pandas as pd
import mlflow
import requests

# Iniciar sessão Spark
spark = SparkSession.builder.appName("DfPandasparaSpark").getOrCreate()

# realizar extração

if df_spk.count() > 0:
    llm_client = openai.OpenAI(api_key=DATABRICKS_TOKEN,
                           base_url="https://dbc-d80f50a9-af23.cloud.databricks.com/serving-endpoints"
                           #"https://dbc-d80f50a9-af23.cloud.databricks.com/serving-endpoints"
                           )
    descricao_agente = "Atue como um médico oncologista especialista em laudos de mamografia."

    # Coleta os dados localmente    
    df_local = df_spk.select("ficha", "id_item", "id_subitem", "id_cliente", "dth_pedido", "dth_resultado", "sigla_exame", "laudo_tratado", "linha_cuidado", "_datestamp").limit(15).toPandas()
    

    # Aplica o LLM localmente
    # respostas = batch_generate(descricao_agente, df_local["laudo_tratado"].tolist(), llm_client, batch_size=100)
    #respostas_limpa = [limpar_e_converter(item) for item in respostas]
    # df_local["resposta_llm"] = batch_generate(descricao_agente, df_local["laudo_tratado"].tolist(), llm_client, batch_size=100)
    df_local.loc[:, "resposta_llm"] = batch_generate(descricao_agente, df_local["laudo_tratado"].tolist(), llm_client, batch_size=100)
    df_local= df_local.join(df_local["resposta_llm"].apply(limpar_e_converter).apply(pd.Series)) 
    
    # Converte de volta para Spark
    df_respostas = spark.createDataFrame(df_local) 
    display(df_respostas)


   # FeatureStore - Base histórica
    #fs = FeatureStoreClient()
    #fs.create_table(
    #    name="refined.saude_preventiva.fleury_laudos_mamo_anatomia_patologica",
    #    primary_keys=["id_unidade", "id_cliente", "id_item", "id_subitem", "id_exame"],
    #    schema=df_final.schema,
    #    description="Features extraídas de laudos de mamografia/biopsia (Anatomia Patológica). Siglas: ANATPATP, CTPUNC, FISHHER"
    #)

    # Definição de métricas regex vs llm
    lista_laudos = df_respostas.collect()
    #display(lista_laudos)
    resultados = []
    for row in lista_laudos:
        laudo_txt = row["laudo_tratado"]
        json_mod = limpar_e_converter(row["resposta_llm"])
        pseudo_gold, compar = avalia_extracao_sem_ground_truth(laudo_txt, json_mod)
        resultados.append(compar)


    df_metrics = pd.DataFrame()
    df_metrics["laudos"] = df_respostas.select("laudo_tratado").toPandas()["laudo_tratado"]
    df_metrics["resultados"] = resultados
    resultados_expandidos = pd.json_normalize(df_metrics["resultados"])
    df_metrics = pd.concat(
        [df_metrics.drop(columns=["resultados"]), resultados_expandidos],
        axis=1
    )

    json_metricas = agrega_resultados(resultados) 
  

# COMMAND ----------

import json
# import requests
import mlflow

def get_or_create_experiment(experiment_name):
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment:
        experiment_id = experiment.experiment_id
    else:
        experiment_id = mlflow.create_experiment(experiment_name)
    mlflow.set_experiment(experiment_name)
    return experiment_id


# # criar experimento
experiment_id = get_or_create_experiment("/Shared/saude_preventiva_mama/experiments_fleury_anatomopatologico")
mlflow.autolog()

threshold = 0.8

with mlflow.start_run(experiment_id=experiment_id) as run:
        #mlflow.log_param("modelo", payload["model"])
        mlflow.log_param("modelo", "databricks-llama-4-maverick")
        # mlflow.log_param("prompt_tokens", usage.prompt_tokens)
        # mlflow.log_param("completion_tokens", usage.completion_tokens)
        # mlflow.log_param("total_tokens",usage.total_tokens)
        for campo, stats in json_metricas.items():
            taxa = stats["taxa_acerto"]
            mlflow.log_metric(f"{campo}_taxa_acerto", taxa)
            passou_flag = 1 if taxa >= threshold else 0
            mlflow.log_metric(f"{campo}_passou_threshold", passou_flag)
            mlflow.log_metric(f"{campo}_acertos", stats["acertos"])
            mlflow.log_metric(f"{campo}_total", stats["total"])
            
            run_id = mlflow.active_run().info.run_id
            print(f"Run registrada: {run_id}")

# COMMAND ----------

display(json_metricas)

# COMMAND ----------

from delta.tables import DeltaTable
import traceback
from octoops import Sentinel

WEBHOOK_DS_AI_BUSINESS_STG = 'stg'

OUTPUT_DATA_PATH = "refined.saude_preventiva.fleury_laudos_mama_anatomia_patologica_v2"

# função para salvar dados na tabela
def insert_data(df_spk, output_data_path):  
    # Carrega a tabela Delta existente
    delta_table = DeltaTable.forName(spark, output_data_path)

    # Faz o merge (upsert)
    (delta_table.alias("target")
        .merge(
            df_spk.alias("source"),
            "target.ficha = source.ficha AND target.id_item = source.id_item AND target.id_subitem = source.id_subitem"
           
        )
        .whenMatchedUpdateAll() #atualiza todos os campos se o ID já existir
        .whenNotMatchedInsertAll() #insere se o ID não existir
        .execute())

try:
    if df_respostas.count() > 0:        
        # Inserir tabela catalog
    #    fs.write_table(
    #         name="refined.saude_preventiva.fleury_laudos_mamo_anatomia_patologica",
    #         df=df_final,
    #         mode="merge",
    #     )
        insert_data(df_respostas, OUTPUT_DATA_PATH)
        print('Total de registros salvos na tabela:', df_respostas.count())
    else: 
        error_message = traceback.format_exc()
        error_message = "Fleury AnatomoPatologico - Não há laudos para extração."
        sentinela_ds_ai_business = Sentinel(
            project_name='Monitor_Linhas_Cuidado_Mama',
            env_type=WEBHOOK_DS_AI_BUSINESS_STG,
            task_title='Fleury AnatomoPatologico'
        )

        sentinela_ds_ai_business.alerta_sentinela(
            categoria='Alerta', 
            mensagem=error_message,
            job_id_descritivo='3_fleury_mama_anatomopatologico'
        )
except Exception as e:
    traceback.print_exc()
    raise e

# COMMAND ----------

from openai import OpenAI

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url="https://dbc-d80f50a9-af23.cloud.databricks.com/serving-endpoints"
)

response = client.chat.completions.create(
  messages=[
  {
    "role": "system",
    "content": "You are an AI assistant"
  },
  {
    "role": "user",
    "content": "Tell me about Large Language Models"
  }
  ],
  model="teste-maverick",
  max_tokens=256
)
print(response.choices[0].message.content)

# COMMAND ----------

from openai import OpenAI
import os

# How to get your Databricks token: https://docs.databricks.com/en/dev-tools/auth/pat.html
#DATABRICKS_TOKEN = os.environ.get('DATABRICKS_TOKEN') or 'your_actual_token_here'
# Alternatively in a Databricks notebook you can use this:
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

client = OpenAI(
  api_key=DATABRICKS_TOKEN,
  base_url="https://dbc-d80f50a9-af23.cloud.databricks.com/serving-endpoints"
)

chat_completion = client.chat.completions.create(
  messages=[
  {
    "role": "system",
    "content": "You are an AI assistant"
  },
  {
    "role": "user",
    "content": "Tell me about Large Language Models"
  }
  ],
  model="teste-maverick",
  max_tokens=256
)

print(chat_completion.choices[0].message.content)
