# Databricks notebook source
# MAGIC %md
# MAGIC # Extração de dados - Anatomo Patologico
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

# %pip install openai -q
# %pip install tqdm -q
# %pip install pandarallel -q
# %pip install databricks-feature-store -q
# %restart_python

# COMMAND ----------

from pyspark.sql import SparkSession
import json
import re
import os
import sys
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

# pd.set_option('display.max_rows', None)       
# pd.set_option('display.max_columns', None)   
# pd.set_option('display.width', None) 
# pd.set_option('display.max_colwidth', None)

# COMMAND ----------

query_append = """
WITH base AS (
    SELECT
        flr.id_unidade,
        flr.id_cliente, 
        flr.id_item, 
        flr.id_subitem, 
        flr.id_exame, 
        flr.dth_pedido,
        flr.laudo_tratado,
        flr.sigla_exame,
        flr.linha_cuidado
        FROM refined.saude_preventiva.pardini_laudos flr
    WHERE
        flr.linha_cuidado   = 'mama'
        flr.sigla_exame IN ("MICROH","MICROH","FPAH")
),
sem_extracao AS (
    SELECT
        b.id_unidade,
        b.id_cliente,
        b.id_item,
        b.id_subitem,
        b.id_exame,
        b.dth_pedido,
        b.sigla_exame,
        b.laudo_tratado,
        b.RAW_CARCINOMA,
        b.HAS_CARCINOMA
    FROM base b
    LEFT JOIN refined.saude_preventiva.pardini_laudos_mamo_anatomia_patologica mb
      ON mb.id_unidade = b.id_unidade
     AND mb.id_cliente = b.id_cliente
     AND mb.id_item    = b.id_item
     AND mb.id_subitem = b.id_subitem
     AND mb.id_exame   = b.id_exame
    WHERE mb.id_unidade IS NULL
)
SELECT *
FROM sem_extracao
"""

query_all_base = """SELECT
        flr.id_unidade,
        -- flr.id_pedido,
        flr.id_cliente, 
        id_item, 
        id_subitem, 
        flr.id_exame, 
        flr.dth_pedido,
        flr.laudo_tratado,
        flr.sigla_exame
    FROM refined.saude_preventiva.pardini_laudos flr
    WHERE
      flr.linha_cuidado = 'mama'
      AND
      flr.sigla_exame IN ("MICROH","MICROH","FPAH")
      """
df_anatomopatologico = spark.sql(query_all_base)
df_anatomopatologico = df_anatomopatologico.filter(F.col("laudo_tratado").rlike("(?i)Topografia: mama"))
df_anatomopatologico = df_anatomopatologico.withColumn("laudo_tratado", F.lower(df_anatomopatologico["laudo_tratado"]))
window = Window.orderBy(F.monotonically_increasing_id())
df_anatomopatologico = df_anatomopatologico.withColumn("index", row_number().over(window) - 1)

# COMMAND ----------

num_linhas = df_anatomopatologico.count()

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

def extrai_indice_mitotico(txt):
    # Ex.: "índice mitótico 3/10 mm2" ou "mitótico: 2 mm2"
    m = re.search(r"mit[oó]tico\s*[:\-]?\s*(\d+)\s*/?\s*\d*\s*mm2", txt, flags=re.IGNORECASE)
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

if num_linhas > 0:
    llm_client = openai.OpenAI(api_key=DATABRICKS_TOKEN,
                           base_url="https://dbc-d80f50a9-af23.cloud.databricks.com/serving-endpoints"
                           )
    descricao_agente = "Atue como um médico oncologista especialista em laudos de mamografia."

    ###################### Apenas para testes #################
    df_anatomopatologico = df_anatomopatologico.limit(200)
    ###########################################################

    rows = df_anatomopatologico.select("laudo_tratado").collect()
    laudos = [row.laudo_tratado for row in rows]

    respostas = batch_generate(descricao_agente, laudos, llm_client, batch_size=10)

    lista_dicts = [limpar_e_converter(item) for item in respostas]

    schema = StructType([
        StructField("descritores_malignidade", ArrayType(StringType()), True),
        StructField("grau_histologico", StringType(), True),
        StructField("grau_nuclear", StringType(), True),
        StructField("formacao_tubulos", StringType(), True),
        StructField("indice_mitotico", StringType(), True),
        StructField("tipo_histologico", StringType(), True),
    ])

    df_lista = spark.createDataFrame(lista_dicts, schema=schema)

    w = Window.orderBy(F.lit(1))

    df_imuno_indexed = (
        df_anatomopatologico
        .withColumn("row_id", F.row_number().over(w) - 1)  # subtrai 1 para ficar zero‐based
    )

    df_lista_indexed = (
        df_lista
        .withColumn("row_id", F.row_number().over(w) - 1)
    )

    df_final = df_imuno_indexed.join(df_lista_indexed, on="row_id").drop("row_id")

    # Base histórica
    #fs = FeatureStoreClient()
    #fs = FeatureStoreClient()
    #fs.create_table(
    #    name="refined.saude_preventiva.pardini_laudos_mamo_anatomia_patologica",
    #    primary_keys=["id_unidade", "id_cliente", "id_item", "id_subitem", "id_exame"],
    #    schema=df_final.schema,
    #    description="Features extraídas de laudos de mamografia/biopsia (Anatomia Patológica). Siglas: "
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
    #    primary_keys = ["id_unidade", "id_cliente", "id_item", "id_subitem", "id_exame"]
    #    fs.write_table(
    #        name="refined.saude_preventiva.pardini_laudos_mamo_anatomia_patologica",
    #        df=df_final,
    #        mode="merge",
    #    )
    #else:
    #    print("Nenhum registro encontrado; nada a fazer.")

    lista_laudos = laudos
    resultados = []
    for laudo_txt, json_mod in zip(lista_laudos, lista_dicts):
        pseudo_gold, compar = avalia_extracao_sem_ground_truth(laudo_txt, json_mod)
        resultados.append(compar)

    df_metrics = pd.DataFrame()
    df_metrics["laudos"] = laudos
    df_metrics["resultados"] = resultados
    resultados_expandidos = pd.json_normalize(df_metrics["resultados"])
    df_metrics = pd.concat(
        [df_metrics.drop(columns=["resultados"]), resultados_expandidos],
        axis=1
    )

    json_metricas = agrega_resultados(resultados)

    # mlflow.set_experiment("/Users/aureliano.paiva@grupofleury.com.br/anatomopatologico_pardini_metricas")

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

#%pip install openpyxl
#df_pandas = df_final.toPandas()
#df_pandas.to_excel("fleury_laudos_mamografia_anatomia_patologica.xlsx", index=False)

# COMMAND ----------

#pd.set_option('display.max_colwidth', None)
#df_pandas2 = df_pandas[df_pandas["grau_histologico"] != "NÃO INFORMADO"].head(100)
#df_pandas2.to_excel("fleury_laudos_mamografia_anatomia_patologica_grauhistologico.xlsx", index=False)
#df_pandas2.head(100)
