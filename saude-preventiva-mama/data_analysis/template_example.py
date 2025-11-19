# Databricks notebook source
# MAGIC %md
# MAGIC A celula a seguir é util para indexar a pasta raiz do projeto facilitando o import e leitura dos dados

# COMMAND ----------

import sys
import os

# Adiciona o diretório pai ao sys.path
current_dir = os.path.dirname(os.path.abspath("__file__"))
PARENT_DIR = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(PARENT_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC Depois de rodar a celula acima é possivel fazer as operações da celula a seguir:

# COMMAND ----------

import pandas as pd
from utils import bom_exemplo

df = pd.read_csv(f'{PARENT_DIR}/data/file.csv')
bom_exemplo(df)
