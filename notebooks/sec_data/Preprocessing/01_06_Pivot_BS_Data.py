# Databricks notebook source
# MAGIC %md
# MAGIC # 01_06_Pivot_BS_Data

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook pivots the balancesheet data, so that every balancesheet has its data in its own role.<br>
# MAGIC The code is based on the jupyter notebook https://github.com/HansjoergW/bfh_cas_bgd_fs2020_sa/blob/master/01_06_Pivot_BS_Data.ipynb

# COMMAND ----------

from pathlib import Path
from typing import List, Tuple, Union, Set
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

import pandas as pd

import shutil          # provides high level file operations
import time            # used to measure execution time
import os
import sys

# COMMAND ----------

# target folder with filtered and partitioned data
all_filtered_folder = "/usr/parq_filtered"

# target folder with the pivoted BalanceSheet data
all_bs_folder = "/usr/parq_bs"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the data

# COMMAND ----------

df_all = spark.read.parquet(all_filtered_folder).cache()
print("Entries in Test: ", "{:_}".format(df_all.count())) # loading all dataset into memory

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deciding which tags to pivot

# COMMAND ----------

bs_tags = pd.read_csv("/dbfs/usr/bs_tags.csv")['tag']
relevant_tags = bs_tags[:100].tolist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pivoting the data

# COMMAND ----------

df_all_bs_ready = df_all.where("stmt = 'BS' and period == ddate").select(['cik','ticker','adsh','form','period','tag','value'])

df_all_bs_pivot = df_all_bs_ready.groupby(["cik","ticker","adsh","form","period"]).pivot("tag",relevant_tags).max('value')

# store the pivoted data as parquet
shutil.rmtree("/dbfs" + all_bs_folder,  ignore_errors=True) # make sure the target folder is empty
df_all_bs_pivot.repartition(8,col("cik")).write.parquet(all_bs_folder)

# COMMAND ----------

