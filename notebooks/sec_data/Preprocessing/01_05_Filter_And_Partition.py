# Databricks notebook source
# MAGIC %md
# MAGIC # 01_05_Filter_And_Partition

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook filters the data and saves the result within custom defined partitions. <br>
# MAGIC The code is based on the jupyter notebook https://github.com/HansjoergW/bfh_cas_bgd_fs2020_sa/blob/master/01_05_Filter_And_Partition.ipynb

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

# MAGIC %md
# MAGIC ## Basic definitions

# COMMAND ----------

# folder with the whole dataset as a single parquet
all_parquet_folder = "/usr/parquet/"

# target folder with filtered and partitioned data
all_filtered_folder = "/usr/parq_filtered"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load dataset

# COMMAND ----------

df_all = spark.read.parquet(all_parquet_folder).cache()

# COMMAND ----------

_ = [print(x, end=", ") for x in df_all.columns] # print the name of the columns for convenience

# COMMAND ----------

print("Entries in Test: ", "{:_}".format(df_all.count())) # loading dataset into memory

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter lines for "10K" and "10Q" and "Primary Financial Statement"

# COMMAND ----------

filter_string = "stmt is not null and version NOT LIKE '00%' and form in ('10-K', '10-Q')"
df_all_filtered = df_all.where(filter_string)

# COMMAND ----------

print("after filter   : ", "{:_}".format(df_all_filtered.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter companies traded at NYSE and NASDAQ

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create list with cik

# COMMAND ----------

df_all_cik_exchange = df_all_filtered[['cik','exchange']].distinct() \
    .where("exchange in ('NASDAQ','NYSE','NYSE ARCA','NYSE MKT') ").selectExpr("cik", "1 as cik_select").cache()

# COMMAND ----------

print("count cik_exchange   : ", "{:_}".format(df_all_cik_exchange.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join the cik list with the filtered dataframe

# COMMAND ----------

df_all_filter_complete = df_all_filtered.join(df_all_cik_exchange, 'cik', "left").where("cik_select == 1")

# COMMAND ----------

print("count cik_exchange   : ", "{:_}".format(df_all_filter_complete.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save as a new Parquet file

# COMMAND ----------

shutil.rmtree("/dbfs" + all_filtered_folder,  ignore_errors=True) # make sure the target folder is empty

df_all_partioned = df_all_filter_complete.repartition(16,col("cik"), col("stmt"))
df_all_partioned.write.parquet(all_filtered_folder)

# COMMAND ----------

