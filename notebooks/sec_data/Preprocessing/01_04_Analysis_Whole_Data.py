# Databricks notebook source
# MAGIC %md
# MAGIC # 01_04_Analysis_Whole_Data

# COMMAND ----------

# MAGIC %md
# MAGIC The goal of this notebook is to conduct some basic analysis on the whole dataset.<br>
# MAGIC It is based on the code from https://github.com/HansjoergW/bfh_cas_bgd_fs2020_sa/blob/master/01_04_Analysis_Whole_Data.ipynb

# COMMAND ----------

# imports
from pathlib import Path
from typing import List, Tuple, Union, Set
from pyspark.sql.dataframe import DataFrame

import pandas as pd

import shutil          # provides high level file operations
import time            # used to measure execution time
import os
import sys

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

%matplotlib inline

sns.set()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic definitions

# COMMAND ----------

# folder with the whole dataset as a single parquet
all_parquet_folder = "/usr/parquet/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading the dataset

# COMMAND ----------

df_all = spark.read.parquet(all_parquet_folder).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Print all the contained column names

# COMMAND ----------

_ = [print(x, end=", ") for x in df_all.columns] # print the name of the columns for convenience

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total number of records

# COMMAND ----------

print("Entries in Test: ", "{:_}".format(df_all.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC To load the data initially into the cache and execute the count()-operation took about 9 minutes.<br>
# MAGIC Executing the same operation another time, after everything was cached, took about 42 seconds.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Number of different CIKs

# COMMAND ----------

print("{:_}".format(df_all.select("cik").distinct().count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Number of reports by type

# COMMAND ----------

p_df_all_reports_by_type = df_all.select(["adsh","form"]).distinct().toPandas()

# COMMAND ----------

ct = pd.crosstab(index=p_df_all_reports_by_type['form'], columns='count')
print(ct[:5])

# COMMAND ----------

my_order = p_df_all_reports_by_type.groupby(by=['form'])['form'].count().sort_values(ascending=False).index
g = sns.catplot(x='form', kind='count', data=p_df_all_reports_by_type,  order=my_order, color='skyblue')
g.set_xticklabels(  ## Beschriftung der x-Achse
    rotation=90, ha='center')  ## alias set_horizontalalignment
g.fig.set_figwidth(12)
g.fig.set_figheight(4)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Number of companies per exchange

# COMMAND ----------

p_df_all_ciks_at_echange = df_all.select(["cik","exchange"]).distinct().toPandas() #  convert to pandas in order to visualize the data

# COMMAND ----------

print("not traded at an exchange: ", p_df_all_ciks_at_echange['exchange'].isnull().sum(axis = 0))
print("traded at an exchange    : ", p_df_all_ciks_at_echange['exchange'].count())

# COMMAND ----------

ct = pd.crosstab(index=p_df_all_ciks_at_echange['exchange'], columns='count')
print(ct) 

# COMMAND ----------

my_order = p_df_all_ciks_at_echange.groupby(by=['exchange'])['exchange'].count().sort_values(ascending=False).index
g = sns.catplot(x='exchange', kind='count', data=p_df_all_ciks_at_echange,  order=my_order, color='skyblue')
g.set_xticklabels(  ## Beschriftung der x-Achse
    rotation=90, ha='center')  ## alias set_horizontalalignment
g.fig.set_figwidth(12)
g.fig.set_figheight(4)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filer status
# MAGIC 
# MAGIC There is the field "afs" which defines the "Filer status with the SEC at the time of submission". This field can have the values 
# MAGIC * LAF=Large Accelerated,
# MAGIC * ACC=Accelerated,
# MAGIC * SRA=Smaller Reporting Accelerated,
# MAGIC * NON=Non-Accelerated,
# MAGIC * SML=Smaller Reporting Filer,
# MAGIC * NULL=not assigned

# COMMAND ----------

p_df_filer_state = df_all.select(["cik","afs"]).distinct().toPandas() #  convert to pandas in order to visualize the data

# COMMAND ----------

print("without filer state: ", p_df_filer_state['afs'].isnull().sum(axis = 0))
print("with filer state   : ", p_df_filer_state['afs'].count())

# COMMAND ----------

ct = pd.crosstab(index=p_df_filer_state['afs'], columns='count')
print(ct) 

# COMMAND ----------

my_order = p_df_filer_state.groupby(by=['afs'])['afs'].count().sort_values(ascending=False).index
g = sns.catplot(x='afs', kind='count', data=p_df_filer_state,  order=my_order, color='skyblue')
g.set_xticklabels(  ## Beschriftung der x-Achse
    rotation=90, ha='center')  ## alias set_horizontalalignment
g.fig.set_figwidth(12)
g.fig.set_figheight(4)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Primary financial statement

# COMMAND ----------

print("with filer state   : ", "{:_}".format(df_all.select('stmt').where("stmt is not null").count()))

# COMMAND ----------

p_df_prim_fin_rep_count = df_all.cube('stmt').count().toPandas()
print(p_df_prim_fin_rep_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tags

# COMMAND ----------

df_all_report_lines = df_all.select(['stmt', 'tag', 'version']).where("stmt is not null and version NOT LIKE '00%'").cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ### How many different tags/version are there in the while dataset

# COMMAND ----------

p_df_stmt_tags = df_all_report_lines.distinct().toPandas()

# COMMAND ----------

p_df_stmt_tags[:10]

# COMMAND ----------

tag_version_per_type = pd.crosstab(index=p_df_stmt_tags['stmt'], columns='count')
tag_version_per_type

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check if there are different versions of the same tag

# COMMAND ----------

version_per_tag = pd.crosstab(index=p_df_stmt_tags[p_df_stmt_tags['stmt'] == 'BS']['tag'], columns='count')
version_per_tag.sort_values('count', ascending=False)[:15]

# COMMAND ----------

p_df_stmt_tags[p_df_stmt_tags['tag'] == 'Cash']['version'].unique()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ignoring the version - how many differnt tags are there

# COMMAND ----------

p_df_stmt_tags_no_Version = p_df_stmt_tags[['stmt', 'tag']].drop_duplicates()
tag_per_type = pd.crosstab(index=p_df_stmt_tags_no_Version['stmt'], columns='count')
tag_per_type

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find the most important tags for the BalanceSheet

# COMMAND ----------

# check in how many reports a tag from a BS appears
p_all_BS_rows = df_all.select(['adsh','stmt', 'tag', 'version']).where("stmt like 'BS' and version NOT LIKE '00%'").distinct().cache()

# COMMAND ----------

p_all_BS_rows.count()

# COMMAND ----------

count_per_BS_tag = p_all_BS_rows.select(['tag']).cube('tag').count().toPandas()

# COMMAND ----------

sorted_count_per_BS_tag = count_per_BS_tag.sort_values('count', ascending=False)
sorted_count_per_BS_tag.reset_index(drop = True, inplace = True)
sorted_count_per_BS_tag = sorted_count_per_BS_tag[1:] # ignoring the first row, which contains the "None" Tag
sorted_count_per_BS_tag[:10]

# COMMAND ----------

display(sorted_count_per_BS_tag)

# COMMAND ----------

display(sorted_count_per_BS_tag[:100])

# COMMAND ----------

sorted_count_per_BS_tag.to_csv("/dbfs/usr/bs_tags.csv",index=False,header=True)

# COMMAND ----------

