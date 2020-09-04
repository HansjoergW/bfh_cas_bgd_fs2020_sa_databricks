# Databricks notebook source
# MAGIC %md
# MAGIC # 01_03_Merge_To_Single_Parquet (on Databricks Cluster)

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook contains the code to Merge all the CSV-files into one single dataframe and store it as Parquet file. It als ensures that the appropriate datatypes are applied to all fields. In an additional step, the Ticker-Symbol information is added as well.<br>
# MAGIC It is based on the code from https://github.com/HansjoergW/bfh_cas_bgd_fs2020_sa/blob/master/01_03_Merge_To_Single_Parquet.ipynb

# COMMAND ----------

from pathlib import Path
from typing import List, Tuple, Union, Set
from pyspark.sql.dataframe import DataFrame

import shutil          # provides high level file operations
import time            # used to measure execution time
import os
import sys

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic definitions

# COMMAND ----------

# The "all"-folder contains the csv files from all of the zipfiles 
all_csv_folders = "/usr/csv_zip/" #  used in spark methods, so it will look directly into the DBFS folder
all_csv_path = Path("/dbfs" + all_csv_folders) # since this is python method, /dbfs has to be added
all_csv_path_list = [x.name for x in all_csv_path.iterdir() if x.is_dir()]
print("All-paths: ", all_csv_path_list)

all_parquet_folder = "/usr/parquet/" # only used in spark methods, so it will look directly into the DBFS folder

cik_ticker_file = "/usr/cik_ticker.csv" # the file with the cik_ticker information -> has to be downloaded first

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schema

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, DoubleType, BooleanType

schema = StructType([  # num.txt  \
                StructField("adsh", 	 StringType(), True), \
                StructField("tag", 	 	 StringType(), True), \
                StructField("version", 	 StringType(), True), \
                StructField("coreg", 	 IntegerType(), True), \
                StructField("ddate", 	 DateType(), True), # date \ 
                StructField("qtrs", 	 StringType(), True), \
                StructField("uom", 	 	 StringType(), True), \
                StructField("value", 	 DoubleType(), True), \
                StructField("footnote",  StringType(), True), \
                      # sub.txt \ 
                StructField("cik", 	 	 IntegerType(), True), \
                StructField("name", 	 StringType(), True), \
                StructField("sic", 	 	 IntegerType(), True), \
                StructField("countryba", StringType(), True), \
                StructField("stprba", 	 StringType(), True), \
                StructField("cityba", 	 StringType(), True), \
                StructField("zipba", 	 StringType(), True), \
                StructField("bas1", 	 StringType(), True), \
                StructField("bas2", 	 StringType(), True), \
                StructField("baph", 	 StringType(), True), \
                StructField("countryma", StringType(), True), \
                StructField("stprma", 	 StringType(), True), \
                StructField("cityma", 	 StringType(), True), \
                StructField("zipma", 	 StringType(), True), \
                StructField("mas1", 	 StringType(), True), \
                StructField("mas2", 	 StringType(), True), \
                StructField("countryinc",StringType(), True), \
                StructField("stprinc", 	 StringType(), True), \
                StructField("ein", 	 	 IntegerType(), True), \
                StructField("former", 	 StringType(), True), \
                StructField("changed", 	 StringType(), True), \
                StructField("afs", 	 	 StringType(), True), \
                StructField("wksi", 	 IntegerType(), True), \
                StructField("fye", 	     StringType(), True), \
                StructField("form", 	 StringType(), True), \
                StructField("period", 	 DateType(), True),  # date \
                StructField("fy", 	 	 IntegerType(), True), \
                StructField("fp", 	 	 StringType(), True), \
                StructField("filed", 	 DateType(), True), # date \
                StructField("accepted",  StringType(), True), # datetime \
                StructField("prevrpt", 	 IntegerType(), True), \
                StructField("detail", 	 IntegerType(), True), \
                StructField("instance",  StringType(), True), \
                StructField("nciks", 	 IntegerType(), True), \
                StructField("aciks", 	 StringType(), True), \
                      # pre.txt \
                StructField("report", 	 IntegerType(), True), \
                StructField("line", 	 IntegerType(), True), \
                StructField("stmt", 	 StringType(), True), \
                StructField("inpth", 	 IntegerType(), True), \
                StructField("rfile", 	 StringType(), True), \
                StructField("plabel", 	 StringType(), True), \
                StructField("negating",  StringType(), True) \
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the whole dataset
# MAGIC Reading the whole dataset into on DataFrame is just a single line since the read.csv() method also accepts wildcards. the "dateFormat" is defined, as the schema definition also contains DatTypes. 

# COMMAND ----------

df_all = spark.read.csv(all_csv_folders + "*", header=True, dateFormat="yyyyMMdd", schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### print all contained column names

# COMMAND ----------

_ = [print(x, end=", ") for x in df_all.columns] # print the name of the columns for convenience

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge TickerSymbol to the dataset
# MAGIC During the further analysis, it could make sense to know the TickerSymbol and the Exchange where the stock is traded. This information is available in a CSV located at http://rankandfiled.com/static/export/cik_ticker.csv. We simply load it into a dataframe and use the join method to join it with the rest.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Download the file with the cik_ticker mapping

# COMMAND ----------

import urllib.request  


with urllib.request.urlopen("http://rankandfiled.com/static/export/cik_ticker.csv", timeout=30) as urldata,\
        open("/dbfs" + cik_ticker_file, 'wb') as out_file:
    shutil.copyfileobj(urldata, out_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the cik-ticker dataset

# COMMAND ----------

from pyspark.sql.functions import col

df_cik_ticker = spark.read.csv(cik_ticker_file, sep="|", header=True)[['CIK','Ticker','Name','Exchange']] # we are only interested in these columns

# renaming the column
df_cik_ticker = df_cik_ticker.withColumnRenamed('Name', "name_cik_tic") \
                                .withColumnRenamed('Ticker', "ticker") \
                                .withColumnRenamed('Exchange', "exchange") \
                                .withColumn("cik", col("CIK").cast(IntegerType())) # convert the StringType to a IntegerType

# COMMAND ----------

display(df_cik_ticker) # use the display function to provide some insight

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join the test dataframe with the cik_ticker dataframe

# COMMAND ----------

# join the all dataset with the ticker information
df_all_join = df_all.join(df_cik_ticker, ["cik"], "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storing as Parquet with default partitions

# COMMAND ----------

shutil.rmtree("/dbfs" + all_parquet_folder,  ignore_errors=True) # make sure the target folder is empty

df_all_join.write.parquet(all_parquet_folder)

# COMMAND ----------

# MAGIC %md
# MAGIC Storing as Parquet on a Worker Node with 4 CPUs took about 12 minutes. (After about 5 minutes, a second workernode was started with another 4 cpus)<br>
# MAGIC Storing as Parquet on a Worker Node with 8 CPUs took about 8 minutes. (After about 5 minutes, a second workernode was started with another 8 cpus)