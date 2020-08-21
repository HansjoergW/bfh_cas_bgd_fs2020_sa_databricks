# Databricks notebook source
# MAGIC %md
# MAGIC # 01_02_Join_SEC_Data (on Databricks Cluster)

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook contains the code to join the four csv files in the zip file and store it as one csv file on the cluster. It is based on the code from https://github.com/HansjoergW/bfh_cas_bgd_fs2020_sa/blob/master/01_02_Join_SEC_Data.ipynb

# COMMAND ----------

from pathlib import Path
from typing import List, Tuple, Union, Set
from pyspark.sql.dataframe import DataFrame

import shutil          # provides high level file operations
import time            # used to measure execution time
import os
import sys
import zipfile

# COMMAND ----------

# basic definitions
all_zip_file_folder = '/dbfs/usr/sec_zips/'
target_csv_folder = '/dbfs/usr/csv_zip/'
extract_temp_folder = '/dbfs/tmp/extract/'

# COMMAND ----------

# Define constants for the names of the filese inside the zip file
SUB_TXT = "sub.txt"
PRE_TXT = "pre.txt"
NUM_TXT = "num.txt"
TAG_TXT = "tag.txt"

# COMMAND ----------

# create a list with paths to all the zip files
all_zip_path = Path(all_zip_file_folder)
zip_files = [str(file) for file in all_zip_path.glob("*.zip")]

# create that csv target folder, if not present
Path(target_csv_folder).mkdir(parents=True, exist_ok=True) # create directory if necessary

# create that extract temp folder, if not present
Path(extract_temp_folder).mkdir(parents=True, exist_ok=True) # create directory if necessary

# COMMAND ----------

# MAGIC %md
# MAGIC In the following method, two adaptions had to be made (compared to the standalone version). <br>
# MAGIC First, there was no f_temp.close() after writing. This resulted in timing issues: when spark.read.csv tried to read from that file it wasn't always already visible and it failed.<br>
# MAGIC Second, it seems the spark commands on databricks always operate on the DBFS. So in order for the code to work it was necessary to remove "/dbfs" from the pfad when calling sprad.read.csv or df.write.csv().

# COMMAND ----------

def read_csv_in_zip_into_df_extract(zip_file: str, data_file: str) -> DataFrame:
    """
       Extracts the data from zipfile and stores it on disk. 
       Uses spark.csv.read to read the data into the df
    """
    with zipfile.ZipFile(zip_file, "r") as container_zip:
        with container_zip.open(data_file) as f:
            # create a unique tempfile to extract the data
            tempfile = extract_temp_folder +Path(zip_file).name.replace(".zip","").replace("/","").replace("\\","")+"_"+data_file
            
            with open(tempfile, "wb+") as f_temp:
                data = f.read()
                f_temp.write(data)
                f_temp.close()
                f_temp_dbfs  = tempfile.replace("/dbfs","")
         
                df = spark.read.csv(f_temp_dbfs, sep='\t', header=True)
                return df

# COMMAND ----------

def join_files(zip_file: str, target_folder: str) -> str:
    """
        Joins the content of the 3 csv files that are contained in the provided zip_file and 
        create on csv file containing all relevant columns inside target_folder.
    """
    
    target_path = target_folder + Path(zip_file).name.replace(".zip","").replace("/","").replace("\\","")
    
    df_sub = read_csv_in_zip_into_df_extract(zip_file, SUB_TXT)
    df_pre = read_csv_in_zip_into_df_extract(zip_file, PRE_TXT)
    df_num = read_csv_in_zip_into_df_extract(zip_file, NUM_TXT)
    
    df_joined = df_num.join(df_sub, ["adsh"]).join(df_pre, ["adsh","tag","version"],"left")
    
    target_path  = target_path.replace("/dbfs","")
    df_joined.write.csv(target_path, compression="gzip", header=True)
    
    return target_path

# COMMAND ----------

for file in zip_files:
    try: 
        print(join_files(file, target_csv_folder))
    except Exception as ex:
        print("failed: ", file, str(ex))

# COMMAND ----------

# MAGIC %md
# MAGIC Using 4 cores, it took about 17 minutes

# COMMAND ----------

# Helper Code to test with one file
join_files(zip_files[-1], target_csv_folder)

# COMMAND ----------

# Helper Code to clear the target_csv_folder 
shutil.rmtree(target_csv_folder)
Path(target_csv_folder).mkdir(parents=True, exist_ok=True) # create directory after it was deleted

# COMMAND ----------

# Helper Code to clear the extract_temp_folder
shutil.rmtree(extract_temp_folder)
Path(extract_temp_folder).mkdir(parents=True, exist_ok=True) # create directory after it was deleted

# COMMAND ----------

shutil.rmtree("/dbfs/user/")

# COMMAND ----------

