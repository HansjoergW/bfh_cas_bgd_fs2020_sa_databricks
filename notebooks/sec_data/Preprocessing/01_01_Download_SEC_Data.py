# Databricks notebook source
# MAGIC %md
# MAGIC # 01_01_Download_SEC_Data (to Databricks Cluster)

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook contains the code to download the business report data of all companies provided by the SEC (US Security and Exchange Commission).
# MAGIC There is a zip-file for each quarter since 2009, but only in files starting from 2012 do contain all report data off all companies which reported in that period.
# MAGIC 
# MAGIC The same code as in https://github.com/HansjoergW/bfh_cas_bgd_fs2020_sa/blob/master/02_download_from_sec.ipynb udf example is used.

# COMMAND ----------

import urllib.request  # used to download resources from the web 
import shutil          # provides high level file operations
import time            # used to measure execution time
import os

# COMMAND ----------

# basic definitions
output_path = '/dbfs/usr/sec_zips/'

# COMMAND ----------

from pathlib import Path
Path(output_path).mkdir(parents=True, exist_ok=True) # create directory if necessary

# COMMAND ----------

# MAGIC %md
# MAGIC ## prepare download urls

# COMMAND ----------

# definitions to create download urls
sec_base_path = "https://www.sec.gov/files/dera/data/financial-statement-data-sets/"
start_year = 2009        # start year to download the data
end_year   = 2020        # end year for download
format_str = "{}q{}.zip" # all file names are like 2020q1.zip 

# COMMAND ----------

# create list with all download links
download_urls = []
for year in range(start_year, end_year + 1):
    for quarter in range(1,5):
        download_urls.append(sec_base_path + format_str.format(year, quarter))

# COMMAND ----------

# MAGIC %md
# MAGIC Unfortunately, the file for 2020q1.zip is located at a different location. For what reason ever. Of course, we could parse the site https://www.sec.gov/dera/data/financial-statement-data-sets.html to extract download links directly from there, but since this thesis isn't about parsing and scrapping data from html, we simply add the proper link to the list.

# COMMAND ----------

download_urls.append("https://www.sec.gov/files/node/add/data_distribution/2020q1.zip")

# COMMAND ----------

# MAGIC %md
# MAGIC Convert the list to a spark dataframe. Using the display() function, we can diretcly show the content as a table.

# COMMAND ----------

from pyspark.sql.types import StringType

download_urls_df = spark.createDataFrame(download_urls, StringType())
download_urls_df = download_urls_df.withColumnRenamed("value","url")
download_urls_df.printSchema()
display(download_urls_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## get spark context

# COMMAND ----------

spark # there is no need to intialize the spark context by yourself, databricks takes care of it and directly provides the instantiated spark variable

# COMMAND ----------

# MAGIC %md
# MAGIC ## download the data

# COMMAND ----------

# the download function as defined in https://github.com/HansjoergW/bfh_cas_bgd_fs2020_sa/blob/master/02_download_from_sec.ipynb

def downloader_function(url):
  """
  
  """
  
  # From URL construct the destination path and filename.
  file_name = os.path.basename(urllib.parse.urlparse(url).path)
  file_path = os.path.join(output_path, file_name) 

  # Check if the file has already been downloaded.
  if os.path.exists(file_path):
      return "already downloaded"

  # Download and write to file.
  try:
      with urllib.request.urlopen(url, timeout=30) as urldata,\
              open(file_path, 'wb') as out_file:
          shutil.copyfileobj(urldata, out_file)
          return "success"
  except Exception as ex:
      return "failed"

# COMMAND ----------

# wrap the function into a udf function
from pyspark.sql.functions import udf
downloader_udf = udf(lambda s: downloader_function(s), StringType())

# COMMAND ----------

start_time = time.time()
result_df =  download_urls_df.select('url', downloader_udf('url').alias('result'))
result_df_failed = result_df.filter("result='failed'").collect()
execution_time = (time.time() - start_time)
print("execution time:      ", execution_time)
display(result_df_failed)

# COMMAND ----------

# MAGIC %md
# MAGIC By using just 4 cores (and therefore 4 tasks in parallel) it took about 30 seconds to download the whole 1.5 GB into DBFS storage.
# MAGIC 
# MAGIC By using a configuration with 8 cores, it took about 17 seconds to download the whole 1.5 GB into DBFS storage.

# COMMAND ----------

# remove content of target directory
shutil.rmtree(output_path)

# COMMAND ----------

