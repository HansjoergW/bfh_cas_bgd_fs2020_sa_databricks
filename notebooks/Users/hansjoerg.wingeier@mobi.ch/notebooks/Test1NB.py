# Databricks notebook source
# MAGIC %md
# MAGIC # Basic Filesystem Handling

# COMMAND ----------

# MAGIC %md
# MAGIC ## List the subfolders of some important folders

# COMMAND ----------

import os
print("cwd              ", os.getcwd()) # display the current working directory
print("content of cwd   ", os.listdir()) # content
print("root dir         ", os.listdir("/"))
print("lokal disk       ", os.listdir("/local_disk0"))
print("content of /dbfs ", os.listdir("/dbfs/")) # root of the dbfs system

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the content of a the root of dbfs with dbutils.fs.ls

# COMMAND ----------

dbutils.fs.ls("/")  # equals to os.listdir("/dbfs/")
# display(dbutils.fs.ls("/")) # displays the content as a table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example on how to download data from the net and store it to the dbfs

# COMMAND ----------

import requests
url = "https://www.sec.gov/include/ticker.txt"
r = requests.get(url, allow_redirects=True)
open('/dbfs/usr/ticker.txt', 'wb').write(r.content)
lines = open('/dbfs/usr/ticker.txt', 'r').readlines()
print(lines[0:5])

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Store data in the FileStore

# COMMAND ----------

dbutils.fs.put("/FileStore/mytestfolder/pure.txt", "some text", overwrite=True)
# content of the FileStore can be accessed from a browser
# access by browser https://adb-2780677283537437.17.azuredatabricks.net/files/mytestfolder/pure.txt?o=<oauthId in url of browser>

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Show help pages of dbutils

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help("put")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Useful links
# MAGIC 
# MAGIC * https://docs.databricks.com/data/index.html Basics about data in Databricks
# MAGIC * https://docs.databricks.com/data/databricks-file-system.html DBFS access
# MAGIC * https://docs.databricks.com/data/filestore.html FileStore -> web-accessbile section of the DBFS
# MAGIC * https://towardsdatascience.com/databricks-how-to-save-files-in-csv-on-your-local-computer-3d0c70e6a9ab examples on towardsdatascience