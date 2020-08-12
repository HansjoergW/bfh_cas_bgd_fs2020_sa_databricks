# Databricks notebook source
import os
print(os.getcwd())

os.listdir("../../tmp")
dbutils.fs.ls("/usr/")

# COMMAND ----------

display(dbutils.fs.ls("/usr/"))

# COMMAND ----------

import requests
url = "https://www.sec.gov/include/ticker.txt"
r = requests.get(url, allow_redirects=True)
open('/dbfs/usr/ticker.txt', 'wb').write(r.content)
# dbutils.fs.cp('dbfs:/usr/ticker.txt','dbfs:/FileStore/ticker.txt')
#open('/usr/ticker.txt', 'r').readlines()

# COMMAND ----------

type(dbutils)

# COMMAND ----------

dbutils.fs.put("/usr/mytestfolder/pure.txt", "some text")
# access by browser https://adb-2780677283537437.17.azuredatabricks.net/files/mytestfolder/pure.txt?o=<oauthId in url of browser>

# COMMAND ----------

os.listdir("/usr/")

# COMMAND ----------

