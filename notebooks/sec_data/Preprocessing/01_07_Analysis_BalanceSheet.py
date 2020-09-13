# Databricks notebook source
# MAGIC %md
# MAGIC # 01_07_Analysis_BalanceSheet

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook conducts some example analysis on the BalanceSheet data. <br>
# MAGIC It is based on the Jupyter Notebook https://github.com/HansjoergW/bfh_cas_bgd_fs2020_sa/blob/master/01_07_Analysis_BalanceSheet.ipynb.

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

import matplotlib.pyplot as plt
import seaborn as sns;
sns.set()

# COMMAND ----------

# folder with the pivoted BalanceSheet data
all_bs_folder = "/usr/parq_bs"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the dataset

# COMMAND ----------

df_all = spark.read.parquet(all_bs_folder).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic statistics

# COMMAND ----------

print(df_all.shape)
print(df_all.info())

# COMMAND ----------

# MAGIC %md
# MAGIC we expect that most of the BalanceSheets will have a value in the first few data columns.. so lets check that

# COMMAND ----------

nas_sorted = df_all.iloc[:,5:].isna().sum().sort_values()
nas_sorted.head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC As we can see, only Assets and LiabilitiesAndStockholdersEquity seem to be present. it might be that there is something special with entry that don't have an value in Assets.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation

# COMMAND ----------

df_cleaned = df_all[df_all.Assets.notnull() & df_all.LiabilitiesAndStockholdersEquity.notnull()]
df_cleaned.shape

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze a single company -> Apple

# COMMAND ----------

df_apple = df_cleaned[df_cleaned.ticker == 'AAPL']  
df_apple = df_apple.sort_values('period').set_index('period')  
df_apple.shape  

# COMMAND ----------

fig = plt.figure(figsize=(15,5))
plt.plot(df_apple.Assets, label = "Assets")
plt.plot(df_apple.AssetsCurrent, label = "AssetsCurrent")
plt.plot(df_apple.Assets - df_apple.AssetsCurrent, label="diff")
plt.plot(df_apple.AssetsNoncurrent, label = "AssetsNonCurrent")
plt.legend()
plt.show()

# COMMAND ----------

fig = plt.figure(figsize=(15,5))
plt.plot(df_apple.Liabilities, label = "Liabilities")
plt.plot(df_apple.LiabilitiesCurrent, label = "LiabilitiesCurrent")
plt.plot(df_apple.Liabilities - df_apple.LiabilitiesCurrent, label="diff")
plt.plot(df_apple.LiabilitiesNoncurrent, label = "LiabilitiesNonCurrent")
plt.legend()
plt.show()

# COMMAND ----------

# current ratio
(df_apple.AssetsCurrent / df_apple.LiabilitiesCurrent).plot(figsize=(15,5))

# COMMAND ----------

fig = plt.figure(figsize=(15,5))
plt.plot(df_apple.StockholdersEquity, label = "Equity")
plt.plot(df_apple.RetainedEarningsAccumulatedDeficit, label = "Earnings")
plt.legend()
plt.show()

# COMMAND ----------

df_apple.CommonStockSharesAuthorized.plot(figsize=(15,5))

# COMMAND ----------

df_apple[18:21][['CommonStockSharesAuthorized']]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze the whole dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ### Correlation between earnings in the past and future earnings

# COMMAND ----------

# First, we only consider "yearly" reports: 10-K.
yearly_earnings = df_cleaned[df_cleaned.form == "10-K"]

# we are only interested in the ticker, cik and Earnings columns. the period is in the index
yearly_earnings = yearly_earnings[['period','ticker', 'cik','RetainedEarningsAccumulatedDeficit']]

# extracting the year from the index as a column
yearly_earnings['year'] =  pd.DatetimeIndex(yearly_earnings.period).year
yearly_earnings.head()

# COMMAND ----------

# MAGIC %md
# MAGIC It could happen that for some special reason there is more than one yearly report during a year. We cannot keep thes entries, since pivoting the table will not be possible anymore. The following lines remove such companies from the dataset.

# COMMAND ----------

# we create a crosstab between cik and year, normally every cell should contain a 1. Therfore we select the ones "> 1".
# This produces a TRUE / FALSE Matrix witth cik and year. Then we sum rowwise.
counts = (pd.crosstab(yearly_earnings.cik, yearly_earnings.year)>1).sum(axis=1)

# the indexes of rows in the counts table with counts > 0 are the ciks with multiple entries in at least one year and have to be removed
multiple_entries = counts[counts > 0].index.tolist()

# removing the entries
yearly_earnings = yearly_earnings[~yearly_earnings['cik'].isin(multiple_entries)]

# COMMAND ----------

# next we can pivot the data. we want to hove a column for every year and every cik should have its own row
earnings_per_year = yearly_earnings.pivot(index="cik", columns='year', values = 'RetainedEarningsAccumulatedDeficit')
earnings_per_year.head()

# COMMAND ----------

# we wanna calculate the average "earnings"-gain between 2012 and 2016, as well as between 2016 and 2017, so we just need these columns
earnings_per_year_selected = earnings_per_year[[2012,2016,2017]]

# we removing entries with uncomplete date
earnings_per_year_selected  = earnings_per_year_selected .dropna()

# COMMAND ----------

# calculating the "earning" gains
# a value of 1 means 100 percent gain per year
earnings_per_year_selected['12-16'] = ((earnings_per_year_selected[2016]  / earnings_per_year_selected[2012]) ** 0.25) - 1
earnings_per_year_selected['16-17'] = (earnings_per_year_selected[2017] /  earnings_per_year_selected[2016]) -1

# COMMAND ----------

# by just plotting we see that there are extrem outliers
ax = sns.regplot(data = earnings_per_year_selected, x = '12-16', y = '16-17',  marker="+", fit_reg=True)
ax.grid(b=True, which='major', color='b', linestyle='-')

# COMMAND ----------

# because auf the outlier, it makes sence to reduce the dataset. We are only interested in values which have +/1 100 percent gains
earnings_per_year_selected = earnings_per_year_selected[earnings_per_year_selected['12-16'] < 1]
earnings_per_year_selected = earnings_per_year_selected[earnings_per_year_selected['12-16'] > -1]
earnings_per_year_selected = earnings_per_year_selected[earnings_per_year_selected['16-17'] < 1]
earnings_per_year_selected = earnings_per_year_selected[earnings_per_year_selected['16-17'] > -1]

# COMMAND ----------

fig, ax = plt.subplots(figsize=(11,10))
ax.grid(b=True, which='major', color='b', linestyle='-')
sns.regplot(data = earnings_per_year_selected, x = '12-16', y = '16-17', ax=ax, marker="+", fit_reg=True)


# COMMAND ----------

