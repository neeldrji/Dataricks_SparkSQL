# Databricks notebook source
# MAGIC %md
# MAGIC # SPARK SQL PRACTICE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating dataframe

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
        .load("abfss://source@sparksqlstorageacc.dfs.core.windows.net/raw_data/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Displaying Dataframe

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### creating tempviews

# COMMAND ----------

df.createOrReplaceTempView("ecom")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Ecom

# COMMAND ----------

# MAGIC %sql
# MAGIC select Product_category, Count(*) From ecom 
# MAGIC group by Product_category

# COMMAND ----------

# MAGIC %md
# MAGIC # External vs managed tables

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Creating catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS katalog MANAGED LOCATION 'abfss://source@sparksqlstorageacc.dfs.core.windows.net/raw_data/'

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema katalog.sparksql
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ### CTAS-Crating tables from Views | Managed table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table katalog.sparksql.ecome
# MAGIC as select * from ecom

# COMMAND ----------

# MAGIC %md
# MAGIC ### Droping table

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table katalog.sparksql.ecome

# COMMAND ----------

# MAGIC %md
# MAGIC ### Undroping the table

# COMMAND ----------

# MAGIC %sql
# MAGIC undrop table katalog.sparksql.ecome

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from katalog.sparksql.Ecome

# COMMAND ----------

# MAGIC %md
# MAGIC ### External Table

# COMMAND ----------

# MAGIC %md
# MAGIC 'We need to use Location inorder to Create External table'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table katalog.sparksql.ecome_ext
# MAGIC location 'abfss://source@sparksqlstorageacc.dfs.core.windows.net/raw_data/External_ecome'
# MAGIC as 
# MAGIC select * from ecom;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from katalog.sparksql.Ecome_ext