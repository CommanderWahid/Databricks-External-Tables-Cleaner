# Databricks notebook source
# MAGIC %md 
# MAGIC ### Setup Phase
# MAGIC ###### Import libraries, initialize notebook parameters and get logger instance

# COMMAND ----------
from context import (logs, get_tables, get_tables_details,
                     drop_table_definition_without_storage)
 # COMMAND ----------
dbutils.widgets.text('store', 'hive_metastore')  
dbutils.widgets.text('schema', 'default') 
dbutils.widgets.dropdown('debug', 'True', ['True','False']) 

store = dbutils.widgets.get("store") 
schema = dbutils.widgets.get("schema") 
debug = dbutils.widgets.get("debug") 
 # COMMAND ----------

logger = logs(name='CleanTableLogger',level='info', debug=eval(f'{debug}'))
# COMMAND ----------

# MAGIC %md 
# MAGIC ### Main code

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Get tables details

# COMMAND ----------
logger.trace(f"Get the list of tables and their details store={store} schema={schema}") 

tables = get_tables(spark,store=f'{store}',schema=f'{schema}',istemporary=False) 
tabledetailsDF = get_tables_details(spark,store=f'{store}',schema=f'{schema}',tables=tables)

# COMMAND ----------
# MAGIC %md 
# MAGIC ###### Drop tables without storage

# COMMAND ----------
tocheck = tabledetailsDF.count()  
logger.trace(f'Found {tocheck} tables to be inspected')

deleted = drop_table_definition_without_storage(spark,tabledetailsDF,logger) 
logger.trace(f'Cleaning tables without data from the store {store} and the catalog {schema} ==> {deleted} out of {tocheck}') 
