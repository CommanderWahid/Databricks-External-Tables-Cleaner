from context import (logs, get_tables, get_tables_details,
                     drop_table_definition_without_storage)

dbutils.widgets.text('store', 'hive_metastore')  
dbutils.widgets.text('schema', 'default') 
dbutils.widgets.dropdown('debug', 'True', ['True','False']) 

store = dbutils.widgets.get("store")
schema = dbutils.widgets.get("schema")  
debug = dbutils.widgets.get("debug") 

logger = logs(name="CleanTableLogger", level='info', debug=eval(f'{debug}'))

logger.trace(f"Get the list of tables and their details store={store} schema={schema}") 
tables = get_tables(spark,store=f'{store}',schema=f'{schema}',istemporary=False) 
tabledetailsDF = get_tables_details(spark,store=f'{store}',schema=f'{schema}',tables=tables)

tocheck = tabledetailsDF.count()  
logger.trace(f'Found {tocheck} tables to be inspected')

deleted = drop_table_definition_without_storage(spark,tabledetailsDF,logger)
logger.trace(f'Cleaning tables without data from the store {store} and the catalog {schema} ==> {deleted} out of {tocheck}') 