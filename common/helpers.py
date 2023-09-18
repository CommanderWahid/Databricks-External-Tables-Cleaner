from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.dbutils import DBUtils
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType, BooleanType, TimestampType
from delta.tables import DeltaTable 
from pyspark.sql.functions import col, regexp_extract

def file_exists(
      spark: SparkSession,
      dir: str
    ) -> bool: 

    """
    Check if a file or folder exists on destination location.
    
    Parameters
    ----------
    spark: SparkSession 
       The entry point to spark on the remote cluster.
    dir: str
       The path of the file or directory.
       The path should be the absolute DBFS path (/absolute/path/).
    
    Raises
    ------
    RuntimeError
       if the user does not have permission to check the target file or directory.
    
    Returns
    -------
    bool
       True: if the path does exist.
       False: if the path doesn't exist or current permissions are insufficient.
    """

    try:
        dbutils = DBUtils(spark)
        dbutils.fs.ls(dir)
    except Exception as e:
       if 'java.nio.file.AccessDeniedException' in str(e):
          raise
       return False
    
    return True


def get_tables(
        spark: SparkSession, 
        store: str,
        schema: str,
        istemporary: bool = False
   )-> list:
    """
    Fetch a list of tables on store.schema and filtering on istemporary

    Parameters
    ----------
    spark: SparkSession 
       The entry point to spark on the remote cluster.
    store: str 
       The metastore name (e.g. hive: 'hive_metastore', unity catalog: 'main')
    schema: str
       The database name.
    istemporary: bool
       True: temporaray objects.
       False: persistent objects.

    Returns
    -------
    list
       A list of object names depending on istemporary value. 
    """
    df = (
         spark
         .sql(f'show tables in {store}.{schema}')
         .where(f"isTemporary = '{str(istemporary)}'")
         .select('tableName')
         .collect()
    )
    ret = [row.tableName for row in df]

    return ret

def get_tables_details(
        spark: SparkSession, 
        store: str,
        schema: str,
        tables:list
   )-> DataFrame:
   """
   Get metadata details of a list of tables.

   Parameters
   ----------
   spark: SparkSession 
      The entry point to spark on the remote cluster.
   store: str 
      The metastore name (e.g. hive: 'hive_metastore', unity catalog: 'main')
   schema: str
      The database name.
   tables: list 
      List of table names.

   Returns
   -------
   DataFrame
      A dataframe containing the details of each table. 
      Here is the structure of the DataFrame [Database,Table,Provider,Type,Location]
   """

   tableDetailsDF = create_empty_dataframe(spark,
                                          ['Database','Table','Provider','Type','Location'],
                                          ['string','string','string','string','string'])
   spark.sql(f"USE CATALOG {store}")
   
   for row in tables:  
      df = ( 
          spark 
          .sql(f"SHOW TABLE EXTENDED IN {store}.{schema} LIKE '{row}';") 
          .select(col('information')) 
      )
      detailsDF = ( 
          df 
          .withColumn('Database', regexp_extract(col('information'), 'Database: (.*)',1)) 
          .withColumn('Table', regexp_extract(col('information'), 'Table: (.*)',1)) 
          .withColumn('Provider', regexp_extract(col('information'), 'Provider: (.*)',1)) 
          .withColumn('Type', regexp_extract(col('information'), 'Type: (.*)',1)) 
          .withColumn('Location', regexp_extract(col('information'), 'Location: (.*)',1)) 
          .drop(col('information')) 
      )
      tableDetailsDF = tableDetailsDF.union(detailsDF) 
   
   tableDetailsDF = tableDetailsDF.where("lower(provider) in ('delta','parquet') and lower(type) <> 'view'")

   return tableDetailsDF



def create_empty_dataframe(
        spark: SparkSession, 
        columns:list, 
        types:list
   )-> DataFrame:
   """
   Create an empty spark dataframe. 

   Parameters
   ----------
   spark: SparkSession 
      The entry point to spark on the remote cluster.
   columns:  list 
      A list of column names.
   types: list
      A list of column types.
      **** Spark data types **** 
   
   Raises
    ------
    RuntimeError
       if the two lists: columns and types are not equals on size.
       column type not handled by the function.

   Returns
   -------
   DataFrame
      A spark dataframe
   """
   if (len(columns)!=len(types)):
       raise "Size mismatch between columns and types!"
   
   fields = []
   for field_name, field_type in zip(columns,types):
      if field_type == "string":
         type_ = StringType()
      elif field_type == "integer":
         type_ = IntegerType()
      elif field_type == "long":
         type_ = LongType()
      elif field_type == "float":
         type_ = FloatType()
      elif field_type == "double":
         type_ = DoubleType()
      elif field_type == "boolean":
         type_ = BooleanType()
      elif field_type == "timestamp":
         type_ = TimestampType()
      else:
         raise TypeError(f"Type: {field_type} is not handled!")

      fields.append(StructField(field_name, type_, True))

   schema = StructType(fields)
   
   df =  spark.createDataFrame([], schema)

   return df

class logs:
   """
   A custom logging class. to delete spark from here no need if we use sdk 
   """
   def __init__(self,name:str,level:str = 'info',debug:bool = True):
        self.name = name
        self.level = level
        self.debug = debug
        self.logger = self.get_logger()

   def get_logger(self):
      """
      Create an instance of log4j_logger.

      Returns
      -------
      debug = True  -> None.
      debug = False -> Instance of log4j_logger to push info to log4j log file.
      """
      logger = None

      if not self.debug: 
         # instantiate log4j using default spark context sc 
         log4jLogger = spark.sparkContext._jvm.org.apache.log4j 
         # get logger from the log manager 
         logger = log4jLogger.LogManager.getLogger(self.name) 

      return logger
      
   def trace(self,msg):
      """
      Write output messages to console or save them in log4j log file.
      """ 
      if (self.debug): 
         print(f'{self.level.upper()}:{self.name}:{msg}') 
      else:
         if self.level == 'debug':
            self.logger.info(msg)
         elif self.level == 'info': 
            self.logger.info(msg) 
         elif self.level == 'warning': 
            self.warn(msg) 
         elif self.level == 'error': 
            self.error(msg) 
         else: 
            pass

def drop_table_definition_without_storage(
      spark: SparkSession,
      df: DataFrame,
      log:logs
   )-> int:
   """
   Drop from the metastore tables without data files in the storage layer.

   Parameters
   ----------
   spark: SparkSession 
      The entry point to spark on the remote cluster.
   df:  dataframe 
      A dataframe containg a list of tables with their metadata.
      Here is the structure of the dataframe [Database,Table,Provider,Type,Location]
   log: logs
      An instance of custom log class.

   Returns
   -------
   int
      The number of deleted tables.
   """

   deleted = 0 

   for row in df.collect(): 
      log.trace(f'----------------- Checking table deletion for {row.Table} -----------------') 

      if row.Provider.lower() == 'delta': 
         if DeltaTable.isDeltaTable(spark,row.Location): 
            log.trace(f'isDeltaTable -> The data already exist in the storage layer. No need for deletion.') 
         else: 
            log.trace(f"Is not a valid delta table -> The data doesn''t exist in the storage layer. Moving forward to delete the table from the schema {row.Database}.") 
            log.trace(f'drop table {row.Database}.{row.Table} ==> in progress ...') 
            spark.sql(f'drop table {row.Database}.{row.Table}') 
            log.trace(f'drop table {row.Database}.{row.Table} ==> done.') 
            deleted += 1 
      else: 
         if file_exists(row.Location): 
            log.trace(f'isParquetTable -> The data already exist in the storage layer. No need for deletion.') 
         else: 
            log.trace(f"Is not a valid parquet table -> The data doesn''t exist in the storage layer. Moving forward to delete the table definition from the catalog {row.Database}.") 
            log.trace(f'drop table {row.Database}.{row.Table} ==> in progress ...')
            spark.sql(f'drop table {row.Database}.{row.Table}') 
            log.trace(f'drop table {row.Database}.{row.Table} ==> done.') 
            deleted += 1 

   return deleted

