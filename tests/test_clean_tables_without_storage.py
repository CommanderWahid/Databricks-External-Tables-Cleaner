import pytest
from context import *
from databricks.connect import DatabricksSession
import pandas as pd


@pytest.fixture(scope="session")
def spark() -> DatabricksSession:
  # Create a DatabricksSession (the entry point to Spark functionality) on
  # the cluster in the remote Databricks workspace. Unit tests do not
  # have access to this DatabricksSession by default.
  return DatabricksSession.builder.getOrCreate()


def test_file_exists_should_work_when_path_is_invalid(spark):
    # ARRANGE
    path = '\mnt'
    
    # ACT
    output = file_exists(spark,path)
    expected = False

    # ASSERT 
    assert (output == expected)

def test_file_should_return_true_when_target_folder_exists(spark):
   # ARRANGE
    path = '/mnt'

    # ACT
    output = file_exists(spark,path)
    expected = True

    # ASSERT 
    assert (output == expected)

def test_file_should_return_false_when_target_folder_doesnt_exist(spark):
   # ARRANGE
    path = '/mnt/hello'

    # ACT
    output = file_exists(spark,path)
    expected = False

    # ASSERT 
    assert (output == expected)


def test_get_tables_should_return_just_temporary_tables(spark):
    # ARRANGE
    store = 'hive_metastore'
    schema = 'dbunittest_1'
    spark.sql(f'create schema if not exists {store}.{schema};')
    spark.sql('create or replace temporary view mytemp as select 1 as id;')
    
    # ACT
    output = get_tables(spark,store,schema,True)
    expected = ['mytemp']
    
    # ASSERT
    assert(output==expected) 

    # CLEAN
    spark.sql(f'drop schema if exists {store}.{schema} cascade;')


def test_get_tables_should_return_none_temporary_tables(spark):
    # ARRANGE
    store = 'hive_metastore'
    schema = 'dbunittest_2'
    spark.sql(f'create schema if not exists {store}.{schema};')
    spark.sql(f'create view if not exists {store}.{schema}.myview as select 1 as id;')
    spark.sql(f'create table if not exists {store}.{schema}.mytable (id int);')
    
    # ACT
    output = get_tables(spark,store,schema,False)
    expected = ['mytable','myview']

    #ASSERT
    assert(output==expected)

    # CLEAN
    spark.sql(f'drop schema if exists {store}.{schema} cascade;')


def test_get_table_details_should_not_return_type_as_view_and_provider_notin_parquet_delta(spark):
    # ARRANGE
    store = 'hive_metastore'
    schema = 'dbunittest_3'
    tables = ['mytable_1','mytable_2','myview']
    
    spark.sql(f'create schema if not exists {store}.{schema};')
    spark.sql(f'create view if not exists {store}.{schema}.myview as select 1 as id;')
    spark.sql(f'create table if not exists {store}.{schema}.mytable_1 (id int);')
    spark.sql(f'create table if not exists {store}.{schema}.mytable_2 (id int);')

    # ACT
    output_DF = get_tables_details(spark,store,schema,tables)
    output_Without_Views_DF = output_DF.where("lower(Type) = 'view'")
    output_Without_Not_Parquet_Delta_DF = output_DF.where("lower(Provider) not in ('parquet','delta')")
    
    # ASSERT
    assert (output_Without_Views_DF.isEmpty() == True)
    assert (output_Without_Not_Parquet_Delta_DF.isEmpty() == True)

    # CLEAN
    spark.sql(f'drop schema if exists {store}.{schema} cascade;')

 
def test_create_empty_dataframe(spark):
    # ARRANGE
    columns = ['col1','col2','col3']
    dtypes =    ['str','int32','bool']
    types = ['string','integer','boolean']
 
    expected_pdf = pd.DataFrame(columns=columns, index=None, dtype=str)
    for col, dtype in zip(columns,dtypes):
        expected_pdf[col] = expected_pdf[col].astype(dtype)

    # ACT
    output = create_empty_dataframe(spark,columns,types)
    output_pdf = output.toPandas()

    # ASSERT
    assert(output_pdf.equals(expected_pdf))


def test_drop_table_definition_without_storage_return_zero_when_empty_dataframe_as_argument(spark):
    # ARRANGE
    columns = ['Database','Table','Provider','Type','Location']
    types = ['string','string','string','string','string']

    empty_df = create_empty_dataframe(spark,columns,types)
    log = logs(name='unit_test')
    expected = 0

    # ACT
    output = drop_table_definition_without_storage(spark,empty_df,log)

    # ASSERT 
    assert (output == expected)

