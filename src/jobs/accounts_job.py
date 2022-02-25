from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from enum import Enum

'''ETL logic for accounts job'''

class DWColumns(Enum):
    ACCOUNT_ID = 'account_id'
    FIRST_NAME = 'first_name'
    LAST_NAME = 'last_name'
    CREATION_DATE = 'creation_date'
    UPDATED_DATE = 'updated_date'
    POSTAL_CODE = 'postal_code'
    CARDHOLDER_COUNTRY = 'cardholder_country'
    CARD_STATUS = 'card_status'
    YEAR = 'year'
    MONTH = 'month'
    DAY = 'day'


source_cols = {
    'account_id': StringType(),
    'first_name': StringType(),
    'last_name': StringType(),
    'creation_date': TimestampType(),
    'updated_date': TimestampType(),
    'Postal_Code': IntegerType(),
    'Cardholder_Country': StringType(),
    'Card_status': StringType()
}


data_warehouse_cols = {
    DWColumns.ACCOUNT_ID.value: StringType(),
    DWColumns.FIRST_NAME.value: StringType(),
    DWColumns.LAST_NAME.value: StringType(),
    DWColumns.CREATION_DATE.value: TimestampType(),
    DWColumns.UPDATED_DATE.value: TimestampType(),
    DWColumns.POSTAL_CODE.value: IntegerType(),
    DWColumns.CARDHOLDER_COUNTRY.value: StringType(),
    DWColumns.CARD_STATUS.value: StringType(),
    DWColumns.YEAR.value: IntegerType(),
    DWColumns.MONTH.value: IntegerType(),
    DWColumns.DAY.value: IntegerType()
}


partition_cols = [
    DWColumns.YEAR.value,
    DWColumns.MONTH.value,
    DWColumns.DAY.value
]


accounts_source_schema = (
    StructType([ StructField(k, v, False) for k,v in source_cols.items() ])
)


accounts_data_warehouse_schema = (
    StructType([ StructField(k, v, False) for k,v in data_warehouse_cols.items() ])
)


filtered_date_warehouse_cols = [k for k in data_warehouse_cols.keys() if k not in partition_cols]


data_warehouse_file_path = 'data_warehouse/accounts'


def extract_accounts_data_from_source(spark: SparkSession) -> DataFrame:
    '''Extracts data from accounts.tsv'''

    file_path = 'client_data_files/accounts.tsv'

    return (
        spark.read.format('csv')
            .option('delimiter', '\t') # supports .tsv file
            .schema(accounts_source_schema)
            .option('Header', True)
            .option('timestampFormat', 'M/d/y H:m:s')
            .load(file_path)
    )


def extract_accounts_data_from_data_warehouse(spark: SparkSession) -> DataFrame:
    '''Extracts data from the data warehouse in parquet format'''

    file_path = data_warehouse_file_path

    return (
        spark.read.schema(accounts_data_warehouse_schema)
            .parquet(file_path)
    )


def transform_accounts_data(df: DataFrame, spark: SparkSession) -> DataFrame:
    '''Transforms the accounts data to fit the requirements'''

    df = rename_cols(df)
    df = add_partition_cols(df)

    dw_df = extract_accounts_data_from_data_warehouse(spark)
    dw_df.cache() # allows read/write from the same parquet
    dw_df.checkpoint() # workaround that allows us to evaluate the cache now

    df = union_data_warehouse(df, dw_df)
    df = filter_to_latest_updated_records(df)

    return df
    

def load_accounts_data(df: DataFrame, spark: SparkSession) -> None:
    '''Loads the accounts data in parquet format partitioned by year, month, and day'''

    file_path = data_warehouse_file_path

    (
        df.write.partitionBy(partition_cols)
            .mode('overwrite')
            .parquet(file_path)
    )

    return None


def rename_cols(df: DataFrame) -> DataFrame:
    '''Renames the ingested source columns to conform to a naming standard'''

    for source_col, date_warehouse_col in zip(source_cols.keys(), filtered_date_warehouse_cols):
        df = df.withColumnRenamed(source_col, date_warehouse_col)

    return df


def union_data_warehouse(df: DataFrame, dw_df: DataFrame) -> DataFrame:
    '''Unions the existing data warehouse with the current batch of data'''

    df = dw_df.union(df)

    return df


def filter_to_latest_updated_records(df: DataFrame) -> DataFrame:
    '''Filters the data for an account to have 1 record representing the latest updated_date'''

    updated_date_col = DWColumns.UPDATED_DATE.value
    row_num_col = 'row_num'

    window = (
        Window.partitionBy(DWColumns.ACCOUNT_ID.value)
            .orderBy(col(updated_date_col).desc())
    )

    df = df.withColumn(row_num_col, row_number().over(window))
    df = df.where(col(row_num_col) == 1)
    df = df.drop(row_num_col)

    return df


def add_partition_cols(df: DataFrame) -> DataFrame:
    '''Adds the year, month, day partition cols'''

    timestamp_col = DWColumns.UPDATED_DATE.value

    df = df.withColumn(DWColumns.YEAR.value, year(col(timestamp_col)))
    df = df.withColumn(DWColumns.MONTH.value, month(col(timestamp_col)))
    df = df.withColumn(DWColumns.DAY.value, dayofmonth(col(timestamp_col)))

    return df