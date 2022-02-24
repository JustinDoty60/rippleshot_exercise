from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth, first
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from enum import Enum

'''ETL logic for accounts job'''

class Columns(Enum):
    ACCOUNT_ID = 'acount_id'
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


initial_cols = {
    'account_id': StringType(),
    'first_name': StringType(),
    'last_name': StringType(),
    'creation_date': TimestampType(),
    'updated_date': TimestampType(),
    'Postal_Code': IntegerType(),
    'Cardholder_Country': StringType(),
    'Card_status': StringType()
}


renamed_cols = [
    Columns.ACCOUNT_ID.value,
    Columns.FIRST_NAME.value,
    Columns.LAST_NAME.value,
    Columns.CREATION_DATE.value,
    Columns.UPDATED_DATE.value,
    Columns.POSTAL_CODE.value,
    Columns.CARDHOLDER_COUNTRY.value,
    Columns.CARD_STATUS.value
]


def extract_accounts_data(spark: SparkSession) -> DataFrame:
    """Extracts data from accounts.tsv
    :param spark: Spark session object
    :return: Spark DataFrame
    """
    file_path = 'client_data_files/accounts.tsv'

    accounts_schema = (
        StructType([ StructField(k, v, True) for k,v in initial_cols.items() ])
    )

    return (
        spark.read.format("csv")
            .option('delimiter', '\t') # supports .tsv file
            .schema(accounts_schema)
            .option('Header', True)
            .option("timestampFormat", "M/d/y H:m:s")
            .load(file_path)
    )


def transform_accounts_data(df: DataFrame) -> DataFrame:
    """Transforms the accounts data to fit the requirements
    :param df: Spark DataFrame
    :return: Spark DataFrame
    """
    df = rename_cols(df)
    df = filter_to_latest_updated_records(df)
    df = add_partition_cols(df)
    df = deduplicate_rows(df)

    return df
    

def load_accounts_data(df: DataFrame) -> None:
    """Loads the accounts data in parquet format partitioned by year, month, and day
    :param df: Spark DataFrame
    :return: Spark DataFrame
    """
    file_path = 'data_warehouse/accounts'

    (
        df.write.partitionBy('year', 'month', 'day')
            .mode('append')
            .parquet(file_path)
    )

    return None


def rename_cols(df: DataFrame) -> DataFrame:
    """Renames the ingested columns to conform to a naming standard
    :param df: Spark DataFrame
    :return: Spark DataFrame
    """
    for initial_col, renamed_col in zip(initial_cols, renamed_cols):
        df = df.withColumnRenamed(initial_col, renamed_col)

    return df


def add_partition_cols(df: DataFrame) -> DataFrame:
    """Adds the year, month, day partition cols
    :param df: Spark DataFrame
    :return: Spark DataFrame
    """
    timestamp_col = Columns.UPDATED_DATE.value

    df = df.withColumn('year', year(col(timestamp_col)))
    df = df.withColumn('month', month(col(timestamp_col)))
    df = df.withColumn('day', dayofmonth(col(timestamp_col)))

    return df


def filter_to_latest_updated_records(df: DataFrame) -> DataFrame:
    """Filters the data for an account to have 1 record representing the latest updated_date
    :param df: Spark DataFrame
    :return: Spark DataFrame
    """
    timestamp_col = Columns.UPDATED_DATE.value
    max_timstamp_col = 'max_ts'

    window = (
        Window.partitionBy(Columns.ACCOUNT_ID.value)
            .orderBy(col(timestamp_col).desc())
    )

    df = df.withColumn(max_timstamp_col, first(timestamp_col).over(window))
    df = df.where(col(max_timstamp_col) == col(timestamp_col))
    df = df.drop(max_timstamp_col)

    return df


def deduplicate_rows(df: DataFrame) -> DataFrame:
    """Deduplicates rows
    :param df: Spark DataFrame
    :return: Spark DataFrame
    """
    return df.distinct()