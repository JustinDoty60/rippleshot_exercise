from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import when, col, year, month, dayofmonth
from enum import Enum

'''ETL logic for auths job'''

class DWColumns(Enum):
    ACCOUNT_ID = 'account_id'
    TRANSMIT_TIME = 'transmit_time'
    TRANSACTION_TYPE = 'transaction_type'
    MERCHANT_COUNTRY = 'merchant_country'
    AMOUNT = 'amount'
    MERCHANT_NAME = 'merchant_name'
    YEAR = 'year'
    MONTH = 'month'
    DAY = 'day'


source_cols = [
    'account_id',
    'Transmit Time',
    'Transaction Type',
    'Merchant Country',
    'Amount',
    'merchant_name'
]


data_warehouse_cols = [
    DWColumns.ACCOUNT_ID.value,
    DWColumns.TRANSMIT_TIME.value,
    DWColumns.TRANSACTION_TYPE.value,
    DWColumns.MERCHANT_COUNTRY.value,
    DWColumns.AMOUNT.value,
    DWColumns.MERCHANT_NAME.value,
    DWColumns.YEAR.value,
    DWColumns.MONTH.value,
    DWColumns.DAY.value
]


partition_cols = [
    DWColumns.YEAR.value,
    DWColumns.MONTH.value,
    DWColumns.DAY.value
]


def extract_auths_data(spark: SparkSession) -> DataFrame:
    """Extracts data from auths.csv"""

    file_path = 'client_data_files/auths.csv'

    return (
        spark.read.format('csv')
            .option('delimiter', ',')
            .option('inferSchema', True) # not ideal, but a workaround for empty cols
            .option('Header', True)
            .option('timestampFormat', 'M/d/y H:m:s')
            .load(file_path)
    )


def transform_auths_data(df: DataFrame) -> DataFrame:
    """Transforms the auths data to fit the requirements"""

    df = remove_empty_cols(df)
    df = rename_cols(df)
    df = clean_merchant_name_col(df)
    df = add_partition_cols(df)
    df = deduplicate_rows(df)

    return df
    

def load_auths_data(df: DataFrame) -> None:
    """Loads the auths data in parquet format partitioned by year, month, and day"""

    file_path = 'data_warehouse/auths'

    (
        df.write.partitionBy(partition_cols)
            .mode('append')
            .parquet(file_path)
    )

    return None


def remove_empty_cols(df: DataFrame) -> DataFrame:
    """Removes the empty cols that were ingested"""

    return df.select(*source_cols)


def rename_cols(df: DataFrame) -> DataFrame:
    """Renames the ingested source columns to conform to a naming standard"""

    filtered_date_warehouse_cols = [col for col in data_warehouse_cols if col not in partition_cols]

    for initial_col, renamed_col in zip(source_cols, filtered_date_warehouse_cols):
        df = df.withColumnRenamed(initial_col, renamed_col)

    return df


def clean_merchant_name_col(df: DataFrame) -> DataFrame:
    """Renames merchant_name data to conform to a naming standard"""

    walmart_vals = ['walmrt', 'walmart', 'Walmrt']
    target_vals = ['target', 'Target']
    google_vals = ['google']
    amazon_vals = ['amazon', 'amzn']
    walgreens_vals = ['Walgreens']
    home_depot_vals = ['Home depot']
    cvs_vals = ['Cvs']

    # could utilize regex for a more sustainable solution
    return df.withColumn(
        "merchant_name",
        when(df['merchant_name'].isin(walmart_vals), 'walmart')
        .when(df['merchant_name'].isin(target_vals), 'target')
        .when(df['merchant_name'].isin(google_vals), 'google')
        .when(df['merchant_name'].isin(amazon_vals), 'amazon')
        .when(df['merchant_name'].isin(walgreens_vals), 'walgreens')
        .when(df['merchant_name'].isin(home_depot_vals), 'home_depot')
        .when(df['merchant_name'].isin(cvs_vals), 'cvs')
        .otherwise(df["merchant_name"])
    )   


def add_partition_cols(df: DataFrame) -> DataFrame:
    """Adds the year, month, day partition cols"""

    timestamp_col = DWColumns.TRANSMIT_TIME.value

    df = df.withColumn(DWColumns.YEAR.value, year(col(timestamp_col)))
    df = df.withColumn(DWColumns.MONTH.value, month(col(timestamp_col)))
    df = df.withColumn(DWColumns.DAY.value, dayofmonth(col(timestamp_col)))

    return df


def deduplicate_rows(df: DataFrame) -> DataFrame:
    """Deduplicates rows"""

    return df.distinct()