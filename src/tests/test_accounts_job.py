from pyspark.sql.functions import to_timestamp, col
from tests.helpers import schema_test, data_test
from pyspark.sql.types import IntegerType, TimestampType
from jobs.accounts_job import (
    rename_cols, 
    add_partition_cols, 
    union_data_warehouse,
    filter_to_latest_updated_records,
    DWColumns,
    source_cols,
    data_warehouse_cols as dw_cols,
    filtered_date_warehouse_cols,
    partition_cols
)

'''Pytest script for accounts_job'''


def test_rename_cols(sql_context):

    input_schema = source_cols
    input_data = [
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', 'John', 'Doe', '2/7/2021 2:20:30', '2/7/2021 2:20:30', 50023, 'USA', 'active')
    ]
    
    input = sql_context.createDataFrame(input_data).toDF(*input_schema)

    expected_schema = filtered_date_warehouse_cols
    expected_data = [
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', 'John', 'Doe', '2/7/2021 2:20:30', '2/7/2021 2:20:30', 50023, 'USA', 'active')
    ]

    expected_output = sql_context.createDataFrame(expected_data).toDF(*expected_schema)

    real_output = rename_cols(input)

    assert schema_test(expected_output, real_output)
    assert data_test(expected_output, real_output)


def test_add_partition_cols(sql_context):

    timestamp_col = DWColumns.UPDATED_DATE.value
    timestamp_format = 'M/d/y H:m:s'

    input_schema = [timestamp_col]
    input_data = [
        ('2/7/2022 2:20:30'),
        ('2/1/2022 2:20:30'),
        ('2/6/2022 2:20:30'),
        ('2/8/2022 2:20:30')
    ]
    
    input = sql_context.createDataFrame(input_data, 'string').toDF(*input_schema)
    input = input.withColumn(
        timestamp_col, 
        to_timestamp(timestamp_col, timestamp_format)
    )

    expected_schema = [timestamp_col] + partition_cols
    expected_data = [
        ('2/7/2022 2:20:30', 2022, 2, 7),
        ('2/1/2022 2:20:30', 2022, 2, 1),
        ('2/6/2022 2:20:30', 2022, 2, 6),
        ('2/8/2022 2:20:30', 2022, 2, 8)
    ]

    expected_output = sql_context.createDataFrame(expected_data).toDF(*expected_schema)
    expected_output = expected_output.withColumn(
        timestamp_col, 
        to_timestamp(timestamp_col, timestamp_format)
    )
    expected_output = expected_output.select(
        col(timestamp_col).cast(TimestampType()),
        col(DWColumns.YEAR.value).cast(IntegerType()),
        col(DWColumns.MONTH.value).cast(IntegerType()),
        col(DWColumns.DAY.value).cast(IntegerType())
    )

    real_output = add_partition_cols(input)

    assert schema_test(expected_output, real_output)
    assert data_test(expected_output, real_output)


def test_union_data_warehouse(sql_context):

    data_warehouse_cols = dw_cols.keys()

    input_schema = data_warehouse_cols
    input_data = [
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', 'John', 'Doe', '2/7/2021 2:20:30', '2/7/2021 2:20:30', 50023, 'USA', 'active', 2021, 2, 7)
    ]
    
    input = sql_context.createDataFrame(input_data).toDF(*input_schema)

    dw_input_schema = data_warehouse_cols
    dw_input_data = [
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', 'John', 'Doe', '2/7/2021 2:20:30', '2/7/2021 2:20:30', 50023, 'USA', 'active', 2021, 2, 7),
        ('bd1f9bb3-51e0-48ce-bd21-489aff77c5b2', 'Elaine', 'Smith', '3/14/2020 2:20:30', '3/14/2020 2:20:30', 20035, 'USA', 'closed', 2020, 3, 14),
        ('fce69fd8-0748-4f12-865e-0d387eea8cc5', 'Bob', 'Doe', '1/15/2022 2:20:30', '1/15/2022 2:20:30', 54789, 'USA', 'closed', 2022, 1, 15)
    ]
    
    dw_input = sql_context.createDataFrame(dw_input_data).toDF(*dw_input_schema)

    expected_schema = data_warehouse_cols
    expected_data = [
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', 'John', 'Doe', '2/7/2021 2:20:30', '2/7/2021 2:20:30', 50023, 'USA', 'active', 2021, 2, 7),
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', 'John', 'Doe', '2/7/2021 2:20:30', '2/7/2021 2:20:30', 50023, 'USA', 'active', 2021, 2, 7),
        ('bd1f9bb3-51e0-48ce-bd21-489aff77c5b2', 'Elaine', 'Smith', '3/14/2020 2:20:30', '3/14/2020 2:20:30', 20035, 'USA', 'closed', 2020, 3, 14),
        ('fce69fd8-0748-4f12-865e-0d387eea8cc5', 'Bob', 'Doe', '1/15/2022 2:20:30', '1/15/2022 2:20:30', 54789, 'USA', 'closed', 2022, 1, 15)
    ]

    expected_output = sql_context.createDataFrame(expected_data).toDF(*expected_schema)

    real_output = union_data_warehouse(input, dw_input)

    assert schema_test(expected_output, real_output)
    assert data_test(expected_output, real_output)


def test_filter_to_latest_updated_records(sql_context):

    input_schema = [DWColumns.ACCOUNT_ID.value, DWColumns.UPDATED_DATE.value]
    input_data = [
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/7/2021 2:20:30'),
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/7/2021 2:20:30'),
        ('bd1f9bb3-51e0-48ce-bd21-489aff77c5b2', '3/14/2020 2:20:30'),
        ('bd1f9bb3-51e0-48ce-bd21-489aff77c5b2', '3/15/2020 2:20:30'),
        ('fce69fd8-0748-4f12-865e-0d387eea8cc5', '1/15/2022 2:20:30'),
        ('65cf133d-8e32-41f3-bf87-96c718688d6b', '2/15/2020 2:20:30')
    ]
    
    input = sql_context.createDataFrame(input_data).toDF(*input_schema)

    expected_schema = [DWColumns.ACCOUNT_ID.value, DWColumns.UPDATED_DATE.value]
    expected_data = [
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/7/2021 2:20:30'),
        ('bd1f9bb3-51e0-48ce-bd21-489aff77c5b2', '3/15/2020 2:20:30'),
        ('fce69fd8-0748-4f12-865e-0d387eea8cc5', '1/15/2022 2:20:30'),
        ('65cf133d-8e32-41f3-bf87-96c718688d6b', '2/15/2020 2:20:30')    
    ]

    expected_output = sql_context.createDataFrame(expected_data).toDF(*expected_schema)

    real_output = filter_to_latest_updated_records(input)

    assert schema_test(expected_output, real_output)
    assert data_test(expected_output, real_output)