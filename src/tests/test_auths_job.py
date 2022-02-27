from pyspark.sql.functions import to_timestamp, col
from tests.helpers import schema_test, data_test
from pyspark.sql.types import IntegerType, TimestampType
from jobs.auths_job import (
    extract_auths_data_from_source,
    extract_auths_data_from_data_warehouse,
    remove_empty_cols, 
    rename_cols, 
    clean_merchant_name_col,
    add_partition_cols, 
    union_data_warehouse,
    deduplicate_rows,
    DWColumns,
    source_cols,
    data_warehouse_cols as dw_cols,
    filtered_data_warehouse_cols,
    partition_cols
)

'''Pytest script for auths_job'''


def test_extract_auths_data_from_source(spark_session):

    df = extract_auths_data_from_source(spark_session)

    assert df


def test_extract_auths_data_from_data_warehouse(spark_session):

    df = extract_auths_data_from_data_warehouse(spark_session)

    assert df


def test_remove_empty_cols(spark_session):

    input_schema = source_cols + ['col_1', 'col_2']
    input_data = [
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/7/2022 2:20:30', 'debit', 'USA', 44.33, 'amazon', 'cv_1', 'cv_2')
    ]
    
    input = spark_session.createDataFrame(input_data).toDF(*input_schema)

    expected_schema = source_cols
    expected_data = [
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/7/2022 2:20:30', 'debit', 'USA', 44.33, 'amazon')
    ]

    expected_output = spark_session.createDataFrame(expected_data).toDF(*expected_schema)

    real_output = remove_empty_cols(input)

    assert schema_test(expected_output, real_output)
    assert data_test(expected_output, real_output)


def test_rename_cols(spark_session):

    input_schema = source_cols
    input_data = [
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/7/2022 2:20:30', 'debit', 'USA', 44.33, 'amazon')
    ]
    
    input = spark_session.createDataFrame(input_data).toDF(*input_schema)

    expected_schema = filtered_data_warehouse_cols
    expected_data = [
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/7/2022 2:20:30', 'debit', 'USA', 44.33, 'amazon')
    ]

    expected_output = spark_session.createDataFrame(expected_data).toDF(*expected_schema)

    real_output = rename_cols(input)

    assert schema_test(expected_output, real_output)
    assert data_test(expected_output, real_output)


def test_clean_merchant_name_col(spark_session):

    input_schema = [DWColumns.MERCHANT_NAME.value]
    input_data = [
        ('walmrt'),
        ('walmart'),
        ('Walmrt'),
        ('target'),
        ('Target'),
        ('google'),
        ('amazon'),
        ('amzn'),
        ('Walgreens'),
        ('Home depot'),
        ('Cvs')
    ]
    
    input = spark_session.createDataFrame(input_data, 'string').toDF(*input_schema)

    expected_schema = [DWColumns.MERCHANT_NAME.value]
    expected_data = [
        ('walmart'),
        ('walmart'),
        ('walmart'),
        ('target'),
        ('target'),
        ('google'),
        ('amazon'),
        ('amazon'),
        ('walgreens'),
        ('home_depot'),
        ('cvs')
    ]

    expected_output = spark_session.createDataFrame(expected_data, 'string').toDF(*expected_schema)

    real_output = clean_merchant_name_col(input)

    assert schema_test(expected_output, real_output)
    assert data_test(expected_output, real_output)


def test_add_partition_cols(spark_session):

    timestamp_col = DWColumns.TRANSMIT_TIME.value
    timestamp_format = 'M/d/y H:m:s'

    input_schema = [timestamp_col]
    input_data = [
        ('2/7/2022 2:20:30'),
        ('2/1/2022 2:20:30'),
        ('2/6/2022 2:20:30'),
        ('2/8/2022 2:20:30')
    ]
    
    input = spark_session.createDataFrame(input_data, 'string').toDF(*input_schema)
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

    expected_output = spark_session.createDataFrame(expected_data).toDF(*expected_schema)
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


def test_union_data_warehouse(spark_session):

    data_warehouse_cols = dw_cols.keys()

    input_schema = data_warehouse_cols
    input_data = [
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/7/2022 2:20:30', 'debit', 'USA', 44.33, 'amazon', 2022, 2, 7)
    ]
    
    input = spark_session.createDataFrame(input_data).toDF(*input_schema)

    dw_input_schema = data_warehouse_cols
    dw_input_data = [
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/7/2022 2:20:30', 'debit', 'USA', 44.33, 'amazon', 2022, 2, 7),
        ('76ed7880-1f89-40b0-92a9-de8e31a1cf06', '2/6/2022 2:20:30', 'debit', 'USA', 44.3, 'Cvs', 2022, 2, 6),
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/8/2022 2:20:30', 'debit', 'USA', 44.0, 'Walgreens', 2022, 2, 8)
    ]
    
    dw_input = spark_session.createDataFrame(dw_input_data).toDF(*dw_input_schema)

    expected_schema = data_warehouse_cols
    expected_data = [
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/7/2022 2:20:30', 'debit', 'USA', 44.33, 'amazon', 2022, 2, 7),
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/7/2022 2:20:30', 'debit', 'USA', 44.33, 'amazon', 2022, 2, 7),
        ('76ed7880-1f89-40b0-92a9-de8e31a1cf06', '2/6/2022 2:20:30', 'debit', 'USA', 44.3, 'Cvs', 2022, 2, 6),
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/8/2022 2:20:30', 'debit', 'USA', 44.0, 'Walgreens', 2022, 2, 8)
    ]

    expected_output = spark_session.createDataFrame(expected_data).toDF(*expected_schema)

    real_output = union_data_warehouse(input, dw_input)

    assert schema_test(expected_output, real_output)
    assert data_test(expected_output, real_output)


def test_deduplicate_rows(spark_session):

    data_warehouse_cols = dw_cols.keys()

    input_schema = data_warehouse_cols
    input_data = [
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/7/2022 2:20:30', 'debit', 'USA', 44.33, 'amazon', 2022, 2, 7),
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/7/2022 2:20:30', 'debit', 'USA', 44.33, 'amazon', 2022, 2, 7),
        ('76ed7880-1f89-40b0-92a9-de8e31a1cf06', '2/6/2022 2:20:30', 'debit', 'USA', 44.3, 'Cvs', 2022, 2, 6),
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/8/2022 2:20:30', 'debit', 'USA', 44.0, 'Walgreens', 2022, 2, 8)
    ]
    
    input = spark_session.createDataFrame(input_data).toDF(*input_schema)

    expected_schema = data_warehouse_cols
    expected_data = [
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/7/2022 2:20:30', 'debit', 'USA', 44.33, 'amazon', 2022, 2, 7),
        ('76ed7880-1f89-40b0-92a9-de8e31a1cf06', '2/6/2022 2:20:30', 'debit', 'USA', 44.3, 'Cvs', 2022, 2, 6),
        ('3fd41ae8-2d94-4b2b-94da-7677c67a6b00', '2/8/2022 2:20:30', 'debit', 'USA', 44.0, 'Walgreens', 2022, 2, 8)
    ]

    expected_output = spark_session.createDataFrame(expected_data).toDF(*expected_schema)

    real_output = deduplicate_rows(input)

    assert schema_test(expected_output, real_output)
    assert data_test(expected_output, real_output)