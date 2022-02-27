import findspark
findspark.init()

from pyspark.sql import SparkSession
import pytest

'''Method that allows the SparkContext to only be created once per test run'''

@pytest.fixture(scope='session')
def spark_session() -> None:
    spark = (
        SparkSession
            .builder
            .appName('unit_tests')
            .getOrCreate()
    )

    return spark