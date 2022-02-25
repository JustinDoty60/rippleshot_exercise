import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import SQLContext
import pytest

'''Method that allows the SparkContext to only be created once per test'''

@pytest.fixture(scope='session')
def sql_context() -> None:
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    yield sql_context
    spark_context.stop()