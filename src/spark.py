from pyspark.sql import SparkSession

'''Library for spark related functions.'''

def get_spark() -> SparkSession:
    """Generates a SparkSession object
    :param: None
    :return: Spark DataFrame
    """
    return (
        SparkSession
            .builder
            .appName('Rippleshot')
            .getOrCreate()
    )
