from pyspark.sql import SparkSession

'''Library for spark related functions.'''

def get_spark() -> SparkSession:
    """Generates a SparkSession object"""
    
    spark = (
        SparkSession
            .builder
            .appName('Rippleshot')
            .getOrCreate()
    )

    spark.sparkContext.setCheckpointDir('/checkpoint/')

    return spark