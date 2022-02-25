from pyspark.sql.dataframe import DataFrame

'''Helper methods for testing compare between the schema and data of 2 dataframes.
Took directly from this source: https://towardsdatascience.com/testing-pyspark-dataframe-transformations-d3d16c798a84'''

def schema_test(df1: DataFrame, df2: DataFrame, check_nullable = True):
    '''Tests whether 2 Spark dataframes have the same schema'''

    field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)

    fields1 = [*map(field_list, df1.schema.fields)]
    fields2 = [*map(field_list, df2.schema.fields)]
    
    if check_nullable:
        res = set(fields1) == set(fields2)
    else:
        res = set([field[:-1] for field in fields1]) == set([field[:-1] for field in fields2])

    return res


def data_test(df1: DataFrame, df2: DataFrame):
    '''Tests whether 2 Spark dataframes have the same data'''

    data1 = df1.collect()
    data2 = df2.collect()

    return set(data1) == set(data2)