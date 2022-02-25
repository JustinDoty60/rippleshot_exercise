from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import date_format, col, sum
from jobs.auths_job import DWColumns as auths_DWColumns
from jobs.accounts_job import DWColumns as accounts_DWColumns

'''Methods that generates reports to .csv files. Can change in the future for another file type.'''

def generate_amount_of_transactions_report(auths_df: DataFrame, accounts_df: DataFrame) -> None:
    '''Generates a report of how many transactions happened per account first name and last name, per day'''

    auths_account_id = auths_DWColumns.ACCOUNT_ID.value
    auths_transmit_time = auths_DWColumns.TRANSMIT_TIME.value
    auths_cols = [auths_account_id, auths_transmit_time]
    auths_df = auths_df[auths_cols]

    auths_df = auths_df.withColumn(
        auths_transmit_time,  
        date_format(col(auths_transmit_time), 'yyyy-MM-dd')
    )
    auths_df = auths_df.groupBy(auths_account_id, auths_transmit_time).count()


    accounts_account_id = accounts_DWColumns.ACCOUNT_ID.value
    accounts_first_name = accounts_DWColumns.FIRST_NAME.value
    accounts_last_name = accounts_DWColumns.LAST_NAME.value
    accounts_cols = [accounts_account_id, accounts_first_name, accounts_last_name]
    accounts_df = accounts_df[accounts_cols]
    

    report_df = auths_df.join(accounts_df,
        auths_df.account_id == accounts_df.account_id,
        'inner'
    )

    report_df = report_df.selectExpr(
        accounts_first_name,
        accounts_last_name,
        auths_transmit_time,
        'count as num_transactions'
    )

    report_df = report_df.orderBy(col(auths_transmit_time).asc())

    write_report(report_df, 'amount_of_transactions_report')

    return None


def generate_top_3_merchants_report(auths_df: DataFrame) -> None:
    '''Generates a report of the top 3 merchants by dollars spent'''

    auths_merchant_name = auths_DWColumns.MERCHANT_NAME.value
    auths_amount = auths_DWColumns.AMOUNT.value
    auths_cols = [auths_merchant_name, auths_amount]

    report_df = auths_df[auths_cols]
    report_df = report_df.groupBy(auths_merchant_name).sum(auths_amount)

    dollars_spent_col = 'dollars_spent'

    report_df = report_df.withColumnRenamed('sum(amount)', dollars_spent_col)
    report_df = report_df.orderBy(col(dollars_spent_col).desc()).limit(3)

    write_report(report_df, 'top_3_merchants_report')

    return None


def write_report(df: DataFrame, report_name: str) -> None:
    '''Writes a report out in CSV format'''

    (
        df.coalesce(1)
            .write.format('com.databricks.spark.csv')
            .option('header', 'true')
            .mode('overwrite')
            .save(f'reports/{report_name}')
    )