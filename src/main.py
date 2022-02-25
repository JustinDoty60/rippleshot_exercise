from spark import get_spark
from jobs.auths_job import extract_auths_data, transform_auths_data, load_auths_data
from jobs.accounts_job import extract_accounts_data_from_source, transform_accounts_data, load_accounts_data
from reports import generate_amount_of_transactions_report, generate_top_3_merchants_report

'''Rippleshot Data Engineering Take Home.'''

def main():
    spark = get_spark()

    # ETL for auths and accounts could be done in parallel
    auths_df = extract_auths_data(spark)
    auths_df = transform_auths_data(auths_df, spark)
    load_auths_data(auths_df)

    accounts_df = extract_accounts_data_from_source(spark)
    accounts_df = transform_accounts_data(accounts_df, spark)
    load_accounts_data(accounts_df, spark)

    auths_df = spark.read.parquet('data_warehouse/auths')
    accounts_df = spark.read.parquet('data_warehouse/accounts')

    generate_amount_of_transactions_report(auths_df, accounts_df)
    generate_top_3_merchants_report(auths_df)


if __name__ == '__main__':
    main()