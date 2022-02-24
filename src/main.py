from spark import get_spark
from jobs.auths_job import extract_auths_data, transform_auths_data, load_auths_data
from jobs.accounts_job import extract_accounts_data, transform_accounts_data, load_accounts_data

'''Rippleshot Data Engineering Take Home.'''

def main():
    spark = get_spark()

    auths_df = extract_auths_data(spark)
    auths_df = transform_auths_data(auths_df)
    load_auths_data(auths_df)

    accounts_df = extract_accounts_data(spark)
    accounts_df = transform_accounts_data(accounts_df)
    load_accounts_data(accounts_df)

    accounts_df.show(30)
    accounts_df.printSchema()


if __name__ == '__main__':
    main()