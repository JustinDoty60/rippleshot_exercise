from spark import get_spark
from jobs.auths_job import extract_auths_data, transform_auths_data, load_auths_data

'''Rippleshot Data Engineering Take Home.'''

def main():
    spark = get_spark()

    auths_df = extract_auths_data(spark)
    auths_df = transform_auths_data(auths_df)
    load_auths_data(auths_df)

    auths_df.sort(auths_df.merchant_name.desc()).show(30)
    auths_df.printSchema()


if __name__ == '__main__':
    main()