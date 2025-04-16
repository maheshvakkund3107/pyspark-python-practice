import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col

if __name__ == '__main__':
    users = [(1,
              'Corrie',
              'Van den Oord',
              'cvandenoord0@etsy.com',
              True,
              1000.55,
              datetime.date(2021, 1, 15),
              datetime.datetime(2021, 2, 10, 1, 15)),
             (2,
              'Nikolaus',
              'Brewitt',
              'nbrewitt1@dailymail.co.uk',
              True,
              900.0,
              datetime.date(2021, 2, 14),
              datetime.datetime(2021, 2, 18, 3, 33)),
             (3,
              'Orelie',
              'Penney',
              'openney2@vistaprint.com',
              True,
              850.55,
              datetime.date(2021, 1, 21),
              datetime.datetime(2021, 3, 15, 15, 16, 55)),
             (4,
              'Ashby',
              'Maddocks',
              'amaddocks3@home.pl',
              False,
              None,
              None,
              datetime.datetime(2021, 4, 10, 17, 45, 30)),
             (5,
              'Kurt',
              'Rome',
              'krome4@shutterfly.com',
              False,
              None,
              None,
              datetime.datetime(2021, 4, 2, 0, 55, 18))]

    users_schema = [
        'id',
        'first_name',
        'last_name',
        'email',
        'is_customer',
        'amount_paid',
        'customer_from',
        'last_updated_ts'
    ]

    sparkSession = SparkSession.builder.appName("Specifying schema as string").master("local").getOrCreate()
    users_df = sparkSession.createDataFrame(users, users_schema)

    users_df. \
        withColumnRenamed("id", "user_id"). \
        withColumnRenamed("first_name", "user_first_name"). \
        withColumnRenamed("last_name", "user_last_name"). \
        show()

    # Using select
    users_df. \
        select(
        col('id').alias('user_id'),
        col('first_name').alias('user_first_name'),
        col('last_name').alias('user_last_name'),
        concat(col('first_name'), lit(', '), col('last_name')).alias('user_full_name')
    ). \
        show()

    users_df. \
        select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name'),
        concat(users_df['first_name'], lit(', '), users_df['last_name']).alias('user_full_name')
    ). \
        show()

    # Using withColumn and alias (first select and then withColumn)
    users_df. \
        select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name')
    ). \
        withColumn('user_full_name', concat(col('user_first_name'), lit(', '), col('user_last_name'))). \
        show()

    # Using withColumn and alias (first withColumn and then select)
    users_df. \
        withColumn('user_full_name', concat(col('first_name'), lit(', '), col('last_name'))). \
        select(
            users_df['id'].alias('user_id'),
            users_df['first_name'].alias('user_first_name'),
            users_df['last_name'].alias('user_last_name'),
            'user_full_name'
        ). \
        show()

    users_df. \
        withColumn('user_full_name', concat(users_df['first_name'], lit(', '), users_df['last_name'])). \
        select(
            users_df['id'].alias('user_id'),
            users_df['first_name'].alias('user_first_name'),
            users_df['last_name'].alias('user_last_name'),
            'user_full_name'
        ). \
        show()