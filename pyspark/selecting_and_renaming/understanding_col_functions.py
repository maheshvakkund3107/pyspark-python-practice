import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, date_format

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

    cols = ['id', 'first_name', 'last_name']
    users_df.select(*cols).show()

    users_df.select(
        col('id'),
        date_format('customer_from', 'yyyyMMdd')
    ).show()

    users_df.select(
        col('id'),
        date_format('customer_from', 'yyyyMMdd')
    ).printSchema()

    users_df.select(
        col('id'),
        date_format('customer_from', 'yyyyMMdd').alias('customer_from')
    ).show()

    users_df.select(
        col('id'),
        date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')
    ).show()

    users_df.select(
        col('id'),
        date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')
    ).printSchema()

    cols = [col('id'), date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')]
    users_df.select(*cols).show()