import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

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

    users_df.createOrReplaceTempView('users')

    sparkSession.sql("""
        SELECT id, (amount_paid + 25) AS amount_paid
        FROM users
    """).show()

    users_df. \
        selectExpr('id', '(amount_paid + 25) AS amount_paid'). \
        show()

    users_df.select('id', col('amount_paid') + lit(25.0)).show()