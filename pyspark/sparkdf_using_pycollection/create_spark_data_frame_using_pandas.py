import datetime
import pandas as pd
from pyspark.sql.session import SparkSession

if __name__ == '__main__':
    users = [
        {
            "id": 1,
            "first_name": "Corrie",
            "last_name": "Van den Oord",
            "email": "cvandenoord0@etsy.com",
            "is_customer": True,
            "amount_paid": 1000.55,
            "customer_from": datetime.date(2021, 1, 15),
            "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
        },
        {
            "id": 2,
            "first_name": "Nikolaus",
            "last_name": "Brewitt",
            "email": "nbrewitt1@dailymail.co.uk",
            "is_customer": True,
            "amount_paid": 900.0,
            "customer_from": datetime.date(2021, 2, 14),
            "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
        },
        {
            "id": 3,
            "first_name": "Orelie",
            "last_name": "Penney",
            "email": "openney2@vistaprint.com",
            "is_customer": True,
            "amount_paid": 850.55,
            "customer_from": datetime.date(2021, 1, 21),
            "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
        },
        {
            "id": 4,
            "first_name": "Ashby",
            "last_name": "Maddocks",
            "email": "amaddocks3@home.pl",
            "is_customer": False,
            "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
        },
        {
            "id": 5,
            "first_name": "Kurt",
            "last_name": "Rome",
            "email": "krome4@shutterfly.com",
            "is_customer": False,
            "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
        }
    ]

    sparkSession = SparkSession.builder.appName("Create Spark Dataframe using pandas").master("local").getOrCreate()
    sparkSession.createDataFrame(pd.DataFrame(users)).show()

