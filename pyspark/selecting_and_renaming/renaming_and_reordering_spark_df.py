import datetime

from pyspark import Row
from pyspark.sql import SparkSession

if __name__ == '__main__':
    users = [
        {
            "id": 1,
            "first_name": "Corrie",
            "last_name": "Van den Oord",
            "email": "cvandenoord0@etsy.com",
            "phone_numbers": ["+1 234 567 8901", "+1 234 567 8911"],
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
            "phone_numbers": ["+1 234 567 8923", "+1 234 567 8934"],
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
            "phone_numbers": ["+1 714 512 9752", "+1 714 512 6601"],
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
            "phone_numbers": None,
            "is_customer": False,
            "amount_paid": None,
            "customer_from": None,
            "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
        },
        {
            "id": 5,
            "first_name": "Kurt",
            "last_name": "Rome",
            "email": "krome4@shutterfly.com",
            "phone_numbers": ["+1 817 934 7142"],
            "is_customer": False,
            "amount_paid": None,
            "customer_from": None,
            "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
        }
    ]

    sparkSession = SparkSession.builder.appName("ArrayTypeColumns").getOrCreate()
    users_df = sparkSession.createDataFrame([Row(**user) for user in users])

    # required columns from original list
    required_columns = ['id', 'first_name', 'last_name', 'email', 'phone_numbers']

    # new column name list
    target_column_names = ['user_id', 'user_first_name', 'user_last_name', 'user_email', 'user_phone_numbers']

    users_df. \
        select(required_columns). \
        show()

    users_df. \
        select(required_columns). \
        toDF(*target_column_names). \
        show()
