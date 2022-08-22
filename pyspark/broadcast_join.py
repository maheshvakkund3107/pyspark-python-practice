from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

if __name__ == '__main__':
    peoples = [("andrea", "medellin"),
               ("rodolfo", "medellin"),
               ("abdul", "bangalore")
               ]

    sparkSession = SparkSession.builder.appName("BroadCast Join").master("local[*]").getOrCreate()
    peoples_df = sparkSession.createDataFrame(peoples, schema="""
    first_name string,
    city string
    """)

    peoples_df.printSchema()

    cities = [
        ("medellin", "colombia", 2.5),
        ("bangalore", "india", 12.3)
    ]

    cities_df = sparkSession.createDataFrame(cities, schema="""
    city string,
    country string,
    population float
    """)

    cities_df.printSchema()

    peoples_df.join(
        broadcast(cities_df),
        peoples_df["city"] == cities_df["city"]
    ).show()

    peoples_df.join(
        broadcast(cities_df),
        peoples_df["city"] == cities_df["city"]
    ).explain()

    broadcast_df = peoples_df.join(
        broadcast(cities_df),
        peoples_df["city"] == cities_df["city"]
    ).drop(cities_df["city"]).explain(True)
