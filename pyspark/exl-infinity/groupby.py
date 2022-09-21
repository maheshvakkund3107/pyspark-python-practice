from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Windowing Spark").master("local[2]").getOrCreate()
    df = spark.sparkContext\
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe", "topic1") \
        .load("/Users/maheshvakkund/PycharmProjects/pythonProject/python-pyspark/pyspark/exl-infinity/sales.csv")

    df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("topic", "topic1") \
        .save()
