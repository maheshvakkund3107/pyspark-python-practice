from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, min, max, countDistinct, count, avg, approx_count_distinct, sumDistinct, first, \
    last, mean

if __name__ == '__main__':
    sparkSession = SparkSession.builder.appName("Aggregation").master("local").getOrCreate()
    readDf = sparkSession. \
        read. \
        option("header", True). \
        option("inferSchema", "true"). \
        format("csv"). \
        load("/Users/maheshvakkund/Documents/XEROX/java-spark/data-master/sales_data/sales_data.csv")
    readDf.groupBy("company").avg("num_sales")
    readDf.groupBy("company").min("num_sales")
    readDf.groupBy("company").max("num_sales")
    readDf.groupBy("company").sum("num_sales")
    readDf.groupBy("company").count()

    readDf.select(countDistinct("num_sales").alias("distinct_count"))

    readdf = sparkSession. \
        read. \
        format("csv"). \
        option("header", "true"). \
        option("inferSchema", "true"). \
        load("/Users/maheshvakkund/Documents/XEROX/java-spark/data-master/sales_data/201508_station_data.csv")
    readdf.printSchema()
    readdf.select(count("*"),
                  sum("dockcount").alias("sum_of_dock_count"),
                  avg("dockcount").alias("avg_of_dock_count"),
                  countDistinct("landmark").alias("count_distinct_of_dock_count"),
                  approx_count_distinct("station_id").alias("approx_distinct"),
                  sumDistinct("station_id").alias("station_id"))
    readdf.select(first("station_id"),
                  last("station_id"),
                  min("dockcount"),
                  max("dockcount"))
    readdf.select(mean("dockcount"))
