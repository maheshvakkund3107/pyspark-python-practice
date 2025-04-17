from pyspark.sql.session import SparkSession
from pyspark.sql.functions import substring, col, split, lit, explode, concat, lpad, rpad, current_date, \
    current_timestamp, to_date, to_timestamp, date_add, date_sub, datediff, months_between, add_months

if __name__ == '__main__':
    sparkSession = SparkSession. \
        builder. \
        appName("extracting_substring"). \
        getOrCreate()

    datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
                 ("2016-02-29", "2016-02-29 08:08:08.999"),
                 ("2017-10-31", "2017-12-31 11:59:59.123"),
                 ("2019-11-30", "2019-08-31 00:00:00.000")
                 ]
    datetimesDF = sparkSession. \
        createDataFrame(datetimes, schema="date STRING, time STRING")

    datetimesDF. \
        select("*"). \
        withColumn("date_add_date", date_add("date", 10)). \
        withColumn("date_sub_date", date_sub("date", 10)). \
        withColumn("date_add_time", date_add("time", 10)). \
        withColumn("date_sub_time", date_sub("time", 10)). \
        show(truncate=False)

    datetimesDF. \
        withColumn("datediff_date", datediff(current_date(), "date")). \
        withColumn("datediff_time", datediff(current_timestamp(), "time")). \
        show()

    datetimesDF. \
        withColumn("months_between_date", months_between(current_date(), "date")). \
        withColumn("months_between_time", months_between(current_timestamp(), "time")). \
        withColumn("add_months_date", add_months("date", 3)). \
        withColumn("add_months_time", add_months("time", 3)). \
        show(truncate=False)
