from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

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
        withColumn("date_ym", date_format("date", "yyyyMM").cast('int')). \
        withColumn("time_ym", date_format("time", "yyyyMM").cast('int')). \
        show(truncate=False)

    datetimesDF. \
        withColumn("date_dt", date_format("date", "yyyyMMddHHmmss")). \
        withColumn("date_ts", date_format("time", "yyyyMMddHHmmss")). \
        show(truncate=False)

    datetimesDF. \
        withColumn("date_dt", date_format("date", "yyyyMMddHHmmss").cast('long')). \
        withColumn("date_ts", date_format("time", "yyyyMMddHHmmss").cast('long')). \
        show(truncate=False)

    datetimesDF. \
        withColumn("date_yd", date_format("date", "yyyyDDD").cast('int')). \
        withColumn("time_yd", date_format("time", "yyyyDDD").cast('int')). \
        show(truncate=False)

    datetimesDF. \
        withColumn("date_desc", date_format("date", "MMMM d, yyyy")). \
        show(truncate=False)

    datetimesDF. \
        withColumn("day_name_abbr", date_format("date", "EE")). \
        show(truncate=False)

    datetimesDF. \
        withColumn("day_name_full", date_format("date", "EEEE")). \
        show(truncate=False)



