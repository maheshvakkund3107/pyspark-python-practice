from pyspark.sql.functions import current_date, \
    current_timestamp, date_add, date_sub, datediff, months_between, add_months, trunc, \
    date_trunc, year, month, weekofyear, dayofyear, dayofmonth, dayofweek, hour, minute, second, to_date, lit, \
    to_timestamp, col
from pyspark.sql.session import SparkSession

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

    datetimesDF. \
        withColumn("date_trunc", trunc("date", "MM")). \
        withColumn("time_trunc", trunc("time", "yy")). \
        show(truncate=False)

    datetimesDF. \
        withColumn("date_trunc", date_trunc('MM', "date")). \
        withColumn("time_trunc", date_trunc('yy', "time")). \
        show(truncate=False)

    datetimesDF. \
        withColumn("date_dt", date_trunc("HOUR", "date")). \
        withColumn("time_dt", date_trunc("HOUR", "time")). \
        withColumn("time_dt1", date_trunc("dd", "time")). \
        show(truncate=False)

    datetimesDF.select(
        current_date().alias('current_date'),
        year(current_date()).alias('year'),
        month(current_date()).alias('month'),
        weekofyear(current_date()).alias('weekofyear'),
        dayofyear(current_date()).alias('dayofyear'),
        dayofmonth(current_date()).alias('dayofmonth'),
        dayofweek(current_date()).alias('dayofweek')
    ).show(truncate=False)

    datetimesDF.select(
        current_timestamp().alias('current_timestamp'),
        year(current_timestamp()).alias('year'),
        month(current_timestamp()).alias('month'),
        dayofmonth(current_timestamp()).alias('dayofmonth'),
        hour(current_timestamp()).alias('hour'),
        minute(current_timestamp()).alias('minute'),
        second(current_timestamp()).alias('second')
    ).show(truncate=False)

    datetimes = [(20140228, "28-Feb-2014 10:00:00.123"),
                 (20160229, "20-Feb-2016 08:08:08.999"),
                 (20171031, "31-Dec-2017 11:59:59.123"),
                 (20191130, "31-Aug-2019 00:00:00.000")
                 ]
    datetimesDF = sparkSession. \
        createDataFrame(datetimes, schema="date BIGINT, time STRING")

    l = [("X",)]
    df = sparkSession.createDataFrame(l).toDF("dummy")

    df. \
        show(truncate=False)

    df. \
        select(to_date(lit('20210302'), 'yyyyMMdd').alias('to_date')). \
        show()

    df. \
        select(to_date(lit('2021061'), 'yyyyDDD').alias('to_date')). \
        show()

    df. \
        select(to_date(lit('02-03-2021'), 'dd-MM-yyyy').alias('to_date')). \
        show()

    df. \
        select(to_date(lit('02-Mar-2021'), 'dd-MMM-yyyy').alias('to_date')). \
        show()

    df. \
        select(to_date(lit('02-March-2021'), 'dd-MMMM-yyyy').alias('to_date')). \
        show()

    df. \
        select(to_date(lit('March 2, 2021'), 'MMMM d, yyyy').alias('to_date')). \
        show()

    df. \
        select(to_timestamp(lit('02-Mar-2021'), 'dd-MMM-yyyy').alias('to_date')). \
        show()

    df. \
        select(to_timestamp(lit('02-Mar-2021 17:30:15'), 'dd-MMM-yyyy HH:mm:ss').alias('to_date')). \
        show()

    datetimesDF. \
        withColumn('to_date', to_date(col('date').cast('string'), 'yyyyMMdd')). \
        withColumn('to_timestamp', to_timestamp(col('time'), 'dd-MMM-yyyy HH:mm:ss.SSS')). \
        show(truncate=False)
