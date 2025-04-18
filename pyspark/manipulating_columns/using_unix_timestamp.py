from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, col

if __name__ == '__main__':
    sparkSession = SparkSession. \
        builder. \
        appName("extracting_substring"). \
        getOrCreate()

    datetimes = [(20140228, "2014-02-28", "2014-02-28 10:00:00.123"),
                 (20160229, "2016-02-29", "2016-02-29 08:08:08.999"),
                 (20171031, "2017-10-31", "2017-12-31 11:59:59.123"),
                 (20191130, "2019-11-30", "2019-08-31 00:00:00.000")
                 ]
    datetimesDF = sparkSession. \
        createDataFrame(datetimes). \
        toDF("dateid", "date", "time")

    datetimesDF. \
        withColumn("unix_date_id", unix_timestamp(col("dateid").cast("string"), "yyyyMMdd")). \
        withColumn("unix_date", unix_timestamp("date", "yyyy-MM-dd")). \
        withColumn("unix_time", unix_timestamp("time")). \
        show()
