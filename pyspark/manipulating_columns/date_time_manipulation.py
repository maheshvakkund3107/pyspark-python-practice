from pyspark.sql.session import SparkSession
from pyspark.sql.functions import substring, col, split, lit, explode, concat, lpad, rpad, current_date, \
    current_timestamp, to_date, to_timestamp

if __name__ == '__main__':
    sparkSession = SparkSession. \
        builder. \
        appName("extracting_substring"). \
        getOrCreate()

    l = [("X",)]
    df = sparkSession.createDataFrame(l).toDF("dummy")
    df. \
        select(current_date()). \
        show()  # yyyy-MM-dd

    df. \
        select(current_timestamp()). \
        show(truncate=False)  # yyyy-MM-dd HH:mm:ss.SSS

    df. \
        select(to_date(lit('20210228'), 'yyyyMMdd').alias('to_date')). \
        show()

    df. \
        select(to_timestamp(lit('20210228 1725'), 'yyyyMMdd HHmm').alias('to_timestamp')). \
        show()


