from pyspark.sql.session import SparkSession
from pyspark.sql.functions import substring, col, split, lit, explode, concat, lpad, rpad, ltrim, rtrim, trim, expr

if __name__ == '__main__':
    sparkSession = SparkSession. \
        builder. \
        appName("extracting_substring"). \
        getOrCreate()

    l = [("   Hello.    ",)]
    df = sparkSession. \
        createDataFrame(l, "dummy STRING")

    df. \
        withColumn("ltrim", ltrim(col("dummy"))). \
        withColumn("rtrim", rtrim(col("dummy"))). \
        withColumn("trim", trim(col("dummy"))). \
        show()

    df. \
        withColumn("ltrim", expr("ltrim(dummy)")). \
        withColumn("rtrim", expr("rtrim('.', rtrim(dummy))")). \
        withColumn("trim", trim(col("dummy"))). \
        show()

    df. \
        withColumn("ltrim", expr("trim(LEADING ' ' FROM dummy)")). \
        withColumn("rtrim", expr("trim(TRAILING '.' FROM rtrim(dummy))")). \
        withColumn("trim", expr("trim(BOTH ' ' FROM dummy)")). \
        show()
