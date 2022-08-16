from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

if __name__ == '__main__':
    sparkSession = SparkSession.builder.appName("Data Processing Overview").master("local[2]").getOrCreate();
    ordersDf = sparkSession. \
        read. \
        option("header", True). \
        option("inferSchema", True). \
        format("csv"). \
        load("/Users/maheshvakkund/Documents/XEROX/java-spark/data-master/retail_db/orders/part-00000");

    employee = [(1, "Scott", "Tiger", 1000.0, "united states"),
                (2, "Henry", "Ford", 1250.0, "India"),
                (3, "Nick", "Junior", 750.0, "united KINGDOM"),
                (4, "Bill", "Gomes", 1500.0, "AUSTRALIA")]

    schema = StructType([StructField("id", IntegerType(), False),
                         StructField("first_name", StringType(), False),
                         StructField("last_name", StringType(), False),
                         StructField("salary", DoubleType(), False),
                         StructField("nationality", StringType(), False)])

    df = sparkSession.createDataFrame(employee, schema)
    df.select("*").withColumn("full_name", concat(df["first_name"], lit("-"), df["last_name"])).show()
