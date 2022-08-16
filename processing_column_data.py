from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, to_date, col, upper, lower, initcap, length, substring, explode, split, \
    current_date, current_timestamp, lit, to_timestamp, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

if __name__ == '__main__':
    sparkSession = SparkSession.builder.appName("Data Processing Overview").master("local[2]").getOrCreate()
    ordersDf = sparkSession. \
        read. \
        option("header", True). \
        option("inferSchema", True). \
        format("csv"). \
        load(
        "/Users/maheshvakkund/Documents/XEROX/java-spark/data-master/retail_db/orders/part-00000")
    ordersDf. \
        write. \
        mode("overwrite").format("parquet").save(
        "/Users/maheshvakkund/Documents/XEROX/java-spark/write")
    ordersDf = ordersDf.select("*").withColumn("order_month", date_format(to_date(col("order_date")), "yyyyMM"))

    ordersDf.filter(col("order_month") == 201401)
    ordersDf.groupBy(col("order_month")).count().alias("count_order_month").show()

    employee = [
        (1, "Scott", "Tiger", 1000.0,
         "united states", "+1 123 456 7890", "123 45 6789", "10"
         ),
        (2, "Henry", "Ford", 1250.0,
         "India", "+91 234 567 8901", "456 78 9123", ""
         ),
        (4, "Bill", "Gomes", 1500.0,
         "AUSTRALIA", "+61 987 654 3210,+61 876 543 2109", "789 12 6118", ""
         ),
        (3, "Nick", "Junior", 750.0,
         "united KINGDOM", "+44 111 111 1111,+44 222 222 2222", "222 33 4444", "10"
         )]

    schema = StructType(
        [StructField("employee_id", IntegerType(), False),
         StructField("first_name", StringType(), False),
         StructField("last_name", StringType(), False),
         StructField("salary", DoubleType(), False),
         StructField("nationality", StringType(), False),
         StructField("phone_number", StringType(), False),
         StructField("ssn", StringType(), False),
         StructField("bonus", StringType(), False),
         ])

    employeesDf = sparkSession.createDataFrame(employee, schema)

    employeesDf.groupBy("nationality").count()
    employeesDf.orderBy("employee_id")

    employeesDf.select(upper(col("first_name")), upper(col("last_name")))
    employeesDf.groupBy(upper(col("nationality"))).count()

    employeesDf.orderBy(col("employee_id").desc())

    employeesDf. \
        select("employee_id", "nationality"). \
        withColumn("nationality_upper", upper(col("nationality"))). \
        withColumn("nationality_lower", lower(col("nationality"))). \
        withColumn("nationality_initcap", initcap(col("nationality"))). \
        withColumn("nationality_length", length(col("nationality")))

    employeesDf. \
        select("employee_id", "phone_number", "ssn"). \
        withColumn("phone_last4", substring(col("phone_number"), -4, 4).cast("int")). \
        withColumn("ssn_last4", substring(col("ssn"), 8, 4).cast("int")).printSchema()

    employeesDf.select("employee_id", "phone_number", "ssn").withColumn("phone_numbers",
                                                                        explode(split(col("phone_number"), ",")))

    employeesDf.select(current_date())
    employeesDf.select(current_timestamp())

    employeesDf.select(to_date(lit("20220826"), "yyyyMMdd"))
    employeesDf.select(to_timestamp(lit("20220826 2354"), "yyyyMMdd HHmm"))
    employeesDf.withColumn("bonus", col("bonus").cast("int"))
    employeesDf.withColumn("bonus", coalesce(col("bonus").cast("int"), lit(0)))
