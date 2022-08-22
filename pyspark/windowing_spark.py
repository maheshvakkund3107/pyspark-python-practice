from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, rank, dense_rank, percent_rank
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == '__main__':
    employee = [
        ("James", "Sales", 3000),
        ("Michael", "Sales", 4600),
        ("Robert", "Sales", 4100),
        ("Maria", "Finance", 3000),
        ("James", "Sales", 3000),
        ("Scott", "Finance", 3300),
        ("Jen", "Finance", 3900),
        ("Jeff", "Marketing", 3000),
        ("Kumar", "Marketing", 2000),
        ("Saif", "Sales", 4100)]

    schema = StructType([StructField("employee_name", StringType(), False),
                         StructField("dept", StringType(), False),
                         StructField("salary", IntegerType(), False)])

    sparkSession = SparkSession.builder.appName("Windowing Spark").master("local[2]").getOrCreate()
    employeeDF = sparkSession.createDataFrame(employee, schema)

    windowSpec = Window.partitionBy("dept").orderBy("salary")
    employeeDF.withColumn("row_number", row_number().over(windowSpec)).show()
    employeeDF.withColumn("rank", rank().over(windowSpec)).show()
    employeeDF.withColumn("dense_rank", dense_rank().over(windowSpec)).show()
    employeeDF.withColumn("percent_rank", percent_rank().over(windowSpec)).show()
