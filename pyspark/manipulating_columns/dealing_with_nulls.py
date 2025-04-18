from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, coalesce, lit, col, expr

if __name__ == '__main__':
    sparkSession = SparkSession. \
        builder. \
        appName("extracting_substring"). \
        getOrCreate()

    employees = [(1, "Scott", "Tiger", 1000.0, 10,
                  "united states", "+1 123 456 7890", "123 45 6789"
                  ),
                 (2, "Henry", "Ford", 1250.0, None,
                  "India", "+91 234 567 8901", "456 78 9123"
                  ),
                 (3, "Nick", "Junior", 750.0, '',
                  "united KINGDOM", "+44 111 111 1111", "222 33 4444"
                  ),
                 (4, "Bill", "Gomes", 1500.0, 10,
                  "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
                  )
                 ]

    employeesDF = sparkSession. \
        createDataFrame(employees,
                        schema="""employee_id INT, first_name STRING, 
                        last_name STRING, salary FLOAT, bonus STRING, nationality STRING,
                        phone_number STRING, ssn STRING"""
                        )

    employeesDF. \
        withColumn('bonus', coalesce('bonus', lit(0))). \
        show()

    employeesDF. \
        withColumn('bonus1', col('bonus').cast('int')). \
        show()

    employeesDF. \
        withColumn('bonus1', coalesce(col('bonus').cast('int'), lit(0))). \
        show()

    employeesDF. \
        withColumn('bonus', expr("nvl(bonus, 0)")). \
        show()

    employeesDF. \
        withColumn('bonus', expr("nvl(nullif(bonus, ''), 0)")). \
        show()

    employeesDF. \
        withColumn('payment', col('salary') + (col('salary') * coalesce(col('bonus').cast('int'), lit(0)) / 100)). \
        show()
