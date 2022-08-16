import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

if __name__ == '__main__':
    session = SparkSession.builder.appName("Special Functions").master("local[2]").getOrCreate()
    employees = [(1, "Scott", "Tiger", 1000.0,
                  "united states", "+1 123 456 7890", "123 45 6789"
                  ),
                 (2, "Henry", "Ford", 1250.0,
                  "India", "+91 234 567 8901", "456 78 9123"
                  ),
                 (3, "Nick", "Junior", 750.0,
                  "united KINGDOM", "+44 111 111 1111", "222 33 4444"
                  ),
                 (4, "Bill", "Gomes", 1500.0,
                  "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
                  )
                 ]
    employeesDf = session.createDataFrame(employees, schema="""
    emp_id int,
    first_name string,
    last_name string,
    salary float,
    nationality string,
    phone_number string,
    ssn string
    """)
    # employeesDf.printSchema()
    # employeesDf.show()

    employeesDf.select("first_name", "last_name")
    employeesDf.groupby("nationality").count()
    employeesDf.orderBy("emp_id")
    employeesDf.select(upper(col("first_name")), upper(col("last_name")))
    employeesDf.orderBy(col("emp_id").desc()).show()

