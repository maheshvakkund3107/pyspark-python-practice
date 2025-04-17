from pyspark.sql.session import SparkSession
from pyspark.sql.functions import substring, col, split, lit, explode

if __name__ == '__main__':
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
    sparkSession = SparkSession. \
        builder. \
        appName("extracting_substring"). \
        getOrCreate()

    l = [('X',)]
    df = sparkSession. \
        createDataFrame(l, "dummy STRING")

    df. \
        select(split(lit("Hello World, how are you"), " ")). \
        show(truncate=False)

    df. \
        select(split(lit("Hello World, how are you"), " ")[2]). \
        show(truncate=False)

    df. \
        select(explode(split(lit("Hello World, how are you"), " ")).alias('word')). \
        show(truncate=False)

    employees = [(1, "Scott", "Tiger", 1000.0,
                  "united states", "+1 123 456 7890,+1 234 567 8901", "123 45 6789"
                  ),
                 (2, "Henry", "Ford", 1250.0,
                  "India", "+91 234 567 8901", "456 78 9123"
                  ),
                 (3, "Nick", "Junior", 750.0,
                  "united KINGDOM", "+44 111 111 1111,+44 222 222 2222", "222 33 4444"
                  ),
                 (4, "Bill", "Gomes", 1500.0,
                  "AUSTRALIA", "+61 987 654 3210,+61 876 543 2109", "789 12 6118"
                  )
                 ]

    employeesDF = sparkSession. \
        createDataFrame(employees,
                        schema="""employee_id INT, first_name STRING, 
                        last_name STRING, salary FLOAT, nationality STRING,
                        phone_numbers STRING, ssn STRING"""
                        )
    employeesDF = employeesDF. \
        select('employee_id', 'phone_numbers', 'ssn'). \
        withColumn('phone_number', explode(split('phone_numbers', ',')))

    employeesDF.show(truncate=False)

    employeesDF. \
        select("employee_id", "phone_number", "ssn"). \
        withColumn("area_code", split("phone_number", " ")[1].cast("int")). \
        withColumn("phone_last4", split("phone_number", " ")[3].cast("int")). \
        withColumn("ssn_last4", split("ssn", " ")[2].cast("int")). \
        show()
