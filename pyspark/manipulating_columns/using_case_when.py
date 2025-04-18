from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit, expr, when

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
        withColumn('bonus1', coalesce(col('bonus').cast('int'), lit(0))). \
        show()

    employeesDF. \
        withColumn(
        'bonus',
        expr("""
                CASE WHEN bonus IS NULL OR bonus = '' THEN 0
                ELSE bonus
                END
                """)
    ). \
        show()

    employeesDF. \
        withColumn(
        'bonus',
        when((col('bonus').isNull()) | (col('bonus') == lit('')), 0).otherwise(col('bonus'))
    ). \
        show()

    persons = [
        (1, 1),
        (2, 13),
        (3, 18),
        (4, 60),
        (5, 120),
        (6, 0),
        (7, 12),
        (8, 160)
    ]

    personsDF = sparkSession.createDataFrame(persons, schema='id INT, age INT')
    personsDF. \
        withColumn(
        'category',
        expr("""
                CASE
                WHEN age BETWEEN 0 AND 2 THEN 'New Born'
                WHEN age > 2 AND age <= 12 THEN 'Infant'
                WHEN age > 12 AND age <= 48 THEN 'Toddler'
                WHEN age > 48 AND age <= 144 THEN 'Kid'
                ELSE 'Teenager or Adult'
                END
            """)
    ). \
        show()

    personsDF. \
        withColumn(
        'category',
        when(col('age').between(0, 2), 'New Born').
        when((col('age') > 2) & (col('age') <= 12), 'Infant').
        when((col('age') > 12) & (col('age') <= 48), 'Toddler').
        when((col('age') > 48) & (col('age') <= 144), 'Kid').
        otherwise('Teenager or Adult')
    ). \
        show()