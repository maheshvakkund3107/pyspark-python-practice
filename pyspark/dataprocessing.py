from pyspark.sql import SparkSession

if __name__ == '__main__':
    sparkSession = SparkSession.builder.appName("DataProcessing").master("local[2]").getOrCreate()
    csv_df = sparkSession.read.csv(
        '/Users/maheshvakkund/PycharmProjects/pythonProject/python-pyspark/data-master/retail_db/orders',
        header=True, schema="""
        order_id int,
        order_date string,
        order_customer_id int,
        order_status string    
        """)
    # csv_df.printSchema()
    # csv_df.show()

    json_df = sparkSession.read.json(
        "/Users/maheshvakkund/PycharmProjects/pythonProject/python-pyspark/data-master/retail_db_json/orders")
    # json_df.printSchema()
    # json_df.show()

    csv_df = sparkSession.read.csv(
        '/Users/maheshvakkund/PycharmProjects/pythonProject/python-pyspark/data-master/retail_db/orders',
        inferSchema=True, header=True)
    # csv_df.printSchema()
    # csv_df.show()

    employees = [(1, "Scott", "Tiger", 1000.0, "united states"),
                 (2, "Henry", "Ford", 1250.0, "India"),
                 (3, "Nick", "Junior", 750.0, "united KINGDOM"),
                 (4, "Bill", "Gomes", 1500.0, "AUSTRALIA")
                 ]
    employees_df = sparkSession.createDataFrame(employees, schema="""
        emp_id int,
        first_name string,
        last_name string,
        sal float,
        nationality string
    """)
    # employees_df.show()
    # employees_df.select("first_name", "last_name").show()
    # employees_df.drop("nationality").show()
    # employees_df.withColumn("full_name", concat("first_name", lit("-"), "last_name")).show()



