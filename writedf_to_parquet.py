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
    csv_df.write. \
        mode("Overwrite"). \
        option("compression", "none"). \
        parquet("/Users/maheshvakkund/PycharmProjects/pythonProject/python-pyspark/data-master/writepath")

    csv_df.write. \
        mode("Overwrite"). \
        option("compression", "none"). \
        format('parquet'). \
        save("/Users/maheshvakkund/PycharmProjects/pythonProject/python-pyspark/data-master/writepath")
