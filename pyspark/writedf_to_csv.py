from pyspark.sql import SparkSession

if __name__ == '__main__':
    sparkSession = SparkSession.builder.appName("DataProcessing").master("local[2]").getOrCreate()
    csv_df = sparkSession.read.csv(
        '/Users/maheshvakkund/PycharmProjects/pythonProject/python-pyspark/data-master/retail_db/order_items',
        header=True, schema="""
            order_id int,
            order_date string,
            order_customer_id int,
            order_status string    
            """)
    csv_df. \
        coalesce(1). \
        write. \
        mode("Overwrite"). \
        csv("/Users/maheshvakkund/PycharmProjects/pythonProject/python-pyspark/data-master/writepath",
            mode="overwrite",
            sep=",",
            compression="gzip")

    csv_df. \
        write. \
        option("compression", "gzip"). \
        mode("overwrite"). \
        format("csv"). \
        save("/Users/maheshvakkund/PycharmProjects/pythonProject/python-pyspark/data-master/writepath")
