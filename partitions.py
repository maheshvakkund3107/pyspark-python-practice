import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, unix_timestamp

if __name__ == '__main__':
    sparkSession = SparkSession.builder.appName("DataProcessing").master("local[2]").getOrCreate()
    csv_df = sparkSession.read.csv(
        '/Users/maheshvakkund/PycharmProjects/pythonProject/python-pyspark/data-master/sales.csv',
        header=True, inferSchema=True)
    csv_df.printSchema()

    csv_df = csv_df.withColumn("Order_Date", to_date(unix_timestamp("Order Date", "M/d/yyyy").cast("timestamp")))

    csv_df.show(1)
    st = time.time()
    csv_df. \
        coalesce(1). \
        write. \
        partitionBy("Region"). \
        mode("overwrite"). \
        format("csv"). \
        save("/Users/maheshvakkund/PycharmProjects/pythonProject/python-pyspark/data-master/writepath")
    csv_df. \
        coalesce(1). \
        write. \
        partitionBy("Region", "Country"). \
        mode("overwrite"). \
        format("csv"). \
        save("/Users/maheshvakkund/PycharmProjects/pythonProject/python-pyspark/data-master/writepath")
    csv_df. \
        coalesce(1). \
        write. \
        partitionBy("Region", "Country", "Order_Date"). \
        mode("overwrite"). \
        format("csv"). \
        save("/Users/maheshvakkund/PycharmProjects/pythonProject/python-pyspark/data-master/writepath")

    et = time.time()
    elapsed_time = et - st
    print('Execution time:', elapsed_time, 'seconds')
