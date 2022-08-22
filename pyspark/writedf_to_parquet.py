import time

from pyspark.sql import SparkSession

if __name__ == '__main__':
    sparkSession = SparkSession.builder.appName("DataProcessing").master("local[*]").getOrCreate()
    csv_df = sparkSession.read.csv(
        '/Users/maheshvakkund/PycharmProjects/pythonProject/python-pyspark/data-master/sales.csv',
        header=True, inferSchema=True)
    st = time.time()
    csv_df.repartition(4).write. \
        mode("Overwrite"). \
        option("compression", "none"). \
        format('parquet'). \
        save("/Users/maheshvakkund/PycharmProjects/pythonProject/python-pyspark/data-master/writepath")
    et = time.time()
    elapsed_time = et - st
    print('Execution time:', elapsed_time, 'seconds')

    st = time.time()
    csv_df.coalesce(4).write. \
        mode("Overwrite"). \
        option("compression", "none"). \
        format('parquet'). \
        save("/Users/maheshvakkund/PycharmProjects/pythonProject/python-pyspark/data-master/writepath")
    et = time.time()
    elapsed_time = et - st
    print('Execution time:', elapsed_time, 'seconds')


