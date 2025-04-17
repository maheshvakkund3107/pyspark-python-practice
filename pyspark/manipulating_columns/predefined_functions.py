from pyspark.sql.functions import date_format
from pyspark.sql.session import SparkSession

if __name__ == '__main__':
    sparkSession = SparkSession. \
        builder. \
        appName("PredefinedFunctions"). \
        getOrCreate()

    orders = sparkSession. \
        read. \
        csv(
        '/Users/maheshvakkund/sourcecodes/pyspark-python-practice/data-main/retail_db/orders',
        schema='order_id INT, order_date STRING, order_customer_id INT, order_status STRING'
    )

    orders. \
        select('*', date_format('order_date', 'yyyyMM').alias('order_month')). \
        show()

    orders. \
        withColumn('order_month', date_format('order_date', 'yyyyMM')). \
        show()

    orders. \
        filter(date_format('order_date', 'yyyyMM') == 201401). \
        show()

    orders. \
        groupBy(date_format('order_date', 'yyyyMM').alias('order_month')). \
        count(). \
        show()
