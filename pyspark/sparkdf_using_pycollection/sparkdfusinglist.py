from pyspark.sql.session import SparkSession

if __name__ == '__main__':
    ages_list = [12,34,42,25,56]
    sparkSession = SparkSession.builder.appName("sparkdfusinglist").master("local").getOrCreate()
    df=sparkSession.createDataFrame(ages_list,'int')
    df.show()
    df.printSchema()