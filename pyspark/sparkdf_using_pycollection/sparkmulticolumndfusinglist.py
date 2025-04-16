from pyspark.sql import SparkSession

if __name__ == '__main__':
    ages_list = [(12,), (34,), (42,), (25,), (56,)]
    sparkSession = SparkSession.builder.appName("sparkmulticolumndfusinglist").master("local").getOrCreate()
    df = sparkSession.createDataFrame(ages_list, 'age int')
    df.printSchema()
    df.show()

    users_list = [(1, "Scott"), (2, "Donald"), (3, "Mickey"), (4, "Elvis")]
    user_df=sparkSession.createDataFrame(users_list, 'user_id int, name string')
    user_df.printSchema()
    user_df.show()
