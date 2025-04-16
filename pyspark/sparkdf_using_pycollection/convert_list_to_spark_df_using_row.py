from pyspark import Row
from pyspark.sql import SparkSession

if __name__ == '__main__':
    #List of List
    user_list = [[1, "Scott"], [2, "Donald"], [3, "Mickey"]]
    sparkSession=SparkSession.builder.appName("ConvertListToSparkDfUsingRow").getOrCreate()
    user_list_df=sparkSession.createDataFrame(user_list, 'user_id int, name string')
    user_list_df.show()
    user_list_df.printSchema()

    #List Comprehensions
    users_row =  [Row(*user) for user in user_list]
    user_list_df = sparkSession.createDataFrame(users_row, 'user_id int, name string')
    user_list_df.show()
    user_list_df.printSchema()

    # List of Tuples
    user_list = [(1,"Scott"),(2,"Donald"),(3,"Mickey")]
    user_list_df=sparkSession.createDataFrame(user_list, 'user_id int, name string')
    user_list_df.show()
    user_list_df.printSchema()

    users_row = [Row(*user) for user in user_list]
    users_row = sparkSession.createDataFrame(users_row, 'user_id int, first_name string')
    users_row.show()
    users_row.printSchema()

