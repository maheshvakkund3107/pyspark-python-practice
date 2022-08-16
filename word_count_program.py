from pyspark.sql import SparkSession

if __name__ == '__main__':
    sparkSession = SparkSession.builder.appName("Spark Word Count").master("local[2]").getOrCreate()
    raw_rdd = sparkSession.sparkContext.textFile(
        "/Users/maheshvakkund/Documents/XEROX/java-spark/data-master/mahesh.txt", 2)
    textFile = raw_rdd.flatMap(lambda line: line.split(" "))
    words_rdd = textFile.map(lambda t: (t, 1))
    count_rdd = words_rdd.reduceByKey(lambda c1, c2: c1 + c2)
    print(count_rdd.collect())
