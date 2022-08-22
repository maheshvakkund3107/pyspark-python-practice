from pyspark import StorageLevel
from pyspark.sql import SparkSession

if __name__ == '__main__':
    sparkSession = SparkSession.builder.appName("DataProcessing").master("local[2]").getOrCreate()
    r1 = sparkSession.sparkContext.textFile(
        "/Users/maheshvakkund/Documents/XEROX/java-spark/data-master/mahesh.txt", 2)
    r2 = r1.map(lambda x: x)
    r3 = r2.flatMap(lambda x: x)
    # Rdd will be cached in mem
    r3.persist(StorageLevel.MEMORY_ONLY)
    # Rdd will be cached in mem with replication factor as 2
    r3.persist(StorageLevel.MEMORY_ONLY_2)
    # Rdd will be cached in mem , if mem is not available then it will be cached in disk
    r3.persist(StorageLevel.MEMORY_AND_DISK)
    # Rdd will be cached in mem , if mem is not available then it will be cached in disk with replication factor as 2
    r3.persist(StorageLevel.MEMORY_AND_DISK_2)
    # Rdd will be cached in disk
    r3.persist(StorageLevel.DISK_ONLY)
    # Rdd will be cached in mem  with replication factor as 2
    r3.persist(StorageLevel.DISK_ONLY_2)
    # Rdd will be cached in mem  with replication factor as 3
    r3.persist(StorageLevel.DISK_ONLY_3)
    # Rdd will be cached in off_heap storage
    r3.persist(StorageLevel.OFF_HEAP)
    # Rdd will be cached in mem , if mem is not available then it will be cached in disk, but it will be only be
    # deserialized for mem and not disk
    r3.persist(StorageLevel.MEMORY_AND_DISK_DESER)

    r3.collect()
    r4 = r2.map(lambda x: x)
    r5 = r3.map(lambda x: x)
    r5.persist(StorageLevel.MEMORY_ONLY)
    r5.persist(StorageLevel.MEMORY_ONLY_2)
    r5.persist(StorageLevel.MEMORY_AND_DISK_2)
    r5.persist(StorageLevel.DISK_ONLY)
    r5.persist(StorageLevel.DISK_ONLY_2)
    r5.persist(StorageLevel.DISK_ONLY_3)
    r5.persist(StorageLevel.OFF_HEAP)
    r5.persist(StorageLevel.MEMORY_AND_DISK_DESER)
    r5.collect()
    while True:
        print('infinite while')
