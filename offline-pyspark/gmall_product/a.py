from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# 修复：使用正确的方法获取Hadoop版本
print("Hadoop 版本:", spark.sparkContext._jsc.hadoopConfiguration().get("hadoop.version"))