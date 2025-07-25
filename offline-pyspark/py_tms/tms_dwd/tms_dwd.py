from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import


# spark = SparkSession.builder \
#     .appName("WL_Dimension_Tables") \                          # 设置应用名称
#     .master("local[x]") \                                      # 本地模式运行，使用x个CPU核心
#     .config("hive.metastore.uris", "thrift://cdh01:9083") \    # Hive元数据存储地址
#     .config("spark.driver.host", "localhost") \                # 驱动主机地址
#     .config("spark.driver.bindAddress", "127.0.0.1") \         # 驱动绑定地址
#     .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020")\ # HDFS默认地址
#     .enableHiveSupport() \                                     # 启用Hive支持
#     .getOrCreate()                                             # 创建或获取现有Session

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("WL_Dimension_Tables") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .enableHiveSupport() \
    .getOrCreate()

# 导入Java类用于HDFS操作
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.Path")
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.FileSystem")

# 获取HDFS文件系统实例
fs = spark.sparkContext._jvm.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())

spark.sql("USE tms")

def create_hdfs_dir(path):
    jvm_path = spark.sparkContext._jvm.Path(path)       # 将路径转换为Java的Path对象
    if not fs.exists(jvm_path):     # 检查路径是否已存在
        fs.mkdirs(jvm_path)     # 创建目录(包括所有不存在的父目录)
        print(f"HDFS目录创建成功：{path}")
    else:
        print(f"HDFS目录已存在：{path}")

# 新增：修复Hive表分区的函数（关键）
def repair_hive_table(table_name):
    spark.sql(f"MSCK REPAIR TABLE tms_spark_dim.{table_name}")
    print(f"修复分区完成：tms_spark_dim.{table_name}")

# 新增：打印数据量的函数（验证数据是否存在）
def print_data_count(df, table_name):
    count = df.count()      # 计算DataFrame的行数
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count        # 返回计数值

