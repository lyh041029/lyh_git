from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ODS_Goods_Ranking") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .enableHiveSupport() \
    .getOrCreate()

java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.Path")
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
fs = spark.sparkContext._jvm.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
spark.sql("USE gmall_02")

# 工具函数
def create_hdfs_dir(path):
    jvm_path = spark.sparkContext._jvm.Path(path)
    if not fs.exists(jvm_path):
        fs.mkdirs(jvm_path)
        print(f"HDFS目录创建成功：{path}")
    else:
        print(f"HDFS目录已存在：{path}")

def repair_hive_table(table_name):
    spark.sql(f"MSCK REPAIR TABLE gmall_02.{table_name}")
    print(f"修复分区完成：gmall_02.{table_name}")

def print_data_count(df, table_name):
    count = df.count()
    print(f"{table_name} 数据量：{count} 行")
    return count

# ====================== 商品销售原始表 ods_goods_sales ======================
create_hdfs_dir("/warehouse/work_order/gmall_02/ods/ods_goods_sales")
spark.sql("DROP TABLE IF EXISTS gmall_02.ods_goods_sales")
spark.sql("""
CREATE EXTERNAL TABLE gmall_02.ods_goods_sales (
    goods_id STRING COMMENT '商品ID',
    goods_name STRING COMMENT '商品名称',
    category_id STRING COMMENT '分类ID',
    category_name STRING COMMENT '分类名称',
    sku_id STRING COMMENT 'SKU ID',
    sku_info STRING COMMENT 'SKU信息（颜色/规格等）',
    pay_amount DOUBLE COMMENT '支付金额',
    pay_num INT COMMENT '支付件数',
    pay_buyer_num INT COMMENT '支付买家数',
    visitor_num INT COMMENT '商品详情页访客数',
    sales_time TIMESTAMP COMMENT '销售时间',
    shop_id STRING COMMENT '店铺ID'
) 
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS ORC
LOCATION '/warehouse/work_order/gmall_02/ods/ods_goods_sales'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 读取数据源并写入
csv_path = "hdfs://cdh01:8020/warehouse/work_order/gmall_02/data/ods_goods_sales.csv"
df = spark.read.csv(
    csv_path,
    header=True,
    sep='\t',
    inferSchema=True
)
df_with_partition = df.withColumn("dt", F.date_format(F.col("sales_time"), "yyyyMMdd"))
df_with_partition.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/work_order/gmall_02/ods/ods_goods_sales"
)
print_data_count(df, "ods_goods_sales")
repair_hive_table("ods_goods_sales")

# ====================== 商品流量来源原始表 ods_goods_traffic ======================
create_hdfs_dir("/warehouse/work_order/gmall_02/ods/ods_goods_traffic")
spark.sql("DROP TABLE IF EXISTS gmall_02.ods_goods_traffic")
spark.sql("""
CREATE EXTERNAL TABLE gmall_02.ods_goods_traffic (
    goods_id STRING COMMENT '商品ID',
    traffic_source STRING COMMENT '流量来源（效果广告/手淘搜索等）',
    visitor_num INT COMMENT '来源访客数',
    pay_conversion_rate DOUBLE COMMENT '来源支付转化率',
    traffic_time TIMESTAMP COMMENT '统计时间',
    shop_id STRING COMMENT '店铺ID'
) 
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS ORC
LOCATION '/warehouse/work_order/gmall_02/ods/ods_goods_traffic'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

csv_path = "hdfs://cdh01:8020/warehouse/work_order/gmall_02/data/ods_goods_traffic.csv"
df = spark.read.csv(
    csv_path,
    header=True,
    sep='\t',
    inferSchema=True
)
df_with_partition = df.withColumn("dt", F.date_format(F.col("traffic_time"), "yyyyMMdd"))
df_with_partition.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/work_order/gmall_02/ods/ods_goods_traffic"
)
print_data_count(df, "ods_goods_traffic")
repair_hive_table("ods_goods_traffic")

# ====================== 商品搜索词原始表 ods_goods_search ======================
create_hdfs_dir("/warehouse/work_order/gmall_02/ods/ods_goods_search")
spark.sql("DROP TABLE IF EXISTS gmall_02.ods_goods_search")
spark.sql("""
CREATE EXTERNAL TABLE gmall_02.ods_goods_search (
    goods_id STRING COMMENT '商品ID',
    search_word STRING COMMENT '搜索词',
    search_num INT COMMENT '搜索次数',
    visitor_num INT COMMENT '搜索访客数',
    search_time TIMESTAMP COMMENT '统计时间',
    shop_id STRING COMMENT '店铺ID'
) 
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS ORC
LOCATION '/warehouse/work_order/gmall_02/ods/ods_goods_search'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

csv_path = "hdfs://cdh01:8020/warehouse/work_order/gmall_02/data/ods_goods_search.csv"
df = spark.read.csv(
    csv_path,
    header=True,
    sep='\t',
    inferSchema=True
)
df_with_partition = df.withColumn("dt", F.date_format(F.col("search_time"), "yyyyMMdd"))
df_with_partition.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/work_order/gmall_02/ods/ods_goods_search"
)
print_data_count(df, "ods_goods_search")
repair_hive_table("ods_goods_search")

# ====================== 价格力商品原始表 ods_price_strength_goods ======================
create_hdfs_dir("/warehouse/work_order/gmall_02/ods/ods_price_strength_goods")
spark.sql("DROP TABLE IF EXISTS gmall_02.ods_price_strength_goods")
spark.sql("""
CREATE EXTERNAL TABLE gmall_02.ods_price_strength_goods (
    goods_id STRING COMMENT '商品ID',
    price_strength_star INT COMMENT '价格力星级（1-5）',
    coupon_after_price DOUBLE COMMENT '普惠券后价',
    market_avg_conversion DOUBLE COMMENT '市场平均转化率',
    current_conversion DOUBLE COMMENT '当前转化率',
    last_conversion DOUBLE COMMENT '上期转化率',
    is_high_price TINYINT COMMENT '是否命中高价规则（1=是）',
    low_star_days INT COMMENT '持续低星天数',
    check_time TIMESTAMP COMMENT '检查时间',
    shop_id STRING COMMENT '店铺ID'
) 
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS ORC
LOCATION '/warehouse/work_order/gmall_02/ods/ods_price_strength_goods'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

csv_path = "hdfs://cdh01:8020/warehouse/work_order/gmall_02/data/ods_price_strength_goods.csv"
df = spark.read.csv(
    csv_path,
    header=True,
    sep='\t',
    inferSchema=True
)
df_with_partition = df.withColumn("dt", F.date_format(F.col("check_time"), "yyyyMMdd"))
df_with_partition.write.mode("overwrite").partitionBy("dt").orc(
    "/warehouse/work_order/gmall_02/ods/ods_price_strength_goods"
)
print_data_count(df, "ods_price_strength_goods")
repair_hive_table("ods_price_strength_goods")

spark.stop()