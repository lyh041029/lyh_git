from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("DWD_Goods_Ranking") \
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

# 设置数据库
spark.sql("USE gmall_02")

def create_hdfs_dir(path):
    """创建HDFS目录"""
    jvm_path = spark.sparkContext._jvm.Path(path)
    if not fs.exists(jvm_path):
        fs.mkdirs(jvm_path)
        print(f"HDFS目录创建成功：{path}")
    else:
        print(f"HDFS目录已存在：{path}")

def repair_hive_table(table_name):
    """修复Hive表分区"""
    spark.sql(f"MSCK REPAIR TABLE gmall_02.{table_name}")
    print(f"修复分区完成：gmall_02.{table_name}")

def print_data_count(df, table_name):
    """打印数据量用于验证"""
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count

def force_delete_hdfs_path(path):
    jvm_path = spark.sparkContext._jvm.Path(path)
    if fs.exists(jvm_path):
        # 递归删除所有文件和子目录
        fs.delete(jvm_path, True)
        print(f"已强制删除HDFS路径及所有内容：{path}")
    else:
        print(f"HDFS路径不存在：{path}")


# 处理日期
process_date = "20250807"

# ====================== 商品销售明细 dwd_goods_sales_detail ======================
create_hdfs_dir("/warehouse/work_order/gmall_02/dwd/dwd_goods_sales_detail")
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_02.dwd_goods_sales_detail (
    goods_id STRING COMMENT '商品ID',
    goods_name STRING COMMENT '商品名称',
    category_id STRING COMMENT '分类ID',
    category_name STRING COMMENT '分类名称',
    sku_id STRING COMMENT 'SKU ID',
    sku_info STRING COMMENT 'SKU信息',
    pay_amount DOUBLE COMMENT '支付金额',
    pay_num INT COMMENT '支付件数',
    pay_buyer_num INT COMMENT '支付买家数',
    visitor_num INT COMMENT '商品详情页访客数',
    pay_conversion_rate DOUBLE COMMENT '支付转化率=支付买家数/访客数',
    sales_date DATE COMMENT '销售日期',
    shop_id STRING COMMENT '店铺ID'
) 
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS PARQUET
LOCATION '/warehouse/work_order/gmall_02/dwd/dwd_goods_sales_detail'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 清洗销售数据并计算转化率
dwd_sales = spark.table("gmall_02.ods_goods_sales").filter(F.col("dt") == process_date) \
    .withColumn("pay_conversion_rate",
                F.round(F.col("pay_buyer_num") / F.col("visitor_num"), 4)) \
    .withColumn("sales_date", F.to_date(F.col("sales_time"))) \
    .select(
    "goods_id", "goods_name", "category_id", "category_name",
    "sku_id", "sku_info", "pay_amount", "pay_num", "pay_buyer_num",
    "visitor_num", "pay_conversion_rate", "sales_date", "shop_id", "dt"
).filter(
    (F.col("visitor_num") > 0) &  # 过滤无效访客数
    (F.col("goods_id").isNotNull())
)

dwd_sales.write.mode("append").partitionBy("dt").parquet(
    "/warehouse/work_order/gmall_02/dwd/dwd_goods_sales_detail"
)
repair_hive_table("dwd_goods_sales_detail")
print(f"商品销售明细数据量：{dwd_sales.count()} 行")

# ====================== 价格力商品明细 dwd_price_strength_detail ======================
create_hdfs_dir("/warehouse/work_order/gmall_02/dwd/dwd_price_strength_detail")
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_02.dwd_price_strength_detail (
    goods_id STRING COMMENT '商品ID',
    price_strength_star INT COMMENT '价格力星级',
    coupon_after_price DOUBLE COMMENT '普惠券后价',
    price_warn_flag STRING COMMENT '价格力预警（normal/warn）',
    goods_strength_warn_flag STRING COMMENT '商品力预警（normal/warn）',
    check_date DATE COMMENT '检查日期',
    shop_id STRING COMMENT '店铺ID'
) 
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS PARQUET
LOCATION '/warehouse/work_order/gmall_02/dwd/dwd_price_strength_detail'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 清洗价格力数据并生成预警标识
dwd_price = spark.table("gmall_02.ods_price_strength_goods").filter(F.col("dt") == process_date) \
    .withColumn("price_warn_flag",
                F.when(
                    ( (F.col("price_strength_star") <= 2) & (F.col("low_star_days") >= 3) ) |
                    (F.col("is_high_price") == 1),
                    "warn"
                ).otherwise("normal")) \
    .withColumn("goods_strength_warn_flag",
                F.when(
                    (F.col("current_conversion") < F.col("market_avg_conversion")) &
                    (F.col("current_conversion") < F.col("last_conversion")),
                    "warn"
                ).otherwise("normal")) \
    .withColumn("check_date", F.to_date(F.col("check_time"))) \
    .select(
    "goods_id", "price_strength_star", "coupon_after_price",
    "price_warn_flag", "goods_strength_warn_flag", "check_date", "shop_id", "dt"
)

dwd_price.write.mode("append").partitionBy("dt").parquet(
    "/warehouse/work_order/gmall_02/dwd/dwd_price_strength_detail"
)
repair_hive_table("dwd_price_strength_detail")
print(f"价格力商品明细数据量：{dwd_price.count()} 行")

spark.stop()