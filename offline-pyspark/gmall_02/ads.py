# 关联销售汇总与流销词数据
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ADS_Goods_Ranking") \
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


# -------------------------- 核心修改：生成30天日期列表 --------------------------
def generate_date_range(end_date, days=30):
    """生成从end_date向前推days天的日期列表（格式：yyyyMMdd）"""
    end = datetime.strptime(end_date, "%Y%m%d")
    date_list = [(end - timedelta(days=i)).strftime("%Y%m%d") for i in range(days)]
    return date_list

# 定义结束日期（原date_list），生成向前30天的日期列表
end_date = "20250807"  # 结束日期
date_list = generate_date_range(end_date, days=30)  # 30天日期列表：["20250710", "20250709", ..., "20250611"]
print(f"处理的日期范围：{date_list[0]} 至 {date_list[-1]}（共30天）")

# ====================== 商品排行看板 ads_goods_ranking_board ======================
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_02.ads_goods_ranking_board (
    goods_id STRING COMMENT '商品ID',
    goods_name STRING COMMENT '商品名称',
    category_name STRING COMMENT '分类名称',
    stat_period STRING COMMENT '统计周期',
    sales_rank INT COMMENT '销售额排名',
    num_rank INT COMMENT '销量排名',
    total_sales DOUBLE COMMENT '总销售额',
    total_num INT COMMENT '总销量',
    avg_conversion DOUBLE COMMENT '平均转化率',
    flow_top10 ARRAY<STRUCT<source:STRING, visitor:INT, conversion:DOUBLE>> COMMENT 'TOP10流量来源',
    sku_top5 ARRAY<STRUCT<sku:STRING, pay_num:INT, stock:INT>> COMMENT 'TOP5 SKU销售',
    word_top10 ARRAY<STRUCT<word:STRING, search_num:INT>> COMMENT 'TOP10搜索词',
    end_date DATE COMMENT '周期结束日期',
    shop_id STRING COMMENT '店铺ID'
) 
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS PARQUET
LOCATION '/warehouse/work_order/gmall_02/ads/ads_goods_ranking_board'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")




# 定义 dws_goods_sales_summary 表的 schema，明确 total_num 为 LongType
sales_summary_schema = StructType([
    StructField("goods_id", StringType(), True),
    StructField("shop_id", StringType(), True),
    StructField("total_num", LongType(), True),  # 强制指定为 Long 类型
    StructField("total_sales", DoubleType(), True),
    StructField("stat_date", StringType(), True),  # 根据实际字段补充
    StructField("dt", StringType(), True)  # 分区字段
    # 补充其他字段...
])

# 替换原来的sales_data读取部分
sales_data = spark.read \
    .schema(sales_summary_schema) \
    .format("parquet") \
    .load("/warehouse/work_order/gmall_02/dws/dws_goods_sales_summary") \
    .filter(F.col("dt").isin(date_list)) \
    .alias("s")

goods_info = spark.table("gmall_02.dwd_goods_sales_detail") \
    .select("goods_id", "goods_name", "category_name").dropDuplicates()
flow_schema = StructType([
    StructField("goods_id", StringType(), True),
    StructField("flow_top10", ArrayType(
        StructType([
            StructField("source", StringType(), True),
            StructField("visitor", IntegerType(), True),  # 明确为Int类型（原可能被误判为Long）
            StructField("conversion", DoubleType(), True)
        ])
    ), True),
    StructField("sku_top5", ArrayType(
        StructType([
            StructField("sku", StringType(), True),
            StructField("pay_num", IntegerType(), True),
            StructField("stock", IntegerType(), True)
        ])
    ), True),
    StructField("word_top10", ArrayType(
        StructType([
            StructField("word", StringType(), True),
            StructField("search_num", IntegerType(), True)  # 明确为Int类型
        ])
    ), True),
    StructField("stat_date", StringType(), True),
    StructField("shop_id", StringType(), True),
    StructField("dt", StringType(), True)  # 分区字段
])

# 使用明确的schema读取flow_data（修复核心）
# 修正flow_data的读取方式
flow_data = spark.read \
    .table("gmall_02.dws_goods_flow_sales_word") \
    .filter(F.col("dt").isin(date_list)) \
    .alias("f")

# 同时检查sales_data的关联逻辑，确保stat_date字段明确
ranking_data = sales_data \
    .join(goods_info, on="goods_id", how="left") \
    .join(flow_data, on=["goods_id", "shop_id"], how="left") \
    .withColumn("sales_rank", F.row_number().over(
    Window.partitionBy("s.stat_date", "category_name").orderBy(F.col("total_sales").desc())  # 明确s.stat_date
)) \
    .withColumn("num_rank", F.row_number().over(
    Window.partitionBy("s.stat_date", "category_name").orderBy(F.col("total_num").desc())  # 明确s.stat_date
)) \
    .select(
    "goods_id", "goods_name", "category_name",
    F.col("s.stat_date").alias("stat_date"),  # 明确stat_date来源
    "sales_rank", "num_rank", "total_sales", "total_num",
    # 移除不存在的avg_conversion字段
    "flow_top10", "sku_top5", "word_top10",
    F.lit(end_date).alias("end_date"),  # 使用定义的end_date变量
    "shop_id",
    F.col("s.dt").alias("dt")
)

repair_hive_table("ads_goods_ranking_board")
print(f"商品排行看板数据量：{ranking_data.count()} 行")

# ====================== 价格力商品看板 ads_price_strength_board ======================
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_02.ads_price_strength_board (
    goods_id STRING COMMENT '商品ID',
    price_strength_star INT COMMENT '价格力星级',
    coupon_after_price DOUBLE COMMENT '普惠券后价',
    price_warn_flag STRING COMMENT '价格力预警',
    goods_strength_warn_flag STRING COMMENT '商品力预警',
    strength_rank INT COMMENT '价格力排名',
    check_date DATE COMMENT '检查日期',
    shop_id STRING COMMENT '店铺ID'
) 
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS PARQUET
LOCATION '/warehouse/work_order/gmall_02/ads/ads_price_strength_board'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 价格力商品排名
price_data = spark.table("gmall_02.dwd_price_strength_detail").filter(F.col("dt").isin(date_list)) \
    .withColumn("strength_rank", F.row_number().over(
    Window.partitionBy("shop_id").orderBy(F.col("price_strength_star").desc())
)) \
    .select(
    "goods_id", "price_strength_star", "coupon_after_price",
    "price_warn_flag", "goods_strength_warn_flag", "strength_rank",
    "check_date", "shop_id", "dt"
)

price_data.write.mode("overwrite").partitionBy("dt").parquet(
    "/warehouse/work_order/gmall_02/ads/ads_price_strength_board"
)
repair_hive_table("ads_price_strength_board")
print(f"价格力商品看板数据量：{price_data.count()} 行")

# 数据验证
print("===== 商品排行TOP3 =====")
ranking_data.filter(F.col("sales_rank") <=3).select(
    "goods_name", "category_name", "total_sales", "sales_rank"
).show(3)

print("===== 价格力预警商品 =====")
price_data.filter(F.col("price_warn_flag") == "warn").select(
    "goods_id", "price_strength_star"
).show(3)

spark.stop()
