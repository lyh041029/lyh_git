# 工单编号：大数据-电商数仓-09-流量主题店内路径看板
# 功能：处理店内路径及页面访问数据，生成可视化看板所需指标
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import datetime

spark = SparkSession.builder \
    .appName("电商数仓-流量主题店内路径看板") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.driver.extraJavaOptions", r"-Djava.library.path=E:\hadoop\hadoop-3.2.0\bin") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

# 1. 初始化SparkSession
# spark = SparkSession.builder \
#     .appName("电商数仓-流量主题店内路径看板") \
#     .config("spark.sql.shuffle.partitions", "200") \
#     .master("local[*]") \
#     .enableHiveSupport() \
#     .getOrCreate()


# 2. 定义数据schema（基于文档中无线端/PC端数据需求）
ods_schema = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("session_id", StringType(), nullable=False),
    StructField("page_type", StringType(), nullable=True),  # 店铺页/商品详情页/店铺其他页
    StructField("page_name", StringType(), nullable=True),  # 首页/活动页/商品A详情页等
    StructField("refer_page_type", StringType(), nullable=True),  # 来源页面类型
    StructField("refer_page_name", StringType(), nullable=True),  # 来源页面名称
    StructField("visit_time", TimestampType(), nullable=False),
    StructField("stay_duration", IntegerType(), nullable=True),  # 停留时长(秒)
    StructField("device_type", StringType(), nullable=False),  # 无线端/PC端
    StructField("is_new_user", BooleanType(), nullable=True),
    StructField("order_count", IntegerType(), nullable=True),  # 下单买家数相关
    StructField("dt", StringType(), nullable=False)  # 分区日期
])

# 3. 读取ODS层数据（模拟100万+数据量，满足文档性能优化要求）
# 实际场景可替换为Hive表读取：spark.table("ods.ods_user_path_log")
mock_data_path = "G:/dashuju/zshixun/daima/ider_daima/lyh_git/offline-pyspark/gmall_product/spark-warehouse"
ods_df = spark.read \
    .schema(ods_schema) \
    .parquet(mock_data_path) \
    .filter("dt >= '2025-01-01'")  # 支持多维度时间查询

# 4. DWD层数据清洗（处理页面类型分类及空值）
dwd_df = ods_df.withColumn(
    "page_type",
    when(col("page_type").isin("店铺页", "商品详情页", "店铺其他页"), col("page_type"))
    .otherwise("店铺其他页")  # 兜底处理，符合文档定义
).withColumn(
    "refer_page_type",
    when(col("refer_page_type").isin("店铺页", "商品详情页", "店铺其他页"), col("refer_page_type"))
    .otherwise("未知")
).withColumn(
    "stay_duration",
    when(col("stay_duration").isNull(), 0).otherwise(col("stay_duration"))
).select(
    "user_id", "session_id", "page_type", "page_name",
    "refer_page_type", "refer_page_name", "visit_time",
    "stay_duration", "device_type", "is_new_user", "order_count", "dt"
)

# 5. 生成ADS层页面访问排行指标（按访客数排行）
# 支持日/7天/30天维度（通过dt过滤实现）
ads_page_rank_df = dwd_df.groupBy(
    "dt", "device_type", "page_type", "page_name"
).agg(
    countDistinct("user_id").alias("visitor_count"),  # 访客数
    count("*").alias("pv"),  # 浏览量
    avg("stay_duration").alias("avg_stay_duration"),  # 平均停留时长
    sum("order_count").alias("order_buyer_count")  # 下单买家数
).withColumn(
    "rank_num",
    row_number().over(
        Window.partitionBy("dt", "device_type")
        .orderBy(col("visitor_count").desc())
    )
).select(
    "dt", "device_type", "page_type", "page_name",
    "visitor_count", "pv", "avg_stay_duration",
    "order_buyer_count", "rank_num"
)

# 6. 生成店内路径流转指标（不做去重，符合文档数据特性）
ads_path_flow_df = dwd_df.filter("refer_page_type != '未知'").groupBy(
    "dt", "device_type",
    col("refer_page_type").alias("source_page_type"),
    col("refer_page_name").alias("source_page_name"),
    col("page_type").alias("target_page_type"),
    col("page_name").alias("target_page_name")
).agg(
    count("*").alias("flow_count")  # 流转次数，支持来源≠去向场景
).select(
    "dt", "device_type", "source_page_type", "source_page_name",
    "target_page_type", "target_page_name", "flow_count"
)

# 7. 生成PC端来源页面TOP20（文档中PC端数据需求）
pc_source_top20_df = dwd_df.filter("device_type = 'PC端'").groupBy(
    "dt", "refer_page_type", "refer_page_name"
).agg(
    countDistinct("user_id").alias("visitor_count")
).withColumn(
    "source_ratio",  # 来源占比
    col("visitor_count") / sum("visitor_count").over(Window.partitionBy("dt"))
).withColumn(
    "rank",
    row_number().over(Window.partitionBy("dt").orderBy(col("visitor_count").desc()))
).filter("rank <= 20")

# 8. 数据写入ADS层（分区存储，支持快速查询）
ads_page_rank_df.write.mode("overwrite").partitionBy("dt", "device_type") \
    .saveAsTable("ads.ads_page_visit_rank")

ads_path_flow_df.write.mode("overwrite").partitionBy("dt", "device_type") \
    .saveAsTable("ads.ads_instore_path_flow")

pc_source_top20_df.write.mode("overwrite").partitionBy("dt") \
    .saveAsTable("ads.ads_pc_source_top20")

# 9. 打印测试数据样例
print("页面访问排行Top5数据：")
ads_page_rank_df.orderBy("dt", "device_type", "rank_num").limit(5).show()

print("店内路径流转样例数据：")
ads_path_flow_df.orderBy("dt", "flow_count", ascending=False).limit(5).show()

spark.stop()