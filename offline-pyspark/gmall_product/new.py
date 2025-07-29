# 工单编号：大数据-电商数仓-10-流量主题页面分析看板
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("liuliangzhuti") \
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate()

# 1. ODS层：原始数据读取（模拟100万+条数据）
# 定义 schema
ods_schema = StructType([
    StructField("page_id", StringType(), True),
    StructField("page_type", StringType(), True),  # 首页/自定义承接页/商品详情页
    StructField("user_id", StringType(), True),
    StructField("click_module", StringType(), True),
    StructField("click_time", StringType(), True),
    StructField("stay_duration", IntegerType(), True),
    StructField("is_pay", IntegerType(), True),
    StructField("pay_amount", DoubleType(), True),
    StructField("data_date", StringType(), True)
])

# 读取模拟数据（实际场景可替换为HDFS/数据库路径）
ods_page_traffic = spark.read \
    .schema(ods_schema) \
    .option("header", "true") \
    .csv("hdfs://cdh01:8020/warehouse/gmall_product")  # 模拟数据存储路径

# 2. DWD层：数据清洗转换
dwd_page_traffic_clean = ods_page_traffic \
    .filter(col("click_time").isNotNull() & (col("stay_duration") >= 0)) \
    .withColumn("click_hour", hour(to_timestamp(col("click_time"), "yyyy-MM-dd HH:mm:ss"))) \
    .select(
    "page_id", "page_type", "user_id", "click_module",
    "click_hour", "stay_duration", "is_pay", "pay_amount", "data_date"
)

# 3. DWS层：聚合分析（支持页面概览、点击分布等指标）
# 3.1 页面访问与转化指标
dws_page_agg = dwd_page_traffic_clean \
    .groupBy("page_id", "page_type", "data_date") \
    .agg(
    count("*").alias("pv"),  # 访问量
    countDistinct("user_id").alias("uv"),  # 独立访客数
    avg("stay_duration").alias("avg_stay_duration"),  # 平均停留时长
    sum(when(col("is_pay") == 1, 1).otherwise(0)).alias("pay_users"),  # 支付用户数
    sum("pay_amount").alias("total_pay_amount")  # 总引导支付金额
) \
    .withColumn("pay_conversion_rate",  # 支付转化率
                (col("pay_users") / col("uv") * 100).cast(DecimalType(5, 2)))

# 3.2 点击分布明细（各模块点击数据）
dws_module_click = dwd_page_traffic_clean \
    .groupBy("page_id", "data_date", "click_module") \
    .agg(
    count("*").alias("click_count"),  # 模块点击量
    countDistinct("user_id").alias("click_users")  # 模块点击人数
)

# 4. ADS层：看板展示数据（整合近30天趋势、引导详情等）
# 4.1 页面类型趋势数据（近30天）
ads_page_trend = dws_page_agg \
    .groupBy("page_type", "data_date") \
    .agg(
    sum("pv").alias("total_pv"),
    sum("uv").alias("total_uv"),
    sum("total_pay_amount").alias("total_pay")
) \
    .orderBy("page_type", "data_date")

# 4.2 各页面TOP点击模块（支持装修诊断）
ads_top_module = dws_module_click \
    .withColumn("rn", row_number().over(
    Window.partitionBy("page_id", "data_date").orderBy(col("click_count").desc())
)) \
    .filter(col("rn") == 1) \
    .select(
    "page_id", "data_date",
    col("click_module").alias("top_click_module"),
    col("click_count").alias("top_click_count")
)

# 5. 数据写出（示例：保存到Hive表）
dws_page_agg.write.mode("overwrite").saveAsTable("dws.dws_page_traffic_agg")
ads_page_trend.write.mode("overwrite").saveAsTable("ads.ads_page_trend")
ads_top_module.write.mode("overwrite").saveAsTable("ads.ads_top_module")

spark.stop()