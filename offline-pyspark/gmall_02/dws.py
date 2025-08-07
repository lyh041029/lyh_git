from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("DWS_Goods_Ranking") \
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

# 处理日期（作为结束日期）
process_date = "20250807"
end_date = datetime.strptime(process_date, "%Y%m%d").date()
# 生成30天的日期列表（从 start_date 到 process_date 共30天）
start_date = (end_date - timedelta(days=29)).strftime("%Y%m%d")
date_list = [
    (end_date - timedelta(days=i)).strftime("%Y%m%d")
    for i in range(30)
]

# ====================== 商品销售汇总 dws_goods_sales_summary ======================
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_02.dws_goods_sales_summary (
    goods_id STRING COMMENT '商品ID',
    category_id STRING COMMENT '分类ID',
    stat_period STRING COMMENT '统计周期（day/7day/30day）',
    total_sales DOUBLE COMMENT '总销售额',
    total_num INT COMMENT '总销量',
    total_visitor INT COMMENT '总访客数',
    avg_conversion DOUBLE COMMENT '平均支付转化率',
    end_date DATE COMMENT '周期结束日期',
    shop_id STRING COMMENT '店铺ID'
) 
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS PARQUET
LOCATION '/warehouse/work_order/gmall_02/dws/dws_goods_sales_summary'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 用于存储30天的销售汇总数据
all_days_sales = None

for single_dt in date_list:
    current_end_date = datetime.strptime(single_dt, "%Y%m%d").date()
    # 1. 日度汇总（针对当天）
    daily_sales = spark.table("gmall_02.dwd_goods_sales_detail").filter(F.col("dt") == single_dt) \
        .groupBy("goods_id", "category_id", "shop_id") \
        .agg(
        F.sum("pay_amount").alias("total_sales"),
        F.sum("pay_num").alias("total_num"),
        F.sum("visitor_num").alias("total_visitor"),
        F.avg("pay_conversion_rate").alias("avg_conversion")
    ) \
        .withColumn("stat_period", F.lit("day")) \
        .withColumn("end_date", F.lit(current_end_date)) \
        .withColumn("dt", F.lit(single_dt))

    # 2. 7天汇总（针对当天，往前推6天）
    seven_start = (current_end_date - timedelta(days=6)).strftime("%Y%m%d")
    seven_sales = spark.table("gmall_02.dwd_goods_sales_detail").filter(
        F.col("dt").between(seven_start, single_dt)
    ).groupBy("goods_id", "category_id", "shop_id") \
        .agg(
        F.sum("pay_amount").alias("total_sales"),
        F.sum("pay_num").alias("total_num"),
        F.sum("visitor_num").alias("total_visitor"),
        F.avg("pay_conversion_rate").alias("avg_conversion")
    ) \
        .withColumn("stat_period", F.lit("7day")) \
        .withColumn("end_date", F.lit(current_end_date)) \
        .withColumn("dt", F.lit(single_dt))

    # 3. 30天汇总（针对当天，往前推29天）
    thirty_start = (current_end_date - timedelta(days=29)).strftime("%Y%m%d")
    thirty_sales = spark.table("gmall_02.dwd_goods_sales_detail").filter(
        F.col("dt").between(thirty_start, single_dt)
    ).groupBy("goods_id", "category_id", "shop_id") \
        .agg(
        F.sum("pay_amount").alias("total_sales"),
        F.sum("pay_num").alias("total_num"),
        F.sum("visitor_num").alias("total_visitor"),
        F.avg("pay_conversion_rate").alias("avg_conversion")
    ) \
        .withColumn("stat_period", F.lit("30day")) \
        .withColumn("end_date", F.lit(current_end_date)) \
        .withColumn("dt", F.lit(single_dt))

    # 合并当天的三种统计周期数据
    daily_all = daily_sales.unionByName(seven_sales).unionByName(thirty_sales)

    # 合并到总数据中
    if all_days_sales is None:
        all_days_sales = daily_all
    else:
        all_days_sales = all_days_sales.unionByName(daily_all)

# 将30天的所有数据写入
if all_days_sales is not None:
    all_days_sales.write.mode("overwrite").partitionBy("dt").parquet(
        "/warehouse/work_order/gmall_02/dws/dws_goods_sales_summary"
    )
    repair_hive_table("dws_goods_sales_summary")
    print(f"商品销售汇总数据量：{all_days_sales.count()} 行")

# ====================== 流销词汇总 dws_goods_flow_sales_word ======================
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gmall_02.dws_goods_flow_sales_word (
    goods_id STRING COMMENT '商品ID',
    flow_top10 ARRAY<STRUCT<source:STRING, visitor:INT, conversion:DOUBLE>> COMMENT 'TOP10流量来源',
    sku_top5 ARRAY<STRUCT<sku:STRING, pay_num:INT, stock:INT>> COMMENT 'TOP5 SKU销售',
    word_top10 ARRAY<STRUCT<word:STRING, search_num:INT>> COMMENT 'TOP10搜索词',
    stat_date DATE COMMENT '统计日期',
    shop_id STRING COMMENT '店铺ID'
) 
PARTITIONED BY (dt STRING COMMENT '数据日期')
STORED AS PARQUET
LOCATION '/warehouse/work_order/gmall_02/dws/dws_goods_flow_sales_word'
TBLPROPERTIES ('parquet.compression' = 'snappy');
""")

# 用于存储30天的流销词数据
all_days_flow_sales = None

for single_dt in date_list:
    current_end_date = datetime.strptime(single_dt, "%Y%m%d").date()
    # 聚合流量、SKU、搜索词TOP数据（针对当天）
    flow_top10 = spark.table("gmall_02.ods_goods_traffic").filter(F.col("dt") == single_dt) \
        .groupBy("goods_id", "shop_id") \
        .agg(
        F.collect_list(
            F.struct(
                "traffic_source", "visitor_num", "pay_conversion_rate"
            )
        ).alias("flow_top10")
    )

    sku_top5 = spark.table("gmall_02.dwd_goods_sales_detail").filter(F.col("dt") == single_dt) \
        .groupBy("goods_id", "sku_id", "shop_id") \
        .agg(F.sum("pay_num").alias("pay_num")) \
        .withColumn("rn", F.row_number().over(
        Window.partitionBy("goods_id").orderBy(F.col("pay_num").desc())
    )).filter(F.col("rn") <=5) \
        .groupBy("goods_id", "shop_id") \
        .agg(
        F.collect_list(F.struct("sku_id", "pay_num")).alias("sku_top5")
    )

    word_top10 = spark.table("gmall_02.ods_goods_search").filter(F.col("dt") == single_dt) \
        .groupBy("goods_id", "shop_id") \
        .agg(
        F.collect_list(F.struct("search_word", "search_num")).alias("word_top10")
    )

    # 关联流销词数据（针对当天）
    flow_sales_word = flow_top10 \
        .join(sku_top5, on=["goods_id", "shop_id"], how="left") \
        .join(word_top10, on=["goods_id", "shop_id"], how="left") \
        .withColumn("stat_date", F.lit(current_end_date)) \
        .withColumn("dt", F.lit(single_dt)) \
        .select(
        "goods_id", "flow_top10", "sku_top5", "word_top10",
        "stat_date", "shop_id", "dt"
    )

    # 合并到总数据中
    if all_days_flow_sales is None:
        all_days_flow_sales = flow_sales_word
    else:
        all_days_flow_sales = all_days_flow_sales.unionByName(flow_sales_word)

# 将30天的所有流销词数据写入
if all_days_flow_sales is not None:
    all_days_flow_sales.write.mode("overwrite").partitionBy("dt").parquet(
        "/warehouse/work_order/gmall_02/dws/dws_goods_flow_sales_word"
    )
    repair_hive_table("dws_goods_flow_sales_word")
    print(f"流销词汇总数据量：{all_days_flow_sales.count()} 行")

spark.stop()