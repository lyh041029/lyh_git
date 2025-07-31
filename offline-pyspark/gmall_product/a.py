from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, datediff, current_date, desc, month, year
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
import random
from datetime import datetime, timedelta
import os
os.environ["PYSPARK_PYTHON"] = r"E:\anaconda3\anaconda\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"E:\anaconda3\anaconda\python.exe"
# 初始化SparkSession，配置环境参数解决通信问题
spark = SparkSession.builder \
    .appName("电商数仓-商品主题新品追踪看板") \
    .master("local[1]") \
    .config("spark.pyspark.python", "python") \
    .config("spark.network.timeout", "1200s") \
    .config("spark.executor.heartbeatInterval", "600s") \
    .config("spark.sql.broadcastTimeout", "1200s") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
# 1. 定义数据结构（符合数仓规范）
schema = StructType([
    StructField("product_id", StringType(), nullable=False),  # 商品ID
    StructField("category", StringType(), nullable=False),    # 商品类目
    StructField("title", StringType(), nullable=False),       # 商品标题
    StructField("putaway_date", DateType(), nullable=False),  # 上架日期
    StructField("price", DoubleType(), nullable=False),       # 售价
    StructField("sales", IntegerType(), nullable=False),      # 销量
    StructField("pay_amount", DoubleType(), nullable=False),  # 支付金额
    StructField("tag", StringType(), nullable=False),         # 标签：普通新品/小黑盒
    StructField("is_tmall_new", IntegerType(), nullable=False)# 是否天猫新品(1/0)
])

# 2. 生成1000条模拟数据
def generate_mock_data():
    data = []
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    categories = ["服饰", "数码", "美妆", "家居", "食品"]

    for i in range(1000):
        # 随机生成上架日期
        days = random.randint(0, (end_date - start_date).days)
        putaway_date = start_date + timedelta(days=days)

        # 构造数据
        sales = random.randint(0, 500)
        price = round(random.uniform(9.9, 9999.9), 2)
        data.append((
            f"P{2024000000 + i}",
            random.choice(categories),
            f"{random.choice(categories)}新品{i%100} - 2024夏季款",
            putaway_date,
            price,
            sales,
            round(price * sales, 2),  # 支付金额=售价×销量
            random.choice(["普通新品", "小黑盒新品"]),
            random.randint(0, 1)
        ))

    return spark.createDataFrame(data, schema)

# 3. 数据分层处理（ODS->DWD->DWS->ADS）
def process_data(mock_df):
    # ODS层：原始数据
    ods_df = mock_df
    ods_df.createOrReplaceTempView("ods_new_products")
    print("ODS层数据示例：")
    ods_df.select("product_id", "category", "putaway_date", "tag").show(3)

    # DWD层：数据清洗与转换
    dwd_df = ods_df.filter(
        (col("sales") >= 0) &
        (col("price") >= 0) &
        (col("putaway_date").isNotNull())
    ).withColumn(
        "days_on_shelf", datediff(current_date(), col("putaway_date"))
    ).withColumn(
        "month", month(col("putaway_date"))  # 提取月份用于复盘
    )
    dwd_df.createOrReplaceTempView("dwd_new_products")
    print("\nDWD层清洗后数据：")
    dwd_df.cache().createOrReplaceTempView("dwd_new_products")
    dwd_df.select("product_id", "days_on_shelf", "sales", "pay_amount").show(3)


    # DWS层：聚合计算（核心指标）
    # 3.1 30天内新品整体表现
    dws_overall = dwd_df.filter(col("days_on_shelf") <= 30) \
        .agg(
        sum("pay_amount").alias("total_pay"),
        sum("sales").alias("total_sales"),
        count("product_id").alias("total_products")
    )

    # 3.2 按标签维度聚合
    dws_tag = dwd_df.filter(col("days_on_shelf") <= 30) \
        .groupBy("tag") \
        .agg(
        sum("pay_amount").alias("tag_total_pay"),
        count("product_id").alias("tag_product_count")
    )

    # 3.3 按月度聚合（全年复盘）
    dws_monthly = dwd_df.groupBy("month") \
        .agg(
        count("product_id").alias("monthly_new_count"),
        sum("pay_amount").alias("monthly_pay")
    ).orderBy("month")

    print("\nDWS层30天内整体表现：")
    dws_overall.show()
    print("\nDWS层标签维度数据：")
    dws_tag.show()

    # ADS层：看板展示数据
    # 4.1 最近上新商品（30天内，按上架时间倒序）
    ads_recent = dwd_df.filter(col("days_on_shelf") <= 30) \
        .select("product_id", "title", "category", "price", "putaway_date", "sales") \
        .orderBy(desc("putaway_date"))

    # 4.2 小黑盒新品TOP10（按支付金额排序）
    ads_blackbox_top = dwd_df.filter(
        (col("tag") == "小黑盒新品") &
        (col("days_on_shelf") <= 30)
    ).select("product_id", "title", "pay_amount", "sales") \
        .orderBy(desc("pay_amount")) \
        .limit(10)

    print("\nADS层最近上新商品：")
    ads_recent.show(5, truncate=False)
    print("\nADS层小黑盒新品TOP10：")
    ads_blackbox_top.show(5, truncate=False)

    return ads_recent

# 4. 主函数执行
if __name__ == "__main__":
    # 生成模拟数据
    mock_data = generate_mock_data()

    # 保存原始数据为CSV
    mock_data.write.csv("new_products_data", header=True, mode="overwrite")
    print("\n模拟数据已保存至 new_products_data 目录")

    # 数据处理与看板指标计算
    process_data(mock_data)

    spark.stop()
