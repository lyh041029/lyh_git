import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import mysql.connector  # 替换sqlite3为mysql连接库
from mysql.connector import Error

# --------------------------
# 1. 生成模拟数据（与原代码一致）
# --------------------------
# 生成模拟数据的时间范围（近30天）
end_date = datetime(2025, 1, 25)
start_date = end_date - timedelta(days=29)
dates = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()

# 1. 生成商品基础数据（100个商品）
product_ids = [f"P{str(i).zfill(3)}" for i in range(1, 101)]
categories = ["零食", "美妆", "服饰", "家电", "数码"]
products = pd.DataFrame({
    "product_id": product_ids,
    "product_name": [f"商品{i}" for i in range(1, 101)],
    "category": np.random.choice(categories, 100),
    "price": np.random.uniform(10, 500, 100).round(2),
    "is_price_strength": np.random.choice([True, False], 100, p=[0.3, 0.7])  # 30%为价格力商品
})

# 2. 生成商品销售数据（按天）
sales_data = []
for pid in product_ids:
    for date in dates:
        visitors = np.random.randint(100, 1000)  # 访客数100-1000
        pay_users = np.random.randint(5, int(visitors*0.3))  # 支付买家数5-30%访客
        sales_cnt = np.random.randint(pay_users, pay_users*5)  # 销量（1-5件/买家）
        sales_amt = sales_cnt * products[products["product_id"]==pid]["price"].values[0]
        sales_data.append({
            "product_id": pid,
            "stat_date": date,
            "sales_amt": round(sales_amt, 2),
            "sales_cnt": sales_cnt,
            "pay_users": pay_users,
            "visitors": visitors,
            "pay_conv_rate": round(pay_users / visitors, 4)
        })
sales_df = pd.DataFrame(sales_data)

# 3. 生成流量来源数据（Top10来源）
sources = ["效果广告", "站外广告", "内容广告", "手淘搜索",
           "购物车", "我的淘宝", "手淘推荐", "品牌广告"]
traffic_data = []
for pid in product_ids:
    for date in dates:
        for source in np.random.choice(sources, 8, replace=False):  # 每天随机8个来源
            visitors = np.random.randint(10, 200)
            pay_conv = round(np.random.uniform(0.01, 0.2), 4)
            traffic_data.append({
                "product_id": pid, "stat_date": date, "source_name": source,
                "visitors": visitors, "pay_conv_rate": pay_conv
            })
traffic_df = pd.DataFrame(traffic_data)

# 4. 生成SKU销售数据（每个商品3-5个SKU）
sku_data = []
for pid in product_ids:
    sku_count = np.random.randint(3, 6)
    skus = [f"SKU{pid}{i}" for i in range(1, sku_count+1)]
    colors = ["红色", "蓝色", "黑色", "白色", "黄色"]
    for sku in skus:
        for date in dates:
            pay_cnt = np.random.randint(5, 50)
            stock = np.random.randint(100, 500)
            stock_days = round(stock / (pay_cnt + 1), 1)  # 避免除0
            sku_data.append({
                "product_id": pid, "sku_id": sku, "stat_date": date,
                "color": np.random.choice(colors),
                "pay_cnt": pay_cnt, "stock_cnt": stock, "stock_days": stock_days
            })
sku_df = pd.DataFrame(sku_data)

# 5. 生成搜索词数据（每个商品每天10个搜索词）
search_words = ["轩妈家", "零食", "美妆", "优惠", "新品", "热销",
                "正品", "折扣", "推荐", "爆款", "好用", "便宜"]
search_data = []
for pid in product_ids:
    for date in dates:
        for word in np.random.choice(search_words, 10, replace=False):
            visitors = np.random.randint(20, 300)
            search_data.append({
                "product_id": pid, "stat_date": date, "search_word": word,
                "visitors": visitors
            })
search_df = pd.DataFrame(search_data)

# 6. 生成价格力商品数据（仅针对is_price_strength=True的商品）
price_strength_data = []
price_products = products[products["is_price_strength"]]["product_id"].values
for pid in price_products:
    for date in dates:
        strength_star = np.random.randint(1, 6)
        coupon_price = round(products[products["product_id"]==pid]["price"].values[0] * 0.9, 2)
        price_strength_data.append({
            "product_id": pid, "stat_date": date,
            "strength_star": strength_star,
            "coupon_price": coupon_price,
            "is_low_strength": strength_star <= 2,  # 1-2星为低价格力预警
            "is_low_power": np.random.choice([True, False], p=[0.2, 0.8])  # 20%概率低商品力
        })
price_strength_df = pd.DataFrame(price_strength_data)

# --------------------------
# 2. 连接MySQL并写入数据（核心修改部分）
# --------------------------
try:
    # 连接MySQL数据库（请替换为你的MySQL配置）
    conn = mysql.connector.connect(
        host="cdh03",        # MySQL服务器地址（默认localhost）
        user="root",             # 用户名
        password="root",       # 密码
        database="gmall_02"  # 数据库名（需提前创建）
    )

    if conn.is_connected():
        cursor = conn.cursor()

        # 注意：MySQL中表名和字段名建议用小写，避免大小写敏感问题
        # 写入商品基础信息表（dim_product）
        products.to_sql(
            name="dim_product",
            con=conn,
            if_exists="replace",  # 若表存在则替换
            index=False,
            method="multi"  # 批量插入，提高效率
        )
        print("dim_product表数据写入完成")

        # 写入商品销售数据表（dws_product_sales）
        sales_df.to_sql(
            name="dws_product_sales",
            con=conn,
            if_exists="replace",
            index=False,
            method="multi"
        )
        print("dws_product_sales表数据写入完成")

        # 写入流量来源表（dws_traffic_source）
        traffic_df.to_sql(
            name="dws_traffic_source",
            con=conn,
            if_exists="replace",
            index=False,
            method="multi"
        )
        print("dws_traffic_source表数据写入完成")

        # 写入SKU销售表（dws_sku_sales）
        sku_df.to_sql(
            name="dws_sku_sales",
            con=conn,
            if_exists="replace",
            index=False,
            method="multi"
        )
        print("dws_sku_sales表数据写入完成")

        # 写入搜索词表（dws_search_word）
        search_df.to_sql(
            name="dws_search_word",
            con=conn,
            if_exists="replace",
            index=False,
            method="multi"
        )
        print("dws_search_word表数据写入完成")

        # 写入价格力商品表（dws_price_strength）
        price_strength_df.to_sql(
            name="dws_price_strength",
            con=conn,
            if_exists="replace",
            index=False,
            method="multi"
        )
        print("dws_price_strength表数据写入完成")

        conn.commit()  # 提交事务

except Error as e:
    print(f"MySQL连接或写入错误：{e}")
finally:
    # 关闭连接
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL连接已关闭")