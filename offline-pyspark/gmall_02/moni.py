import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# 设置随机种子，确保数据可复现
np.random.seed(42)
random.seed(42)

# -------------------------- 基础配置 --------------------------
# 生成数据量（每张表10000条）
DATA_SIZE = 10000

# 日期范围（最近30天，用于生成时间字段）
start_date = datetime.now() - timedelta(days=30)
dates = [start_date + timedelta(days=i) for i in range(30)]

# 公共基础数据（多表复用，保证关联性）
categories = [
    {"id": "c1001", "name": "电子产品"},
    {"id": "c1002", "name": "服装鞋帽"},
    {"id": "c1003", "name": "家居用品"},
    {"id": "c1004", "name": "食品饮料"},
    {"id": "c1005", "name": "图书音像"}
]
shops = [f"shop{i:04d}" for i in range(1, 21)]  # 20个店铺ID
all_goods_ids = [f"goods{i:06d}" for i in range(DATA_SIZE)]  # 生成10000个唯一商品ID


# -------------------------- 1. 生成ods_goods_sales --------------------------
def generate_ods_goods_sales():
    data = []
    for i in range(DATA_SIZE):
        # 随机选择商品分类
        category = random.choice(categories)
        # 商品基本信息
        goods_id = all_goods_ids[i]  # 复用全局商品ID
        goods_name = f"{category['name']}_商品_{i % 100 + 1}"  # 按分类生成名称

        # SKU信息（根据分类动态生成）
        colors = ["红色", "蓝色", "黑色", "白色", "绿色"]
        if category["id"] == "c1001":  # 电子产品用存储容量
            sizes = ["16G", "32G", "64G", "128G", "256G"]
        elif category["id"] == "c1002":  # 服装用尺码
            sizes = ["S", "M", "L", "XL", "XXL"]
        else:  # 其他分类用通用规格
            sizes = ["小", "中", "大", "特大", "定制"]
        sku_info = f"{random.choice(colors)}_{random.choice(sizes)}"

        # 销售数据（合理范围随机生成）
        pay_amount = round(random.uniform(9.9, 5000.0), 2)  # 支付金额
        pay_num = random.randint(1, 200)  # 支付件数
        pay_buyer_num = random.randint(1, pay_num)  # 支付买家数（不超过件数）
        visitor_num = random.randint(pay_buyer_num, pay_buyer_num * 50)  # 访客数（多于买家）

        # 销售时间（随机日期+随机小时）
        sales_time = random.choice(dates) + timedelta(
            hours=random.randint(8, 23),
            minutes=random.randint(0, 59)
        )

        data.append({
            "goods_id": goods_id,
            "goods_name": goods_name,
            "category_id": category["id"],
            "category_name": category["name"],
            "sku_id": f"sku{i:08d}",  # 唯一SKU ID
            "sku_info": sku_info,
            "pay_amount": pay_amount,
            "pay_num": pay_num,
            "pay_buyer_num": pay_buyer_num,
            "visitor_num": visitor_num,
            "sales_time": sales_time.strftime("%Y-%m-%d %H:%M:%S"),  # 符合TIMESTAMP格式
            "shop_id": random.choice(shops)
        })
    return pd.DataFrame(data)


# -------------------------- 2. 生成ods_goods_traffic --------------------------
def generate_ods_goods_traffic():
    data = []
    traffic_sources = [
        "效果广告", "手淘搜索", "京东搜索", "拼多多推荐",
        "抖音直播", "快手短视频", "微信小程序", "微博推广", "朋友推荐"
    ]
    for _ in range(DATA_SIZE):
        goods_id = random.choice(all_goods_ids)  # 随机关联商品ID
        source = random.choice(traffic_sources)
        visitor_num = random.randint(10, 1000)  # 来源访客数
        pay_conversion_rate = round(random.uniform(0.005, 0.2), 4)  # 转化率（0.5%-20%）

        # 流量时间
        traffic_time = random.choice(dates) + timedelta(
            hours=random.randint(7, 24),
            minutes=random.randint(0, 59)
        )

        data.append({
            "goods_id": goods_id,
            "traffic_source": source,
            "visitor_num": visitor_num,
            "pay_conversion_rate": pay_conversion_rate,
            "traffic_time": traffic_time.strftime("%Y-%m-%d %H:%M:%S"),
            "shop_id": random.choice(shops)
        })
    return pd.DataFrame(data)


# -------------------------- 3. 生成ods_goods_search --------------------------
def generate_ods_goods_search():
    data = []
    search_words = [
        # 按分类生成相关搜索词
        "智能手机 新款", "笔记本电脑 轻薄", "无线耳机 降噪",  # 电子产品
        "连衣裙 夏季", "牛仔裤 宽松", "运动鞋 透气",  # 服装鞋帽
        "沙发 小户型", "保温杯 大容量", "台灯 护眼",  # 家居用品
        "零食 礼盒", "咖啡 速溶", "牛奶 有机",  # 食品饮料
        "悬疑小说 推荐", "考研英语 真题", "儿童绘本 3-6岁"  # 图书音像
    ]
    for _ in range(DATA_SIZE):
        goods_id = random.choice(all_goods_ids)  # 关联商品ID
        search_word = random.choice(search_words)
        search_num = random.randint(5, 500)  # 搜索次数
        visitor_num = random.randint(1, search_num)  # 搜索访客数（不超过搜索次数）

        # 搜索时间
        search_time = random.choice(dates) + timedelta(
            hours=random.randint(9, 22),
            minutes=random.randint(0, 59)
        )

        data.append({
            "goods_id": goods_id,
            "search_word": search_word,
            "search_num": search_num,
            "visitor_num": visitor_num,
            "search_time": search_time.strftime("%Y-%m-%d %H:%M:%S"),
            "shop_id": random.choice(shops)
        })
    return pd.DataFrame(data)


# -------------------------- 4. 生成ods_price_strength_goods --------------------------
def generate_ods_price_strength_goods():
    data = []
    for _ in range(DATA_SIZE):
        goods_id = random.choice(all_goods_ids)  # 关联商品ID
        price_strength_star = random.randint(1, 5)  # 价格力星级（1-5）
        coupon_after_price = round(random.uniform(5.0, 3000.0), 2)  # 券后价

        # 转化率（市场平均、当前、上期）
        market_avg_conversion = round(random.uniform(0.01, 0.15), 4)  # 1%-15%
        current_conversion = round(
            market_avg_conversion * random.uniform(0.5, 1.5), 4  # 当前转化率围绕市场平均波动
        )
        last_conversion = round(
            current_conversion * random.uniform(0.8, 1.2), 4  # 上期转化率围绕当前波动
        )

        # 其他字段
        is_high_price = random.randint(0, 1)  # 是否高价（0/1）
        low_star_days = random.randint(0, 15) if price_strength_star <= 3 else 0  # 低星天数（仅低星商品有）

        # 检查时间
        check_time = random.choice(dates) + timedelta(
            hours=random.randint(10, 18),
            minutes=random.randint(0, 59)
        )

        data.append({
            "goods_id": goods_id,
            "price_strength_star": price_strength_star,
            "coupon_after_price": coupon_after_price,
            "market_avg_conversion": market_avg_conversion,
            "current_conversion": current_conversion,
            "last_conversion": last_conversion,
            "is_high_price": is_high_price,
            "low_star_days": low_star_days,
            "check_time": check_time.strftime("%Y-%m-%d %H:%M:%S"),
            "shop_id": random.choice(shops)
        })
    return pd.DataFrame(data)


# -------------------------- 生成并保存数据 --------------------------
if __name__ == "__main__":
    # 生成4张表的数据
    df_sales = generate_ods_goods_sales()
    df_traffic = generate_ods_goods_traffic()
    df_search = generate_ods_goods_search()
    df_price = generate_ods_price_strength_goods()

    # 保存为制表符分隔的CSV（无索引，带表头，符合spark.read.csv参数）
    output_dir = "./gmall_data/"  # 输出目录（可自行修改）
    import os
    os.makedirs(output_dir, exist_ok=True)

    df_sales.to_csv(
        f"{output_dir}/ods_goods_sales.csv",
        sep='\t',  # 制表符分隔
        index=False,  # 不保留索引
        header=True  # 保留表头
    )
    df_traffic.to_csv(f"{output_dir}/ods_goods_traffic.csv", sep='\t', index=False, header=True)
    df_search.to_csv(f"{output_dir}/ods_goods_search.csv", sep='\t', index=False, header=True)
    df_price.to_csv(f"{output_dir}/ods_price_strength_goods.csv", sep='\t', index=False, header=True)

    print(f"数据生成完成，共4张表，每张表{DATA_SIZE}条数据，保存至：{output_dir}")