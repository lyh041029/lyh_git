import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# 初始化Faker
fake = Faker('zh_CN')
Faker.seed(42)  # 固定随机种子，保证数据可复现

# 生成数据量
n = 50000

# 时间范围设置为最近一个月（30天）
end_date = datetime.today()
start_date = end_date - timedelta(days=30)

# MySQL连接配置（请替换为实际数据库信息）
mysql_config = {
    'host': 'cdh03',
    'user': 'root',
    'password': 'root',
    'database': 'sx_one_3',
    'port': 3306,
    'charset': 'utf8mb4'
}

# 创建数据库连接引擎
engine = create_engine(
    f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}@{mysql_config['host']}:"
    f"{mysql_config['port']}/{mysql_config['database']}?charset={mysql_config['charset']}"
)

def create_ods_tables():
    """创建ODS层MySQL表，结构对应文档需求的原始数据维度"""
    conn = pymysql.connect(**mysql_config)
    cursor = conn.cursor()
    try:
        # 1. 商品表（对应文档中商品基础信息、价格力星级等）
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ods_product (
            product_id VARCHAR(20) PRIMARY KEY COMMENT '商品唯一标识',
            category_id VARCHAR(10) NOT NULL COMMENT '商品分类ID',
            product_name VARCHAR(100) NOT NULL COMMENT '商品名称',
            price_strength_star TINYINT NOT NULL COMMENT '价格力星级（1-5星）',
            coupon_price DECIMAL(10,2) NOT NULL COMMENT '普惠券后价',
            create_time DATE NOT NULL COMMENT '商品创建时间'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品基础信息原始表';
        """)

        # 2. 订单表（对应文档中销售额、销量、支付买家数等）
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ods_order (
            order_id VARCHAR(20) PRIMARY KEY COMMENT '订单唯一标识',
            product_id VARCHAR(20) NOT NULL COMMENT '关联商品ID',
            sku_id VARCHAR(30) NOT NULL COMMENT '商品SKU ID',
            user_id VARCHAR(20) NOT NULL COMMENT '购买用户ID',
            pay_amount DECIMAL(10,2) NOT NULL COMMENT '支付金额（销售额）',
            pay_time DATETIME NOT NULL COMMENT '支付时间',
            buyer_count INT NOT NULL COMMENT '支付买家数',
            pay_quantity INT NOT NULL COMMENT '支付件数（销量）',
            FOREIGN KEY (product_id) REFERENCES ods_product(product_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '订单交易原始表';
        """)

        # 3. 流量表（对应文档中访客数、流量来源、搜索词等）
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ods_traffic (
            traffic_id VARCHAR(20) PRIMARY KEY COMMENT '流量记录唯一标识',
            product_id VARCHAR(20) NOT NULL COMMENT '关联商品ID',
            visitor_count INT NOT NULL COMMENT '商品访客数（访问详情页人数）',
            source VARCHAR(50) NOT NULL COMMENT '流量来源（如效果广告、手淘搜索等）',
            search_word VARCHAR(100) NULL COMMENT '用户搜索词',
            visit_time DATETIME NOT NULL COMMENT '访问时间',
            FOREIGN KEY (product_id) REFERENCES ods_product(product_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品流量行为原始表';
        """)

        # 4. 库存表（对应文档中SKU库存、可售天数等）
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ods_inventory (
            inventory_id VARCHAR(20) PRIMARY KEY COMMENT '库存记录唯一标识',
            sku_id VARCHAR(30) NOT NULL COMMENT '关联SKU ID',
            current_stock INT NOT NULL COMMENT '当前库存（件）',
            saleable_days INT NOT NULL COMMENT '库存可售天数',
            update_time DATETIME NOT NULL COMMENT '库存更新时间'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品库存原始表';
        """)

        conn.commit()
        print("ODS层MySQL表创建成功")
    except Exception as e:
        conn.rollback()
        print(f"创建表失败：{str(e)}")
    finally:
        cursor.close()
        conn.close()

def generate_and_insert_data():
    """生成模拟数据并插入MySQL，时间范围限定为最近一个月"""
    try:
        # 1. 生成ods_product数据（创建时间可适当早于最近一个月，保证商品存在销售基础）
        product_ids = [f'P{str(i).zfill(8)}' for i in range(1, n+1)]
        category_ids = [f'C{random.randint(1, 20)}' for _ in range(n)]  # 20个分类，支持文档中分类排名需求
        product_names = [fake.word() + '商品' for _ in range(n)]
        price_strength_stars = [random.randint(1, 5) for _ in range(n)]  # 价格力星级，用于文档中的价格力分析
        coupon_prices = [round(random.uniform(10, 500), 2) for _ in range(n)]
        # 商品创建时间：近3个月内，确保最近一个月有可售商品
        create_times = [fake.date_between(start_date='-90d', end_date='today') for _ in range(n)]

        ods_product = pd.DataFrame({
            'product_id': product_ids,
            'category_id': category_ids,
            'product_name': product_names,
            'price_strength_star': price_strength_stars,
            'coupon_price': coupon_prices,
            'create_time': create_times
        })
        ods_product.to_sql('ods_product', engine, if_exists='append', index=False, chunksize=1000)
        print("ods_product数据插入完成")

        # 2. 生成ods_order数据（支付时间限定为最近一个月）
        order_ids = [f'O{str(i).zfill(8)}' for i in range(1, n+1)]
        product_ids_order = [random.choice(product_ids) for _ in range(n)]  # 关联商品ID
        sku_ids = [f'SKU{str(i).zfill(10)}' for i in range(1, n+1)]
        user_ids = [f'U{str(i).zfill(6)}' for i in range(1, n//5 +1)]  # 模拟1万用户
        user_ids = [random.choice(user_ids) for _ in range(n)]
        pay_amounts = [round(random.uniform(20, 1000), 2) for _ in range(n)]  # 销售额，用于文档中的商品排行
        # 支付时间：严格限定在最近一个月，满足文档中30天时间维度需求
        pay_times = [fake.date_time_between(start_date=start_date, end_date=end_date) for _ in range(n)]
        buyer_counts = [random.randint(1, 5) for _ in range(n)]  # 支付买家数，用于计算文档中的支付转化率
        pay_quantity = [random.randint(1, 10) for _ in range(n)]  # 销量，用于文档中的销量排行

        ods_order = pd.DataFrame({
            'order_id': order_ids,
            'product_id': product_ids_order,
            'sku_id': sku_ids,
            'user_id': user_ids,
            'pay_amount': pay_amounts,
            'pay_time': pay_times,
            'buyer_count': buyer_counts,
            'pay_quantity': pay_quantity
        })
        ods_order.to_sql('ods_order', engine, if_exists='append', index=False, chunksize=1000)
        print("ods_order数据插入完成")

        # 3. 生成ods_traffic数据（访问时间限定为最近一个月）
        traffic_ids = [f'T{str(i).zfill(8)}' for i in range(1, n+1)]
        product_ids_traffic = [random.choice(product_ids) for _ in range(n)]
        visitor_counts = [random.randint(10, 500) for _ in range(n)]  # 访客数，用于文档中的流量分析
        # 流量来源与文档中【流】TOP10来源一致
        sources = ['效果广告', '站外广告', '内容广告', '手淘搜索', '购物车', '我的淘宝', '手淘推荐']
        source_list = [random.choice(sources) for _ in range(n)]
        search_words = [fake.word() if random.random() > 0.3 else None for _ in range(n)]  # 搜索词，对应文档【词】
        # 访问时间：严格限定在最近一个月
        visit_times = [fake.date_time_between(start_date=start_date, end_date=end_date) for _ in range(n)]

        ods_traffic = pd.DataFrame({
            'traffic_id': traffic_ids,
            'product_id': product_ids_traffic,
            'visitor_count': visitor_counts,
            'source': source_list,
            'search_word': search_words,
            'visit_time': visit_times
        })
        ods_traffic.to_sql('ods_traffic', engine, if_exists='append', index=False, chunksize=1000)
        print("ods_traffic数据插入完成")

        # 4. 生成ods_inventory数据（更新时间限定为最近一个月）
        inventory_ids = [f'INV{str(i).zfill(8)}' for i in range(1, n+1)]
        sku_ids_inv = [random.choice(sku_ids) for _ in range(n)]  # 关联SKU
        current_stocks = [random.randint(50, 1000) for _ in range(n)]  # 当前库存，用于文档中SKU分析
        saleable_days = [random.randint(3, 30) for _ in range(n)]  # 可售天数，对应文档中库存预警需求
        # 更新时间：严格限定在最近一个月
        update_times = [fake.date_time_between(start_date=start_date, end_date=end_date) for _ in range(n)]

        ods_inventory = pd.DataFrame({
            'inventory_id': inventory_ids,
            'sku_id': sku_ids_inv,
            'current_stock': current_stocks,
            'saleable_days': saleable_days,
            'update_time': update_times
        })
        ods_inventory.to_sql('ods_inventory', engine, if_exists='append', index=False, chunksize=1000)
        print("ods_inventory数据插入完成")

    except SQLAlchemyError as e:
        print(f"数据插入失败：{str(e)}")

if __name__ == '__main__':
    create_ods_tables()
    generate_and_insert_data()
    print(f"所有ODS层表已插入{50000}条模拟数据，时间范围为最近一个月，符合文档中商品排行看板的时间维度需求")