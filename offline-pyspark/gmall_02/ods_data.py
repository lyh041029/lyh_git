import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import pymysql
from pymysql.cursors import DictCursor

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
    'database': 'gmall_02',
    'port': 3306,
    'charset': 'utf8mb4'
}

def create_ods_tables():
    """创建ODS层MySQL表，结构对应文档需求的原始数据维度"""
    conn = pymysql.connect(**mysql_config)
    cursor = conn.cursor()
    try:
        # 1. 商品表
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

        # 2. 订单表
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

        # 3. 流量表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ods_traffic (
            traffic_id VARCHAR(20) PRIMARY KEY COMMENT '流量记录唯一标识',
            product_id VARCHAR(20) NOT NULL COMMENT '关联商品ID',
            visitor_count INT NOT NULL COMMENT '商品访客数（访问详情页人数）',
            source VARCHAR(50) NOT NULL COMMENT '流量来源',
            search_word VARCHAR(100) NULL COMMENT '用户搜索词',
            visit_time DATETIME NOT NULL COMMENT '访问时间',
            FOREIGN KEY (product_id) REFERENCES ods_product(product_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品流量行为原始表';
        """)

        # 4. 库存表
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

def insert_dataframe_to_sql(df, table_name):
    """封装DataFrame插入数据库的通用方法，使用pymysql原生连接"""
    if df.empty:
        print(f"数据框为空，不插入{table_name}数据")
        return

    conn = None
    try:
        # 使用pymysql原生连接
        conn = pymysql.connect(** mysql_config)
        cursor = conn.cursor()

        # 获取列名
        columns = df.columns.tolist()
        # 创建插入SQL模板
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        # 转换数据为元组列表
        data = [tuple(row) for row in df.itertuples(index=False, name=None)]

        # 批量插入，分块处理
        chunk_size = 1000
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i+chunk_size]
            cursor.executemany(sql, chunk)
            conn.commit()

        print(f"{table_name}数据插入完成，共插入{len(data)}条记录")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"{table_name}数据插入失败：{str(e)}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

def generate_and_insert_data():
    """生成模拟数据并插入MySQL，时间范围限定为最近一个月"""
    try:
        # 1. 生成并插入商品表数据
        product_ids = [f'P{str(i).zfill(8)}' for i in range(1, n+1)]
        category_ids = [f'C{random.randint(1, 20)}' for _ in range(n)]
        product_names = [fake.word() + '商品' for _ in range(n)]
        price_strength_stars = [random.randint(1, 5) for _ in range(n)]
        coupon_prices = [round(random.uniform(10, 500), 2) for _ in range(n)]
        create_times = [fake.date_between(start_date='-90d', end_date='today') for _ in range(n)]

        ods_product = pd.DataFrame({
            'product_id': product_ids,
            'category_id': category_ids,
            'product_name': product_names,
            'price_strength_star': price_strength_stars,
            'coupon_price': coupon_prices,
            'create_time': create_times
        })
        insert_dataframe_to_sql(ods_product, 'ods_product')

        # 2. 生成并插入订单表数据
        order_ids = [f'O{str(i).zfill(8)}' for i in range(1, n+1)]
        product_ids_order = [random.choice(product_ids) for _ in range(n)]
        sku_ids = [f'SKU{str(i).zfill(10)}' for i in range(1, n+1)]
        user_ids = [f'U{str(i).zfill(6)}' for i in range(1, n//5 +1)]
        user_ids = [random.choice(user_ids) for _ in range(n)]
        pay_amounts = [round(random.uniform(20, 1000), 2) for _ in range(n)]
        pay_times = [fake.date_time_between(start_date=start_date, end_date=end_date) for _ in range(n)]
        buyer_counts = [random.randint(1, 5) for _ in range(n)]
        pay_quantity = [random.randint(1, 10) for _ in range(n)]

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
        insert_dataframe_to_sql(ods_order, 'ods_order')

        # 3. 生成并插入流量表数据
        traffic_ids = [f'T{str(i).zfill(8)}' for i in range(1, n+1)]
        product_ids_traffic = [random.choice(product_ids) for _ in range(n)]
        visitor_counts = [random.randint(10, 500) for _ in range(n)]
        sources = ['效果广告', '站外广告', '内容广告', '手淘搜索', '购物车', '我的淘宝', '手淘推荐']
        source_list = [random.choice(sources) for _ in range(n)]
        search_words = [fake.word() if random.random() > 0.3 else None for _ in range(n)]
        visit_times = [fake.date_time_between(start_date=start_date, end_date=end_date) for _ in range(n)]

        ods_traffic = pd.DataFrame({
            'traffic_id': traffic_ids,
            'product_id': product_ids_traffic,
            'visitor_count': visitor_counts,
            'source': source_list,
            'search_word': search_words,
            'visit_time': visit_times
        })
        insert_dataframe_to_sql(ods_traffic, 'ods_traffic')

        # 4. 生成并插入库存表数据
        inventory_ids = [f'INV{str(i).zfill(8)}' for i in range(1, n+1)]
        sku_ids_inv = [random.choice(sku_ids) for _ in range(n)]
        current_stocks = [random.randint(50, 1000) for _ in range(n)]
        saleable_days = [random.randint(3, 30) for _ in range(n)]
        update_times = [fake.date_time_between(start_date=start_date, end_date=end_date) for _ in range(n)]

        ods_inventory = pd.DataFrame({
            'inventory_id': inventory_ids,
            'sku_id': sku_ids_inv,
            'current_stock': current_stocks,
            'saleable_days': saleable_days,
            'update_time': update_times
        })
        insert_dataframe_to_sql(ods_inventory, 'ods_inventory')

    except Exception as e:
        print(f"数据处理失败：{str(e)}")
        return

if __name__ == '__main__':
    create_ods_tables()
    generate_and_insert_data()
    print(f"所有ODS层表数据处理完成，计划插入{50000}条模拟数据")
