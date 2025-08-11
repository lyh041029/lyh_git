import pymysql
import random
from faker import Faker
from datetime import datetime, timedelta

# Faker 对象（中文 + 英文混合数据）
faker = Faker('zh_CN')

# MySQL 连接配置
DB_CONFIG = {
    'host': 'cdh03',
    'user': 'root',
    'password': 'root',
    'database': 'gmall_01',
    'charset': 'utf8'
}

# 模拟常量
PRODUCT_COUNT = 200   # 商品数量
USER_COUNT = 1000     # 用户数量
VISIT_COUNT = 20000
FAV_COUNT = 8000
CART_COUNT = 7000
ORDER_COUNT = 10000
PAY_COUNT = 9000
REFUND_COUNT = 3000

# 时间范围限制：过去 30 天内
def random_recent_datetime():
    return faker.date_time_between(start_date='-30d', end_date='now')

# 创建数据库连接
conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor()

# 1. 商品信息表
products = []
for pid in range(1, PRODUCT_COUNT + 1):
    products.append((
        pid,
        faker.word(),
        random.randint(1, 20),
        faker.word(),
        round(random.uniform(10, 2000), 2),
        random_recent_datetime()
    ))
cursor.executemany(
    "INSERT INTO ods_product_info VALUES (%s,%s,%s,%s,%s,%s)", products)

# 2. 商品访客表（带 is_micro_detail）
visits = []
for _ in range(VISIT_COUNT):
    stay_time = random.randint(1, 600)
    is_micro_detail = 1 if stay_time >= 3 else 0
    visits.append((
        random.randint(1, PRODUCT_COUNT),
        random.randint(1, USER_COUNT),
        random_recent_datetime(),
        stay_time,
        random.choice(['PC', 'MOBILE']),
        is_micro_detail
    ))
cursor.executemany(
    "INSERT INTO ods_product_visit(product_id,user_id,visit_time,stay_seconds,terminal_type,is_micro_detail) "
    "VALUES (%s,%s,%s,%s,%s,%s)", visits)

# 3. 收藏表
favs = []
for _ in range(FAV_COUNT):
    favs.append((
        random.randint(1, PRODUCT_COUNT),
        random.randint(1, USER_COUNT),
        random_recent_datetime()
    ))
cursor.executemany(
    "INSERT INTO ods_product_favorite(product_id,user_id,fav_time) VALUES (%s,%s,%s)", favs)

# 4. 加购表
carts = []
for _ in range(CART_COUNT):
    carts.append((
        random.randint(1, PRODUCT_COUNT),
        random.randint(1, USER_COUNT),
        random.randint(1, 5),
        random_recent_datetime()
    ))
cursor.executemany(
    "INSERT INTO ods_product_cart(product_id,user_id,quantity,cart_time) VALUES (%s,%s,%s,%s)", carts)

# 5. 订单表（带 activity_type）
orders = []
for oid in range(1, ORDER_COUNT + 1):
    pid = random.randint(1, PRODUCT_COUNT)
    qty = random.randint(1, 5)
    price = [p[4] for p in products if p[0] == pid][0]
    activity_type = 'JUHUASUAN' if random.random() < 0.2 else 'NONE'
    orders.append((
        oid,
        pid,
        random.randint(1, USER_COUNT),
        qty,
        round(qty * price, 2),
        random_recent_datetime(),
        random.choice(['PC', 'MOBILE']),
        activity_type
    ))
cursor.executemany(
    "INSERT INTO ods_order_info(order_id,product_id,user_id,quantity,order_amount,order_time,terminal_type,activity_type) "
    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", orders)

# 6. 支付表（带 activity_type，与订单匹配）
pays = []
for _ in range(PAY_COUNT):
    order = random.choice(orders)
    pays.append((
        order[0],  # order_id
        order[1],  # product_id
        order[2],  # user_id
        order[4],  # pay_amount
        order[5] + timedelta(minutes=random.randint(1, 60)),  # pay_time
        order[6],  # terminal_type
        order[7]   # activity_type 与订单一致
    ))
cursor.executemany(
    "INSERT INTO ods_payment_info(order_id,product_id,user_id,pay_amount,pay_time,terminal_type,activity_type) "
    "VALUES (%s,%s,%s,%s,%s,%s,%s)", pays)

# 7. 退款表
refunds = []
for _ in range(REFUND_COUNT):
    order = random.choice(orders)
    refunds.append((
        order[0],
        order[1],
        order[2],
        round(order[4] * random.uniform(0.3, 1.0), 2),
        order[5] + timedelta(days=random.randint(1, 7)),
        random.choice(['ONLY_REFUND', 'REFUND_RETURN'])
    ))
cursor.executemany(
    "INSERT INTO ods_refund_info(order_id,product_id,user_id,refund_amount,refund_time,refund_type) "
    "VALUES (%s,%s,%s,%s,%s,%s)", refunds)

# 提交事务
conn.commit()
cursor.close()
conn.close()

print("模拟数据已成功生成并插入 ODS 层！（含 is_micro_detail 与 activity_type）")
