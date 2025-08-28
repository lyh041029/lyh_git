from kafka import KafkaProducer
import json
import time
import random
from faker import Faker
from datetime import datetime

# 初始化 Kafka 生产者（替换为你的 Kafka 地址）
producer = KafkaProducer(
    bootstrap_servers=['cdh01:9092'],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
    batch_size=16384,
    linger_ms=500
)

# 初始化伪造数据工具
fake = Faker(locale='zh_CN')

# 模拟配置：商品列表、行为类型、补贴类型
PRODUCT_LIST = [
    {"product_id": f"prod_{i:06d}", "product_name": f"商品_{i:06d}"}
    for i in range(1, 200)  # 模拟200个商品
]
BEHAVIOR_TYPES = ["view", "add_cart", "pay"]  # 浏览、加购、支付
SUBSIDY_TYPES = ["normal", "100billion", "official_bid"]  # 普通、百亿补贴、官方竞价

# 修复：使用Faker支持的方法生成用户ID（替换原user_id()方法）
# 方案1: 生成随机整数ID（如100000-999999之间）
USER_ID_POOL = [fake.random_int(min=100000, max=999999) for _ in range(500)]

# 方案2: 生成UUID格式的用户ID（如需要字符串类型ID可启用此方案）
# USER_ID_POOL = [fake.uuid4() for _ in range(500)]

def generate_user_behavior():
    """生成单条模拟用户行为数据"""
    product = random.choice(PRODUCT_LIST)
    behavior_type = random.choice(BEHAVIOR_TYPES)

    # 基础字段
    data = {
        "user_id": random.choice(USER_ID_POOL),
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "behavior_type": behavior_type,
        "timestamp": int(time.time() * 1000),  # 毫秒级时间戳（事件时间）
        "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # 格式化时间
    }

    # 按行为类型补充字段
    if behavior_type == "view":
        data.update({"view_count": 1})

    elif behavior_type == "add_cart":
        data.update({"add_cart_num": random.randint(1, 5)})

    elif behavior_type == "pay":
        order_amount = round(random.uniform(9.9, 2999.9), 2)
        freight = round(random.uniform(0, 15), 2)
        red_envelope = round(random.uniform(0, 50), 2)
        coupon = round(random.uniform(0, 30), 2)
        subsidy_type = random.choice(SUBSIDY_TYPES)
        refund_amount = round(random.uniform(0, order_amount * 0.5), 2) if random.random() < 0.1 else 0

        data.update({
            "order_amount": order_amount,
            "freight": freight,
            "red_envelope": red_envelope,
            "coupon": coupon,
            "subsidy_type": subsidy_type,
            "refund_amount": refund_amount
        })

    return data

if __name__ == "__main__":
    print("ODS层流数据模拟脚本启动，持续发送数据到Kafka...")
    try:
        while True:
            # 生成1-5条数据（模拟并发行为）
            for _ in range(random.randint(1, 5)):
                behavior_data = generate_user_behavior()
                # 发送到Kafka Topic
                producer.send(
                    topic="ods_ecommerce_user_behavior",
                    key=behavior_data["product_id"].encode('utf-8'),  # 按商品ID分区
                    value=behavior_data
                )
                print(f"发送数据：{json.dumps(behavior_data, ensure_ascii=False)}")

            # 每0.1-0.5秒发送一次（控制流数据频率）
            time.sleep(random.uniform(0.1, 0.5))
    except KeyboardInterrupt:
        print("脚本手动停止")
    finally:
        producer.close()
        print("Kafka生产者关闭")
