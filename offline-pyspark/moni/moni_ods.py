import pymysql
import random
from datetime import datetime, timedelta

# 数据库连接配置
db_config = {
    "host": "cdh03",
    "user": "root",
    "password": "root",  # 替换为你的数据库密码
    "database": "gmall_09",  # 替换为你的数据库名
    "port": 3306,
    "charset": "utf8mb4"
}

# 生成随机访问时间（近30天内）
def random_access_time():
    start_time = datetime(2025, 1, 1)
    random_days = random.randint(0, 29)
    random_seconds = random.randint(0, 86399)  # 一天内的秒数
    return start_time + timedelta(days=random_days, seconds=random_seconds)

# 生成页面类型、ID和名称（匹配文档中的页面分类：店铺页、商品详情页、其他页）
def get_page_info():
    # 页面类型分布：shop(30%)、goods(50%)、other(20%)
    rand = random.random()
    if rand < 0.3:
        page_type = "shop"
        page_id = f"shop_{random.randint(1, 50)}"  # 50个店铺页
        page_names = ["首页", "活动页", "分类页", "新品页", "会员页"]
        page_name = f"店铺页_{random.choice(page_names)}"
    elif rand < 0.8:
        page_type = "goods"
        page_id = f"goods_{random.randint(1, 200)}"  # 200个商品详情页
        page_name = f"商品详情页_{random.randint(1, 1000)}"
    else:
        page_type = "other"
        page_id = f"other_{random.randint(1, 30)}"  # 30个其他页（订阅页、直播页等）
        page_names = ["订阅页", "直播页", "评价页", "客服页"]
        page_name = f"其他页_{random.choice(page_names)}"
    return page_type, page_id, page_name

# 生成来源页面ID（80%概率有来源，20%直接进店，符合文档中路径流转逻辑）
def get_refer_page_id():
    if random.random() < 0.8:
        # 来源页面类型随机，与当前页面类型逻辑一致
        rand = random.random()
        if rand < 0.3:
            return f"shop_{random.randint(1, 50)}"
        elif rand < 0.8:
            return f"goods_{random.randint(1, 200)}"
        else:
            return f"other_{random.randint(1, 30)}"
    else:
        return None  # 无来源（直接进店）

# 生成设备类型（70%无线端，30%PC端，匹配文档中无线端/PC端数据需求）
def get_device_type():
    return "wireless" if random.random() < 0.7 else "pc"

# 生成停留时长（按页面类型区分，商品页停留更久）
def get_stay_time(page_type):
    if page_type == "goods":
        return random.randint(50, 300)  # 商品详情页：50-300秒
    elif page_type == "shop":
        return random.randint(20, 200)  # 店铺页：20-200秒
    else:
        return random.randint(10, 100)  # 其他页：10-100秒

# 生成是否下单（商品页下单概率更高）
def get_is_pay(page_type):
    if page_type == "goods":
        return 1 if random.random() < 0.05 else 0  # 商品页5%概率下单
    else:
        return 1 if random.random() < 0.005 else 0  # 其他页0.5%概率下单

# 批量插入数据
def insert_test_data(total):
    conn = None
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        # 批量生成数据（每次插入1000条，提高效率）
        batch_size = 1000
        for i in range(0, total, batch_size):
            data = []
            batch_num = min(batch_size, total - i)
            for _ in range(batch_num):
                user_id = f"user_{random.randint(1, 10000)}"  # 1-10000个用户
                access_time = random_access_time()
                page_type, page_id, page_name = get_page_info()
                refer_page_id = get_refer_page_id()
                device_type = get_device_type()
                stay_time = get_stay_time(page_type)
                is_pay = get_is_pay(page_type)

                # 数据格式：(user_id, access_time, page_id, page_name, page_type, refer_page_id, device_type, stay_time, is_pay)
                data.append((
                    user_id, access_time, page_id, page_name, page_type,
                    refer_page_id, device_type, stay_time, is_pay
                ))

            # 批量插入SQL
            sql = """
                INSERT INTO ods_shop_page_access_log 
                (user_id, access_time, page_id, page_name, page_type, refer_page_id, device_type, stay_time, is_pay)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(sql, data)
            conn.commit()
            print(f"已插入 {i + batch_num}/{total} 条数据")

        print(f"成功生成 {total} 条测试数据")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"插入失败：{str(e)}")
    finally:
        if conn:
            conn.close()

# 生成10万条数据
if __name__ == "__main__":
    insert_test_data(100000)