from pyspark.sql import SparkSession 
import pymysql 
import logging 
from typing import Dict, List, Tuple 

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hive_mysql_sync.log'),
        logging.StreamHandler()
    ]
)

class HiveToMySQLSync:
    def __init__(self, spark: SparkSession, hive_db: str, mysql_config: Dict):
        self.spark = spark
        self.hive_db = hive_db
        self.mysql_config = mysql_config
        self.batch_size = 10000  # 批量插入记录数

    def get_mysql_connection(self):
        """获取MySQL连接并设置自动重连"""
        return pymysql.connect(
            host=self.mysql_config['host'],
            port=self.mysql_config['port'],
            user=self.mysql_config['user'],
            password=self.mysql_config['password'],
            database=self.mysql_config['database'],
            autocommit=True,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def get_hive_tables(self) -> List[str]:
        """获取Hive库中所有表名，排除临时表"""
        tables = [row.tableName for row in
                  self.spark.sql(f"SHOW TABLES IN {self.hive_db}").collect()
                  if not row.tableName.startswith('tmp_')]
        logging.info(f"发现 {len(tables)} 张待同步表")
        return tables

    def get_hive_table_schema(self, table_name: str) -> Tuple[List[Tuple], str]:
        """获取表结构和注释，确保dt列只保留一次"""
        # 获取列信息（包括普通字段和分区字段）
        df = self.spark.sql(f"DESCRIBE {self.hive_db}.{table_name}")
        columns = []
        seen_columns = set()  # 用于去重，避免重复列（尤其是dt）

        for row in df.collect():
            # 跳过注释行（如# col_name data_type comment）
            if row.col_name.startswith('#'):
                continue
            # 检查列名是否已存在（解决重复列问题，重点处理dt）
            if row.col_name in seen_columns:
                logging.warning(f"表 {table_name} 中发现重复列 {row.col_name}，已自动去重")
                continue
            # 收集列信息
            comment = row.comment if hasattr(row, 'comment') else ''
            columns.append((row.col_name, row.data_type.lower(), comment))
            seen_columns.add(row.col_name)  # 标记为已处理

        # 获取表注释
        table_comment = self.spark.sql(
            f"SHOW TABLE EXTENDED IN {self.hive_db} LIKE '{table_name}'"
        ).collect()[0][0] or ""

        return columns, table_comment

    def convert_data_type(self, hive_type: str) -> str:
        """增强版类型映射，处理Hive与MySQL之间的类型不兼容问题"""
        type_map = {
            'string': 'VARCHAR(512)',
            'varchar': 'VARCHAR(255)',
            'char': 'CHAR(50)',
            'int': 'INT',
            'bigint': 'BIGINT',
            'double': 'DOUBLE',
            'float': 'FLOAT',
            'boolean': 'TINYINT(1)',
            'timestamp': 'TIMESTAMP',
            'date': 'DATE',
            'binary': 'BLOB'
        }
        if hive_type.startswith('decimal'):
            return hive_type.upper().replace('decimal', 'DECIMAL')
        return type_map.get(hive_type, 'TEXT')

    def generate_mysql_ddl(self, table_name: str, columns: List[Tuple], comment: str) -> str:
        """生成MySQL建表语句，确保dt列只出现一次"""
        column_defs = []
        primary_keys = []

        for col in columns:
            name, col_type, col_comment = col
            mysql_type = self.convert_data_type(col_type)
            comment_clause = f" COMMENT '{col_comment}'" if col_comment else ''

            # 主键处理（仅id作为主键）
            if name.lower() == 'id':
                primary_keys.append(f"`{name}`")

            column_defs.append(f" `{name}` {mysql_type}{comment_clause}")

        # 添加主键约束（若有）
        if primary_keys:
            column_defs.append(f" PRIMARY KEY ({', '.join(primary_keys)})")

        column_defs_str = ',\n '.join(column_defs)
        return f"""
        CREATE TABLE IF NOT EXISTS {self.mysql_config['database']}.{table_name} (
            {column_defs_str}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='{comment}'
        """

    def sync_table_structure(self, table_name: str):
        """同步表结构到MySQL"""
        columns, comment = self.get_hive_table_schema(table_name)
        ddl = self.generate_mysql_ddl(table_name, columns, comment)

        try:
            with self.get_mysql_connection() as conn:
                with conn.cursor() as cursor:
                    # 先删除旧表（避免残留结构导致冲突）
                    cursor.execute(f"DROP TABLE IF EXISTS {self.mysql_config['database']}.{table_name}")
                    cursor.execute(ddl)
            logging.info(f"表 {table_name} 结构同步成功")
        except Exception as e:
            logging.error(f"同步表 {table_name} 结构失败: {str(e)}")
            raise

    def sync_table_data(self, table_name: str):
        """批量同步数据到MySQL（优化批量插入性能）"""
        try:
            # 读取Hive表数据（包含dt列，直接同步）
            df = self.spark.sql(f"SELECT * FROM {self.hive_db}.{table_name}")
            columns = df.schema.names
            # 转换为迭代器，减少内存占用
            rows = df.toLocalIterator()

            with self.get_mysql_connection() as conn:
                with conn.cursor() as cursor:
                    placeholders = ', '.join(['%s'] * len(columns))
                    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                    batch = []
                    count = 0

                    for row in rows:
                        batch.append(tuple(row))
                        count += 1
                        # 达到批量大小则执行插入
                        if count % self.batch_size == 0:
                            cursor.executemany(insert_sql, batch)
                            batch = []
                    # 插入剩余数据
                    if batch:
                        cursor.executemany(insert_sql, batch)

            logging.info(f"表 {table_name} 数据同步完成，总记录数: {count}")
        except Exception as e:
            logging.error(f"同步表 {table_name} 数据失败: {str(e)}")
            raise

    def run_sync(self):
        """执行完整同步流程"""
        tables = self.get_hive_tables()
        for table in tables:
            try:
                self.sync_table_structure(table)
                self.sync_table_data(table)
            except Exception:
                logging.error(f"表 {table} 同步失败，跳过继续处理其他表")
                continue

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("EnhancedHiveMySQLSync") \
        .config("spark.local.dir", "F:\data") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")  \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    config = {
        'host': 'cdh03',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'database': 'gmall_10'
    }

    syncer = HiveToMySQLSync(spark, "gmall_10", config)
    syncer.run_sync()
    spark.stop()