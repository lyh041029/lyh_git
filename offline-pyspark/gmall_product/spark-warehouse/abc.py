mock_data_path = "G:/dashuju/zshixun/daima/ider_daima/lyh_git/offline-pyspark/gmall_product/spark-warehouse"

# 可先添加路径检查代码
import os
if not os.path.exists(mock_data_path):
    raise Exception(f"路径不存在: {mock_data_path}")