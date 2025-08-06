create database if not exists gmall_09;

use gmall_09;

# 店内流转路径
drop table if exists ads_shop_flow_path;
CREATE TABLE if not exists ads_shop_flow_path
(
    stats_date DATE NOT NULL COMMENT '统计日期',
    device_type VARCHAR(16) NOT NULL COMMENT '设备类型',
    refer_page_id VARCHAR(64) NOT NULL COMMENT '来源页面ID',
    refer_page_name VARCHAR(128) COMMENT '来源页面名称',
    target_page_id VARCHAR(64) NOT NULL COMMENT '去向页面ID',
    target_page_name VARCHAR(128) COMMENT '去向页面名称',
    transfer_count INT COMMENT '流转次数',
    PRIMARY KEY (stats_date, device_type, refer_page_id, target_page_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '店内流转路径表';

INSERT INTO ads_shop_flow_path
SELECT
    s.stats_date,
    s.device_type,
    s.refer_page_id,
    p1.page_name AS refer_page_name,
    s.target_page_id,
    p2.page_name AS target_page_name,
    s.transfer_count
FROM dws_shop_path_stats s
    LEFT JOIN dim_page_info p1 ON s.refer_page_id = p1.page_id
    LEFT JOIN dim_page_info p2 ON s.target_page_id = p2.page_id;

select *
from ads_shop_flow_path;