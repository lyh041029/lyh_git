create database if not exists gmall_09;

use gmall_09;

# 店内路径流转汇总（来源→去向）
drop table if exists dws_page_access_stats;
CREATE TABLE if not exists dws_shop_path_stats
(
    stats_date DATE NOT NULL COMMENT '统计日期',
    refer_page_id VARCHAR(64) NOT NULL COMMENT '来源页面ID',
    target_page_id VARCHAR(64) NOT NULL COMMENT '去向页面ID',
    device_type VARCHAR(16) NOT NULL COMMENT '设备类型',
    transfer_count INT COMMENT '流转次数',
    PRIMARY KEY (stats_date, refer_page_id, target_page_id, device_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '店内路径流转汇总表';

INSERT INTO dws_shop_path_stats
SELECT
    DATE(access_time) AS stats_date,
    refer_page_id,
    page_id AS target_page_id,
    device_type,
    COUNT(*) AS transfer_count
FROM dwd_shop_page_access_detail
GROUP BY DATE(access_time),
         refer_page_id,
         page_id,
         device_type;

select *
from dws_shop_path_stats;