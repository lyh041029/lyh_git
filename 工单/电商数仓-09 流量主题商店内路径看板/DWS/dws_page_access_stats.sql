create database if not exists gmall_09;

use gmall_09;

# 页面访问指标汇总（按页面 + 设备 + 日期）
drop table if exists dws_page_access_stats;
CREATE TABLE if not exists dws_page_access_stats
(
    stats_date DATE NOT NULL COMMENT '统计日期',
    page_id VARCHAR(64) NOT NULL COMMENT '页面ID',
    device_type VARCHAR(16) NOT NULL COMMENT '设备类型',
    visitor_count INT COMMENT '访客数（去重）',
    page_view INT COMMENT '浏览量',
    avg_stay_time DECIMAL(10,2) COMMENT '平均停留时长（秒）',
    pay_count INT COMMENT '下单数',
    PRIMARY KEY (stats_date, page_id, device_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '页面访问指标汇总表';


INSERT INTO dws_page_access_stats
SELECT
    DATE(access_time) AS stats_date,
    page_id,
    device_type,
    COUNT(DISTINCT user_id) AS visitor_count,
    COUNT(*) AS page_view,
    AVG(stay_time) AS avg_stay_time,
    SUM(is_pay) AS pay_count
FROM dwd_shop_page_access_detail
GROUP BY DATE(access_time),
         page_id,
         device_type;

select * from dws_page_access_stats;

