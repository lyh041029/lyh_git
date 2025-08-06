create database if not exists gmall_09;

use gmall_09;

# 无线端入店与承接数据

drop table if exists ads_wireless_in_store_stats;
CREATE TABLE if not exists ads_wireless_in_store_stats
(
    stats_period VARCHAR(10) NOT NULL COMMENT '统计周期（day/7day/30day/month）',
    stats_date DATE NOT NULL COMMENT '统计日期（周期结束日）',
    page_id VARCHAR(64) NOT NULL COMMENT '入店页面ID',
    visitor_count INT COMMENT '访客数',
    pay_count INT COMMENT '下单买家数',
    PRIMARY KEY (stats_period, stats_date, page_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '无线端入店与承接统计表';

-- 示例：按日统计
-- 向 ads_wireless_in_store_stats 表插入按天统计的无线端入店与承接数据
INSERT INTO ads_wireless_in_store_stats
SELECT
    'day' AS stats_period,  -- 统计周期标记为“day”，表示按天统计
    stats_date,  -- 从 dws_page_access_stats 表获取统计日期（单日日期）
    page_id,  -- 从 dws_page_access_stats 表获取页面 ID
    visitor_count,  -- 从 dws_page_access_stats 表获取该页面、该日期、无线端的访客数
    pay_count  -- 从 dws_page_access_stats 表获取该页面、该日期、无线端的下单买家数
FROM dws_page_access_stats  -- 数据来源表：dws 层的页面访问指标汇总表
WHERE device_type = 'wireless';  -- 筛选条件：仅统计设备类型为无线端（wireless）的数据


-- 按7天统计（需通过日期范围聚合，此处简化）
-- 向 ads_wireless_in_store_stats 表插入按7天统计的无线端入店与承接数据（简化版，按最近7天聚合）
INSERT INTO ads_wireless_in_store_stats
SELECT
    '7day' AS stats_period,  -- 统计周期标记为“7day”，表示按7天统计
    MAX(stats_date) AS stats_date,  -- 取最近7天中的最大日期（通常是统计周期的结束日期，如今天）作为该周期的统计日期
    page_id,  -- 按页面 ID 分组，统计每个页面的数据
    SUM(visitor_count) AS visitor_count,  -- 汇总最近7天内，该页面无线端的访客数总和
    SUM(pay_count) AS pay_count  -- 汇总最近7天内，该页面无线端的下单买家数总和
FROM dws_page_access_stats  -- 数据来源表：dws 层的页面访问指标汇总表
WHERE
        device_type = 'wireless'  -- 筛选条件：仅统计设备类型为无线端（wireless）的数据
  AND stats_date BETWEEN DATE_SUB(CURDATE(), INTERVAL 6 DAY) AND CURDATE()  -- 时间范围：最近7天（包含今天，从6天前到今天）
GROUP BY page_id;  -- 分组依据：按页面 ID 分组，确保每个页面的7天数据汇总为一行

select * from ads_wireless_in_store_stats;