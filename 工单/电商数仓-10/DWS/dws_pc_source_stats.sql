create database if not exists gmall_09;

use gmall_09;

# 	PC 端来源页面汇总
drop table if exists dws_page_access_stats;
CREATE TABLE if not exists dws_pc_source_stats
(
    stats_date DATE NOT NULL COMMENT '统计日期',
    refer_page_id VARCHAR(64) NOT NULL COMMENT '来源页面ID',
    visit_count INT COMMENT '访问次数',
    source_ratio DECIMAL(5,2) COMMENT '来源占比（%）',
    PRIMARY KEY (stats_date, refer_page_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'PC端来源页面汇总表';


-- MySQL 5.7 兼容版（无窗口函数）
-- 向 dws_pc_source_stats 表插入数据，统计 PC 端来源页面的访问量及占比（MySQL 5.7 兼容，不依赖窗口函数）
INSERT IGNORE INTO dws_pc_source_stats
SELECT
    -- a 子查询中的 stats_date（统计日期）
    a.stats_date,
    -- a 子查询中的 refer_page_id（来源页面 ID）
    a.refer_page_id,
    -- a 子查询中的 visit_count（该来源页面当日访问量）
    a.visit_count,
    -- 计算来源页面访问量占当日总访问量的比例，用 NULLIF 处理分母为 0 的情况（避免除法报错）
    -- 公式：(来源页面访问量 / 当日总访问量) * 100 ，得到占比百分比
    (a.visit_count / NULLIF(b.total_visit, 0)) * 100 AS source_ratio
FROM (
         -- 子查询 a：统计 PC 端每个来源页面在每日的访问量
         SELECT
             -- 将访问时间 access_time 转换为日期格式，作为统计日期 stats_date
             DATE(access_time) AS stats_date,
             -- 来源页面 ID
             refer_page_id,
             -- 统计每个 (stats_date, refer_page_id) 分组的记录数，即该来源页面当日的访问量
             COUNT(*) AS visit_count
         FROM dwd_shop_page_access_detail
         WHERE device_type = 'pc'  -- 筛选设备类型为 PC 端的数据
               -- 按统计日期和来源页面 ID 分组，确保每个分组对应一个来源页面在一天的访问情况
         GROUP BY DATE(access_time), refer_page_id
     ) AS a
    -- 关联子查询 b，通过 stats_date 字段关联，获取每日的总访问量
    JOIN
    (
            -- 子查询 b：统计 PC 端每日的总访问量
            SELECT
                -- 将访问时间 access_time 转换为日期格式，作为统计日期 stats_date
                DATE(access_time) AS stats_date,
                -- 统计每个 stats_date 分组的记录数，即当日 PC 端的总访问量
                COUNT(*) AS total_visit
            FROM dwd_shop_page_access_detail
            WHERE device_type = 'pc'  -- 筛选设备类型为 PC 端的数据
                  -- 按统计日期分组，确保每个分组对应一天的总访问量
            GROUP BY DATE(access_time)
    ) AS b ON a.stats_date = b.stats_date;  -- 关联条件：统计日期一致

select * from dws_pc_source_stats;

