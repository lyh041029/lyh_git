create database if not exists gmall_09;

use gmall_09;

#  PC 端来源页面 TOP20
drop table if exists ads_pc_source_top20;
CREATE TABLE if not exists ads_pc_source_top20
(
    stats_date DATE NOT NULL COMMENT '统计日期',
    refer_page_id VARCHAR(64) NOT NULL COMMENT '来源页面ID',
    refer_page_name VARCHAR(128) COMMENT '来源页面名称',
    visit_count INT COMMENT '访问次数',
    source_ratio DECIMAL(5,2) COMMENT '来源占比（%）',
    rank_num INT COMMENT '排名',
    PRIMARY KEY (stats_date, refer_page_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'PC端来源页面TOP20表';

INSERT INTO ads_pc_source_top20
    SELECT
        stats_date,
        s.refer_page_id,
        p.page_name AS refer_page_name,
        s.visit_count,
        s.source_ratio,
        ROW_NUMBER() OVER (PARTITION BY stats_date ORDER BY s.visit_count DESC) AS rank_num
FROM dws_pc_source_stats s
    LEFT JOIN dim_page_info p ON s.refer_page_id = p.page_id QUALIFY rank_num <= 20;

select *
from ads_pc_source_top20;