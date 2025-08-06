create database if not exists gmall_09;

use gmall_09;

# 页面访问排行
drop table if exists ads_page_visit_rank;
CREATE TABLE if not exists ads_page_visit_rank
(
    stats_date DATE NOT NULL COMMENT '统计日期',
    page_type VARCHAR(32) NOT NULL COMMENT '页面类型',
    page_id VARCHAR(64) NOT NULL COMMENT '页面ID',
    page_name VARCHAR(128) COMMENT '页面名称',
    visitor_count INT COMMENT '访客数',
    rank_num INT COMMENT '排名',
    PRIMARY KEY (stats_date, page_type, page_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '页面访问排行表';

INSERT INTO ads_page_visit_rank
SELECT
    stats_date,
    page_type,
    page_id,
    page_name,
    visitor_count,
    rank_num
FROM (
         SELECT
             s.stats_date,
             p.page_type,
             s.page_id,
             p.page_name,
             s.visitor_count,
             @rank := CASE
                          WHEN @current_date = s.stats_date AND @current_type = p.page_type
                              THEN @rank + 1
                          ELSE 1
                 END AS rank_num,
             @current_date := s.stats_date,
             @current_type := p.page_type
         FROM (
                  SELECT
                      s.stats_date,
                      p.page_type,
                      s.page_id,
                      p.page_name,
                      s.visitor_count
                  FROM dws_page_access_stats s
                           JOIN dim_page_info p ON s.page_id = p.page_id
                  ORDER BY s.stats_date, p.page_type, s.visitor_count DESC
              ) AS sorted_data,
              (SELECT @rank := 0, @current_date := NULL, @current_type := NULL) AS init_vars
     ) AS ranked_data;


select *
from ads_page_visit_rank;
