use sx_one_3;


-- 3.1 ads_product_ranking（商品排行榜，文档核心需求）
CREATE TABLE ads_product_ranking (
                                     product_id VARCHAR(255) COMMENT '商品ID',
                                     product_name VARCHAR(255) COMMENT '商品名称',
                                     category_id VARCHAR(255) COMMENT '分类ID',
                                     sales_rank INT COMMENT '销售额排名',
                                     quantity_rank INT COMMENT '销量排名',
                                     stat_period VARCHAR(10) COMMENT '统计周期(7d/30d)',
                                     dt VARCHAR(20) COMMENT '分区日期',
                                     PRIMARY KEY (product_id, stat_period, dt)  -- 添加主键约束，根据业务需求调整
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品排行表';


INSERT INTO ads_product_ranking (
    product_id,
    product_name,
    category_id,
    sales_rank,
    quantity_rank,
    stat_period,
    dt
)
SELECT
    main_query.product_id,
    main_query.product_name,
    main_query.category_id,
    @sales_rank := @sales_rank + 1 AS sales_rank,
    @quantity_rank := @quantity_rank + 1 AS quantity_rank,
    main_query.stat_period,
    '20250805'
FROM
    (
        -- 主查询：确保包含所有需要的字段
        SELECT
            s.product_id,
            p.product_name,  -- 明确包含product_name
            s.category_id,
            s.total_sales,
            s.total_quantity,
            s.stat_period
        FROM
            dws_product_sale_agg s
                INNER JOIN
            dwd_product p ON s.product_id = p.product_id AND p.dt = '20250805'
        WHERE
                s.stat_period = '7d' AND s.dt = '20250805'
        ORDER BY
            s.total_sales DESC,  -- 按销售额排序
            s.total_quantity DESC  -- 按销量排序
    ) AS main_query,
    (SELECT @sales_rank := 0, @quantity_rank := 0) AS rank_init;


select * from ads_product_ranking;




-- 3.2 ads_traffic_source_top10（Top10流量来源，文档【流】需求）
drop table if exists ads_traffic_source_top10;
CREATE TABLE ads_traffic_source_top10 (
                                          source VARCHAR(255) COMMENT '流量来源',
                                          total_visitor INT COMMENT '总访客数',
                                          pay_conversion_rate varchar(20) COMMENT '支付转化率',
                                          rank_num INT COMMENT '排名',
                                          dt VARCHAR(20) COMMENT '分区日期',
                                          PRIMARY KEY (source, dt)  -- 组合主键，确保数据唯一性
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Top10流量来源表';



INSERT INTO ads_traffic_source_top10 (
    source,
    total_visitor,
    pay_conversion_rate,
    rank_num,
    dt
)
SELECT
    source,
    total_visitor,
    -- 保留百分比格式，将计算结果转换为字符串并添加百分号
    CONCAT(ROUND(avg_conversion, 2), '%') AS pay_conversion_rate,
    @rank := @rank + 1 AS rank_num,
    '20250805' AS dt
FROM (
         -- 聚合计算基础数据，先将带百分号的字符串转换为数值计算平均值
         SELECT
             source,
             SUM(total_visitor) AS total_visitor,
             -- 处理带百分号的字符串，转换为数值后计算平均值
             AVG(REPLACE(pay_conversion_rate, '%', '') + 0) AS avg_conversion
         FROM dws_product_traffic_agg
         WHERE dt = '20250805'
         GROUP BY source
         ORDER BY total_visitor DESC
         LIMIT 10
     ) t,
     (SELECT @rank := 0) r;


select * from ads_traffic_source_top10;




-- 3.3 ads_price_strength_warn（价格力预警表，文档预警需求）
CREATE TABLE ads_price_strength_warn (
                                         product_id VARCHAR(255) COMMENT '商品ID',
                                         warn_type VARCHAR(50) COMMENT '预警类型(价格力/商品力)',
                                         price_strength_star INT COMMENT '价格力星级',
                                         dt VARCHAR(20) COMMENT '分区日期',
                                         PRIMARY KEY (product_id, dt, warn_type)  -- 组合主键确保数据唯一性
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='价格力预警表';


INSERT INTO ads_price_strength_warn (
    product_id,
    warn_type,
    price_strength_star,
    dt
)
-- 价格力预警数据
SELECT
    product_id,
    '价格力预警' AS warn_type,
    price_strength_star,
    '20250805' AS dt
FROM dws_price_strength_agg
WHERE is_price_warn = 1 AND dt = '20250805'

UNION ALL

-- 商品力预警数据
SELECT
    product_id,
    '商品力预警' AS warn_type,
    price_strength_star,
    '20250805' AS dt
FROM dws_price_strength_agg
WHERE is_product_warn = 1 AND dt = '20250805';


select * from ads_price_strength_warn ;


