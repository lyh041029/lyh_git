use sx_one_3;

CREATE TABLE dws_product_sale_agg
(
    product_id        VARCHAR(255) COMMENT '商品ID',
    category_id       VARCHAR(255) COMMENT '分类ID',
    total_sales       DECIMAL(12, 2) COMMENT '总销售额',
    total_quantity    INT COMMENT '总销量',
    total_buyer_count INT COMMENT '总支付买家数',
    stat_period       VARCHAR(10) COMMENT '统计周期(7d/30d)',
    dt                VARCHAR(8) COMMENT '分区日期',
    PRIMARY KEY (product_id, stat_period, dt),
    INDEX idx_dt (dt),
    INDEX idx_category_id (category_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '商品销售汇总表';



INSERT INTO dws_product_sale_agg (product_id,
                                  category_id,
                                  total_sales,
                                  total_quantity,
                                  total_buyer_count,
                                  stat_period,
                                  dt)
SELECT o.product_id,
       p.category_id,
       SUM(o.pay_amount)   AS total_sales,
       SUM(o.pay_quantity) AS total_quantity,
       SUM(o.buyer_count)  AS total_buyer_count,
       '7d'                AS stat_period,
       '20250805'          AS dt
FROM dwd_order_detail o
         JOIN dwd_product p ON o.product_id = p.product_id AND p.dt = '20250805'
WHERE o.dt BETWEEN DATE_SUB('20250805', INTERVAL 6 DAY) AND '20250805' -- MySQL中用DATE_SUB处理日期
GROUP BY o.product_id, p.category_id;


select *
from dws_product_sale_agg;


-- 2.2 dws_product_traffic_agg（商品流量汇总）
drop table if exists dws_product_traffic_agg;
CREATE TABLE dws_product_traffic_agg
(
    product_id          VARCHAR(64) COMMENT '商品ID',   -- 缩短长度
    source              VARCHAR(32) COMMENT '流量来源', -- 缩短长度（如"app"、"web"等无需过长）
    total_visitor       INT COMMENT '总访客数',
    pay_conversion_rate VARCHAR(32) COMMENT '支付转化率=总买家数/总访客数',
    search_word         VARCHAR(128) COMMENT '搜索词',  -- 缩短长度（搜索词通常无需255字符）
    dt                  VARCHAR(8) COMMENT '分区日期',  -- 日期固定8位无需修改
    -- 调整后的联合主键，总长度控制在限制内
    PRIMARY KEY (product_id, source, search_word, dt),
    INDEX idx_dt (dt),
    INDEX idx_source (source)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '商品流量汇总表';


INSERT INTO dws_product_traffic_agg (product_id,
                                     source,
                                     total_visitor,
                                     pay_conversion_rate, -- 注意：若此字段定义为数值类型，需先修改为字符串类型
                                     search_word,
                                     dt)
SELECT t.product_id,
       t.source,
       SUM(t.visitor_count)             AS total_visitor,
       -- 计算转化率*100并拼接百分号，保留2位小数
       CONCAT(ROUND(
                      CASE
                          WHEN SUM(t.visitor_count) = 0 THEN 0
                          ELSE (SUM(o.buyer_count) / SUM(t.visitor_count)) * 100
                          END, 2), '%') AS pay_conversion_rate,
       COALESCE(t.search_word, '')      AS search_word,
       '20250805'                       AS dt
FROM dwd_traffic_detail t
         LEFT JOIN dwd_order_detail o ON t.product_id = o.product_id AND o.dt = '20250805'
WHERE t.dt = '20250805'
GROUP BY t.product_id, t.source, COALESCE(t.search_word, '');


select *
from dws_product_traffic_agg;


-- 2.3 dws_price_strength_agg（价格力商品汇总）
CREATE TABLE dws_price_strength_agg
(
    product_id          VARCHAR(64) COMMENT '商品ID',
    price_strength_star TINYINT COMMENT '价格力星级（通常星级为1-5，用TINYINT足够存储）',
    is_price_warn       TINYINT COMMENT '价格力预警(1=是,0=否)',
    is_product_warn     TINYINT COMMENT '商品力预警(1=是,0=否)',
    dt                  VARCHAR(8) COMMENT '分区日期',
    -- 联合主键确保同一商品在同一日期的数据唯一性
    PRIMARY KEY (product_id, dt),
    -- 为dt字段创建索引，提升按日期查询效率（模拟分区查询）
    INDEX idx_dt (dt)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '价格力商品汇总表';



INSERT INTO dws_price_strength_agg (product_id,
                                    price_strength_star,
                                    is_price_warn,
                                    is_product_warn,
                                    dt)
SELECT p.product_id,
       -- 对同一商品的星级取最大值（可根据业务调整为MIN/MAX/FIRST_VALUE等）
       MAX(p.price_strength_star)                                                  AS price_strength_star,
       -- 基于去重后的星级计算预警
       CASE WHEN MAX(p.price_strength_star) <= 2 OR RAND() < 0.1 THEN 1 ELSE 0 END AS is_price_warn,
       -- 处理转化率（如果存在多条流量数据，取第一条非空值）
       CASE
           WHEN (REPLACE(MAX(t.pay_conversion_rate), '%', '') + 0) < 5
               AND RAND() < 0.08 THEN 1
           ELSE 0 END                                                              AS is_product_warn,
       '20250805'                                                                  AS dt
FROM dwd_product p
         LEFT JOIN dws_product_traffic_agg t
                   ON p.product_id = t.product_id AND t.dt = '20250805'
WHERE p.dt = '20250805'
-- 核心：按product_id分组，强制每个商品只保留一条记录
GROUP BY p.product_id;


select *
from dws_price_strength_agg;



