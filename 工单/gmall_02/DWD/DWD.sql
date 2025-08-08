use sx_one_3;
-- 1.1 dwd_product（商品维度表，每日快照）
set @dt = '20250805';

drop table if exists dwd_product;
CREATE TABLE dwd_product
(
    product_id          VARCHAR(255) COMMENT '商品ID',
    category_id         VARCHAR(255) COMMENT '分类ID',
    product_name        VARCHAR(255) COMMENT '商品名称',
    price_strength_star INT COMMENT '价格力星级(1-5)',
    coupon_price        DECIMAL(10, 2) COMMENT '普惠券后价',
    create_time         DATE COMMENT '创建时间',
    dt                  VARCHAR(20) COMMENT '分区日期',
    PRIMARY KEY (product_id, dt)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
    COMMENT '商品维度明细层';


-- 从ODS加载并清洗（去重、格式校验）
INSERT INTO dwd_product (product_id,
                         category_id,
                         product_name,
                         price_strength_star,
                         coupon_price,
                         create_time,
                         dt)
SELECT product_id,
       category_id,
       product_name,
       -- 异常值处理：确保星级在1-5之间，否则设为默认值3
       CASE WHEN price_strength_star BETWEEN 1 AND 5 THEN price_strength_star ELSE 3 END AS price_strength_star,
       coupon_price,
       STR_TO_DATE(create_time, '%Y-%m-%d %H:%i:%s'),
       @dt
FROM ods_product
WHERE product_id IS NOT NULL;



select *
from dwd_product;


-- 1.2 dwd_order_detail（订单事实表）
CREATE TABLE dwd_order_detail
(
    order_id     VARCHAR(255) NOT NULL COMMENT '订单ID',
    product_id   VARCHAR(255) COMMENT '商品ID',
    sku_id       VARCHAR(255) COMMENT 'SKU ID',
    user_id      VARCHAR(255) COMMENT '用户ID',
    pay_amount   DECIMAL(10, 2) COMMENT '支付金额',
    pay_time     DATETIME COMMENT '支付时间',
    buyer_count  INT COMMENT '支付买家数',
    pay_quantity INT COMMENT '支付件数',
    dt           VARCHAR(8)   NOT NULL COMMENT '分区日期',
    PRIMARY KEY (order_id, dt)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
    COMMENT '订单明细事实表';


INSERT INTO dwd_order_detail (order_id,
                              product_id,
                              sku_id,
                              user_id,
                              pay_amount,
                              pay_time,
                              buyer_count,
                              pay_quantity,
                              dt)
SELECT order_id,
       product_id,
       sku_id,
       user_id,
       pay_amount,
       pay_time,
       buyer_count,
       pay_quantity,
       @dt AS dt
FROM ods_order
WHERE pay_time IS NOT NULL
  AND DATE(pay_time) = '20250805';


select *
from dwd_order_detail;


-- 1.3 dwd_traffic_detail（流量事实表）
CREATE TABLE dwd_traffic_detail
(
    traffic_id    VARCHAR(255) COMMENT '流量ID',
    product_id    VARCHAR(255) COMMENT '商品ID',
    visitor_count INT COMMENT '访客数',
    source        VARCHAR(255) COMMENT '流量来源',
    search_word   VARCHAR(255) COMMENT '搜索词',
    visit_time    DATETIME COMMENT '访问时间',
    dt            VARCHAR(8) COMMENT '分区日期',
    PRIMARY KEY (traffic_id),
    INDEX idx_dt (dt)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '流量明细事实表';



INSERT INTO dwd_traffic_detail (traffic_id,
                                product_id,
                                visitor_count,
                                source,
                                search_word,
                                visit_time,
                                dt)
SELECT traffic_id,
       product_id,
       visitor_count,
       source,
       search_word,
       visit_time,
       @dt AS dt
FROM ods_traffic
WHERE visitor_count > 0 -- 过滤无效访客数
  AND DATE(visit_time) = '20250805';

select *
from dwd_traffic_detail;


-- 1.4 dwd_inventory_detail（库存事实表）
CREATE TABLE dwd_inventory_detail
(
    inventory_id  VARCHAR(255) COMMENT '库存ID',
    sku_id        VARCHAR(255) COMMENT 'SKU ID',
    current_stock INT COMMENT '当前库存',
    saleable_days INT COMMENT '可售天数',
    update_time   DATETIME COMMENT '更新时间',
    dt            VARCHAR(8) COMMENT '分区日期',
    PRIMARY KEY (inventory_id),
    INDEX idx_dt (dt),
    INDEX idx_sku_id (sku_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '库存明细事实表';


INSERT INTO dwd_inventory_detail (inventory_id,
                                  sku_id,
                                  current_stock,
                                  saleable_days,
                                  update_time,
                                  dt)
SELECT inventory_id,
       sku_id,
       current_stock,
       saleable_days,
       update_time,
       @dt AS dt
FROM ods_inventory
WHERE DATE(update_time) = '20250805';

select *
from dwd_inventory_detail;