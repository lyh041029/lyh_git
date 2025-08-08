create database if not exists sx_one_3;

use sx_one_3;


-- ODS层商品基础信息表：存储商品ID、分类、价格力星级等基础数据
CREATE TABLE ods_product
(
    product_id          VARCHAR(255) COMMENT '商品唯一标识',
    category_id         VARCHAR(255) COMMENT '商品分类ID',
    product_name        VARCHAR(255) COMMENT '商品名称',
    price_strength_star INT COMMENT '价格力星级（1-5星）',
    coupon_price        DECIMAL(10, 2) COMMENT '普惠券后价',
    create_time         DATETIME COMMENT '商品创建时间',
    PRIMARY KEY (product_id) -- MySQL通常需要主键
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
    COMMENT ='商品基础信息原始表，用于支撑商品排行及价格力分析';


select * from ods_product;


-- ODS层订单交易表：存储销售额、销量、支付买家数等交易数据
CREATE TABLE ods_order
(
    order_id     VARCHAR(255) COMMENT '订单唯一标识',
    product_id   VARCHAR(255) COMMENT '关联商品ID',
    sku_id       VARCHAR(255) COMMENT '商品SKU ID',
    user_id      VARCHAR(255) COMMENT '购买用户ID',
    pay_amount   DECIMAL(10, 2) COMMENT '支付金额（销售额）',
    pay_time     DATETIME COMMENT '支付时间',
    buyer_count  INT COMMENT '支付买家数',
    pay_quantity INT COMMENT '支付件数（销量）',
    PRIMARY KEY (order_id) -- 添加主键约束
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
    COMMENT '订单交易原始表，用于计算商品销售额、销量排名';


select * from ods_order;


-- ODS层流量行为表：存储访客数、流量来源、搜索词等流量数据
CREATE TABLE ods_traffic
(
    traffic_id    VARCHAR(255) COMMENT '流量记录唯一标识',
    product_id    VARCHAR(255) COMMENT '关联商品ID',
    visitor_count INT COMMENT '商品访客数（访问详情页人数）',
    source        VARCHAR(255) COMMENT '流量来源（如效果广告、手淘搜索等）',
    search_word   VARCHAR(255) COMMENT '用户搜索词',
    visit_time    DATETIME COMMENT '访问时间',
    PRIMARY KEY (traffic_id) -- 设置主键约束
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
    COMMENT '商品流量行为原始表，用于分析流量来源及支付转化率';


select * from ods_traffic;


-- ODS层库存信息表：存储SKU库存、可售天数等库存数据
CREATE TABLE ods_inventory
(
    inventory_id  VARCHAR(255) COMMENT '库存记录唯一标识',
    sku_id        VARCHAR(255) COMMENT '关联SKU ID',
    current_stock INT COMMENT '当前库存（件）',
    saleable_days INT COMMENT '库存可售天数',
    update_time   DATETIME COMMENT '库存更新时间',
    PRIMARY KEY (inventory_id) -- 设置主键约束，确保库存记录唯一性
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
    COMMENT '商品库存原始表，用于展示Top5 SKU库存情况';


select * from ods_inventory;