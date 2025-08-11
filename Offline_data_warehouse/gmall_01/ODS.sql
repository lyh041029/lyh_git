-- ==============================================================
-- 使用数据库
-- ==============================================================
CREATE DATABASE IF NOT EXISTS gmall_01;
USE gmall_01;

-- ==============================================================
-- 1. 商品信息表
-- ==============================================================
DROP TABLE IF EXISTS ods_product_info;
CREATE TABLE ods_product_info
(
    product_id    BIGINT PRIMARY KEY COMMENT '商品ID',
    product_name  VARCHAR(100) COMMENT '商品名称',
    category_id   BIGINT COMMENT '分类ID',
    category_name VARCHAR(50) COMMENT '分类名称',
    price         DECIMAL(10, 2) COMMENT '商品价格',
    create_time   DATETIME COMMENT '创建时间'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT = '商品信息表';


select * from ods_product_info;

-- ==============================================================
-- 2. 商品访客表（新增 is_micro_detail）
-- ==============================================================
DROP TABLE IF EXISTS ods_product_visit;
CREATE TABLE ods_product_visit
(
    visit_id        BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '访问ID',
    product_id      BIGINT COMMENT '商品ID',
    user_id         BIGINT COMMENT '用户ID',
    visit_time      DATETIME COMMENT '访问时间',
    stay_seconds    INT COMMENT '停留时长(秒)',
    terminal_type   ENUM ('PC','MOBILE') COMMENT '终端类型',
    is_micro_detail TINYINT(1) DEFAULT 0 COMMENT '是否微详情访客(停留>=3秒)'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT = '商品访客表';

select * from ods_product_visit;

-- ==============================================================
-- 3. 商品收藏表
-- ==============================================================
DROP TABLE IF EXISTS ods_product_favorite;
CREATE TABLE ods_product_favorite
(
    fav_id     BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '收藏ID',
    product_id BIGINT COMMENT '商品ID',
    user_id    BIGINT COMMENT '用户ID',
    fav_time   DATETIME COMMENT '收藏时间'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT = '商品收藏表';


select * from ods_product_favorite;

-- ==============================================================
-- 4. 商品加购表
-- ==============================================================
DROP TABLE IF EXISTS ods_product_cart;
CREATE TABLE ods_product_cart
(
    cart_id    BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '购物车ID',
    product_id BIGINT COMMENT '商品ID',
    user_id    BIGINT COMMENT '用户ID',
    quantity   INT COMMENT '商品数量',
    cart_time  DATETIME COMMENT '加购时间'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT = '商品加购表';

select * from ods_product_cart;

-- ==============================================================
-- 5. 订单信息表（新增 activity_type）
-- ==============================================================
DROP TABLE IF EXISTS ods_order_info;
CREATE TABLE ods_order_info
(
    order_id      BIGINT PRIMARY KEY COMMENT '订单ID',
    product_id    BIGINT COMMENT '商品ID',
    user_id       BIGINT COMMENT '用户ID',
    quantity      INT COMMENT '商品数量',
    order_amount  DECIMAL(10, 2) COMMENT '订单金额',
    order_time    DATETIME COMMENT '下单时间',
    terminal_type ENUM ('PC','MOBILE') COMMENT '终端类型',
    activity_type ENUM('NONE','JUHUASUAN') DEFAULT 'NONE' COMMENT '活动类型'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT = '订单信息表';


select * from ods_order_info;

-- ==============================================================
-- 6. 支付信息表（新增 activity_type）
-- ==============================================================
DROP TABLE IF EXISTS ods_payment_info;
CREATE TABLE ods_payment_info
(
    payment_id    BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '支付ID',
    order_id      BIGINT COMMENT '订单ID',
    product_id    BIGINT COMMENT '商品ID',
    user_id       BIGINT COMMENT '用户ID',
    pay_amount    DECIMAL(10, 2) COMMENT '支付金额',
    pay_time      DATETIME COMMENT '支付时间',
    terminal_type ENUM ('PC','MOBILE') COMMENT '终端类型',
    activity_type ENUM('NONE','JUHUASUAN') DEFAULT 'NONE' COMMENT '活动类型'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT = '支付信息表';

select * from ods_payment_info;

-- ==============================================================
-- 7. 退款信息表
-- ==============================================================
DROP TABLE IF EXISTS ods_refund_info;
CREATE TABLE ods_refund_info
(
    refund_id     BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '退款ID',
    order_id      BIGINT COMMENT '订单ID',
    product_id    BIGINT COMMENT '商品ID',
    user_id       BIGINT COMMENT '用户ID',
    refund_amount DECIMAL(10, 2) COMMENT '退款金额',
    refund_time   DATETIME COMMENT '退款时间',
    refund_type   ENUM ('ONLY_REFUND','REFUND_RETURN') COMMENT '退款类型'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT = '退款信息表';

select * from ods_refund_info;
