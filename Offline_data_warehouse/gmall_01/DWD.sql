USE gmall_01;

-- ==============================================================
-- 1. DWD 商品信息表
-- ==============================================================
DROP TABLE IF EXISTS dwd_product_info;
CREATE TABLE dwd_product_info
AS
SELECT
    product_id,
    product_name,
    category_id,
    category_name,
    price,
    create_time
FROM ods_product_info;


select * from dwd_product_info;

-- ==============================================================
-- 2. DWD 商品访客明细表（新增 is_micro_detail）
-- ==============================================================
DROP TABLE IF EXISTS dwd_product_visit;
CREATE TABLE dwd_product_visit
AS
SELECT
    v.product_id,
    p.product_name,
    p.category_id,
    p.category_name,
    v.user_id,
    v.visit_time,
    v.stay_seconds,
    v.terminal_type,
    v.is_micro_detail
FROM ods_product_visit v
         JOIN ods_product_info p ON v.product_id = p.product_id
WHERE v.stay_seconds > 0
  AND v.visit_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);

select * from dwd_product_visit;

-- ==============================================================
-- 3. DWD 商品收藏明细表
-- ==============================================================
DROP TABLE IF EXISTS dwd_product_favorite;
CREATE TABLE dwd_product_favorite
AS
SELECT
    f.product_id,
    p.product_name,
    p.category_id,
    p.category_name,
    f.user_id,
    f.fav_time
FROM ods_product_favorite f
         JOIN ods_product_info p ON f.product_id = p.product_id
WHERE f.fav_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);


select * from  dwd_product_favorite;

-- ==============================================================
-- 4. DWD 商品加购明细表
-- ==============================================================
DROP TABLE IF EXISTS dwd_product_cart;
CREATE TABLE dwd_product_cart
AS
SELECT
    c.product_id,
    p.product_name,
    p.category_id,
    p.category_name,
    c.user_id,
    c.quantity,
    c.cart_time
FROM ods_product_cart c
         JOIN ods_product_info p ON c.product_id = p.product_id
WHERE c.cart_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);


select * from dwd_product_cart;

-- ==============================================================
-- 5. DWD 订单明细表（新增 activity_type）
-- ==============================================================
DROP TABLE IF EXISTS dwd_order_info;
CREATE TABLE dwd_order_info
AS
SELECT
    o.order_id,
    o.product_id,
    p.product_name,
    p.category_id,
    p.category_name,
    o.user_id,
    o.quantity,
    o.order_amount,
    o.order_time,
    o.terminal_type,
    o.activity_type
FROM ods_order_info o
         JOIN ods_product_info p ON o.product_id = p.product_id
WHERE o.order_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);


select * from dwd_order_info;

-- ==============================================================
-- 6. DWD 支付明细表（新增 activity_type）
-- ==============================================================
DROP TABLE IF EXISTS dwd_payment_info;
CREATE TABLE dwd_payment_info
AS
SELECT
    pay.order_id,
    pay.product_id,
    p.product_name,
    p.category_id,
    p.category_name,
    pay.user_id,
    pay.pay_amount,
    pay.pay_time,
    pay.terminal_type,
    pay.activity_type
FROM ods_payment_info pay
         JOIN ods_product_info p ON pay.product_id = p.product_id
         JOIN ods_order_info o ON pay.order_id = o.order_id
WHERE pay.pay_amount > 0
  AND pay.pay_time >= o.order_time
  AND pay.pay_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);

select * from dwd_payment_info;

-- ==============================================================
-- 7. DWD 退款明细表
-- ==============================================================
DROP TABLE IF EXISTS dwd_refund_info;
CREATE TABLE dwd_refund_info
AS
SELECT
    r.order_id,
    r.product_id,
    p.product_name,
    p.category_id,
    p.category_name,
    r.user_id,
    r.refund_amount,
    r.refund_time,
    r.refund_type
FROM ods_refund_info r
         JOIN ods_product_info p ON r.product_id = p.product_id
WHERE r.refund_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);


select * from dwd_refund_info;
