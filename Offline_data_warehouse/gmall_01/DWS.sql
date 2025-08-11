use gmall_01;

-- ==============================================================
-- 1. 商品主题日汇总表
-- ==============================================================
DROP TABLE IF EXISTS dws_product_day_summary;
CREATE TABLE dws_product_day_summary (
                                         summary_date        DATE COMMENT '汇总日期',
                                         product_id          BIGINT COMMENT '商品ID',
                                         product_name        VARCHAR(100) COMMENT '商品名称',
                                         category_id         BIGINT COMMENT '分类ID',
                                         category_name       VARCHAR(50) COMMENT '分类名称',
                                         uv                  BIGINT COMMENT '访客数',
                                         pv                  BIGINT COMMENT '浏览量',
                                         avg_stay_seconds    DECIMAL(10, 2) COMMENT '平均停留时长(秒)',
                                         bounce_rate         DECIMAL(10, 4) COMMENT '跳出率',
                                         micro_detail_uv     BIGINT COMMENT '微详情访客数',
                                         fav_users           BIGINT COMMENT '收藏用户数',
                                         cart_users          BIGINT COMMENT '加购用户数',
                                         cart_qty            BIGINT COMMENT '加购数量',
                                         order_users         BIGINT COMMENT '下单用户数',
                                         order_qty           BIGINT COMMENT '下单数量',
                                         order_amount        DECIMAL(10, 2) COMMENT '下单金额',
                                         pay_users           BIGINT COMMENT '支付用户数',
                                         pay_qty             BIGINT COMMENT '支付商品数量',
                                         pay_amount          DECIMAL(10, 2) COMMENT '支付金额',
                                         juhuasuan_pay_amount DECIMAL(10, 2) COMMENT '聚划算支付金额',
                                         active_products     BIGINT COMMENT '动销商品数',
                                         item_unit_price     DECIMAL(10, 2) COMMENT '件单价',
                                         refund_amount       DECIMAL(10, 2) COMMENT '退款金额',
                                         PRIMARY KEY (summary_date, product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='商品主题日汇总表';




INSERT INTO dws_product_day_summary
SELECT
    DATE(v.visit_time) AS summary_date,
    v.product_id,
    v.product_name,
    v.category_id,
    v.category_name,
    COUNT(DISTINCT v.user_id) AS uv,
    COUNT(*) AS pv,
    ROUND(AVG(v.stay_seconds), 2) AS avg_stay_seconds,
    ROUND(SUM(CASE WHEN v.stay_seconds <= 3 THEN 1 ELSE 0 END) / COUNT(*), 4) AS bounce_rate,
    COUNT(DISTINCT CASE WHEN v.is_micro_detail = 1 THEN v.user_id END) AS micro_detail_uv,
    COUNT(DISTINCT f.user_id) AS fav_users,
    COUNT(DISTINCT c.user_id) AS cart_users,
    SUM(c.quantity) AS cart_qty,
    COUNT(DISTINCT o.user_id) AS order_users,
    SUM(o.quantity) AS order_qty,
    SUM(o.order_amount) AS order_amount,
    COUNT(DISTINCT pay.user_id) AS pay_users,
    SUM(CASE WHEN pay.pay_amount > 0 THEN 1 ELSE 0 END) AS pay_qty,  -- ✅ MySQL 写法
    SUM(pay.pay_amount) AS pay_amount,
    SUM(CASE WHEN pay.activity_type = 'JUHUASUAN' THEN pay.pay_amount ELSE 0 END) AS juhuasuan_pay_amount,
    COUNT(DISTINCT CASE WHEN pay.pay_amount > 0 THEN v.product_id END) AS active_products,
    ROUND(SUM(pay.pay_amount) / NULLIF(SUM(o.quantity), 0), 2) AS item_unit_price,
    SUM(r.refund_amount) AS refund_amount
FROM dwd_product_visit v
    LEFT JOIN dwd_product_favorite f ON v.product_id = f.product_id AND DATE(f.fav_time) = DATE(v.visit_time)
    LEFT JOIN dwd_product_cart c ON v.product_id = c.product_id AND DATE(c.cart_time) = DATE(v.visit_time)
    LEFT JOIN dwd_order_info o ON v.product_id = o.product_id AND DATE(o.order_time) = DATE(v.visit_time)
    LEFT JOIN dwd_payment_info pay ON v.product_id = pay.product_id AND DATE(pay.pay_time) = DATE(v.visit_time)
    LEFT JOIN dwd_refund_info r ON v.product_id = r.product_id AND DATE(r.refund_time) = DATE(v.visit_time)
WHERE v.visit_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY DATE(v.visit_time), v.product_id, v.product_name, v.category_id, v.category_name;



select * from dws_product_day_summary;



-- ==============================================================
-- 2. 用户主题日汇总表
-- ==============================================================
DROP TABLE IF EXISTS dws_user_day_summary;
CREATE TABLE dws_user_day_summary (
                                      summary_date DATE COMMENT '汇总日期',
                                      user_id BIGINT COMMENT '用户ID',
                                      visit_products BIGINT COMMENT '访问商品数',
                                      pv BIGINT COMMENT '浏览量',
                                      fav_count BIGINT COMMENT '收藏次数',
                                      cart_count BIGINT COMMENT '加购次数',
                                      order_count BIGINT COMMENT '下单次数',
                                      pay_count BIGINT COMMENT '支付次数',
                                      total_pay_amount DECIMAL(10, 2) COMMENT '支付总金额',
                                      total_refund_amount DECIMAL(10, 2) COMMENT '退款总金额',
                                      PRIMARY KEY (summary_date, user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='用户主题日汇总表';




INSERT INTO dws_user_day_summary
SELECT
    summary_date,
    user_id,
    COUNT(DISTINCT product_id) AS visit_products,
    SUM(pv) AS pv,
    SUM(fav_flag) AS fav_count,
    SUM(cart_flag) AS cart_count,
    SUM(order_flag) AS order_count,
    SUM(pay_flag) AS pay_count,
    SUM(pay_amount) AS total_pay_amount,
    SUM(refund_amount) AS total_refund_amount
FROM (
         SELECT
             DATE(v.visit_time) AS summary_date,
     v.user_id,
     v.product_id,
     COUNT(*) AS pv,
     CASE WHEN f.user_id IS NOT NULL THEN 1 ELSE 0 END AS fav_flag,
             CASE WHEN c.user_id IS NOT NULL THEN 1 ELSE 0 END AS cart_flag,
             CASE WHEN o.user_id IS NOT NULL THEN 1 ELSE 0 END AS order_flag,
             CASE WHEN pay.user_id IS NOT NULL THEN 1 ELSE 0 END AS pay_flag,
             COALESCE(pay.pay_amount,0) AS pay_amount,
             COALESCE(r.refund_amount,0) AS refund_amount
         FROM dwd_product_visit v
                  LEFT JOIN dwd_product_favorite f ON v.product_id = f.product_id AND v.user_id = f.user_id AND DATE(f.fav_time) = DATE(v.visit_time)
                  LEFT JOIN dwd_product_cart c ON v.product_id = c.product_id AND v.user_id = c.user_id AND DATE(c.cart_time) = DATE(v.visit_time)
                  LEFT JOIN dwd_order_info o ON v.product_id = o.product_id AND v.user_id = o.user_id AND DATE(o.order_time) = DATE(v.visit_time)
                  LEFT JOIN dwd_payment_info pay ON v.product_id = pay.product_id AND v.user_id = pay.user_id AND DATE(pay.pay_time) = DATE(v.visit_time)
                  LEFT JOIN dwd_refund_info r ON v.product_id = r.product_id AND v.user_id = r.user_id AND DATE(r.refund_time) = DATE(v.visit_time)
         WHERE v.visit_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
         GROUP BY DATE(v.visit_time), v.user_id, v.product_id, f.user_id, c.user_id, o.user_id, pay.user_id, pay.pay_amount, r.refund_amount
     ) t
GROUP BY summary_date, user_id;


select * from dws_user_day_summary;


-- ==============================================================
-- 3. 活动主题日汇总表
-- ==============================================================
DROP TABLE IF EXISTS dws_activity_day_summary;
CREATE TABLE dws_activity_day_summary (
                                          summary_date DATE COMMENT '汇总日期',
                                          activity_type VARCHAR(20) COMMENT '活动类型',
                                          uv BIGINT COMMENT '访客数',
                                          total_order_amount DECIMAL(10, 2) COMMENT '总下单金额',
                                          total_pay_amount DECIMAL(10, 2) COMMENT '总支付金额',
                                          total_refund_amount DECIMAL(10, 2) COMMENT '总退款金额',
                                          PRIMARY KEY (summary_date, activity_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='活动主题日汇总表';



INSERT INTO dws_activity_day_summary
SELECT
    summary_date,
    activity_type,
    COUNT(DISTINCT user_id) AS uv,
    SUM(order_amount) AS total_order_amount,
    SUM(pay_amount) AS total_pay_amount,
    SUM(refund_amount) AS total_refund_amount
FROM (
         SELECT
             DATE(o.order_time) AS summary_date,
     o.activity_type,
     o.user_id,
     o.order_amount,
     COALESCE(pay.pay_amount,0) AS pay_amount,
     COALESCE(r.refund_amount,0) AS refund_amount
    FROM dwd_order_info o
                  LEFT JOIN dwd_payment_info pay ON o.order_id = pay.order_id
    LEFT JOIN dwd_refund_info r ON o.order_id = r.order_id
WHERE o.order_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    ) t
GROUP BY summary_date, activity_type;


select * from  dws_activity_day_summary;




-- ==============================================================
-- 4. 交易主题日汇总表
-- ==============================================================
DROP TABLE IF EXISTS dws_trade_day_summary;
CREATE TABLE dws_trade_day_summary (
                                       summary_date DATE COMMENT '汇总日期',
                                       uv BIGINT COMMENT '访客数',
                                       order_count BIGINT COMMENT '订单数',
                                       total_order_amount DECIMAL(10, 2) COMMENT '总下单金额',
                                       pay_user_count BIGINT COMMENT '支付用户数',
                                       total_pay_amount DECIMAL(10, 2) COMMENT '总支付金额',
                                       total_refund_amount DECIMAL(10, 2) COMMENT '总退款金额',
                                       PRIMARY KEY (summary_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='交易主题日汇总表';

INSERT INTO dws_trade_day_summary
SELECT
    summary_date,
    COUNT(DISTINCT user_id) AS uv,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(order_amount) AS total_order_amount,
    COUNT(DISTINCT pay_user_id) AS pay_user_count,
    SUM(pay_amount) AS total_pay_amount,
    SUM(refund_amount) AS total_refund_amount
FROM (
         SELECT
             DATE(o.order_time) AS summary_date,
     o.order_id,
     o.user_id,
     o.order_amount,
     pay.user_id AS pay_user_id,
     pay.pay_amount,
     r.refund_amount
    FROM dwd_order_info o
                  LEFT JOIN dwd_payment_info pay ON o.order_id = pay.order_id
    LEFT JOIN dwd_refund_info r ON o.order_id = r.order_id
WHERE o.order_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    ) t
GROUP BY summary_date;



select * from dws_trade_day_summary;




