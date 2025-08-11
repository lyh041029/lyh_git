USE gmall_01;

-- ==============================================================
-- 1. 商品主题 - 日指标
-- ==============================================================
DROP TABLE IF EXISTS ads_product_day;
CREATE TABLE ads_product_day (
                                 summary_date        DATE COMMENT '日期',
                                 product_id          BIGINT COMMENT '商品ID',
                                 product_name        VARCHAR(100) COMMENT '商品名称',
                                 category_id         BIGINT COMMENT '分类ID',
                                 category_name       VARCHAR(50) COMMENT '分类名称',
                                 uv                  BIGINT COMMENT '访客数',
                                 pv                  BIGINT COMMENT '浏览量',
                                 avg_stay_seconds    DECIMAL(10, 2) COMMENT '平均停留时长(秒)',
                                 bounce_rate         DECIMAL(10, 4) COMMENT '跳出率',
                                 fav_users           BIGINT COMMENT '收藏用户数',
                                 cart_users          BIGINT COMMENT '加购用户数',
                                 order_users         BIGINT COMMENT '下单用户数',
                                 pay_users           BIGINT COMMENT '支付用户数',
                                 pay_amount          DECIMAL(10, 2) COMMENT '支付金额',
                                 refund_amount       DECIMAL(10, 2) COMMENT '退款金额',
                                 PRIMARY KEY (summary_date, product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='ADS-商品主题日指标';

INSERT INTO ads_product_day (
    summary_date, product_id, product_name, category_id, category_name,
    uv, pv, avg_stay_seconds, bounce_rate, fav_users, cart_users,
    order_users, pay_users, pay_amount, refund_amount
)
SELECT
    summary_date,
    product_id,
    product_name,
    category_id,
    category_name,
    uv,
    pv,
    avg_stay_seconds,
    bounce_rate,
    fav_users,
    cart_users,
    order_users,
    pay_users,
    pay_amount,
    refund_amount
FROM dws_product_day_summary;

select * from ads_product_day;


-- ==============================================================
-- 2. 商品主题 - 周指标
-- ==============================================================
DROP TABLE IF EXISTS ads_product_week;
CREATE TABLE ads_product_week (
                                  week_start          DATE COMMENT '周开始日期',
                                  product_id          BIGINT COMMENT '商品ID',
                                  product_name        VARCHAR(100) COMMENT '商品名称',
                                  category_id         BIGINT COMMENT '分类ID',
                                  category_name       VARCHAR(50) COMMENT '分类名称',
                                  uv                  BIGINT COMMENT '访客数',
                                  pv                  BIGINT COMMENT '浏览量',
                                  avg_stay_seconds    DECIMAL(10, 2) COMMENT '平均停留时长(秒)',
                                  bounce_rate         DECIMAL(10, 4) COMMENT '跳出率',
                                  fav_users           BIGINT COMMENT '收藏用户数',
                                  cart_users          BIGINT COMMENT '加购用户数',
                                  order_users         BIGINT COMMENT '下单用户数',
                                  pay_users           BIGINT COMMENT '支付用户数',
                                  pay_amount          DECIMAL(10, 2) COMMENT '支付金额',
                                  refund_amount       DECIMAL(10, 2) COMMENT '退款金额',
                                  PRIMARY KEY (week_start, product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='ADS-商品主题周指标';

INSERT INTO ads_product_week (
    week_start, product_id, product_name, category_id, category_name,
    uv, pv, avg_stay_seconds, bounce_rate, fav_users, cart_users,
    order_users, pay_users, pay_amount, refund_amount
)
SELECT
    MIN(summary_date) AS week_start,
    product_id,
    product_name,
    category_id,
    category_name,
    SUM(uv) AS uv,
    SUM(pv) AS pv,
    ROUND(AVG(avg_stay_seconds),2) AS avg_stay_seconds,
    ROUND(SUM(bounce_rate) / COUNT(*),4) AS bounce_rate,
    SUM(fav_users) AS fav_users,
    SUM(cart_users) AS cart_users,
    SUM(order_users) AS order_users,
    SUM(pay_users) AS pay_users,
    SUM(pay_amount) AS pay_amount,
    SUM(refund_amount) AS refund_amount
FROM dws_product_day_summary
GROUP BY YEARWEEK(summary_date,1), product_id, product_name, category_id, category_name;


select * from ads_product_week;

-- ==============================================================
-- 3. 商品主题 - 月指标
-- ==============================================================
DROP TABLE IF EXISTS ads_product_month;
CREATE TABLE ads_product_month (
                                   month_key           CHAR(7) COMMENT '月份',
                                   product_id          BIGINT COMMENT '商品ID',
                                   product_name        VARCHAR(100) COMMENT '商品名称',
                                   category_id         BIGINT COMMENT '分类ID',
                                   category_name       VARCHAR(50) COMMENT '分类名称',
                                   uv                  BIGINT COMMENT '访客数',
                                   pv                  BIGINT COMMENT '浏览量',
                                   avg_stay_seconds    DECIMAL(10, 2) COMMENT '平均停留时长(秒)',
                                   bounce_rate         DECIMAL(10, 4) COMMENT '跳出率',
                                   fav_users           BIGINT COMMENT '收藏用户数',
                                   cart_users          BIGINT COMMENT '加购用户数',
                                   order_users         BIGINT COMMENT '下单用户数',
                                   pay_users           BIGINT COMMENT '支付用户数',
                                   pay_amount          DECIMAL(10, 2) COMMENT '支付金额',
                                   refund_amount       DECIMAL(10, 2) COMMENT '退款金额',
                                   PRIMARY KEY (month_key, product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='ADS-商品主题月指标';

INSERT INTO ads_product_month (
    month_key, product_id, product_name, category_id, category_name,
    uv, pv, avg_stay_seconds, bounce_rate, fav_users, cart_users,
    order_users, pay_users, pay_amount, refund_amount
)
SELECT
    DATE_FORMAT(summary_date,'%Y-%m') AS month_key,
    product_id,
    product_name,
    category_id,
    category_name,
    SUM(uv) AS uv,
    SUM(pv) AS pv,
    ROUND(AVG(avg_stay_seconds),2) AS avg_stay_seconds,
    ROUND(SUM(bounce_rate) / COUNT(*),4) AS bounce_rate,
    SUM(fav_users) AS fav_users,
    SUM(cart_users) AS cart_users,
    SUM(order_users) AS order_users,
    SUM(pay_users) AS pay_users,
    SUM(pay_amount) AS pay_amount,
    SUM(refund_amount) AS refund_amount
FROM dws_product_day_summary
GROUP BY DATE_FORMAT(summary_date,'%Y-%m'), product_id, product_name, category_id, category_name;


select * from ads_product_month;

-- ==============================================================
-- 4. 平台交易概览（日）
-- ==============================================================
DROP TABLE IF EXISTS ads_trade_day;
CREATE TABLE ads_trade_day (
                               summary_date DATE COMMENT '日期',
                               uv BIGINT COMMENT '访客数',
                               order_count BIGINT COMMENT '订单数',
                               total_order_amount DECIMAL(10, 2) COMMENT '下单总金额',
                               pay_user_count BIGINT COMMENT '支付用户数',
                               total_pay_amount DECIMAL(10, 2) COMMENT '支付总金额',
                               total_refund_amount DECIMAL(10, 2) COMMENT '退款总金额',
                               PRIMARY KEY (summary_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='ADS-交易主题日指标';

INSERT INTO ads_trade_day (
    summary_date, uv, order_count, total_order_amount,
    pay_user_count, total_pay_amount, total_refund_amount
)
SELECT
    summary_date,
    uv,
    order_count,
    total_order_amount,
    pay_user_count,
    total_pay_amount,
    total_refund_amount
FROM dws_trade_day_summary;


select * from ads_trade_day;

-- ==============================================================
-- 5. 平台交易概览（周）
-- ==============================================================
DROP TABLE IF EXISTS ads_trade_week;
CREATE TABLE ads_trade_week (
                                week_start DATE COMMENT '周开始日期',
                                uv BIGINT COMMENT '访客数',
                                order_count BIGINT COMMENT '订单数',
                                total_order_amount DECIMAL(10, 2) COMMENT '下单总金额',
                                pay_user_count BIGINT COMMENT '支付用户数',
                                total_pay_amount DECIMAL(10, 2) COMMENT '支付总金额',
                                total_refund_amount DECIMAL(10, 2) COMMENT '退款总金额',
                                PRIMARY KEY (week_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='ADS-交易主题周指标';

INSERT INTO ads_trade_week (
    week_start, uv, order_count, total_order_amount,
    pay_user_count, total_pay_amount, total_refund_amount
)
SELECT
    MIN(summary_date) AS week_start,
    SUM(uv) AS uv,
    SUM(order_count) AS order_count,
    SUM(total_order_amount) AS total_order_amount,
    SUM(pay_user_count) AS pay_user_count,
    SUM(total_pay_amount) AS total_pay_amount,
    SUM(total_refund_amount) AS total_refund_amount
FROM dws_trade_day_summary
GROUP BY YEARWEEK(summary_date,1);


select  * from ads_trade_week;


-- ==============================================================
-- 6. 平台交易概览（月）
-- ==============================================================
DROP TABLE IF EXISTS ads_trade_month;
CREATE TABLE ads_trade_month (
                                 month_key CHAR(7) COMMENT '月份',
                                 uv BIGINT COMMENT '访客数',
                                 order_count BIGINT COMMENT '订单数',
                                 total_order_amount DECIMAL(10, 2) COMMENT '下单总金额',
                                 pay_user_count BIGINT COMMENT '支付用户数',
                                 total_pay_amount DECIMAL(10, 2) COMMENT '支付总金额',
                                 total_refund_amount DECIMAL(10, 2) COMMENT '退款总金额',
                                 PRIMARY KEY (month_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='ADS-交易主题月指标';

INSERT INTO ads_trade_month (
    month_key, uv, order_count, total_order_amount,
    pay_user_count, total_pay_amount, total_refund_amount
)
SELECT
    DATE_FORMAT(summary_date,'%Y-%m') AS month_key,
    SUM(uv) AS uv,
    SUM(order_count) AS order_count,
    SUM(total_order_amount) AS total_order_amount,
    SUM(pay_user_count) AS pay_user_count,
    SUM(total_pay_amount) AS total_pay_amount,
    SUM(total_refund_amount) AS total_refund_amount
FROM dws_trade_day_summary
GROUP BY DATE_FORMAT(summary_date,'%Y-%m');


select * from ads_trade_month;
