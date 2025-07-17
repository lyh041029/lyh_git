use gmall_dwd;

DROP TABLE IF EXISTS dwd_trade_cart_add_inc;
CREATE EXTERNAL TABLE dwd_trade_cart_add_inc
(
    `id`                  STRING COMMENT '编号',
    `user_id`            STRING COMMENT '用户ID',
    `sku_id`             STRING COMMENT 'SKU_ID',
    `date_id`            STRING COMMENT '日期ID',
    `create_time`        STRING COMMENT '加购时间',
    `sku_num`            BIGINT COMMENT '加购物车件数'
) COMMENT '交易域加购事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_add_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


insert into dwd_trade_cart_add_inc partition (dt='20250717')
select
    id,
    user_id,
    sku_id,
    substr(create_time,1,7),
    create_time,
    sku_num
from gmall.ods_cart_info
where dt='20250717';


select * from dwd_trade_cart_add_inc;




DROP TABLE IF EXISTS dwd_trade_order_detail_inc;
CREATE EXTERNAL TABLE dwd_trade_order_detail_inc
(
    `id`                     STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单ID',
    `user_id`               STRING COMMENT '用户ID',
    `sku_id`                STRING COMMENT '商品ID',
    `province_id`          STRING COMMENT '省份ID',
    `activity_id`          STRING COMMENT '参与活动ID',
    `activity_rule_id`    STRING COMMENT '参与活动规则ID',
    `coupon_id`             STRING COMMENT '使用优惠券ID',
    `date_id`               STRING COMMENT '下单日期ID',
    `create_time`           STRING COMMENT '下单时间',
    `sku_num`                BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
) COMMENT '交易域下单事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_order_detail_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


insert overwrite table dwd_trade_order_detail_inc partition (dt='20250717')
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    create_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount
from
    (
        select
            id,
            order_id,
            sku_id,
            create_time,
            sku_num,
            sku_num * order_price split_original_amount,
            split_total_amount,
            split_activity_amount,
            split_coupon_amount
        from gmall.ods_order_detail
        where dt='20250717'
    ) od
        left join
    (
        select
            id,
            user_id,
            province_id
        from gmall.ods_order_info
        where dt='20250717'
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            order_detail_id,
            activity_id,
            activity_rule_id
        from gmall.ods_order_detail_activity
        where dt='20250717'
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            order_detail_id,
            coupon_id
        from gmall.ods_order_detail_coupon
        where dt='20250717'
    ) cou
    on od.id = cou.order_detail_id;

select * from dwd_trade_order_detail_inc;


DROP TABLE IF EXISTS dwd_trade_pay_detail_suc_inc;
CREATE EXTERNAL TABLE dwd_trade_pay_detail_suc_inc
(
    `id`                      STRING COMMENT '编号',
    `order_id`               STRING COMMENT '订单ID',
    `user_id`                STRING COMMENT '用户ID',
    `sku_id`                 STRING COMMENT 'SKU_ID',
    `province_id`           STRING COMMENT '省份ID',
    `activity_id`           STRING COMMENT '参与活动ID',
    `activity_rule_id`     STRING COMMENT '参与活动规则ID',
    `coupon_id`              STRING COMMENT '使用优惠券ID',
    `payment_type_code`     STRING COMMENT '支付类型编码',
    `payment_type_name`     STRING COMMENT '支付类型名称',
    `date_id`                STRING COMMENT '支付日期ID',
    `callback_time`         STRING COMMENT '支付成功时间',
    `sku_num`                 BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '应支付原始金额',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '支付活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',
    `split_payment_amount`  DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域支付成功事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_pay_detail_suc_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert into dwd_trade_pay_detail_suc_inc partition (dt='20250717')
select
    od.id,
    pay.order_id,
    pay.user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    dic_name,
    substr(pay.create_time,1,7),
    callback_time,
    sku_num,
    (split_activity_amount+split_coupon_amount+split_total_amount),
    split_activity_amount,
    split_coupon_amount,
    split_total_amount
from
    (
        select
            id,
            order_id,
            sku_id,
            create_time,
            sku_num,
            sku_num * order_price split_original_amount,
            split_total_amount,
            split_activity_amount,
            split_coupon_amount
        from gmall.ods_order_detail
        where dt='20250717'
    ) od
        left join
    (
        select
            id,
            user_id,
            province_id
        from gmall.ods_order_info
        where dt='20250717'
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            order_detail_id,
            activity_id,
            activity_rule_id
        from gmall.ods_order_detail_activity
        where dt='20250717'
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            order_detail_id,
            coupon_id
        from gmall.ods_order_detail_coupon
        where dt='20250717'
    ) cou
    on od.id = cou.order_detail_id
        left join
    (
        select * from gmall.ods_payment_info
        where dt='20250717'
          and payment_status='1602'
    )pay
    on pay.order_id=oi.id
        left join
    (
        select * from gmall.ods_base_dic
        where dt='20250717'
          and parent_code='11'
    )dic
    on dic.dic_code=pay.payment_type;

select * from dwd_trade_pay_detail_suc_inc;


DROP TABLE IF EXISTS dwd_trade_cart_full;
CREATE EXTERNAL TABLE dwd_trade_cart_full
(
    `id`         STRING COMMENT '编号',
    `user_id`   STRING COMMENT '用户ID',
    `sku_id`    STRING COMMENT 'SKU_ID',
    `sku_name`  STRING COMMENT '商品名称',
    `sku_num`   BIGINT COMMENT '现存商品件数'
) COMMENT '交易域购物车周期快照事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert into dwd_trade_cart_full partition (dt="20250717")
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from gmall.ods_cart_info
where dt='20250717'
  and is_ordered=0;

select * from dwd_trade_cart_full;


DROP TABLE IF EXISTS dwd_trade_trade_flow_acc;
CREATE EXTERNAL TABLE dwd_trade_trade_flow_acc
(
    `order_id`               STRING COMMENT '订单ID',
    `user_id`                STRING COMMENT '用户ID',
    `province_id`           STRING COMMENT '省份ID',
    `order_date_id`         STRING COMMENT '下单日期ID',
    `order_time`             STRING COMMENT '下单时间',
    `payment_date_id`        STRING COMMENT '支付日期ID',
    `payment_time`           STRING COMMENT '支付时间',
    `finish_date_id`         STRING COMMENT '确认收货日期ID',
    `finish_time`             STRING COMMENT '确认收货时间',
    `order_original_amount` DECIMAL(16, 2) COMMENT '下单原始价格',
    `order_activity_amount` DECIMAL(16, 2) COMMENT '下单活动优惠分摊',
    `order_coupon_amount`   DECIMAL(16, 2) COMMENT '下单优惠券优惠分摊',
    `order_total_amount`    DECIMAL(16, 2) COMMENT '下单最终价格分摊',
    `payment_amount`         DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域交易流程累积快照事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_trade_flow_acc/'
TBLPROPERTIES ('orc.compress' = 'snappy');


with oi as (
    select
        *
    from gmall.ods_order_info
    where dt='20250717'
),
     pay as(
         select
             *
         from gmall.ods_payment_info
         where dt='20250717'
           and payment_status='1602'
     ),
     log as (
         select
             *
         from gmall.ods_order_status_log
         where dt='20250717'
           and order_status='1004'
     )
insert into dwd_trade_trade_flow_acc partition (dt='20250717')
select
    oi.id,
    oi.user_id,
    oi.province_id,
    substr(oi.create_time,1,7),
    oi.create_time,
    substr(pay.callback_time,1,7),
    pay.callback_time,
    substr(log.create_time,1,7),
    log.create_time,
    (oi.total_amount+oi.activity_reduce_amount+oi.coupon_reduce_amount),
    oi.activity_reduce_amount,
    oi.coupon_reduce_amount,
    oi.total_amount,
    oi.total_amount
from oi
         left join pay on pay.order_id=oi.id
         left join log on log.order_id=oi.id;

select * from dwd_trade_trade_flow_acc;

DROP TABLE IF EXISTS dwd_tool_coupon_used_inc;
CREATE EXTERNAL TABLE dwd_tool_coupon_used_inc
(
    `id`           STRING COMMENT '编号',
    `coupon_id`    STRING COMMENT '优惠券ID',
    `user_id`      STRING COMMENT '用户ID',
    `order_id`     STRING COMMENT '订单ID',
    `date_id`      STRING COMMENT '日期ID',
    `payment_time` STRING COMMENT '使用(支付)时间'
) COMMENT '优惠券使用（支付）事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_tool_coupon_used_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");


insert into dwd_tool_coupon_used_inc partition (dt='20250717')
select
    id,
    coupon_id,
    user_id,
    order_id,
    substr(used_time,1,7),
    used_time
from gmall.ods_coupon_use
where dt='20250717'
  and used_time is not null;

select * from dwd_tool_coupon_used_inc;


DROP TABLE IF EXISTS dwd_interaction_favor_add_inc;
CREATE EXTERNAL TABLE dwd_interaction_favor_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `date_id`     STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '收藏时间'
) COMMENT '互动域收藏商品事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_interaction_favor_add_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

insert into dwd_interaction_favor_add_inc partition (dt='20250717')
select
    id,
    user_id,
    sku_id,
    substr(create_time,1,7),
    create_time
from gmall.ods_favor_info
where is_cancel='0';

select * from dwd_interaction_favor_add_inc;


DROP TABLE IF EXISTS dwd_traffic_page_view_inc;
CREATE EXTERNAL TABLE dwd_traffic_page_view_inc
(
    `province_id`    STRING COMMENT '省份ID',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备ID',
    `operate_system` STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员ID',
    `version_code`   STRING COMMENT 'APP版本号',
    `page_item`       STRING COMMENT '目标ID',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`    STRING COMMENT '上页ID',
    `page_id`          STRING COMMENT '页面ID ',
    `from_pos_id`     STRING COMMENT '点击坑位ID',
    `from_pos_seq`    STRING COMMENT '点击坑位位置',
    `refer_id`         STRING COMMENT '营销渠道ID',
    `date_id`          STRING COMMENT '日期ID',
    `view_time`       STRING COMMENT '跳入时间',
    `session_id`      STRING COMMENT '所属会话ID',
    `during_time`     BIGINT COMMENT '持续时间毫秒'
) COMMENT '流量域页面浏览事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_page_view_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');


insert overwrite table dwd_traffic_page_view_inc partition (dt='20250717')
SELECT
    get_json_object(log, '$.common.ar') AS province_id,
    get_json_object(log, '$.common.ba') AS brand,
    get_json_object(log, '$.common.ch') AS channel,
    get_json_object(log, '$.common.is_new') AS is_new,
    get_json_object(log, '$.common.md') AS model,
    get_json_object(log, '$.common.mid') AS mid_id,
    get_json_object(log, '$.common.os') AS operate_system,
    get_json_object(log, '$.common.uid') AS user_id,
    get_json_object(log, '$.common.vc') AS version_code,
    get_json_object(log, '$.page.item') AS page_item,
    get_json_object(log, '$.page.item_type') AS page_item_type,
    get_json_object(log, '$.page.last_page_id') AS last_page_id,
    get_json_object(log, '$.page.page_id') AS page_id,
    get_json_object(log, '$.page.from_pos_id') AS from_pos_id,
    get_json_object(log, '$.page.from_pos_seq') AS from_pos_seq,
    get_json_object(log, '$.page.refer_id') AS refer_id,
    date_format(from_utc_timestamp(get_json_object(log, '$.ts'), "UTC"), 'yyyy-MM-dd') AS date_id,
    date_format(from_utc_timestamp(get_json_object(log, '$.ts'), "UTC"), 'yyyy-MM-dd HH:mm:ss') AS view_time,
    get_json_object(log, '$.common.sid') AS session_id,
    get_json_object(log, '$.page.during_time') AS during_time
FROM gmall.ods_z_log
WHERE dt = '20250717'
  AND get_json_object(log, '$.page') IS NOT NULL;

select * from dwd_traffic_page_view_inc;


DROP TABLE IF EXISTS dwd_user_register_inc;
CREATE EXTERNAL TABLE dwd_user_register_inc
(
    `user_id`          STRING COMMENT '用户ID',
    `date_id`          STRING COMMENT '日期ID',
    `create_time`     STRING COMMENT '注册时间',
    `channel`          STRING COMMENT '应用下载渠道',
    `province_id`     STRING COMMENT '省份ID',
    `version_code`    STRING COMMENT '应用版本',
    `mid_id`           STRING COMMENT '设备ID',
    `brand`            STRING COMMENT '设备品牌',
    `model`            STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户注册事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_user_register_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

with use_r as (
    select
        *
    from gmall.ods_user_info
    where dt='20250717'
),
     log as (
         select
             get_json_object(log,'$.common.ch') channel,
             get_json_object(log,'$.common.ar') province_id,
             get_json_object(log,'$.common.vc') version_code,
             get_json_object(log,'$.common.mid') mid_id,
             get_json_object(log,'$.common.ba') brand,
             get_json_object(log,'$.common.md') model,
             get_json_object(log,'$.common.os') operate_system,
             get_json_object(log,'$.common.uid') uid
         from gmall.ods_z_log
         WHERE dt = '20250717'
           AND get_json_object(log, '$.page') IS NOT NULL
     )
insert into dwd_user_register_inc partition (dt='20250717')
select
    use_r.id,
    substr(create_time,1,7),
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,model,
    operate_system
from use_r
         left join log on use_r.id=log.uid;

select * from dwd_user_register_inc;


DROP TABLE IF EXISTS dwd_user_login_inc;
CREATE EXTERNAL TABLE dwd_user_login_inc
(
    `user_id`         STRING COMMENT '用户ID',
    `date_id`         STRING COMMENT '日期ID',
    `login_time`     STRING COMMENT '登录时间',
    `channel`         STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份ID',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`          STRING COMMENT '设备ID',
    `brand`           STRING COMMENT '设备品牌',
    `model`           STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户登录事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_user_login_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");


insert overwrite table dwd_user_login_inc partition (dt = '20250717')
select user_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') login_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system
from (
         select user_id,
                channel,
                province_id,
                version_code,
                mid_id,
                brand,
                model,
                operate_system,
                ts
         from (select get_json_object(log,'$.common.uid') user_id,
                      get_json_object(log,'$.common.ch')  channel,
                      get_json_object(log,'$.common.ar')  province_id,
                      get_json_object(log,'$.common.vc')  version_code,
                      get_json_object(log,'$.common.mid') mid_id,
                      get_json_object(log,'$.common.ba')  brand,
                      get_json_object(log,'$.common.md')  model,
                      get_json_object(log,'$.common.os')  operate_system,
                      get_json_object(log,'$.ts') ts,
                      row_number() over (partition by get_json_object(log,'$.common.sid') order by get_json_object(log,'$.ts')) rn
               from gmall.ods_z_log
               where dt = '20250717'
                 and get_json_object(log,'$.page') is not null
                 and get_json_object(log,'$.common.uid') is not null) t1
         where rn = 1
     ) t2;

select * from dwd_user_login_inc;