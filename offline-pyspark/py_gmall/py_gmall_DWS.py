from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveIntegration") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/home/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("info")
date = "20250717"

spark.sql("use gmall_dws;")

#------------------------------------------------ dws_trade_user_sku_order_1d
spark.sql(f"""
    WITH od AS (
    SELECT
        dt,
        user_id,
        sku_id,
        COUNT(id) AS order_count,
        SUM(sku_num) AS total_sku_num,
        SUM(split_original_amount) AS split_original_amount,
        SUM(split_activity_amount) AS split_activity_amount,
        SUM(split_coupon_amount) AS split_coupon_amount,
        SUM(split_total_amount) AS split_total_amount
    FROM gmall_dwd.dwd_trade_order_detail_inc
    GROUP BY dt, user_id, sku_id
),
     sku AS (
         SELECT
             id,
             sku_name,
             category1_id,
             category1_name,
             category2_id,
             category2_name,
             category3_id,
             category3_name,
             tm_id,
             tm_name
         FROM gmall_dim.dim_sku_full
         WHERE dt={date}
     )
INSERT INTO dws_trade_user_sku_order_1d PARTITION (dt={date})
SELECT
    od.user_id,
    od.sku_id,
    sku.sku_name,
    sku.category1_id,
    sku.category1_name,
    sku.category2_id,
    sku.category2_name,
    sku.category3_id,
    sku.category3_name,
    sku.tm_id,
    sku.tm_name,
    od.order_count,
    od.total_sku_num,
    od.split_original_amount,
    od.split_activity_amount,
    od.split_coupon_amount,
    od.split_total_amount
FROM od
         LEFT JOIN sku ON od.sku_id = sku.id
WHERE od.dt = {date};
""").show()



#------------------------------------------------ dws_trade_user_order_1d
spark.sql(f"""
    insert into dws_trade_user_order_1d partition (dt='20250717')
select
    user_id,
    count(order_id),
    sum(sku_num),
    sum(split_original_amount),
    sum(split_activity_amount),
    sum(split_coupon_amount),
    sum(split_total_amount)
from gmall_dwd.dwd_trade_order_detail_inc
where dt={date}
group by user_id,dt;
""").show()

#------------------------------------------------ dws_trade_user_cart_add_1d
spark.sql(
    f"""
        insert overwrite table dws_trade_user_cart_add_1d partition(dt={date})
select
    user_id,
    count(*),
    sum(sku_num)
from gmall_dwd.dwd_trade_cart_add_inc
where dt={date}
group by user_id,dt;
    """
).show()

#------------------------------------------------ dws_trade_user_payment_1d

spark.sql(f"""
    insert overwrite table dws_trade_user_payment_1d partition(dt={date})
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_payment_amount)
from gmall_dwd.dwd_trade_pay_detail_suc_inc
where dt={date}
group by user_id,dt;
""").show()


#------------------------------------------------ dws_trade_province_order_1d

spark.sql(f"""
    insert overwrite table dws_trade_province_order_1d partition(dt={date})
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d
from
    (
        select
            province_id,
            count(distinct(order_id)) order_count_1d,
            sum(split_original_amount) order_original_amount_1d,
            sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
            sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
            sum(split_total_amount) order_total_amount_1d
        from gmall_dwd.dwd_trade_order_detail_inc
        where dt={date}
        group by province_id,dt
    )o
        left join
    (
        select
            id,
            province_name,
            area_code,
            iso_code,
            iso_3166_2
        from gmall_dim.dim_province_full
        where dt={date}
    )p
    on o.province_id=p.id;
""").show()


#------------------------------------------------ dws_tool_user_coupon_coupon_used_1d

spark.sql(f"""
        insert overwrite table dws_tool_user_coupon_coupon_used_1d partition(dt={date})
select
    user_id,
    coupon_id,
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    benefit_rule,
    used_count
from
    (
        select
            user_id,
            coupon_id,
            count(*) used_count
        from gmall_dwd.dwd_tool_coupon_used_inc
        where dt={date}
        group by dt,user_id,coupon_id
    )t1
        left join
    (
        select
            id,
            coupon_name,
            coupon_type_code,
            coupon_type_name,
            benefit_rule
        from gmall_dim.dim_coupon_full
        where dt={date}
    )t2
    on t1.coupon_id=t2.id;
""").show()

#------------------------------------------------ dws_interaction_sku_favor_add_1d

spark.sql(f"""
    insert overwrite table dws_interaction_sku_favor_add_1d partition(dt={date})
select
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    favor_add_count
from
    (
        select
            sku_id,
            count(*) favor_add_count
        from gmall_dwd.dwd_interaction_favor_add_inc
        where dt={date}
        group by dt,sku_id
    )favor
        left join
    (
        select
            id,
            sku_name,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            tm_id,
            tm_name
        from gmall_dim.dim_sku_full
        where dt={date}
    )sku
    on favor.sku_id=sku.id;
""").show()


#------------------------------------------------ dws_traffic_session_page_view_1d

spark.sql(f"""
        insert overwrite table dws_traffic_session_page_view_1d partition(dt={date})
select
    session_id,
    mid_id,
    brand,
    model,
    operate_system,
    version_code,
    channel,
    sum(during_time),
    count(*)
from gmall_dwd.dwd_traffic_page_view_inc
where dt={date}
group by session_id,mid_id,brand,model,operate_system,version_code,channel;
""").show()


#------------------------------------------------ dws_traffic_page_visitor_page_view_1d

spark.sql(f"""
        insert overwrite table dws_traffic_page_visitor_page_view_1d partition(dt={date})
select
    mid_id,
    brand,
    model,
    operate_system,
    page_id,
    sum(during_time),
    count(*)
from gmall_dwd.dwd_traffic_page_view_inc
where dt={date}
group by mid_id,brand,model,operate_system,page_id;
""").show()


#------------------------------------------------ dws_trade_user_sku_order_nd

spark.sql(f"""
    insert overwrite table dws_trade_user_sku_order_nd partition(dt={date})
select
    user_id,
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    sum(order_count_1d),
    sum(order_num_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d),
    sum(order_count_1d),
    sum(order_num_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_user_sku_order_1d
where dt<={date}
group by  user_id,sku_id,sku_name,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;
""").show()


#------------------------------------------------ dws_trade_province_order_nd

spark.sql(f"""
        insert overwrite table dws_trade_province_order_nd partition(dt={date})
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(order_count_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d),
    sum(order_count_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws_trade_province_order_1d
where dt<={date}
group by province_id,province_name,area_code,iso_code,iso_3166_2;
""").show()


#------------------------------------------------ dws_trade_user_order_td

spark.sql(f"""
    insert overwrite table dws_trade_user_order_td partition(dt={date})
select
    user_id,
    min(dt) order_date_first,
    max(dt) order_date_last,
    sum(order_count_1d) order_count,
    sum(order_num_1d) order_num,
    sum(order_original_amount_1d) original_amount,
    sum(activity_reduce_amount_1d) activity_reduce_amount,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount,
    sum(order_total_amount_1d) total_amount
from dws_trade_user_order_1d
group by user_id;
""").show()

#------------------------------------------------ dws_user_user_login_td

spark.sql(f"""
    insert overwrite table dws_user_user_login_td partition (dt = {date})
select
    u.id,
    nvl(login_date_last, date_format(create_time, 'yyyy-MM-dd')),
    date_format(create_time, 'yyyy-MM-dd'),
    nvl(login_count_td, 1)
from (
         select id,
                create_time
         from gmall_dim.dim_user_zip
         where dt = {date}
     ) u
         left join
     (
         select user_id,
                max(dt)  login_date_last,
                count(*) login_count_td
         from gmall_dwd.dwd_user_login_inc
         where dt={date}
         group by user_id
     ) l
     on u.id = l.user_id;
""").show()
