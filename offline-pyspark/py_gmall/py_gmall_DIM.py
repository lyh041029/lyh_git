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

spark.sql("use gmall_dim;")


#------------------------------------------------ dim_sku_full
spark.sql(f"""
    with sku as (
    select
        *
    from gmall.ods_sku_info
    where dt={date}
),
     spu as(
         select
             *
         from gmall.ods_spu_info
         where dt={date}
     ),
     c3 as(
         select
             *
         from gmall.ods_base_category3
         where dt={date}
     ),
     c2 as(
         select
             *
         from gmall.ods_base_category2
         where dt={date}
     ),
     c1 as (
         select
             *
         from gmall.ods_base_category1
         where dt={date}
     ),
     tm as(
         select
             *
         from gmall.ods_base_trademark
         where dt={date}
     ),
     attr as (
         select
             *
         from gmall.ods_sku_attr_value
         where dt={date}
     ),
     sale_attr as (
         select
             *
         from gmall.ods_sku_sale_attr_value
         where dt={date}
     )
insert into dim_sku_full partition (dt={date} )
select
    sku.id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    cast(sku.is_sale as boolean),
    spu.id,
    spu.spu_name,
    c3.id,
    c3.name,
    c2.id,
    c2.name,
    c1.id,
    c1.name,
    tm.id,
    tm.tm_name,
    attr.id,
    attr.value_id,
    attr.attr_name,
    attr.value_name,
    sale_attr.id,
    sale_attr.sale_attr_value_id,
    sale_attr.sale_attr_name,
    sale_attr.sale_attr_value_name,
    sku.create_time
from sku
         left join spu on sku.spu_id=spu.id
         left join c3 on spu.category3_id=c3.id
         left join c2 on c3.category2_id=c2.id
         left join c1 on c2.category1_id=c1.id
         left join tm on tm.id=sku.tm_id
         left join attr on attr.sku_id=sku.id
         left join sale_attr on sale_attr.sku_id=sku.id;
""").show()

#------------------------------------------------ dim_coupon_full

spark.sql(f"""
    insert overwrite table dim_coupon_full partition(dt={date})
select
    id,
    coupon_name,
    coupon_type,
    coupon_dic.dic_name,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    case coupon_type
        when '3201' then concat('满',condition_amount,'元减',benefit_amount,'元')
        when '3202' then concat('满',condition_num,'件打', benefit_discount,' 折')
        when '3203' then concat('减',benefit_amount,'元')
        end benefit_rule,
    create_time,
    range_type,
    range_dic.dic_name,
    limit_num,
    taken_count,
    start_time,
    end_time,
    operate_time,
    expire_time
from
    (
        select
            id,
            coupon_name,
            coupon_type,
            condition_amount,
            condition_num,
            activity_id,
            benefit_amount,
            benefit_discount,
            create_time,
            range_type,
            limit_num,
            taken_count,
            start_time,
            end_time,
            operate_time,
            expire_time
        from gmall.ods_coupon_info
        where dt={date}
    )ci
        left join
    (
        select
            dic_code,
            dic_name
        from gmall.ods_base_dic
        where dt={date}
          and parent_code='32'
    )coupon_dic
    on ci.coupon_type=coupon_dic.dic_code
        left join
    (
        select
            dic_code,
            dic_name
        from gmall.ods_base_dic
        where dt={date}
          and parent_code='33'
    )range_dic
    on ci.range_type=range_dic.dic_code;
""").show()

#------------------------------------------------ dim_activity_full

spark.sql(f"""
        insert into dim_activity_full partition (dt={date})
select
    rule.id,
    info.id,
    info.activity_name,
    dic.dic_code,
    dic.dic_name,
    info.activity_desc,
    info.start_time,
    info.end_time,
    rule.create_time,
    rule.condition_amount,
    rule.condition_num,
    rule.benefit_amount,
    rule.benefit_discount,
    case rule.activity_type
        when '3101' then concat('满',condition_amount,'元减',benefit_amount,'元')
        when '3102' then concat('满',condition_num,'件打', benefit_discount,' 折')
        when '3103' then concat('打', benefit_discount,'折')
        end benefit_rule,
    rule.benefit_level
from (
         select
             *
         from gmall.ods_activity_rule
         where dt={date}
     ) rule
         left join(
    select
        *
    from gmall.ods_activity_info
    where dt={date}
)info
                  on rule.activity_id=info.id
         left join(
    select
        *
    from gmall.ods_base_dic
    where dt={date}
      and parent_code='31'
)dic on dic.dic_code=rule.activity_type;
""").show()

#------------------------------------------------ dim_province_full

spark.sql(f"""
    insert into dim_province_full partition (dt={date})
select
    pro.id,
    pro.name,
    pro.area_code,
    pro.iso_code,
    pro.iso_3166_2,
    reg.id,
    reg.region_name
from (
         select
             *
         from gmall.ods_base_province
         where dt={date}
     ) pro
         left join (
    select
        *
    from gmall.ods_base_region
    where dt={date}
)reg
                   on pro.region_id=reg.id;
""").show()

#------------------------------------------------ dim_promotion_pos_full

spark.sql(f"""
insert into dim_promotion_pos_full partition (dt="20250717")
select
    id,
    pos_location,
    pos_type,
    promotion_type,
    create_time,
    operate_time
from gmall.ods_promotion_pos
where dt={date};
""").show()


#------------------------------------------------ dim_promotion_refer_full

spark.sql(f"""
    insert into dim_promotion_refer_full partition (dt='20250717')
select
    id,
    refer_name,create_time,
    operate_time
from gmall.ods_promotion_refer
where dt={date};
""").show()

#------------------------------------------------ dim_user_zip


spark.sql(f"""
    insert into dim_user_zip partition (dt={date})
select
    id,
    name,
    phone_num,
    email,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    '20250717' start_date,
    '99991231' end_date
from gmall.ods_user_info;
""").show()


