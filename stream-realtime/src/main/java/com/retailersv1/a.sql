select
    od.id,
    od.order_id,
    od.user_id,
    od.sku_id,
    od.province_id,
    od.activity_id,
    od.activity_rule_id,
    od.coupon_id,
    date_fromat(from_unixtime(cast(oc.poperate_time as bigint)/1000),'yyyy-MM-dd')order_cancel_date_id,
    oc.operate_time,
    od.sku_num,
    od.split_original_amount,
    od.split_activity_amount,
    od.split_coupon_amount,
    od.split_total_amount,
    oc.ts_ms
from dwd_order_detail od
join order_cancel oc
on od.order_id = oc.id
































































