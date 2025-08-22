package com.retailersv1.dwd;


import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import javax.security.auth.login.Configuration;
import java.time.Duration;

/**
 * 下单事实表处理类
 * 
 * 该类负责处理用户下单的详细信息，从ODS层读取订单相关数据，
 * 包括订单详情、订单信息、活动信息和优惠券信息，
 * 通过多表关联整合数据，最终形成完整的下单事实表写入DWD层。
 */
public class DbusDwdOrderDetail {
    // ODS层Kafka主题配置 - 用于读取原始数据库变更数据
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    // DWD层订单详情Kafka主题配置 - 用于写入处理后的订单详情数据
    private static final String DWD_TRADE_ORDER_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.detail");

    public static void main(String[] args) {
        // 创建Flink流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置环境参数，如检查点等通用配置
        EnvironmentSettingUtils.defaultParameter(env);
        // 设置并行度为2
        env.setParallelism(2);
        // 设置状态后端为HashMapStateBackend
        env.setStateBackend(new HashMapStateBackend());

        // 创建表环境，用于执行SQL操作
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 设置状态保留时间：10秒
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // 1. 创建ODS层数据表，用于读取原始数据库变更数据
        tEnv.executeSql("CREATE TABLE ods_data_kafka_topic (\n" +
                "  `op` STRING,\n" +                      // 操作类型：c-插入, u-更新, d-删除
                "  `before` MAP<STRING,STRING>,\n" +     // 变更前数据
                "  `after` MAP<STRING,STRING>,\n" +      // 变更后数据
                "  `source` MAP<STRING,STRING>,\n" +     // 数据源信息
                "  `ts_ms` BIGINT,\n" +                  // 时间戳(毫秒)
                "   proc_time AS proctime()" +           // 处理时间
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "test"));

        // 2. 从ODS层数据中过滤出订单详情数据
        Table orderDetail = tEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +               // 订单详情ID
                        "`after`['order_id'] order_id," +   // 订单ID
                        "`after`['sku_id'] sku_id," +       // SKU ID
                        "`after`['sku_name'] sku_name," +   // SKU名称
                        "`after`['create_time'] create_time," + // 创建时间
                        "`after`['source_id'] source_id," + // 来源ID
                        "`after`['source_type'] source_type," + // 来源类型
                        "`after`['sku_num'] sku_num," +     // SKU数量
                        // 计算原始金额：sku_num * order_price
                        "cast(cast(`after`['sku_num'] as decimal(16,2)) * " +
                        "   cast(`after`['order_price'] as decimal(16,2)) as String) split_original_amount," +
                        "`after`['split_total_amount'] split_total_amount," +  // 分摊总金额
                        "`after`['split_activity_amount'] split_activity_amount," + // 分摊活动金额
                        "`after`['split_coupon_amount'] split_coupon_amount," + // 分摊的优惠券金额
                        "ts_ms " +                          // 时间戳
                        "from ods_data_kafka_topic " +
                        " where `source`['table'] ='order_detail'");
        tEnv.createTemporaryView("order_detail", orderDetail);

        // 3. 过滤出订单信息数据
        Table orderInfo = tEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +               // 订单ID
                        "`after`['user_id'] user_id," +     // 用户ID
                        "`after`['province_id'] province_id " + // 省份ID
                        "from ods_data_kafka_topic " +
                        "where `source`['table'] ='order_info' ");
        tEnv.createTemporaryView("order_info", orderInfo);

        // 4. 过滤订单活动详情表数据
        Table orderDetailActivity = tEnv.sqlQuery(
                "select " +
                        "`after`['order_detail_id'] order_detail_id, " + // 订单详情ID
                        "`after`['activity_id'] activity_id, " +         // 活动ID
                        "`after`['activity_rule_id'] activity_rule_id " + // 活动规则ID
                        "from ods_data_kafka_topic " +
                        "where `source`['table'] ='order_detail_activity' ");
        tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // 5. 过滤订单优惠券详情表数据
        Table orderDetailCoupon = tEnv.sqlQuery(
                "select " +
                        "`after`['order_detail_id'] order_detail_id, " + // 订单详情ID
                        "`after`['coupon_id'] coupon_id " +              // 优惠券ID
                        "from ods_data_kafka_topic " +
                        "where `source`['table'] ='order_detail_coupon' ");
        tEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        // 6. 四张表关联，整合订单详情信息
        Table result = tEnv.sqlQuery(
                "select " +
                        "od.id," +                          // 订单详情ID
                        "od.order_id," +                    // 订单ID
                        "oi.user_id," +                     // 用户ID
                        "od.sku_id," +                      // SKU ID
                        "od.sku_name," +                    // SKU名称
                        "oi.province_id," +                 // 省份ID
                        "act.activity_id," +                // 活动ID
                        "act.activity_rule_id," +           // 活动规则ID
                        "cou.coupon_id," +                  // 优惠券ID
                        // 格式化日期
                        "date_format(FROM_UNIXTIME(cast(od.create_time as bigint) / 1000), 'yyyy-MM-dd') date_id," +
                        "od.create_time," +                 // 创建时间
                        "od.sku_num," +                     // SKU数量
                        "od.split_original_amount," +       // 原始金额
                        "od.split_activity_amount," +       // 活动优惠金额
                        "od.split_coupon_amount," +         // 优惠券优惠金额
                        "od.split_total_amount," +          // 总金额
                        "od.ts_ms " +                       // 时间戳
                        "from order_detail od " +
                        "join order_info oi on od.order_id=oi.id " +              // 关联订单信息
                        "left join order_detail_activity act " +                  // 左关联活动信息
                        "on od.id=act.order_detail_id " +
                        "left join order_detail_coupon cou " +                    // 左关联优惠券信息
                        "on od.id=cou.order_detail_id ");

        // 7. 创建DWD层订单详情表
        tEnv.executeSql(
                "create table "+DWD_TRADE_ORDER_DETAIL+"(" +
                        "id string," +                      // 订单详情ID
                        "order_id string," +                // 订单ID
                        "user_id string," +                 // 用户ID
                        "sku_id string," +                  // SKU ID
                        "sku_name string," +                // SKU名称
                        "province_id string," +             // 省份ID
                        "activity_id string," +             // 活动ID
                        "activity_rule_id string," +        // 活动规则ID
                        "coupon_id string," +               // 优惠券ID
                        "date_id string," +                 // 日期ID
                        "create_time string," +             // 创建时间
                        "sku_num string," +                 // SKU数量
                        "split_original_amount string," +   // 原始金额
                        "split_activity_amount string," +   // 活动优惠金额
                        "split_coupon_amount string," +     // 优惠券优惠金额
                        "split_total_amount string," +      // 总金额
                        "ts bigint," +                      // 时间戳
                        "primary key(id) not enforced " +    // 主键约束
                        ")" + SqlUtil.getUpsertKafkaDDL(DWD_TRADE_ORDER_DETAIL));

        // 8. 将关联结果插入到DWD层目标表
        result.executeInsert(DWD_TRADE_ORDER_DETAIL);
    }

}