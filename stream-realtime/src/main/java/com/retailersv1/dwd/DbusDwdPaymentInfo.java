package com.retailersv1.dwd;

import com.ibm.icu.impl.number.parse.RequireNumberValidator;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.bouncycastle.math.ec.WNafL2RMultiplier;

/**
 * 支付事实表处理类
 * 
 * 该类负责处理用户支付信息，从ODS层读取支付相关信息，
 * 并与订单详情表进行关联，获取完整的支付信息，
 * 同时关联HBase字典表获取支付类型名称，
 * 最终形成包含支付详情的支付事实表写入DWD层。
 */
public class DbusDwdPaymentInfo {
    // ODS层Kafka主题配置 - 用于读取原始数据库变更数据
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    // DWD层订单详情Kafka主题配置 - 用于读取订单详情数据
    private static final String DWD_ORDER_TOPIC = ConfigUtils.getString("kafka.dwd.trade.order.detail");
    // DWD层支付信息Kafka主题配置 - 用于写入处理后的支付信息数据
    private static final String DWD_PYAMENTINFO_TOPIC = ConfigUtils.getString("kafka.dwd.payment.info");

    public static void main(String[] args) throws Exception {
        // 创建Flink流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为4
        env.setParallelism(4);

        // 设置环境参数，如检查点等通用配置
        EnvironmentSettingUtils.defaultParameter(env);

        // 创建表环境，用于执行SQL操作
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置状态后端为MemoryStateBackend
        env.setStateBackend(new MemoryStateBackend());

        // 1. 创建ODS层专业数据表，用于读取原始数据库变更数据
        tableEnv.executeSql("CREATE TABLE ods_professional (\n" +
                "  `op` STRING,\n" +                      // 操作类型：c-插入, u-更新, d-删除
                "  `before` MAP<STRING,STRING>,\n" +     // 变更前数据
                "  `after` MAP<STRING,STRING>,\n" +      // 变更后数据
                "  `source` MAP<STRING,STRING>,\n" +     // 数据源信息
                "  `ts_ms` BIGINT,\n" +                  // 时间戳(毫秒)
                "   proc_time AS proctime()," +          // 处理时间
                " time_ltz AS TO_TIMESTAMP_LTZ(ts_ms, 3),\n" + // 本地时间戳
                " WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND" + // 水位线定义，用于事件时间处理
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "test"));
        
        // 2. 创建DWD层订单信息表，用于读取订单详情数据
        tableEnv.executeSql(
                "create table dwd_order_info(" +
                        "id string," +                   // 订单详情ID
                        "order_id string," +             // 订单ID
                        "user_id string," +              // 用户ID
                        "sku_id string," +               // SKU ID
                        "sku_name string," +             // SKU名称
                        "province_id string," +          // 省份ID
                        "activity_id string," +          // 活动ID
                        "activity_rule_id string," +     // 活动规则ID
                        "coupon_id string," +            // 优惠券ID
                        "date_id string," +              // 日期ID
                        "create_time string," +          // 创建时间
                        "sku_num string," +              // SKU数量
                        "split_original_amount string," + // 原始金额
                        "split_activity_amount string," + // 活动优惠金额
                        "split_coupon_amount string," +  // 优惠券优惠金额
                        "split_total_amount string," +   // 总金额
                        "ts bigint ," +                  // 时间戳
                        " time_ltz AS TO_TIMESTAMP_LTZ(ts, 3),\n" + // 本地时间戳
                        " WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND" + // 水位线定义
                        ")" + SqlUtil.getKafka(DWD_ORDER_TOPIC, "test"));

        //tableEnv.executeSql("select * from dwd_order_info").print();


        // 3. 从ODS层数据中过滤出支付成功的支付信息数据
        // 条件：表名为payment_info，操作为更新，支付状态为1602(支付成功)
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "`after`['user_id'] user_id," +         // 用户ID
                "`after`['order_id'] order_id," +       // 订单ID
                // 对支付类型进行MD5加密处理，作为字典码
                "MD5(CAST(`after`['payment_type'] AS STRING)) payment_type,\n" +
                "`after`['callback_time'] callback_time," + // 回调时间
                "`proc_time`," +                        // 处理时间
                "ts_ms, " +                             // 时间戳(毫秒)
                "time_ltz " +                           // 本地时间戳
                "from ods_professional " +
                "where `source`['table'] = 'payment_info' " +
                "and `op` = 'u' " +
                "and `after`['payment_status']='1602' ");
        tableEnv.createTemporaryView("payment_info", paymentInfo);

        // 4. 创建HBase字典表，用于存储支付类型的字典信息
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code STRING,\n" +                 // 字典码
                " info ROW<dic_name STRING>,\n" +       // 字典信息(包含字典名称)
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" + // 主键约束
                ")"+SqlUtil.getHbaseDDL("dim_base_dic"));

        // 5. 关联支付信息、订单信息和字典表，获取完整的支付信息
        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +       // 订单详情ID
                        "od.order_id," +                 // 订单ID
                        "od.user_id," +                  // 用户ID
                        "od.sku_id," +                   // SKU ID
                        "od.sku_name," +                 // SKU名称
                        "od.province_id," +              // 省份ID
                        "od.activity_id," +              // 活动ID
                        "od.activity_rule_id," +         // 活动规则ID
                        "od.coupon_id," +                // 优惠券ID
                        "pi.payment_type payment_type_code ," + // 支付类型码(MD5处理后)
                        "dic.dic_name payment_type_name," + // 支付类型名称(从字典表获取)
                        "pi.callback_time," +            // 回调时间
                        "od.sku_num," +                  // SKU数量
                        "od.split_original_amount," +    // 原始金额
                        "od.split_activity_amount," +    // 活动优惠金额
                        "od.split_coupon_amount," +      // 优惠券优惠金额
                        "od.split_total_amount split_payment_amount," + // 支付金额(即总金额)
                        "pi.ts_ms " +                    // 时间戳
                        "from payment_info pi " +
                        "join dwd_order_info od " +
                        "on pi.order_id=od.order_id " +
                        // 基于事件时间的窗口关联，确保在5秒时间窗口内关联到正确的订单信息
                        "and od.time_ltz >= pi.time_ltz - interval '5' seconds " +
                        "and od.time_ltz <= pi.time_ltz + interval '5' seconds " +
                        // 基于处理时间的时态表关联，确保关联到对应时间点的字典数据
                        "join base_dic for system_time as of pi.proc_time as dic " +
                        "on pi.payment_type=dic.dic_code ");



        // 6. 创建DWD层支付信息表
        tableEnv.executeSql("create table "+DWD_PYAMENTINFO_TOPIC+"(" +
                "order_detail_id string," +             // 订单详情ID
                "order_id string," +                    // 订单ID
                "user_id string," +                     // 用户ID
                "sku_id string," +                      // SKU ID
                "sku_name string," +                    // SKU名称
                "province_id string," +                 // 省份ID
                "activity_id string," +                 // 活动ID
                "activity_rule_id string," +            // 活动规则ID
                "coupon_id string," +                   // 优惠券ID
                "payment_type_code string," +           // 支付类型码
                "payment_type_name string," +           // 支付类型名称
                "callback_time string," +               // 回调时间
                "sku_num string," +                     // SKU数量
                "split_original_amount string," +       // 原始金额
                "split_activity_amount string," +        // 活动优惠金额
                "split_coupon_amount string," +         // 优惠券优惠金额
                "split_payment_amount string," +        // 支付金额
                "ts bigint ," +                         // 时间戳
                "primary key(order_detail_id) not enforced " + // 主键约束
                ")" + SqlUtil.getUpsertKafkaDDL(DWD_PYAMENTINFO_TOPIC));

        // 7. 将关联结果插入到DWD层目标表
        result.executeInsert(DWD_PYAMENTINFO_TOPIC);


    }
}