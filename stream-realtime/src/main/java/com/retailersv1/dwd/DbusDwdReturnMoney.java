package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 退款事实表处理类
 * 
 * 该类负责处理用户退款信息，从ODS层读取退款相关数据，
 * 包括退款支付信息、退单信息和订单信息，
 * 通过多表关联整合数据，并关联HBase字典表获取支付类型名称，
 * 最终形成完整的退款事实表写入DWD层。
 */
public class DbusDwdReturnMoney {
    // ODS层Kafka主题配置 - 用于读取原始数据库变更数据
    private static final String TOPIC_DB = ConfigUtils.getString("kafka.cdc.db.topic");

    private static final String DWD_RETURN_MONEY_TOPIC = ConfigUtils.getString("kafka.dwd.return.money.topic");

    public static void main(String[] args) {

        // 创建Flink流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);
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
                ")" + SqlUtil.getKafka(TOPIC_DB, "retailersv_dwd_trade_order_refund"));

        // 2. 创建HBase字典表，用于存储支付类型的字典信息
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code STRING,\n" +                 // 字典码
                " info ROW<dic_name STRING>,\n" +       // 字典信息(包含字典名称)
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" + // 主键约束
                ")"+SqlUtil.getHbaseDDL("dim_base_dic"));

        // 3. 过滤退款成功表数据
        // 条件：表名为refund_payment，操作为更新，更新前退款状态不为空，更新后退款状态为1602(退款成功)
        Table refundPayment = tableEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +               // 退款支付ID
                        "`after`['order_id'] order_id," +   // 订单ID
                        "`after`['sku_id'] sku_id," +       // SKU ID
                        // 对支付类型进行MD5加密处理，作为字典码
                        "MD5(CAST(`after`['payment_type'] AS STRING)) payment_type,\n" +
                        "`after`['callback_time'] callback_time," + // 回调时间
                        "`after`['total_amount'] total_amount," +   // 总金额
                        "proc_time, " +                     // 处理时间
                        "ts_ms " +                          // 时间戳
                        "from ods_professional " +
                        "where  `source`['table'] = 'refund_payment' " +
                        "and `op` = 'u' " +
                        "and `before`['refund_status'] is not null " +
                        "and `after`['refund_status']='1602'");

        tableEnv.createTemporaryView("refund_payment", refundPayment);

        // 4. 过滤退单表中的退单成功的数据
        // 条件：表名为order_refund_info，操作为更新，更新前退款状态不为空，更新后退款状态为0705(退单成功)
        Table orderRefundInfo = tableEnv.sqlQuery(
                "select " +
                        "`after`['order_id'] order_id," +   // 订单ID
                        "`after`['sku_id'] sku_id," +       // SKU ID
                        "`after`['refund_num'] refund_num " + // 退款数量
                        "from ods_professional " +
                        "where  `source`['table'] ='order_refund_info' " +
                        "and `op`='u' " +
                        "and `before`['refund_status'] is not null " +
                        "and `after`['refund_status']='0705'");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 5. 过滤订单表中的退款成功的数据
        // 条件：表名为order_info，操作为更新，更新前订单状态不为空，更新后订单状态为1006(退款完成)
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "`after`['id'] id," +               // 订单ID
                        "`after`['user_id'] user_id," +     // 用户ID
                        "`after`['province_id'] province_id " + // 省份ID
                        "from ods_professional " +
                        "where `source`['table'] ='order_info' " +
                        "and `op`='u' " +
                        "and `before`['order_status'] is not null " +
                        "and `after`['order_status']='1006'");
        tableEnv.createTemporaryView("order_info", orderInfo);

        // 6. 多表关联，整合退款信息
        Table result = tableEnv.sqlQuery(
                "select " +
                        "rp.id," +                          // 退款支付ID
                        "oi.user_id," +                     // 用户ID
                        "rp.order_id," +                    // 订单ID
                        "rp.sku_id," +                      // SKU ID
                        "oi.province_id," +                 // 省份ID
                        "rp.payment_type," +                // 支付类型码(MD5处理后)
                        "dic.info.dic_name payment_type_name," + // 支付类型名称(从字典表获取)
                        // 格式化日期
                        "date_format(FROM_UNIXTIME(cast(rp.callback_time as bigint) / 1000), 'yyyy-MM-dd') date_id," +
                        "rp.callback_time," +                // 回调时间
                        "ori.refund_num," +                  // 退款数量
                        "rp.total_amount," +                 // 总金额
                        "rp.ts_ms " +                       // 时间戳
                        "from refund_payment rp " +
                        "join order_refund_info ori " +
                        "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " + // 关联退单信息
                        "join order_info oi " +
                        "on rp.order_id=oi.id " +            // 关联订单信息
                        // 基于处理时间的时态表关联，确保关联到对应时间点的字典数据
                        "join base_dic for system_time as of rp.proc_time as dic " +
                        "on rp.payment_type=dic.dic_code ");

        // 7. 创建DWD层退款信息表并写出到Kafka
        tableEnv.executeSql("create table "+DWD_RETURN_MONEY_TOPIC+"(" +
                "id string," +                          // 退款支付ID
                "user_id string," +                     // 用户ID
                "order_id string," +                    // 订单ID
                "sku_id string," +                      // SKU ID
                "province_id string," +                 // 省份ID
                "payment_type_code string," +           // 支付类型码
                "payment_type_name string," +           // 支付类型名称
                "date_id string," +                     // 日期ID
                "callback_time string," +               // 回调时间
                "refund_num string," +                  // 退款数量
                "refund_amount string," +               // 退款金额
                "ts bigint, " +                         // 时间戳
                "primary key(id) not enforced " +       // 主键约束
                ")" + SqlUtil.getUpsertKafkaDDL(DWD_RETURN_MONEY_TOPIC));
        
        // 执行插入操作，将结果写入Kafka
        result.executeInsert(DWD_RETURN_MONEY_TOPIC);
    }
}