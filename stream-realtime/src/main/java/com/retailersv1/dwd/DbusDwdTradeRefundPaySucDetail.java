package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbusDwdTradeRefundPaySucDetail {
    // 日志实例（消除SLF4J警告后可正常打印日志）
    private static final Logger LOG = LoggerFactory.getLogger(DbusDwdTradeRefundPaySucDetail.class);

    // 从配置文件读取Kafka Topic（确保配置文件中Topic名含`.`时无需额外处理）
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_TRADE_REFUND_PAY_SUC_DETAIL = ConfigUtils.getString("kafka.dwd.trade.refund.pay.suc.detail");

    public static void main(String[] args) {
        try {
            // 1. 初始化Flink流执行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            // 默认参数配置（如并行度、 checkpoint 等，由工具类封装）
            EnvironmentSettingUtils.defaultParameter(env);

            // 状态后端配置：本地调试用MemoryStateBackend，生产环境建议替换为RocksDB
            env.setStateBackend(new MemoryStateBackend());
            // 生产环境RocksDB状态后端配置（需添加对应依赖，注释可按需启用）
            // import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
            // env.setStateBackend(new RocksDBStateBackend("hdfs:///flink/state/dwd_trade_refund", true));

            // 2. 初始化Table环境（绑定流环境）
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
            LOG.info("Flink Table Environment initialized successfully");

            // 3. 读取Kafka CDC统一源表（ods层）
            String createOdsTableSql = "CREATE TABLE ods_ecommerce_data (\n" +
                    "  `op` STRING COMMENT 'CDC操作类型（c=插入, u=更新, d=删除）',\n" +
                    "  `before` MAP<STRING,STRING> COMMENT '更新前数据',\n" +
                    "  `after` MAP<STRING,STRING> COMMENT '更新后数据',\n" +
                    "  `source` MAP<STRING,STRING> COMMENT 'CDC源信息（含表名、数据库名）',\n" +
                    "  `ts_ms` BIGINT COMMENT 'CDC事件时间戳（毫秒）',\n" +
                    "  proc_time AS proctime() COMMENT '处理时间（用于维表Lookup Join）',\n" +
                    "  et AS to_timestamp_ltz(ts_ms, 3) COMMENT '事件时间（带时区）',\n" +
                    "  WATERMARK FOR et AS et - INTERVAL '3' SECOND COMMENT '水位线（允许3秒延迟）'\n" +
                    ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "first");
            tEnv.executeSql(createOdsTableSql);
            LOG.info("ODS table [ods_ecommerce_data] created successfully");

            // 4. 读取HBase字典维表（base_dic）
            String createBaseDicTableSql = "CREATE TABLE base_dic (\n" +
                    "  dic_code STRING COMMENT '字典编码（如支付方式编码）',\n" +
                    "  info ROW<dic_name STRING> COMMENT '字典详情（含字典名称）',\n" +
                    "  PRIMARY KEY (dic_code) NOT ENFORCED COMMENT 'HBase主键（非强制约束）'\n" +
                    ")" + SqlUtil.getHbaseDDL("dim_base_dic");
            tEnv.executeSql(createBaseDicTableSql);
            LOG.info("HBase dim table [base_dic] created successfully");

            // 5. 过滤退款支付表（refund_payment）的更新数据（退款状态=1602表示成功）
            Table refundPaymentTable = tEnv.sqlQuery("SELECT " +
                    "`after`['id'] AS id COMMENT '退款支付ID'," +
                    "`after`['order_id'] AS order_id COMMENT '订单ID'," +
                    "`after`['sku_id'] AS sku_id COMMENT '商品SKU ID'," +
                    "MD5(CAST(`after`['payment_type'] AS STRING)) AS payment_type COMMENT '支付方式编码（MD5加密）'," +
                    "`after`['callback_time'] AS callback_time COMMENT '支付回调时间'," +
                    "`after`['total_amount'] AS total_amount COMMENT '退款金额（原字段为STRING，与目标表匹配）'," +
                    "proc_time AS pt COMMENT '处理时间（用于关联字典表）'," +
                    "ts_ms AS ts_ms COMMENT '事件时间戳'\n" +
                    "FROM ods_ecommerce_data\n" +
                    "WHERE `source`['table'] = 'refund_payment'  -- 筛选退款支付表\n" +
                    "  AND `op` = 'u'  -- 仅保留更新操作\n" +
                    "  AND `before`['refund_status'] IS NOT NULL  -- 确保更新前有状态（排除初始化数据）\n" +
                    "  AND `after`['refund_status'] = '1602'");  // 退款状态=1602（成功）
            tEnv.createTemporaryView("refund_payment", refundPaymentTable);
            LOG.info("Temporary view [refund_payment] created successfully");

            // 6. 过滤退单表（order_refund_info）的更新数据（退款状态=0705表示成功）
            Table orderRefundInfoTable = tEnv.sqlQuery("SELECT " +
                    "`after`['order_id'] AS order_id COMMENT '订单ID'," +
                    "`after`['sku_id'] AS sku_id COMMENT '商品SKU ID'," +
                    "`after`['refund_num'] AS refund_num COMMENT '退款数量'\n" +
                    "FROM ods_ecommerce_data\n" +
                    "WHERE `source`['table'] = 'order_refund_info'  -- 筛选退单表\n" +
                    "  AND `op` = 'u'  -- 仅保留更新操作\n" +
                    "  AND `before`['refund_status'] IS NOT NULL  -- 确保更新前有状态\n" +
                    "  AND `after`['refund_status'] = '0705'");  // 退单状态=0705（成功）
            tEnv.createTemporaryView("order_refund_info", orderRefundInfoTable);
            LOG.info("Temporary view [order_refund_info] created successfully");

            // 7. 过滤订单表（order_info）的更新数据（订单状态=1006表示退款成功）
            Table orderInfoTable = tEnv.sqlQuery("SELECT " +
                    "`after`['id'] AS id COMMENT '订单ID（与refund_payment.order_id关联）'," +
                    "`after`['user_id'] AS user_id COMMENT '用户ID'," +
                    "`after`['province_id'] AS province_id COMMENT '省份ID'\n" +
                    "FROM ods_ecommerce_data\n" +
                    "WHERE `source`['table'] = 'order_info'  -- 筛选订单表\n" +
                    "  AND `op` = 'u'  -- 仅保留更新操作\n" +
                    "  AND `before`['order_status'] IS NOT NULL  -- 确保更新前有状态\n" +
                    "  AND `after`['order_status'] = '1006'");  // 订单状态=1006（退款成功）
            tEnv.createTemporaryView("order_info", orderInfoTable);
            LOG.info("Temporary view [order_info] created successfully");

            // 8. 四表关联（业务表内连接 + 字典表Lookup Join）
            Table resultTable = tEnv.sqlQuery("SELECT " +
                    "rp.id AS id COMMENT '退款支付ID（主键）'," +
                    "oi.user_id AS user_id COMMENT '用户ID'," +
                    "rp.order_id AS order_id COMMENT '订单ID'," +
                    "rp.sku_id AS sku_id COMMENT '商品SKU ID'," +
                    "oi.province_id AS province_id COMMENT '省份ID'," +
                    "rp.payment_type AS payment_type_code COMMENT '支付方式编码'," +
                    "dic.info.dic_name AS payment_type_name COMMENT '支付方式名称（来自字典表）'," +
                    "DATE_FORMAT(rp.callback_time, 'yyyy-MM-dd') AS date_id COMMENT '日期ID（用于分区）'," +
                    "rp.callback_time AS callback_time COMMENT '支付回调时间'," +
                    "ori.refund_num AS refund_num COMMENT '退款数量'," +
                    "rp.total_amount AS refund_amount COMMENT '退款金额（与目标表字段名匹配）'," +
                    "rp.ts_ms AS ts_ms COMMENT '事件时间戳'\n" +
                    "FROM refund_payment rp\n" +
                    "INNER JOIN order_refund_info ori  -- 退款支付与退单表关联（双字段匹配）\n" +
                    "  ON rp.order_id = ori.order_id AND rp.sku_id = ori.sku_id\n" +
                    "INNER JOIN order_info oi  -- 关联订单表（获取用户、省份信息）\n" +
                    "  ON rp.order_id = oi.id\n" +
                    "LEFT JOIN base_dic FOR SYSTEM_TIME AS OF rp.pt AS dic  -- 维表Lookup Join（处理时间关联）\n" +
                    "  ON rp.payment_type = dic.dic_code");  // 支付方式编码匹配字典编码
            LOG.info("Join query executed successfully, result table ready");

            // 9. 创建DWD层目标表（Upsert-Kafka，表名用反引号包裹以支持含`.`的Topic名）
            // 关键修复：表名 `DWD_TRADE_REFUND_PAY_SUC_DETAIL` 用反引号包裹
            String createDwdTableSql = "CREATE TABLE `" + DWD_TRADE_REFUND_PAY_SUC_DETAIL + "` (\n" +
                    "id STRING COMMENT '退款支付ID（主键）',\n" +
                    "user_id STRING COMMENT '用户ID',\n" +
                    "order_id STRING COMMENT '订单ID',\n" +
                    "sku_id STRING COMMENT '商品SKU ID',\n" +
                    "province_id STRING COMMENT '省份ID',\n" +
                    "payment_type_code STRING COMMENT '支付方式编码',\n" +
                    "payment_type_name STRING COMMENT '支付方式名称',\n" +
                    "date_id STRING COMMENT '日期ID（格式：yyyy-MM-dd）',\n" +
                    "callback_time STRING COMMENT '支付回调时间',\n" +
                    "refund_num STRING COMMENT '退款数量',\n" +
                    "refund_amount STRING COMMENT '退款金额',\n" +
                    "ts_ms BIGINT COMMENT '事件时间戳（毫秒）',\n" +
                    "PRIMARY KEY (id) NOT ENFORCED COMMENT 'Upsert-Kafka主键（用于去重更新）'\n" +
                    ")" + SqlUtil.getUpsertKafkaDDL(DWD_TRADE_REFUND_PAY_SUC_DETAIL);
            tEnv.executeSql(createDwdTableSql);
            LOG.info("DWD table [`" + DWD_TRADE_REFUND_PAY_SUC_DETAIL + "`] created successfully");

            // 10. 将关联结果写入目标表（Upsert-Kafka）
            resultTable.executeInsert(DWD_TRADE_REFUND_PAY_SUC_DETAIL)
                    .print();  // 打印执行结果（成功/失败信息）
            LOG.info("Data written to DWD table [`" + DWD_TRADE_REFUND_PAY_SUC_DETAIL + "`] successfully");

            // 执行Flink作业（若未自动触发，需显式调用）
            env.execute("DbusDwdTradeRefundPaySucDetailJob");

        } catch (Exception e) {
            // 捕获并打印所有异常（便于排查问题）
            LOG.error("Job execution failed: ", e);
            e.printStackTrace();
            System.exit(1);  // 异常退出，避免进程挂起
        }
    }
}