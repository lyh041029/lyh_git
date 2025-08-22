package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 评价事实表处理类
 * 
 * 该类负责处理用户商品评价数据，从ODS层读取评价信息，
 * 并与HBase中的字典表进行关联，获取评价类型的名称，
 * 最终形成包含评价详情和评价类型名称的评价事实表写入DWD层。
 */
public class DbusDwdCommentAndDic {
    // ODS层Kafka主题配置 - 用于读取原始数据库变更数据
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    // DWD层评价信息Kafka主题配置 - 用于写入处理后的评价数据
    private static final String DWD_COMMENT_INFO = ConfigUtils.getString("kafka.dwd.comment.info");

    public static void main(String[] args) {
        // 创建Flink流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置环境参数，如检查点等通用配置
        EnvironmentSettingUtils.defaultParameter(env);

        // 创建表环境，用于执行SQL操作
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 创建ODS层数据表，用于读取原始数据库变更数据
        tEnv.executeSql("CREATE TABLE ods_data_kafka_topic (\n" +
                "  `op` STRING,\n" +                      // 操作类型：c-插入, u-更新, d-删除
                "  `before` MAP<STRING,STRING>,\n" +     // 变更前数据
                "  `after` MAP<STRING,STRING>,\n" +      // 变更后数据
                "  `source` MAP<STRING,STRING>,\n" +     // 数据源信息
                "  `ts_ms` BIGINT,\n" +                  // 时间戳(毫秒)
                "   proc_time AS proctime()" +           // 处理时间
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "test"));

        // 2. 从ODS层数据中过滤出评价信息数据
        Table commentInfo = tEnv.sqlQuery("select\n" +
                "`after`['id'] id,\n" +                 // 评价ID
                "`after`['user_id'] user_id,\n" +       // 用户ID
                "`after`['sku_id'] sku_id,\n" +         // SKU ID
                // 对评价类型进行MD5加密处理，作为字典码
                "MD5(CAST(`after`['appraise'] AS STRING)) appraise,\n" +
                "`after`['comment_txt'] comment_txt,\n" + // 评价内容
                "ts_ms,\n" +                            // 时间戳
                "proc_time\n" +                         // 处理时间
                "from ods_data_kafka_topic\n" +
                "where `source`['table'] = 'comment_info'");

        // 3. 注册评价信息临时视图，供后续关联使用
        tEnv.createTemporaryView("comment_info", commentInfo);

        // 4. 创建HBase字典表，用于存储评价类型的字典信息
        tEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code STRING,\n" +                 // 字典码
                " info ROW<dic_name STRING>,\n" +       // 字典信息(包含字典名称)
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" + // 主键约束
                ")"+SqlUtil.getHbaseDDL("dim_base_dic"));

        // 5. 关联comment_info和base_dic表，获取评价类型的名称
        Table joinTable = tEnv.sqlQuery("SELECT id,\n" +
                "      user_id,\n" +                    // 用户ID
                "      sku_id,\n" +                     // SKU ID
                "      appraise,\n" +                   // 评价类型码(MD5处理后)
                "      dic.info.dic_name appraise_name,\n" + // 评价类型名称(从字典表获取)
                "      comment_txt,\n" +                // 评价内容
                "      ts_ms\n" +                       // 时间戳
                "FROM comment_info AS c\n" +
                // 基于处理时间的时态表关联，确保关联到对应时间点的字典数据
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.proc_time AS dic\n" +
                "    ON c.appraise = dic.dic_code");

        // 6. 创建DWD层评价信息表
        tEnv.executeSql("CREATE TABLE "+DWD_COMMENT_INFO+" (\n" +
                "      id String,\n" +                  // 评价ID
                "      user_id string,\n" +             // 用户ID
                "      sku_id string,\n" +              // SKU ID
                "      appraise CHAR(32),\n" +          // 评价类型码
                "      appraise_name string,\n" +       // 评价类型名称
                "      comment_txt string,\n" +         // 评价内容
                "      ts BIGINT,\n" +                  // 时间戳
                "  PRIMARY KEY (id) NOT ENFORCED\n" +    // 主键约束
                ")"+SqlUtil.getUpsertKafkaDDL(DWD_COMMENT_INFO));

        // 7. 将关联结果插入到DWD层目标表
        joinTable.executeInsert(DWD_COMMENT_INFO);
    }
}