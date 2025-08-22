package com.retailersv1.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 加购事实表处理类
 *
 * 该类负责处理用户将商品加入购物车的行为数据，从ODS层读取购物车信息变更数据，
 * 过滤出有效的加购操作（包括新增和增加数量的操作），并生成加购事实表写入DWD层。
 */
public class DbusDwdCartAdd {
    // ODS层Kafka主题配置 - 用于读取原始数据库变更数据
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    // DWD层购物车信息Kafka主题配置 - 用于写入处理后的加购数据
    private static final String DWD_CART_INFO = ConfigUtils.getString("kafka.dwd.cart.info");

    public static void main(String[] args) {
        // 创建Flink流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置环境参数，如检查点等通用配置
        EnvironmentSettingUtils.defaultParameter(env);

        // 创建表环境，用于执行SQL操作
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 创建ODS层数据表，用于读取原始数据库变更数据
        tEnv.executeSql("CREATE TABLE ods_data_kafka_topic (\n" +
                "  `op` STRING,\n" +                      // 操作类型：c-插入, u-更新, d-删除, r-读取
                "  `before` MAP<STRING,STRING>,\n" +     // 变更前数据
                "  `after` MAP<STRING,STRING>,\n" +      // 变更后数据
                "  `source` MAP<STRING,STRING>,\n" +     // 数据源信息
                "  `ts_ms` BIGINT,\n" +                  // 时间戳(毫秒)
                "   proc_time AS proctime()" +           // 处理时间
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "test"));

        // 2. 从ODS层数据中过滤出有效的加购操作
        // 条件：表名为cart_info，操作为插入或者更新（且数量增加）
        Table cartInfo = tEnv.sqlQuery("select\n" +
                "`after`['id'] id,\n" +                 // 购物车记录ID
                "`after`['user_id'] user_id,\n" +       // 用户ID
                "`after`['sku_id'] sku_id,\n" +         // SKU ID
                // 计算加购数量：
                // - 如果是读取操作(r)，加购数量为当前sku_num
                // - 如果是更新操作(u)，加购数量为after['sku_num'] - before['sku_num']的差值
                " if(`op` = 'r',cast(`after`['sku_num'] as int),(cast(`after`['sku_num'] as int) - cast(`before`['sku_num'] as int))) sku_num,\n" +
                "ts_ms\n" +                             // 时间戳
                "from ods_data_kafka_topic\n" +
                "where `source`['table'] = 'cart_info'\n" +
                "and (\n" +
                "(`op` = 'r')\n" +                      // 读取操作（新加入购物车）
                "or\n" +
                "(`op` = 'u' and `before`['sku_num'] is not null and (cast(`after`['sku_num'] as int) > cast(`before`['sku_num'] as int)) )\n" +
                ")");                                   // 更新操作且商品数量增加

        // 3. 创建DWD层购物车信息表并写入数据
        tEnv.executeSql("CREATE TABLE "+DWD_CART_INFO+" (\n" +
                "      id String,\n" +                  // 购物车记录ID
                "      user_id string,\n" +             // 用户ID
                "      sku_id string,\n" +              // SKU ID
                "      sku_num INT,\n" +                // 加购数量
                "      ts BIGINT,\n" +                  // 时间戳
                "  PRIMARY KEY (id) NOT ENFORCED\n" +    // 主键约束
                ")"+SqlUtil.getUpsertKafkaDDL(DWD_CART_INFO));
        
        // 执行插入操作，将处理后的加购数据写入DWD层
        cartInfo.executeInsert(DWD_CART_INFO);
    }
}
