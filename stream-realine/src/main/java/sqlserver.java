import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
//import com.ververica.cdc.connectors.sqlserver.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class sqlserver {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties debeziumProperties = new Properties();
//        debeziumProperties.put("snapshot.mode","schema_only");
        debeziumProperties.put("snapshot.mode","initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        // 构建SQL Server CDC数据源
        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("cdh03")  // SQL Server主机名
                .port(1433)         // SQL Server端口
                .username("sa")     // 用户名
                .password("Lyh041029") // 密码
                .database("test")   // 数据库名
                .tableList("dbo.test_data") // 监听的表列表
                .startupOptions(StartupOptions.initial()) // 启动模式：从最早的变更日志开始读取
                .debeziumProperties(debeziumProperties)  // Debezium配置属性
                .deserializer(new JsonDebeziumDeserializationSchema()) // 反序列化器
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, "_transaction_log_source1");
        dataStreamSource.print().setParallelism(1);
        env.execute("sqlserver_data_test");
    }
}
