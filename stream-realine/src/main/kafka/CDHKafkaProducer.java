package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 优化后的Kafka生产者：确保发送的数据符合Doris导入要求，增强健壮性和可维护性
 */
public class CDHKafkaProducer {
    // 配置参数：提取为常量，方便集中管理
    private static final String TOPIC_NAME = "realtime_v3_traffic_origin_data_info";
    private static final String BOOTSTRAP_SERVERS = "cdh01:9092";
    private static final boolean ENABLE_KERBEROS = false;
    private static final String JAAS_CONFIG_PATH = "/absolute/path/to/kafka_client_jaas.conf";
    // 补充Doris必填字段的默认日期（与数据时间匹配）
    private static final String DEFAULT_DS = "20250730";
    private static final long DEFAULT_TS = 1753849179000L;

    public static void main(String[] args) {
        // 1. 验证Kerberos配置（仅启用时）
        if (ENABLE_KERBEROS) {
            if (JAAS_CONFIG_PATH == null || JAAS_CONFIG_PATH.trim().isEmpty() || JAAS_CONFIG_PATH.equals("/absolute/path/to/kafka_client_jaas.conf")) {
                System.err.println("错误：Kerberos启用时，必须设置正确的JAAS配置文件路径");
                return;
            }
            System.setProperty("java.security.auth.login.config", JAAS_CONFIG_PATH);
        }

        // 2. 创建生产者配置（优化配置项，增加超时和压缩配置）
        Properties props = getKafkaProperties();

        // 3. 待发送的JSON数据（补充缺失的ds、ts字段，确保Doris分区和映射正常）
        String jsonData1 = "{\"id\":808540,\"msid\":\"b17f479bcb9544b59d2b7ff23c56c8fb\",\"datatype\":4609,\"data\":\"NDQwMzA1MDEAAAAzMDMwMzAzMDMwM0Q1WC1YUiA0LjBcMAAAAAAAAAAAMDExNjgwMzAwMDAwMDAwMDAwMA==\",\"upexgmsgregister\":{\"msgId\":4609,\"platformId\":\"44030501\",\"producerId\":\"30303030303\",\"terminalId\":\".0\\\\0\",\"terminalSimCode\":\"0116803\",\"terminalModelType\":\"D5X-XR 4\"},\"vehicleno\":\"粤BJJ312\",\"datalen\":61,\"vehiclecolor\":2,\"ds\":\"" + DEFAULT_DS + "\",\"ts\":" + DEFAULT_TS + "}";
        String jsonData2 = "{\"id\":808537,\"msid\":\"ddd59bf95e824ccdb16e89db30bb168e8\",\"datatype\":4610,\"data\":\"AB4HB+kMEycGyHWfAVsnuAAjACYAAKydAGAABgAMAAMAAAAA\",\"vehicleno\":\"粤BML606,\",\"datalen\":36,\"vehiclecolor\":2,\"vec1\":35,\"vec2\":38,\"vec3\":44189,\"encrypy\":0,\"altitude\":6,\"alarm\":{\"stolen\":0,\"oilError\":0,\"vssError\":0,\"inOutArea\":0,\"overSpeed\":0,\"inOutRoute\":0,\"cameraError\":0,\"illegalMove\":0,\"stopTimeout\":0,\"earlyWarning\":0,\"icModuleError\":0,\"emergencyAlarm\":0,\"fatigueDriving\":0,\"ttsModuleError\":0,\"gnssModuleError\":0,\"illegalIgnition\":0,\"rolloverWarning\":0,\"overspeedWarning\":0,\"terminalLcdError\":0,\"collisionRollover\":0,\"laneDepartureError\":0,\"roadDrivingTimeout\":0,\"banOnDrivingWarning\":0,\"driverFatigueMonitor\":0,\"gnssAntennaDisconnect\":0,\"gnssAntennaShortCircuit\":0,\"cumulativeDrivingTimeout\":0,\"terminalMainPowerFailure\":0,\"terminalMainPowerUnderVoltage\":0},\"state\":{\"acc\":0,\"lat\":0,\"lon\":0,\"door\":1,\"oilPath\":0,\"location\":0,\"operation\":0,\"loadRating\":0,\"electricCircuit\":0,\"latLonEncryption\":0,\"laneDepartureWarning\":0,\"forwardCollisionWarning\":0},\"lon\":113.800607,\"lat\":22.75116,\"msgId\":4610,\"direction\":96,\"dateTime\":\"20250730121939\",\"ds\":\"20250730\",\"ts\":1753849179000}";

        // 4. 发送消息（使用try-with-resources自动关闭生产者，增加同步确认）
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            // 发送第一条数据（同步确认，确保发送结果）
            sendWithConfirmation(producer, TOPIC_NAME, jsonData1, "第一条");
            // 发送第二条数据
            sendWithConfirmation(producer, TOPIC_NAME, jsonData2, "第二条");

            producer.flush();
            System.out.println("所有数据已成功提交至Kafka，等待Doris消费");
        } catch (Exception e) {
            System.err.println("数据发送失败：" + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 带确认的消息发送方法（同步获取发送结果，便于排查问题）
     */
    private static void sendWithConfirmation(Producer<String, String> producer, String topic, String data, String label) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, data);
        RecordMetadata metadata = producer.send(record).get(); // 同步等待结果
        System.out.println(label + "数据发送成功 - 主题: " + metadata.topic() + ", 分区: " + metadata.partition() + ", 偏移量: " + metadata.offset());
    }

    /**
     * 优化Kafka生产者配置（增加超时、压缩、重试间隔等参数）
     */
    private static Properties getKafkaProperties() {
        Properties props = new Properties();
        // 基础配置
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 可靠性配置
        props.put("acks", "all"); // 所有副本确认后才算成功
        props.put("retries", 3); // 重试次数
        props.put("retry.backoff.ms", 1000); // 重试间隔（毫秒）

        // 性能优化
        props.put("batch.size", 16384); // 批量发送大小（16KB）
        props.put("linger.ms", 5); // 等待5ms合并批次
        props.put("buffer.memory", 33554432); // 缓冲区大小（32MB）
        props.put("compression.type", "snappy"); // 启用snappy压缩，减少网络传输

        // 超时配置
        props.put("request.timeout.ms", 30000); // 请求超时
        props.put("delivery.timeout.ms", 60000); // 发送总超时

        // Kerberos安全配置（按需启用）
        if (ENABLE_KERBEROS) {
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.mechanism", "GSSAPI");
            props.put("sasl.kerberos.service.name", "kafka");
        }
        return props;
    }
}