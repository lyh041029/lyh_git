package com.stream.common.utils;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
/**
 * @BelongsProject: lyh_git
 * @BelongsPackage: com.stream.common.utils
 * @Author: liyuhuan
 * @CreateTime: 2025-08-15  09:45
 * @Description: TODO
 * @Version: 1.0
 */
public class CustomStringSerializationSchema implements KafkaSerializationSchema<String>{
    private final String topic;

    public CustomStringSerializationSchema(String topic) {
        this.topic = topic;
    }
    @Override
    public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
        return new ProducerRecord<>(topic, null, s.getBytes(StandardCharsets.UTF_8));
    }
}
