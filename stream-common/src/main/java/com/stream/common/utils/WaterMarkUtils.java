package com.stream.common.utils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;
import java.util.List;

/**
 * @BelongsProject: lyh_git
 * @BelongsPackage: com.stream.common.utils
 * @Author: liyuhuan
 * @CreateTime: 2025-08-15  15:20
 * @Description: TODO
 * @Version: 1.0
 */
public class WaterMarkUtils {
    public static WatermarkStrategy<JSONObject> getEthWarnWaterMark(long durationSeconds) {
        return WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(durationSeconds))
                .withTimestampAssigner((record, ts) -> {
                    long time;
                    time = record.containsKey("block_timestamp") ? record.getLong("block_timestamp") : record.getLong("timestamp");
                    return time * 1000;
                });
    }

    public static WatermarkStrategy<List<JSONObject>> getEthLiquidityWaterMark(long durationSeconds) {
        return WatermarkStrategy
                .<List<JSONObject>>forBoundedOutOfOrderness(Duration.ofSeconds(durationSeconds))
                .withTimestampAssigner((list, ts) -> {
                    JSONObject record = list.get(0);
                    return record.getLong("window_start_time");
                });
    }

    public static WatermarkStrategy<String> publicAssignWatermarkStrategy(String timestampField, long maxOutOfOrderlessSeconds) {
        return WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(maxOutOfOrderlessSeconds))
                .withTimestampAssigner((event, timestamp) -> {
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(event);
                        if (event != null && jsonObject.containsKey(timestampField)) {
                            return jsonObject.getLong(timestampField);
                        }
                        return 0L;
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.err.println("Failed to parse event or get field '" + timestampField + "': " + event);
                        return 0L;
                    }
                });
    }
}
