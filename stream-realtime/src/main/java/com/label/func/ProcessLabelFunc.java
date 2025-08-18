package com.label.func;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.DateTimeUtils;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * @BelongsProject: lyh_git
 * @BelongsPackage: com.label.func
 * @Author: liyuhuan
 * @CreateTime: 2025-08-18  19:05
 * @Description: TODO
 * @Version: 1.0
 */
public class ProcessLabelFunc extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
    @Override
    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

        left.putAll(right);
        left.put("ds", DateTimeUtils.format(new Date(left.getLong("ts_ms")), "yyyyMMdd"));
        collector.collect(left);

    }
}
