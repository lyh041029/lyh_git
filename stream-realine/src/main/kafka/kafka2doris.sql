-- 创建数据库（若不存在）
CREATE DATABASE IF NOT EXISTS bigdata_car_analysis_data_ws;
USE bigdata_car_analysis_data_ws;

-- 创建车辆交通信息明细表
DROP TABLE IF EXISTS ods_mapping_kf_traffic_car_info_dtl;
CREATE TABLE IF NOT EXISTS ods_mapping_kf_traffic_car_info_dtl (
           id               BIGINT COMMENT "记录唯一ID（主键）",
           msid             VARCHAR(255) COMMENT "消息全局唯一标识",
           datatype         BIGINT COMMENT "数据类型编码（如4609、4610）",
           data             VARCHAR(255) COMMENT "原始数据内容（Base64编码）",
           upexgmsgregister JSON COMMENT "设备注册信息（嵌套JSON）",
           vehicleno        VARCHAR(20) COMMENT "车牌号（如粤BJJ312）",
           datalen          BIGINT COMMENT "data字段的长度（字节数）",
           vehiclecolor     BIGINT COMMENT "车辆颜色编码（2=黑色）",
           vec1             BIGINT COMMENT "预留字段1",
           vec2             BIGINT COMMENT "预留字段2",
           vec3             BIGINT COMMENT "预留字段3",
           encrypy          BIGINT COMMENT "数据加密方式（0=未加密）",
           altitude         BIGINT COMMENT "海拔高度（米）",
           alarm            JSON COMMENT "报警状态集合",
           state            JSON COMMENT "车辆状态集合",
           lon              DOUBLE COMMENT "经度坐标",
           lat              DOUBLE COMMENT "纬度坐标",
           msgId            BIGINT COMMENT "消息类型ID",
           direction        BIGINT COMMENT "行驶方向（度，0-360）",
           dateTime         VARCHAR(14) COMMENT "数据产生时间（yyyyMMddHHmmss）",
           ds               DATE COMMENT "分区日期（yyyyMMdd）",
           ts               BIGINT COMMENT "时间戳（毫秒级）"
) ENGINE = OLAP
    DUPLICATE KEY(`id`)
PARTITION BY RANGE (ds) ()
DISTRIBUTED BY HASH(`vehicleno`) BUCKETS 16
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-180",
    "dynamic_partition.end" = "30",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16",
    "dynamic_partition.create_history_partition" = "true",
    "replication_num" = "2",
    "storage_format" = "V2"
);

CREATE ROUTINE LOAD bigdata_car_analysis_data_ws.ods_mapping_kf_traffic_car_info_dtl_load ON ods_mapping_kf_traffic_car_info_dtl
COLUMNS(
    id, msid, datatype, data, vehicleno, datalen, vehiclecolor,
    vec1, vec2, vec3, encrypy, altitude, alarm, state, lon, lat,
    msgId, direction, dateTime,
    ds,
    ts
)
PROPERTIES
(
    "format" = "json",
    "strict_mode" = "false",
    "max_batch_rows" = "300000",
    "max_batch_interval" = "30",
    "jsonpaths" = "[\"$.id\",\"$.msid\",\"$.datatype\",\"$.data\",\"$.vehicleno\",
                  \"$.datalen\",\"$.vehiclecolor\",\"$.vec1\",\"$.vec2\",\"$.vec3\",
                  \"$.encrypy\",\"$.altitude\",\"$.alarm\",\"$.state\",\"$.lon\",
                  \"$.lat\",\"$.msgId\",\"$.direction\",\"$.dateTime\",\"$.ds\",\"$.ts\"]"
)
FROM KAFKA
(
    "kafka_broker_list" = "cdh01:9092,cdh01:9092,cdh01:9092",
    "kafka_topic" = "realtime_v3_traffic_origin_data_info",
    "property.group.id" = "doris_consumer_group_ods_mapping_kf_traffic_car_info_dtl_load_1",
    "property.offset" = "earliest",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);