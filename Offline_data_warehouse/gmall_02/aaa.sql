create table if not exists bigdata_car_analysis_data_ws.ods_mapping_kf_traffic_car_info_dtl(
                                                                                               id bigint,
                                                                                               msid varchar(255),
    datatype bigint,
    data varchar(255),
    upexgmsgregister json,
    vehicleno varchar(255),
    datalen bigint,
    vehiclecolor bigint,
    vec1 bigint,
    vec2 bigint,
    vec3 bigint,
    encrypy bigint,
    altitude bigint,
    alarm json,
    state json,
    lon double,
    lat double,
    msgId bigint,
    direction bigint,
    dateTime varchar(255),
    ds date,
    ts bigint
    )
    engine=OLAP
    DUPLICATE KEY(`id`)
    PARTITION BY RANGE (ds)()
    DISTRIBUTED BY HASH(`vehicleno`) BUCKETS 16
    PROPERTIES (
                   "dynamic_partition.enable" = "true",
                   "dynamic_partition.time_unit" = "DAY",
                   "dynamic_partition.start" = "-180",
                   "dynamic_partition.end" = "30",
                   "dynamic_partition.prefix" = "p",
                   "dynamic_partition.buckets" = "16",
                   "dynamic_partition.create_history_partition" = "true",
                   "replication_num" = "1"
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