create database if not exists gmall_09;

use gmall_09;


drop table if exists dwd_shop_page_access_detail;
CREATE TABLE if not exists dwd_shop_page_access_detail (
    log_id BIGINT PRIMARY KEY COMMENT '日志唯一ID',
    user_id VARCHAR(64) NOT NULL COMMENT '访客ID',
    access_time DATETIME NOT NULL COMMENT '访问时间',
    page_id VARCHAR(64) NOT NULL COMMENT '当前访问页面ID',
    page_name VARCHAR(128) COMMENT '当前访问页面名称',
    page_type VARCHAR(32) COMMENT '页面类型（shop/goods/other）',
    refer_page_id VARCHAR(64) COMMENT '来源页面ID（空值标记为"direct"）',
    device_type VARCHAR(16) NOT NULL COMMENT '设备类型（wireless/pc）',
    stay_time INT DEFAULT 0 COMMENT '页面停留时长（秒，异常值修正为0）',
    is_pay TINYINT(1) DEFAULT 0 COMMENT '是否下单（0/1）',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '日志生成时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '清洗后的页面访问明细表';

INSERT INTO dwd_shop_page_access_detail
SELECT
    log_id,
    user_id,
    access_time,
    page_id,
    page_name,
    page_type,
    IF(refer_page_id IS NULL, 'direct', refer_page_id) AS refer_page_id,
    device_type,
    IF(stay_time < 0, 0, stay_time) AS stay_time,
    is_pay,
    create_time
FROM ods_shop_page_access_log;

select *
from dwd_shop_page_access_detail;