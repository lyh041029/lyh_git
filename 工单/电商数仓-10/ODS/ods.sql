use gmall_09;

drop table if exists ods_shop_page_access_log;
CREATE TABLE if not exists ods_shop_page_access_log
(
    log_id        BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '日志唯一ID',
    user_id       VARCHAR(64) NOT NULL COMMENT '访客ID',
    access_time   DATETIME    NOT NULL COMMENT '访问时间',
    page_id       VARCHAR(64) NOT NULL COMMENT '当前访问页面ID',
    page_name     VARCHAR(128) COMMENT '当前访问页面名称',
    page_type     VARCHAR(32) COMMENT '页面类型（shop/goods/other）',
    refer_page_id VARCHAR(64) COMMENT '来源页面ID',
    device_type   VARCHAR(16) NOT NULL COMMENT '设备类型（wireless/pc）',
    stay_time     INT COMMENT '页面停留时长（秒）',
    is_pay        TINYINT(1) DEFAULT 0 COMMENT '是否下单（0/1）',
    create_time   DATETIME   DEFAULT CURRENT_TIMESTAMP COMMENT '日志生成时间'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '店内路径原始访问日志表';

select * from ods_shop_page_access_log;

