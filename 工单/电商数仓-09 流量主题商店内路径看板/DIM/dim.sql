create database if not exists gmall_09;

use gmall_09;

drop table if exists dim_page_info;
CREATE TABLE if not exists dim_page_info (
    page_id VARCHAR(64) PRIMARY KEY COMMENT '页面ID',
    page_name VARCHAR(128) NOT NULL COMMENT '页面名称',
    page_type VARCHAR(32) NOT NULL COMMENT '页面类型（shop/goods/other）',
    page_category VARCHAR(64) COMMENT '页面细分类型（如shop下的首页、活动页等）',
    is_valid TINYINT(1) DEFAULT 1 COMMENT '是否有效（1=有效，0=无效）',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '页面维度信息表';

INSERT INTO dim_page_info (page_id, page_name, page_type, page_category)
VALUES  ('p1001', '店铺首页', 'shop', '首页'),
        ('p1002', '商品详情页-手机', 'goods', '详情页'),
        ('p1003', '直播页', 'other', '直播页'),
        ('p1004', '活动页-618', 'shop', '活动页');