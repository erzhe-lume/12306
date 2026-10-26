-- ============================================================
-- 候补订单功能 DDL（二次开发新增模块）
-- 归属域：ticket（候补分配需要协调票务资源）
-- 沿用库：12306_ticket
-- 表说明：
--   t_candidate_order       候补订单主表（一张候补单对应多个乘车人）
--   t_candidate_order_item  候补订单明细表（乘车人维度，记录座位分配结果）
-- 设计说明：
--   - 候补队列查询走 idx_candidate_queue 复合索引
--     (train_id, departure, arrival, status, create_time, candidate_order_sn)
--     覆盖「按车次+区间+状态查待分配候补单、按时间排序」的核心场景
--   - candidate_order_sn 唯一约束，防止重复下单
--   - formal_order_sn 候补成功后回写正式订单号，串联正式订单链路
-- ============================================================

USE 12306_ticket;

CREATE TABLE IF NOT EXISTS t_candidate_order (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
    candidate_order_sn VARCHAR(64) NOT NULL COMMENT '候补订单号',
    formal_order_sn VARCHAR(64) DEFAULT NULL COMMENT '正式订单号',
    user_id VARCHAR(64) NOT NULL COMMENT '用户ID',
    username VARCHAR(256) NOT NULL COMMENT '用户名',
    train_id BIGINT NOT NULL COMMENT '车次ID',
    departure VARCHAR(64) NOT NULL COMMENT '出发站',
    arrival VARCHAR(64) NOT NULL COMMENT '到达站',
    status INT NOT NULL DEFAULT 0 COMMENT '候补状态：WAITING',
    allocate_time DATETIME DEFAULT NULL COMMENT '开始分配时间',
    payment_deadline DATETIME DEFAULT NULL COMMENT '支付截止时间',
    fail_reason VARCHAR(255) DEFAULT NULL COMMENT '失败原因',
    create_time DATETIME DEFAULT NULL COMMENT '创建时间',
    update_time DATETIME DEFAULT NULL COMMENT '更新时间',
    del_flag INT NOT NULL DEFAULT 0 COMMENT '逻辑删除标记',
    PRIMARY KEY (id),
    UNIQUE KEY uk_candidate_order_sn (candidate_order_sn),
    KEY idx_candidate_queue (
        train_id,
        departure,
        arrival,
        status,
        create_time,
        candidate_order_sn
    )
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT = '候补订单表';

CREATE TABLE IF NOT EXISTS t_candidate_order_item (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
    candidate_order_sn VARCHAR(64) NOT NULL COMMENT '候补订单号',
    passenger_id VARCHAR(64) NOT NULL COMMENT '乘车人ID',
    real_name VARCHAR(64) DEFAULT NULL COMMENT '真实姓名',
    id_type INT DEFAULT NULL COMMENT '证件类型',
    id_card VARCHAR(64) DEFAULT NULL COMMENT '证件号',
    phone VARCHAR(32) DEFAULT NULL COMMENT '手机号',
    seat_type INT NOT NULL COMMENT '座位类型',
    ticket_type INT DEFAULT NULL COMMENT '车票类型',
    carriage_number VARCHAR(32) DEFAULT NULL COMMENT '车厢号',
    seat_number VARCHAR(32) DEFAULT NULL COMMENT '座位号',
    amount INT DEFAULT NULL COMMENT '票价',
    status INT DEFAULT NULL COMMENT '明细状态',
    create_time DATETIME DEFAULT NULL COMMENT '创建时间',
    update_time DATETIME DEFAULT NULL COMMENT '更新时间',
    del_flag INT NOT NULL DEFAULT 0 COMMENT '逻辑删除标记',
    PRIMARY KEY (id),
    KEY idx_candidate_order_sn (candidate_order_sn)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT = '候补订单明细表';
