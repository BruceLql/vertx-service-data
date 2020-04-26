# 运营商-用户基本信息表

```mysql
CREATE TABLE `carrier_baseinfo` (
  `id` int(11) unsigned zerofill NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `task_id` varchar(32) DEFAULT NULL COMMENT '任务Id',
  `name` varchar(32) DEFAULT NULL COMMENT 'name 用户姓名',
  `mobile` varchar(11) DEFAULT NULL COMMENT 'mobile 用户手机号',
  `real_name_info` varchar(32) DEFAULT NULL COMMENT '用户认证状态',
  `user_lever` varchar(32) DEFAULT NULL COMMENT '用户级别',
  `brand` varchar(255) DEFAULT NULL COMMENT '所属品牌',
  `package_name` varchar(255) DEFAULT NULL COMMENT 'packageName 套餐名称',
  `in_net_date` date DEFAULT NULL COMMENT '入网时间',
  `net_age` varchar(32) DEFAULT NULL COMMENT '网龄',
  `level` varchar(32) DEFAULT NULL COMMENT '星级水平（仅移动有）',
  `star_score` varchar(32) DEFAULT NULL COMMENT '星级得分（仅移动有）',
  `user_email` varchar(32) DEFAULT NULL COMMENT '电子邮箱',
  `zip_code` varchar(32) DEFAULT NULL COMMENT '邮政编码',
  `user_address` varchar(255) DEFAULT NULL COMMENT '联系地址',
  `idcard` varchar(32) DEFAULT NULL COMMENT '身份证号码',
  `carrier` varchar(32) DEFAULT NULL COMMENT '运营商类型：移动、联通、电信',
  `province` varchar(32) DEFAULT NULL COMMENT 'province 省份',
  `city` varchar(32) DEFAULT NULL COMMENT 'city 城市',
  `state` varchar(32) DEFAULT NULL COMMENT 'state 账户状态',
  `reliability` varchar(32) DEFAULT NULL COMMENT 'reliability 实名认证',
  `carrier_001` varchar(255) DEFAULT NULL COMMENT '预留1',
  `carrier_002` varchar(255) DEFAULT NULL COMMENT '预留2',
  `created_at` bigint(13) DEFAULT NULL COMMENT '创建时间',
  `updated_at` bigint(13) DEFAULT NULL COMMENT '修改时间',
  `deleted_at` bigint(13) DEFAULT NULL COMMENT '0：正常  时间：表示删除时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='用户基本信息表';
```


# 通话记录表

```mysql
CREATE TABLE `carrier_voicecall` (
  `id` int(11) unsigned zerofill NOT NULL AUTO_INCREMENT COMMENT 'id',
  `task_id` varchar(32) DEFAULT NULL COMMENT '创建任务时的monoId ',
  `mobile` varchar(32) DEFAULT NULL COMMENT '手机号码',
  `bill_month` varchar(32) DEFAULT NULL COMMENT '账单月',
  `time` varchar(32) DEFAULT NULL COMMENT '通话时间',
  `peer_number` varchar(32) DEFAULT NULL COMMENT '对方号码',
  `location` varchar(32) DEFAULT NULL COMMENT '通话地(自己的)',
  `location_type` varchar(32) DEFAULT NULL COMMENT '通话地类型. e.g.省内漫游',
  `duration_in_second` int(9) DEFAULT NULL COMMENT '通话时长(单位秒)',
  `dial_type` varchar(32) DEFAULT NULL COMMENT 'DIAL-主叫; DIALED-被叫',
  `fee` int(11) DEFAULT NULL COMMENT '通话费(单位分)',
  `homearea` varchar(32) DEFAULT NULL COMMENT '对方归属地',
  `carrier_001` varchar(255) DEFAULT NULL COMMENT '预留1',
  `carrier_002` varchar(255) DEFAULT NULL COMMENT '预留2',
  
 `created_at` bigint(13) DEFAULT NULL COMMENT '创建时间',
  `updated_at` bigint(13) DEFAULT NULL COMMENT '修改时间',
  `deleted_at` bigint(13) DEFAULT NULL COMMENT '0：正常  时间：表示删除时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='手机通话详情';
```


# 短信记录表


```mysql
CREATE TABLE `carrier_sms` (
  `id` int(11) unsigned zerofill NOT NULL AUTO_INCREMENT COMMENT 'id',
  `task_id` varchar(32) DEFAULT NULL COMMENT '创建任务时的mongoId',
  `mobile` varchar(32) DEFAULT NULL COMMENT '手机号码',
  `bill_month` varchar(32) DEFAULT NULL COMMENT '账单月',
  `time` varchar(32) DEFAULT NULL COMMENT '收/发短信时间',
  `peer_number` varchar(32) DEFAULT NULL COMMENT '对方号码',
  `location` varchar(32) DEFAULT NULL COMMENT '通信地(自己的)',
  `send_type` varchar(32) DEFAULT NULL COMMENT 'SEND-发送; RECEIVE-收取',
  `msg_type` varchar(32) DEFAULT NULL COMMENT 'SMS-短信; MSS-彩信',
  `service_name` varchar(32) DEFAULT NULL COMMENT '业务名称. e.g. 点对点(网内)',
  `fee` double(4,0) DEFAULT NULL COMMENT '通信费(单位分)',
  `carrier_001` varchar(255) DEFAULT NULL COMMENT '预留1',
  `carrier_002` varchar(255) DEFAULT NULL COMMENT '预留2',
 
 `created_at` bigint(13) DEFAULT NULL COMMENT '创建时间',
  `updated_at` bigint(13) DEFAULT NULL COMMENT '修改时间',
  `deleted_at` bigint(13) DEFAULT NULL COMMENT '0：正常  时间：表示删除时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='手机短信详情';
```

# 充值记录表

```mysql
CREATE TABLE `carrier_recharge` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `task_id` varchar(32) DEFAULT NULL COMMENT '创建任务时的mongoId',
  `mobile` varchar(32) DEFAULT NULL COMMENT '手机号码',
  `recharge_time` varchar(255) DEFAULT NULL COMMENT '充值时间',
  `amount_money` int(11) DEFAULT NULL COMMENT '充值金额(单位分)',
  `type` varchar(32) DEFAULT NULL COMMENT '充值方式. e.g. 现金',
  `pay_chanel` varchar(32) DEFAULT NULL COMMENT '充值渠道',
  `pay_addr` varchar(32) DEFAULT NULL COMMENT '支付地址',
  `pay_flag` varchar(32) DEFAULT NULL COMMENT '支付状态\r\n支付状态',
  `carrier_001` varchar(255) DEFAULT NULL COMMENT '预留1',
  `carrier_002` varchar(255) DEFAULT NULL COMMENT '预留2',
  `created_at` bigint(13) DEFAULT NULL COMMENT '创建时间',
  `updated_at` bigint(13) DEFAULT NULL COMMENT '修改时间',
  `deleted_at` bigint(13) DEFAULT NULL COMMENT '0：正常  时间：表示删除时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='充值记录表';
```

# 上网详情表

```mysql
CREATE TABLE `carrier_net_detial` (
  `id` int(11) DEFAULT NULL COMMENT 'id',
  `task_id` varchar(32) DEFAULT NULL COMMENT '创建任务时的mongoId',
  `bill_month` varchar(32) DEFAULT NULL COMMENT '所属账单月',
  `mobile` varchar(32) DEFAULT NULL COMMENT '手机号码',
  `start_time` varchar(255) DEFAULT NULL COMMENT '起始时间',
  `comm_plac` varchar(255) DEFAULT NULL COMMENT '通信地点',
  `net_play_type` varchar(255) DEFAULT NULL COMMENT '上网方式',
  `net_type` varchar(255) DEFAULT NULL COMMENT '网络类型',
  `comm_time` varchar(255) DEFAULT NULL COMMENT '总时长',
  `sum_flow` varchar(255) DEFAULT NULL COMMENT '总流量',
  `meal` varchar(255) DEFAULT NULL COMMENT '套餐优惠',
  `comm_fee` varchar(255) DEFAULT NULL COMMENT '总费用',
  `carrier_001` varchar(255) DEFAULT NULL COMMENT '预留1',
  `carrier_002` varchar(255) DEFAULT NULL COMMENT '预留2',
  `created_at` bigint(13) DEFAULT NULL COMMENT '创建时间',
  `updated_at` bigint(13) DEFAULT NULL COMMENT '修改时间',
  `deleted_at` bigint(13) DEFAULT NULL COMMENT '0：正常  时间：表示删除时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='上网详情表';
```

# 消费记录表(月账单信息)

```mysql
CREATE TABLE `carrier_month_bill` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `task_id` varchar(32) DEFAULT NULL COMMENT '创建任务时的mongoId',
  `mobile` varchar(32) DEFAULT NULL COMMENT '手机号码',
  `bill_month` varchar(255) DEFAULT NULL COMMENT '账单月份',
  `bill_start_date` varchar(255) DEFAULT NULL COMMENT '开始时间',
  `bill_end_date` varchar(255) DEFAULT NULL COMMENT '结束时间',
  `bill_fee` int(11) DEFAULT NULL COMMENT '账单金额(单位：分)',
  `carrier_001` varchar(255) DEFAULT NULL COMMENT '预留1',
  `carrier_002` varchar(255) DEFAULT NULL COMMENT '预留2',
  `created_at` bigint(13) DEFAULT NULL COMMENT '创建时间',
  `updated_at` bigint(13) DEFAULT NULL COMMENT '修改时间',
  `deleted_at` bigint(13) DEFAULT NULL COMMENT '0：正常  时间：表示删除时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='消费记录表(月账单信息)';
```



### 调整表结构

```mysql
-- 在carrier_baseinfo 表的 xx 字段之后，新增一个字段，设置对应的类型，长度，是否为null，默认值，注释
ALTER TABLE carrier_baseinfo ADD COLUMN `user_name` varchar(32)  NULL COMMENT '外部传进来的姓名' AFTER `name`;
ALTER TABLE carrier_baseinfo ADD COLUMN `user_idcard` varchar(32)  NULL COMMENT '外部传进来的身份证号码' AFTER `idcard`;
```





---

#### Mongo 存储清洗后的运营商原始数据、报告

- 分析结果数据表 carrier_report_info


字段名 |  描述
---|---
id |  ID
task_id | 任务ID
mobile  | 手机号
name    | 外部传入的姓名
id_card    | 外部传入的身份证号
item    | 数据项 （raw: 原始数据 report: 报告）
result    |  具体数据
created_at | 创建时间
updated_at | 更新时间
deleted_at | 删除时间 默认 1， 不为1时表示删除时间


