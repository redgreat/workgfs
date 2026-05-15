-- GFS 成本单导入主表/明细（与 workcount_log 同库，如 finance_basic）。
-- 定时任务逻辑见 src/sync_handler.py：main_costsyncinfo（AuditState=1, CostSyncState=0）
-- + main_costsyncdetail（AuditState=1，明细主键 Id char(12)）的 WorkOrderId 从壹好车服拉数写入 workcount_log（含 CostSyncId、CostSyncDetailId），再 CALL 存储过程。

CREATE TABLE `main_costsyncinfo` (
  `Id` char(12) NOT NULL COMMENT '主键(CA)递减',
  `BusinessType` smallint DEFAULT NULL COMMENT '业务类型:1-车务;2-车电;3-充电桩',
  `OperateUserId` char(36) DEFAULT NULL COMMENT '操作人账号',
  `OperateUserLoginName` varchar(20) DEFAULT NULL COMMENT '操作人账号',
  `OperateUserName` varchar(200) DEFAULT NULL COMMENT '操作人姓名',
  `AuditState` smallint DEFAULT NULL COMMENT '验证结果:0-失败;1-成功;2-验证中',
  `CostSyncState` smallint DEFAULT NULL COMMENT '数据同步状态:0-待同步;1-已同步',
  `Remark` varchar(500) DEFAULT NULL COMMENT '备注',
  `CreatedById` char(36) DEFAULT NULL,
  `CreatedAt` datetime DEFAULT NULL,
  `UpdatedById` char(36) DEFAULT NULL,
  `UpdatedAt` datetime DEFAULT NULL,
  `DeletedById` char(36) DEFAULT NULL,
  `DeletedAt` datetime DEFAULT NULL,
  `Deleted` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`Id`),
  KEY `NON-BusinessType` (`BusinessType`),
  KEY `NON-OperateUserId` (`OperateUserId`),
  KEY `NON-AuditState` (`AuditState`),
  KEY `NON-CostSyncState` (`CostSyncState`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COMMENT='业务实现层_成本单导入管理记录';

CREATE TABLE `main_costsyncdetail` (
  `Id` char(12) NOT NULL COMMENT '主键(业务主键，与 workcount_log.CostSyncDetailId 对应)',
  `CostSyncId` char(12) DEFAULT NULL COMMENT '成本单导入管理记录Id(finance_main.main_costsyncinfo.Id)',
  `WorkOrderId` varchar(50) DEFAULT NULL COMMENT '工单Id',
  `AppNo` varchar(100) DEFAULT NULL COMMENT '工单编号',
  `WorkOrderType` varchar(50) DEFAULT NULL COMMENT '工单类型code',
  `WorkOrderTypeName` varchar(100) DEFAULT NULL COMMENT '工单类型',
  `CustomerId` char(12) DEFAULT NULL COMMENT '客户Id(付款公司对应主体名称Id)',
  `CustomerName` varchar(200) DEFAULT NULL COMMENT '客户名称(付款公司对应主体名称)',
  `PayeeAccountName` varchar(200) DEFAULT NULL COMMENT '收款账户名',
  `PayeeUserCode` varchar(20) DEFAULT NULL COMMENT '收款人账号(工号)',
  `BankCardNumber` varchar(200) DEFAULT NULL COMMENT '银行卡号',
  `CostPrice` decimal(18,2) DEFAULT NULL COMMENT '成本金额(含税)',
  `PayCompanyName` varchar(200) DEFAULT NULL COMMENT '付款公司',
  `CostPurpose` varchar(500) DEFAULT NULL COMMENT '成本用途',
  `AuditState` smallint DEFAULT NULL COMMENT '验证结果:0-失败;1-成功;2-验证中',
  `FailReason` varchar(500) DEFAULT NULL COMMENT '失败原因',
  `CreatedById` char(36) DEFAULT NULL,
  `CreatedAt` datetime DEFAULT NULL,
  `UpdatedById` char(36) DEFAULT NULL,
  `UpdatedAt` datetime DEFAULT NULL,
  `DeletedById` char(36) DEFAULT NULL,
  `DeletedAt` datetime DEFAULT NULL,
  `Deleted` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`Id`) USING BTREE,
  KEY `NON-CostSyncId` (`CostSyncId`) USING BTREE,
  KEY `NON-WorkOrderId` (`WorkOrderId`) USING BTREE,
  KEY `NON-AppNo` (`AppNo`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COMMENT='业务实现层_成本单导入管理记录失败明细';

-- 若线上仍为 bigint 自增 Id，需自行迁移为 char(12) 并回填 Id，再与 workcount_log.CostSyncDetailId 对齐。