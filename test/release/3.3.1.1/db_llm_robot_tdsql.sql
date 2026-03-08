-- 新增 enable_scope 字段
ALTER table t_db_source ADD COLUMN  `enable_scope` tinyint NOT NULL DEFAULT 0 COMMENT '检索生效范围 1：停用，2：仅开发域，3：仅发布域，4：开发域&发布域';
ALTER table t_db_table ADD COLUMN  `enable_scope` tinyint NOT NULL DEFAULT 0 COMMENT '检索生效范围 1：停用，2：仅开发域，3：仅发布域，4：开发域&发布域';
