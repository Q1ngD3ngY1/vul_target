-- 新增 enable_scope 字段
ALTER TABLE t_doc ADD COLUMN  `enable_scope` tinyint NOT NULL DEFAULT '0' COMMENT '检索生效范围 1：停用，1：仅开发域，2：仅发布域，3：开发域&发布域';
ALTER TABLE t_doc_qa ADD COLUMN  `enable_scope` tinyint NOT NULL DEFAULT '0' COMMENT '检索生效范围 1：停用，1：仅开发域，2：仅发布域，3：开发域&发布域';

ALTER TABLE t_release ADD  COLUMN `release_mode` int DEFAULT '1' COMMENT '发布模式: 1-普通, 2-修改知识生效域'
