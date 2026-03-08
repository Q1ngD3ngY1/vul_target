-- 数据库表变更记录：

-- NOTICE START: 公有云v325数据库需求，私有化v324需合入
ALTER TABLE t_db_source ADD COLUMN `schema_name` varchar(128) NULL comment '数据库模式名称';
ALTER TABLE t_db_source_prod ADD COLUMN `schema_name` varchar(128) NULL comment '数据库模式名称';
ALTER TABLE t_release_db_source ADD COLUMN `schema_name` varchar(128) NULL comment '数据库模式名称';
-- NOTICE END: 公有云v325数据库需求，私有化v324需合入