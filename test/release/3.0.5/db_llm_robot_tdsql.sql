-- i18n调整了输入框长度，需要同步调整sql中字段长度
ALTER TABLE `t_db_table_column` MODIFY COLUMN `column_name` varchar (300) NOT NULL COMMENT '列名' AFTER `db_table_column_biz_id` , MODIFY COLUMN `column_comment` varchar (4500) NULL DEFAULT '' COMMENT '列注释' AFTER `data_type` , MODIFY COLUMN `alias_name` varchar (300) NULL DEFAULT '' COMMENT '别名' AFTER `column_comment` , MODIFY COLUMN `description` varchar (1500) NULL DEFAULT '' COMMENT '列描述' AFTER `alias_name` , MODIFY COLUMN `unit` varchar (150) NULL DEFAULT '' COMMENT '单位' AFTER `description`

ALTER TABLE `t_db_source` MODIFY COLUMN `alias_name` varchar (300) CHARACTER SET `utf8mb4` COLLATE `utf8mb4_0900_ai_ci` NOT NULL COMMENT '自定义数据库名称' AFTER `db_name`

ALTER TABLE `t_db_source_prod` MODIFY COLUMN `alias_name` varchar (300) CHARACTER SET `utf8mb4` COLLATE `utf8mb4_0900_ai_ci` NOT NULL COMMENT '自定义数据库名称' AFTER `db_name`

ALTER TABLE `t_db_table_prod` MODIFY COLUMN `table_comment` varchar (4500) CHARACTER SET `utf8mb4` COLLATE `utf8mb4_0900_ai_ci` NULL DEFAULT '' COMMENT '列注释' AFTER `table_schema` , MODIFY COLUMN `alias_name` varchar (300) CHARACTER SET `utf8mb4` COLLATE `utf8mb4_0900_ai_ci` NULL DEFAULT '' COMMENT '表中文名' AFTER `table_comment` , MODIFY COLUMN `description` varchar (1500) CHARACTER SET `utf8mb4` COLLATE `utf8mb4_0900_ai_ci` NULL DEFAULT '' COMMENT '表描述' AFTER `alias_name`

ALTER TABLE `t_db_table` MODIFY COLUMN `table_comment` varchar (4500) CHARACTER SET `utf8mb4` COLLATE `utf8mb4_0900_ai_ci` NULL DEFAULT '' COMMENT '列注释' AFTER `table_schema` , MODIFY COLUMN `alias_name` varchar (300) CHARACTER SET `utf8mb4` COLLATE `utf8mb4_0900_ai_ci` NULL DEFAULT '' COMMENT '表中文名' AFTER `table_comment` , MODIFY COLUMN `description` varchar (1500) CHARACTER SET `utf8mb4` COLLATE `utf8mb4_0900_ai_ci` NULL DEFAULT '' COMMENT '表描述' AFTER `alias_name`
