-- task_schedule支持泳道，task_schedule_history不需要变更
alter table t_knowledge_task add column `lane_name` varchar(64) NOT NULL DEFAULT '' COMMENT '泳道名' after update_time;

-- i18n调整了输入框长度，需要同步调整sql中字段长度
ALTER TABLE `t_attribute` MODIFY COLUMN `attr_key` varchar (150) NOT NULL DEFAULT '' COMMENT '属性标识' AFTER `robot_id` , MODIFY COLUMN `name` varchar (300) NOT NULL DEFAULT '' COMMENT '属性描述' AFTER `attr_key`

ALTER TABLE `t_attribute_label` MODIFY COLUMN `name` varchar (300) CHARACTER SET `utf8mb4` COLLATE `utf8mb4_general_ci` NOT NULL DEFAULT '' COMMENT '标签名称' AFTER `attr_id`

ALTER TABLE `t_doc_category` MODIFY COLUMN `name` varchar (384) CHARACTER SET `utf8mb4` COLLATE `utf8mb4_general_ci` NOT NULL COMMENT '名称' AFTER `corp_id`

ALTER TABLE `t_doc_qa_category` MODIFY COLUMN `name` varchar (384) CHARACTER SET `utf8mb4` COLLATE `utf8mb4_general_ci` NOT NULL COMMENT '名称' AFTER `corp_id`

ALTER TABLE `t_synonyms_category` MODIFY COLUMN `name` varchar (384) CHARACTER SET `utf8mb4` COLLATE `utf8mb4_general_ci` NOT NULL COMMENT '名称' AFTER `corp_id`

-- admin已操作变更，这里只做记录，避免遗漏
-- ALTER TABLE `t_release_attribute` MODIFY COLUMN `name` varchar (300) NOT NULL DEFAULT '' COMMENT '属性描述' AFTER `attr_key`
-- ALTER TABLE `t_release_attribute_label` MODIFY COLUMN `name` varchar (300) CHARACTER SET `utf8mb4` COLLATE `utf8mb4_general_ci` NOT NULL DEFAULT '' COMMENT '标签名称' AFTER `label_id`

ALTER TABLE `t_attribute_label_prod` MODIFY COLUMN `name` varchar (300) CHARACTER SET `utf8mb4` COLLATE `utf8mb4_general_ci` NOT NULL DEFAULT '' COMMENT '标签名称' AFTER `label_id`

ALTER TABLE `t_attribute_prod` MODIFY COLUMN `name` varchar (300) CHARACTER SET `utf8mb4` COLLATE `utf8mb4_general_ci` NOT NULL DEFAULT '' COMMENT '属性描述' AFTER `attr_key`
