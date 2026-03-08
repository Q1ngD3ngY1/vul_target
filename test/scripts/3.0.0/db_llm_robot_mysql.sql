-- 数据库表更记录：
-- 1. t_attribute_label和t_attribute_label_prod增加robot_id，作为迁移tdsql使用的shard key，为大表迁移做准备
-- 每次只刷5000行数据，需要手动多次刷数据，直到t_attribute_label和t_attribute_label_prod中不存在robot_id为0的数据
ALTER TABLE t_attribute_label ADD COLUMN `robot_id` bigint NOT NULL DEFAULT 0 COMMENT '机器人ID' AFTER `id`;
ALTER TABLE t_attribute_label_prod ADD COLUMN `robot_id` bigint NOT NULL DEFAULT 0 COMMENT '机器人ID' AFTER `id`;

-- 2. 刷数据，需要按需调大LIMIT值
update t_attribute_label a INNER JOIN t_attribute b on a.attr_id = b.id set a.robot_id= b.robot_id where a.id in (
    SELECT id FROM ( SELECT id FROM t_attribute_label WHERE robot_id = 0 LIMIT 5000 )AS temp );
update t_attribute_label_prod a INNER JOIN t_attribute_prod b on a.attr_id = b.attr_id set a.robot_id= b.robot_id where a.id in (
    SELECT id FROM ( SELECT id FROM t_attribute_label_prod WHERE robot_id = 0 LIMIT 5000 )AS temp );

-- 3. 修改表结构，增加自定义拆分规则
ALTER TABLE t_doc ADD COLUMN `split_rule` varchar(4096) NOT NULL DEFAULT '' COMMENT '文档拆分规则';

ALTER TABLE `t_doc`
    ADD COLUMN `update_period_h` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '文档更新周期小时数：0不更新，24(1天)，72(3天)，168(7天)' AFTER `is_downloadable`,
    ADD COLUMN `next_update_time` DATETIME NOT NULL DEFAULT '1970-01-01 08:00:00' COMMENT '文档下次更新执行时间' AFTER `update_period_h`;
-- 创建索引
ALTER TABLE t_doc
    ADD INDEX