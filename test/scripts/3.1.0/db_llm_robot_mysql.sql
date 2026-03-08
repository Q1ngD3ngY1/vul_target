-- 数据库表更记录：
-- 1. 不满意回复表增加操作人staff_id字段
ALTER TABLE `db_llm_robot`.`t_unsatisfied_reply` ADD COLUMN `staff_id` BIGINT NOT NULL  DEFAULT 0 COMMENT '操作人id' AFTER `user_type`;

