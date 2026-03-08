-- randalchen start --
-- 问答表添加索引
ALTER TABLE `db_llm_robot`.`t_doc_qa` ADD INDEX `idx_robot_origin_doc_id` (`robot_id`,`origin_doc_id`);

-- 审核表添加字段
ALTER TABLE `db_llm_robot`.`t_audit` ADD COLUMN `parent_relate_id` bigint NOT NULL DEFAULT 0 COMMENT '父关联ID';

-- 审核表添加索引
ALTER TABLE `db_llm_robot`.`t_audit` ADD INDEX `idx_parent_relate_id_type` (`corp_id`,`robot_id`,`parent_relate_id`,`type`);
-- randalchen end --

-- zrwang start --
ALTER TABLE t_evaluate_test  ADD COLUMN test_done_num INT NOT NULL DEFAULT -1 COMMENT '-1 历史数据' , ALGORITHM=INSTANT;
-- zrwang end --

-- xshwu start --
alter table `db_llm_robot`.`t_doc` ADD COLUMN `file_name_in_audit` varchar(255) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '审核中的文件名' AFTER file_name;
-- xshwu end --
