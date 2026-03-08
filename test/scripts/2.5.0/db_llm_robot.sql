-- 文档分片页码信息表
CREATE TABLE `t_doc_segment_page_info` (
   `id` bigint NOT NULL AUTO_INCREMENT,
   `page_info_id` bigint NOT NULL DEFAULT '0' COMMENT '页码ID',
   `segment_id` bigint NOT NULL DEFAULT '0' COMMENT '切片ID',
   `doc_id` bigint NOT NULL COMMENT '文档ID',
   `robot_id` bigint NOT NULL COMMENT '机器人ID',
   `corp_id` bigint NOT NULL COMMENT '企业ID',
   `staff_id` bigint NOT NULL DEFAULT '0' COMMENT '员工ID',
   `org_page_numbers` varchar(2048) NOT NULL DEFAULT '' COMMENT '页码信息（json存储）',
   `big_page_numbers` varchar(2048) NOT NULL DEFAULT '' COMMENT '页码信息（json存储）',
   `sheet_data` varchar(2048) NOT NULL DEFAULT '' COMMENT 'sheet信息（json）',
   `is_deleted` int NOT NULL DEFAULT '0' COMMENT '1未删除 2已删除',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '响应时间',
   PRIMARY KEY (`id`, `robot_id`),
   KEY `idx_robot_segment_id` (`robot_id`, `segment_id`),
   KEY `idx_robot_doc_id` (`robot_id`, `doc_id`),
   UNIQUE KEY `idx_robot_page_segment_id` (`robot_id`, `page_info_id`, `segment_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '文档分片页码信息表'
PARTITION BY HASH (`robot_id`) PARTITIONS 8;

-- 参考来源表
ALTER TABLE `t_refer`
    ADD COLUMN `page_infos` varchar(2048) NOT NULL DEFAULT '' COMMENT '页码信息（json存储）' AFTER `org_data`;

ALTER TABLE `t_refer`
    ADD COLUMN `sheet_infos` varchar(2048) NOT NULL DEFAULT '' COMMENT 'sheet信息（json存储）' AFTER `page_infos`;


ALTER TABLE db_llm_robot.t_evaluate_sample_set_record MODIFY content varchar(12000) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '会话样本详情';