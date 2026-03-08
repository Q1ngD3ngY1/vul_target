-- 数据库表更记录：

-- 1. 修改表结构，增加sheet_name字段
ALTER TABLE t_doc_segment_org_data ADD COLUMN `sheet_name` varchar(1024) NOT NULL DEFAULT '' COMMENT '表格sheet名称';
ALTER TABLE t_doc_segment_org_data_temporary ADD COLUMN `sheet_name` varchar(1024) NOT NULL DEFAULT '' COMMENT '表格sheet名称';

-- philxu start --
-- 1. 新增t_knowledge_base表
CREATE TABLE `t_knowledge_base` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `corp_biz_id` bigint NOT NULL DEFAULT 0 COMMENT '企业业务ID',
    `knowledge_biz_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT '知识库业务id',
    `processing_flag` bigint unsigned NOT NULL DEFAULT 0 COMMENT '知识库处理中状态标记',
    `is_deleted` tinyint(4) NOT NULL DEFAULT 0 COMMENT '是否删除',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`,`knowledge_biz_id`),
    UNIQUE KEY `uk_corp_knowledge_biz_id` (`corp_biz_id`, `knowledge_biz_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='知识库表' shardkey=knowledge_biz_id
-- philxu end --



-- t_knowledge_schema_task 表增加 status_code 字段
ALTER TABLE t_knowledge_schema_task ADD COLUMN status_code INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '任务状态码，表示任务处于当前状态的具体原因' AFTER status;

-- t_share_knowledge 表增加 space_id, owner_staff_id 字段
ALTER TABLE t_share_knowledge ADD COLUMN `space_id` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'default_space' COMMENT '空间ID';
ALTER TABLE t_share_knowledge ADD COLUMN `owner_staff_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT '共享知识库拥有者';