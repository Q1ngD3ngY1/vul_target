-- 数据库表更记录：
-- 1. 增加了is_indexed 字段,用于控制数据库是否参与索引
ALTER TABLE t_db_source ADD COLUMN is_indexed TINYINT(1) DEFAULT 1 COMMENT '是否参与索引' AFTER alive;


-- 2. 增加了 prev_is_indexed 字段,用于记录上一次的is_indexed 字段的值
ALTER TABLE t_db_table ADD COLUMN prev_is_indexed TINYINT(1) DEFAULT 1 COMMENT '记录用户设置的是否参与索引' AFTER is_indexed;
UPDATE t_db_table SET prev_is_indexed = is_indexed WHERE id > 0;

-- 3. 新增 learn_status 字段，用于记录状态，暂时为学习状态(1 未学习 2 学习中 3 已学习 4  学习失败)
ALTER TABLE t_db_table ADD COLUMN learn_status TINYINT(1) DEFAULT 3 COMMENT '学习状态(1 未学习 2 学习中 3 已学习 4  学习失败)' AFTER last_sync_time;

-- 3.1 table表新增索引
ALTER TABLE t_db_table ADD KEY `index_db_table_biz_id` (`db_table_biz_id`);
ALTER TABLE t_db_table_prod ADD KEY `index_db_table_biz_id` (`db_table_biz_id`);

-- 4. 新增 top_value 字段，用于记录top值
CREATE TABLE `t_db_table_top_value` (
    `id` bigint NOT NULL AUTO_INCREMENT,
    `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
    `app_biz_id` bigint NOT NULL COMMENT '应用业务ID',
    `db_source_biz_id` bigint NOT NULL COMMENT '数据源ID',
    `db_table_biz_id` bigint NOT NULL COMMENT '数据表业务ID',
    `db_table_column_biz_id` bigint NOT NULL COMMENT '数据列业务ID',
    `business_id` bigint NOT NULL COMMENT '新产生的ID，雪花算法 id ',
    `column_name` varchar(100) NOT NULL COMMENT '原始的列名称',
    `column_value` text NOT NULL COMMENT '列别名',
    `column_comment` varchar(1500) NOT NULL COMMENT '列注释',
    `is_deleted` tinyint(1) NOT NULL DEFAULT 0 COMMENT '0:未删除,1:已删除',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`, `app_biz_id`),
    KEY `idx_biz_id` (`corp_biz_id`, `app_biz_id`, `db_table_biz_id`),
    KEY `idx_value_id` (`business_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='外部数据库数据表top value表' shardkey=app_biz_id;

-- 5. 新增 staff_id 字段，用于记录修改人
ALTER TABLE t_db_source ADD COLUMN staff_id bigint NOT NULL DEFAULT '0' COMMENT '员工ID' after is_deleted
ALTER TABLE t_db_table ADD COLUMN staff_id bigint NOT NULL DEFAULT '0' COMMENT '员工ID' after is_deleted

-- 6. 新增 audit_status 字段，用于记录审核状态
ALTER TABLE t_doc_segment_org_data_temporary ADD COLUMN audit_status tinyint NOT NULL DEFAULT 0 COMMENT '审核状态。0:审核通过；1:内容审核失败；2:图片审核失败；3:图片和内容审核失败';
ALTER TABLE t_doc_segment_sheet_temporary ADD COLUMN audit_status tinyint NOT NULL DEFAULT 0 COMMENT '审核状态。0:审核通过；1:内容审核失败';

-- 7. t_doc_segment_page_info新增索引，优化DescribeSegments接口性能
ALTER TABLE t_doc_segment_page_info ADD INDEX idx_doc_segment_id (segment_id)
