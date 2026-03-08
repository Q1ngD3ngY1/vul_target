-- cooperwang start --

ALTER TABLE t_doc_qa ADD COLUMN `attribute_flag` bigint NOT NULL default 0 comment '问答属性按位存储，第一位QA是否停用';
-- cooperwang end --

-- 文档分片表添加字段，tdsql变更不能有反引号
ALTER TABLE t_doc_segment ADD COLUMN org_data_biz_id bigint NOT NULL DEFAULT 0 COMMENT '关联org_data数据';