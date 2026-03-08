-- 文档解析服务重构，调大org_data字段长度
alter table t_doc_segment_org_data modify column org_data MEDIUMTEXT NOT NULL;
alter table t_doc_segment_org_data_temporary modify column org_data MEDIUMTEXT NOT NULL;

