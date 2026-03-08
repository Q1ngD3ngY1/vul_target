alter table t_doc_segment
    add column `segment_type` varchar(64) NOT NULL DEFAULT '' COMMENT '文档切片类型 segment-文档切片 table-表格' after `file_type`;

alter table t_release_segment
    add column `segment_type` varchar(64) NOT NULL DEFAULT '' COMMENT '文档切片类型 segment-文档切片 table-表格' after `file_type`;

alter table t_overlay_node_info
    add column `segment_type` varchar(64) NOT NULL DEFAULT '' COMMENT '文档切片类型 segment-文档切片 table-表格' after `doc_id`;

update t_doc_segment set segment_type = 'segment' where segment_type != '';

update t_release_segment set segment_type = 'segment' where segment_type != '';

update t_overlay_node_info set segment_type = 'segment' where segment_type != '';