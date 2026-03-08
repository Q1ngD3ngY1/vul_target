ALTER TABLE
    `t_doc_qa`
    ADD
        COLUMN `qa_size` bigint NOT NULL DEFAULT 0 COMMENT '问答字节数（含相似问）'


ALTER TABLE
    `t_qa_similar_question`
    ADD
        COLUMN `qa_size` bigint NOT NULL DEFAULT 0 COMMENT '问答字节数'

ALTER TABLE
    `t_realtime_doc`
    ADD
        COLUMN `page_count` int NOT NULL DEFAULT 0 COMMENT '实时文档解析页数'

ALTER TABLE
    `t_doc`
    ADD
        INDEX `idx_corp_robot_status_expire` USING btree (`corp_id`, `robot_id`, `status`, `expire_end`)
