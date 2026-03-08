

-- harryjhuang start --
-- t_doc_qa 添加来源字段，区分是doc_to_qa task产生的，还是doc diff task生成的
ALTER TABLE `db_llm_robot`.`t_doc_qa_task` ADD COLUMN `source_id` bigint NOT NULL DEFAULT 0 COMMENT '来源ID' AFTER `task_id`;

-- harryjhuang end --

CREATE TABLE `t_doc_diff_task` (
   `business_id` bigint NOT NULL COMMENT '文档对比ID',
   `corp_biz_id` bigint NOT NULL COMMENT '企业ID',
   `robot_biz_id` bigint NOT NULL COMMENT '应用ID',
   `staff_biz_id` bigint NOT NULL DEFAULT '0' COMMENT '员工ID',
   `new_doc_biz_id` bigint NOT NULL COMMENT '新文档ID',
   `old_doc_biz_id` bigint NOT NULL COMMENT '旧文档ID',
   `task_id` bigint NOT NULL DEFAULT '0' COMMENT '异步处理任务ID',
   `doc_qa_task_id` bigint NOT NULL DEFAULT '0' COMMENT '文档生成qa任务ID',
   `new_doc_rename` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '重命名操作新文件名',
   `old_doc_rename` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '重命名操作旧文件名',
   `comparison_reason` int NOT NULL COMMENT '对比原因(1名称相同 2手动添加 3网址相同)',
   `diff_type` int NOT NULL COMMENT '对比类型(1新文档有生成问答对，旧文档没有 2旧文档有生成问答对。新文档没有 3新文档和旧文档都有问答对 4新文档和旧文档都没有问答对)',
   `doc_operation` int NOT NULL COMMENT '文档操作类型(1删除旧文档 2删除新文档 3旧文档重命名 4新文档重命名 5不处理)',
   `doc_operation_status` int NOT NULL COMMENT '文档操作结果(0处理中，1操作成功，2操作失败)',
   `qa_operation` int NOT NULL COMMENT '问答操作类型（根据问答可选操作类型枚举定义）',
   `qa_operation_status` int NOT NULL COMMENT '问答操作结果(0处理中，1操作成功，2操作失败)',
   `qa_operation_result` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '问答操作成功或失败的结果提示',
   `status` int NOT NULL DEFAULT '0' COMMENT '状态(0待处理 1处理中 2已完成 3已失效)',
   `diff_data_process_status` int NOT NULL DEFAULT '0' COMMENT '文档比对详情任务状态(0待处理 1处理中 2已完成 3已失败)',
   `is_deleted` int NOT NULL DEFAULT '0' COMMENT '是否删除(0未删除 1已删除）',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   PRIMARY KEY (`corp_biz_id`, `robot_biz_id`, `business_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '文档对比任务' shardkey = robot_biz_id

CREATE TABLE `t_doc_diff_data` (
   `corp_biz_id` bigint NOT NULL COMMENT '企业ID',
   `robot_biz_id` bigint NOT NULL COMMENT '应用ID',
   `diff_biz_id` bigint NOT NULL COMMENT '文档对比任务ID',
   `diff_index` int NOT NULL COMMENT '文档片段diff序号',
   `diff_data` text NOT NULL COMMENT '文档片段diff详细内容,json格式',
   `is_deleted` tinyint NOT NULL DEFAULT '0' COMMENT '是否删除(0未删除 1已删除)',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   PRIMARY KEY (`corp_biz_id`,`robot_biz_id`,`diff_biz_id`,`diff_index`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '文档对比任务结果' shardkey = diff_biz_id



-- zrwang start --
ALTER TABLE t_doc
    ADD COLUMN original_url varchar(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '原始网页地址', ALGORITHM=INSTANT;
-- zrwang end --

-- philxu start --
ALTER TABLE t_doc ADD COLUMN `processing_flag` bigint NOT NULL DEFAULT '0' COMMENT '处理中标志位';
-- philxu end --