-- cooperwang start --
CREATE TABLE `t_app_share_knowledge` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
    `app_biz_id` bigint unsigned NOT NULL DEFAULT '0' COMMENT '应用业务id',
    `knowledge_biz_id` bigint unsigned NOT NULL DEFAULT '0' COMMENT '引用的共享知识库业务id',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`,`app_biz_id`),
    KEY `idx_app_knowledge_biz_id` (`app_biz_id`,`knowledge_biz_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='应用引用共享知识库表' shardkey=app_biz_id


CREATE TABLE `t_app_share_knowledge_prod` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
    `app_biz_id` bigint unsigned NOT NULL DEFAULT '0' COMMENT '应用业务id',
    `knowledge_biz_id` bigint unsigned NOT NULL DEFAULT '0' COMMENT '引用的共享知识库业务id',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`,`app_biz_id`),
    KEY `idx_app_knowledge_biz_id` (`app_biz_id`,`knowledge_biz_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='应用引用共享知识库已发布表' shardkey=app_biz_id
-- cooperwang end --

CREATE TABLE `t_knowledge_user` (
   `id` bigint NOT NULL AUTO_INCREMENT,
   `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
   `app_biz_id` bigint NOT NULL COMMENT '应用业务ID',
   `business_id` bigint NOT NULL COMMENT '用户业务ID',
   `name` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '用户名称',
   `third_user_id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '用户ID，由客户填写',
   `is_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   PRIMARY KEY (`id`,`app_biz_id`),
   KEY `idx_biz_id` (`corp_biz_id`,`app_biz_id`,`business_id`,`is_deleted`),
   KEY `idx_lke_user_id`
   (`corp_biz_id`,`app_biz_id`,`third_user_id`,`is_deleted`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='用户信息表' shardkey = app_biz_id

CREATE TABLE `t_knowledge_user_role` (
   `id` bigint NOT NULL AUTO_INCREMENT,
   `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
   `app_biz_id` bigint NOT NULL COMMENT '应用业务ID',
   `user_biz_id` bigint NOT NULL COMMENT '用户业务ID',
   `third_user_id` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '用户ID，由客户填写',
   `type` tinyint(4) NOT NULL COMMENT '类型，0默认，1没传第三方用户id兜底配置，2传了匹配为空兜底设置',
   `role_biz_id` bigint NOT NULL COMMENT '角色业务ID',
   `is_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   PRIMARY KEY (`id`,`app_biz_id`),
   KEY `idx_biz_id` (`corp_biz_id`,`app_biz_id`,`user_biz_id`,`is_deleted`),
   KEY `idx_lke_user_id`
   (`corp_biz_id`,`app_biz_id`,`third_user_id`,`is_deleted`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='用户角色关联表' shardkey = app_biz_id

CREATE TABLE `t_knowledge_config` (
   `id` bigint NOT NULL AUTO_INCREMENT,
   `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
   `knowledge_biz_id` bigint NOT NULL COMMENT '知识库业务ID',
   `type` tinyint(4) NOT NULL COMMENT '类型，0第三方权限接口配置',
   `config` text NOT NULL COMMENT '配置json内容',
   `is_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   PRIMARY KEY (`id`,`knowledge_biz_id`),
   UNIQUE KEY `uk_biz_type` (`corp_biz_id`,`knowledge_biz_id`,`type`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='知识库配置表' shardkey = knowledge_biz_id

CREATE TABLE `t_knowledge_role` (
   `id` bigint NOT NULL AUTO_INCREMENT,
   `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
   `app_biz_id` bigint NOT NULL COMMENT '应用业务ID',
   `business_id` bigint NOT NULL COMMENT '角色业务ID',
   `name` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '角色名称',
   `type` tinyint(4) NOT NULL COMMENT '角色类型(1 预置 2 自定义)',
   `description` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '角色描述',
   `search_type` tinyint(4) NOT NULL COMMENT '整体检索范围(1全部知识 2按知识库)',
   `is_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   PRIMARY KEY (`id`,`app_biz_id`),
   KEY `idx_biz_id` (`corp_biz_id`,`app_biz_id`,`business_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='角色信息表' shardkey = app_biz_id

CREATE TABLE `t_knowledge_role_know` (
   `id` bigint NOT NULL AUTO_INCREMENT,
   `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
   `app_biz_id` bigint NOT NULL COMMENT '应用业务ID',
   `role_biz_id` bigint NOT NULL COMMENT '角色业务ID,为0代表预置角色',
   `knowledge_biz_id` bigint NOT NULL COMMENT '知识库业务id',
   `knowledge_type` tinyint(4) NOT NULL COMMENT '知识库类型(1 私有知识库 2 共享知识库)',
   `search_type` tinyint(4) NOT NULL COMMENT '检索范围(1全部知识 2按特定知识 3按标签)',
   `lable_condition` tinyint(4) NOT NULL COMMENT '标签操作符(1AND 2OR)',
   `is_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   PRIMARY KEY (`id`,`app_biz_id`),
   KEY `idx_biz_id` (`corp_biz_id`,`app_biz_id`,`role_biz_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='角色引用知识库表' shardkey = app_biz_id

CREATE TABLE `t_knowledge_role_doc` (
   `id` bigint NOT NULL AUTO_INCREMENT,
   `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
   `app_biz_id` bigint NOT NULL COMMENT '应用业务ID',
   `role_biz_id` bigint NOT NULL COMMENT '角色主键ID',
   `knowledge_biz_id` bigint NOT NULL COMMENT '知识库ID',
   `doc_biz_id` bigint NOT NULL COMMENT '文档主键ID',
   `is_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   PRIMARY KEY (`id`,`app_biz_id`,`knowledge_biz_id`),
   KEY `idx_biz_id` (`corp_biz_id`,`app_biz_id`,`role_biz_id`,`is_deleted`,`knowledge_biz_id`),
   KEY `idx_doc_id` (`knowledge_biz_id`,`doc_biz_id`,`is_deleted`,`app_biz_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='角色文档权限表' shardkey = knowledge_biz_id

CREATE TABLE `t_knowledge_role_qa` (
   `id` bigint NOT NULL AUTO_INCREMENT,
   `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
   `app_biz_id` bigint NOT NULL COMMENT '应用业务ID',
   `role_biz_id` bigint NOT NULL COMMENT '角色业务ID',
   `knowledge_biz_id` bigint NOT NULL COMMENT '知识库ID',
   `qa_biz_id` bigint NOT NULL COMMENT '问答业务ID',
   `is_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   PRIMARY KEY (`id`,`app_biz_id`,`knowledge_biz_id`),
   KEY `idx_biz_id` (`corp_biz_id`,`app_biz_id`,`role_biz_id`,`is_deleted`,`knowledge_biz_id`),
   KEY `idx_qa_id` (`knowledge_biz_id`,`qa_biz_id`,`is_deleted`,`app_biz_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='角色问答权限表' shardkey = knowledge_biz_id

CREATE TABLE `t_knowledge_role_attribute_label` (
   `id` bigint NOT NULL AUTO_INCREMENT,
   `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
   `app_biz_id` bigint NOT NULL COMMENT '应用业务ID',
   `role_biz_id` bigint NOT NULL COMMENT '角色业务ID',
   `knowledge_biz_id` bigint NOT NULL COMMENT '知识库ID',
   `attr_biz_id` bigint NOT NULL COMMENT '属性业务ID',
   `label_biz_id` bigint NOT NULL COMMENT '属性标签业务ID',
   `is_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   PRIMARY KEY (`id`,`app_biz_id`,`knowledge_biz_id`),
   KEY `idx_biz_id` (`corp_biz_id`,`app_biz_id`,`role_biz_id`,`is_deleted`,`knowledge_biz_id`),
   KEY `idx_attr_id` (`knowledge_biz_id`,`attr_biz_id`,`is_deleted`,`app_biz_id`),
   KEY `idx_label_id` (`knowledge_biz_id`,`label_biz_id`,`is_deleted`,`app_biz_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='角色标签权限表' shardkey = knowledge_biz_id

CREATE TABLE `t_knowledge_role_cate` (
   `id` bigint NOT NULL AUTO_INCREMENT,
   `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
   `app_biz_id` bigint NOT NULL COMMENT '应用业务ID',
   `role_biz_id` bigint NOT NULL COMMENT '角色主键ID',
   `knowledge_biz_id` bigint NOT NULL COMMENT '知识库ID',
   `type` tinyint(4) NOT NULL COMMENT '类型',
   `cate_biz_id` bigint NOT NULL COMMENT '分类主键ID',
   `is_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   PRIMARY KEY (`id`,`app_biz_id`,`knowledge_biz_id`),
   KEY `idx_biz_id` (`corp_biz_id`,`app_biz_id`,`role_biz_id`,`is_deleted`,`knowledge_biz_id`),
   KEY `idx_cate_id` (`knowledge_biz_id`,`cate_biz_id`,`is_deleted`,`app_biz_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='角色文档权限表' shardkey = knowledge_biz_id

CREATE TABLE `t_knowledge_role_database` (
   `id` bigint NOT NULL AUTO_INCREMENT,
   `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
   `app_biz_id` bigint NOT NULL COMMENT '应用业务ID',
   `role_biz_id` bigint NOT NULL COMMENT '角色业务ID',
   `knowledge_biz_id` bigint NOT NULL COMMENT '知识库ID',
   `database_biz_id` bigint NOT NULL COMMENT 'DB业务ID',
   `is_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   PRIMARY KEY (`id`,`app_biz_id`,`knowledge_biz_id`),
   KEY `idx_biz_id` (`corp_biz_id`,`app_biz_id`,`role_biz_id`,`is_deleted`,`knowledge_biz_id`),
   KEY `idx_qa_id` (`knowledge_biz_id`,`database_biz_id`,`is_deleted`,`app_biz_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='角色数据库权限表' shardkey = knowledge_biz_id

CREATE TABLE `t_share_knowledge` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
    `business_id` bigint NOT NULL COMMENT '共享知识库业务id=bot_biz_id',
    `name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '共享知识库名称',
    `description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci COMMENT '共享知识库描述',
    `user_biz_id` bigint unsigned NOT NULL COMMENT '用户ID,staff_biz_id',
    `embedding_model` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT 'Embedding模型',
    `qa_extract_model` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '问答对抽取模型',
    `is_deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除(0未删除 1已删除）',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `user_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '用户名称(用于分页搜索)',
    PRIMARY KEY (`id`, `business_id`),
    UNIQUE KEY `uni_business_id` (`business_id`) USING BTREE,
    KEY `idx_share_knowledge` (
    `is_deleted`,
    `corp_biz_id`,
    `business_id`,
    `update_time`
  ) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '共享知识库表' shardkey = business_id

-- 新增干预临时切片(org_data)数据表
CREATE TABLE `t_doc_segment_org_data_temporary` (
    `id` bigint NOT NULL AUTO_INCREMENT,
    `business_id` varchar(255) COLLATE utf8mb4_general_ci NOT NULL COMMENT '业务ID',
    `doc_biz_id` bigint NOT NULL COMMENT '文档ID',
    `app_biz_id` bigint NOT NULL COMMENT '应用ID',
    `corp_biz_id` bigint NOT NULL COMMENT '企业ID',
    `staff_biz_id` bigint NOT NULL DEFAULT 0 COMMENT '员工ID',
    `org_data` text COLLATE utf8mb4_general_ci NOT NULL COMMENT '原始内容',
    `add_method` tinyint NOT NULL DEFAULT 0 COMMENT '添加方式，用于区分是否客户手工新增。0:切片生成（默认）1:客户手工新增',
    `action` tinyint NOT NULL COMMENT '操作类型。0:编辑 1:新增',
    `org_page_numbers` varchar(2048) COLLATE utf8mb4_general_ci NOT NULL COMMENT '原始内容对应的页码。从小到大排列，pdf、doc、ppt、pptx才会返回，docx、md、txt、excel等没有页码的返回空',
    `segment_type` varchar(64) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '文档切片类型 segment-文档切片 table-表格',
    `origin_org_data_id` varchar(255) COLLATE utf8mb4_general_ci NOT NULL COMMENT '对应t_doc_segment_org_data中的id数据',
    `last_org_data_id` varchar(255) COLLATE utf8mb4_general_ci NOT NULL COMMENT '用于合并切片时判断切片的位置信息。仅add_method为1时使用。',
    `after_org_data_id` varchar(255) COLLATE utf8mb4_general_ci NOT NULL COMMENT '仅last_org_data_id为first时使用',
    `last_origin_org_data_id` varchar(255) COLLATE utf8mb4_general_ci NOT NULL COMMENT '上一个原始切片的位置，用于合并切片时判断切片的位置信息。',
    `is_deleted` tinyint NOT NULL DEFAULT 0 COMMENT '0未删除 1已删除',
    `is_disabled` tinyint NOT NULL DEFAULT 0 COMMENT '0启用 1停用',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '响应时间',
    PRIMARY KEY (`id`, `doc_biz_id`),
    KEY `idx_biz_id` (`corp_biz_id`,`app_biz_id`,`doc_biz_id`,`business_id`),
    KEY `idx_last_id` (`corp_biz_id`,`app_biz_id`,`doc_biz_id`,`last_org_data_id`),
    KEY `idx_last_origin_id` (`corp_biz_id`,`app_biz_id`,`doc_biz_id`,`last_origin_org_data_id`),
    KEY `idx_origin_id` (`corp_biz_id`,`app_biz_id`,`doc_biz_id`,`origin_org_data_id`)    
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='干预临时切片(org_data)数据表' shardkey=doc_biz_id

-- 新增干预临时表格文档数据表
CREATE TABLE `t_doc_segment_sheet_temporary` (
    `id` bigint NOT NULL AUTO_INCREMENT,
    `business_id` bigint COLLATE utf8mb4_general_ci NOT NULL COMMENT '业务ID',
    `doc_biz_id` bigint NOT NULL COMMENT '文档ID',
    `app_biz_id` bigint NOT NULL COMMENT '应用ID',
    `corp_biz_id` bigint NOT NULL COMMENT '企业ID',
    `staff_biz_id` bigint NOT NULL DEFAULT 0 COMMENT '员工ID',
    `sheet_order` int NOT NULL COMMENT 'sheet在整个表格文档中的位置',
    `sheet_name` varchar(1024) COLLATE utf8mb4_general_ci NOT NULL COMMENT 'sheet名称',
    `bucket` varchar(255) COLLATE utf8mb4_general_ci NOT NULL COMMENT 'cos bucket',
    `region` varchar(255) COLLATE utf8mb4_general_ci NOT NULL COMMENT 'cos region',
    `cos_url` varchar(1024) COLLATE utf8mb4_general_ci NOT NULL COMMENT 'sheet的cos 文件地址',
    `cos_hash` varchar(255) COLLATE utf8mb4_general_ci NOT NULL COMMENT 'x-cos-hash-crc64ecma 头部中的 CRC64编码进行校验上传到云端的文件和本地文件的一致性',
    `file_name` varchar(1024) COLLATE utf8mb4_general_ci NOT NULL COMMENT 'cos文件的文件名',
    `file_type` varchar(32) COLLATE utf8mb4_general_ci NOT NULL COMMENT 'cos文件的文件类型',
    `sheet_total_num` int NOT NULL COMMENT '一个文档中的sheet总数量',
    `version` int NOT NULL DEFAULT 0 COMMENT '版本号',
    `is_deleted` tinyint NOT NULL DEFAULT 0 COMMENT '0未删除 1已删除',
    `is_disabled` tinyint NOT NULL DEFAULT 0 COMMENT '0启用 1停用',
    `is_disabled_retrieval_enhance` tinyint NOT NULL DEFAULT 0 COMMENT '0启用检索增强 1停用检索增强',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '响应时间',
    PRIMARY KEY (`id`, `doc_biz_id`),
    KEY `idx_biz_id` (`corp_biz_id`,`app_biz_id`,`doc_biz_id`,`business_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='干预临时表格文档数据表' shardkey=doc_biz_id

-- 新增org_data数据表
CREATE TABLE `t_doc_segment_org_data` (
    `id` bigint NOT NULL AUTO_INCREMENT,
    `business_id` bigint NOT NULL COMMENT '业务ID',
    `doc_biz_id` bigint NOT NULL COMMENT '文档ID',
    `app_biz_id` bigint NOT NULL COMMENT '应用ID',
    `corp_biz_id` bigint NOT NULL COMMENT '企业ID',
    `staff_biz_id` bigint NOT NULL DEFAULT 0 COMMENT '员工ID',
    `org_data` text COLLATE utf8mb4_general_ci NOT NULL COMMENT '原始内容',
    `org_page_numbers` varchar(2048) COLLATE utf8mb4_general_ci NOT NULL COMMENT '原始内容对应的页码。从小到大排列，pdf、doc、ppt、pptx才会返回，docx、md、txt、excel等没有页码的返回空',
    `sheet_data` varchar(2048) COLLATE utf8mb4_general_ci NOT NULL COMMENT '当输入文件为excel时，返回当前orgdata和bigdata对应的sheet_data，因为表格的orgdata和bigdata相等，所以这里只返回一个`',
    `segment_type` varchar(64) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '文档切片类型 segment-文档切片 table-表格',
    `add_method` tinyint NOT NULL DEFAULT 0 COMMENT '（切片干预）添加方式 0:初版解析生成 1:手动添加',
    `is_temporary_deleted` tinyint NOT NULL DEFAULT 0 COMMENT '（切片干预）是否删除 0:未删除 1:已删除',
    `is_deleted` tinyint NOT NULL DEFAULT 0 COMMENT '0未删除 1已删除',
    `is_disabled` tinyint NOT NULL DEFAULT 0 COMMENT '0启用 1停用',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '响应时间',
    PRIMARY KEY (`id`, `doc_biz_id`),
    KEY `idx_biz_id` (`corp_biz_id`,`app_biz_id`,`doc_biz_id`,`business_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='org_data数据表' shardkey=doc_biz_id

CREATE TABLE `t_knowledge_schema_task` (
    `id` bigint NOT NULL AUTO_INCREMENT,
    `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
    `app_biz_id` bigint NOT NULL COMMENT '应用业务ID',
    `business_id` bigint NOT NULL COMMENT '业务ID',
    `status` int NOT NULL DEFAULT 0 COMMENT '状态(0待处理 1处理中 2处理成功 3处理失败)',
    `is_deleted` tinyint NOT NULL DEFAULT 0 COMMENT '是否删除',
    `message` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '错误信息',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (`id`,`app_biz_id`),
    KEY `idx_corp_app_biz_id` (`corp_biz_id`,`app_biz_id`,`business_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='知识库schema任务表' shardkey = app_biz_id

CREATE TABLE `t_doc_schema` (
    `id` bigint NOT NULL AUTO_INCREMENT,
    `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
    `app_biz_id` bigint NOT NULL COMMENT '应用业务ID',
    `doc_biz_id` bigint NOT NULL COMMENT '文档业务ID',
    `file_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '文件名',
    `summary` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '文档摘要',
    `vector` blob NOT NULL COMMENT '特征向量',
    `is_deleted` tinyint NOT NULL DEFAULT 0 COMMENT '是否删除',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (`id`,`app_biz_id`),
    KEY `idx_corp_app_doc_biz_id` (`corp_biz_id`,`app_biz_id`,`doc_biz_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='文档schema表' shardkey = app_biz_id

CREATE TABLE `t_doc_cluster_schema` (
    `id` bigint NOT NULL AUTO_INCREMENT,
    `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
    `app_biz_id` bigint NOT NULL COMMENT '应用业务ID',
    `business_id` bigint NOT NULL COMMENT '文档聚类业务ID',
    `version` bigint NOT NULL COMMENT '版本id，对应任务表的自增id',
    `cluster_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '目录名称',
    `summary` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '目录摘要',
    `doc_ids` TEXT NOT NULL COMMENT '文档ID列表,json格式',
    `is_deleted` tinyint NOT NULL DEFAULT 0 COMMENT '是否删除',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (`id`,`app_biz_id`),
    KEY `idx_corp_app_biz_id` (`corp_biz_id`,`app_biz_id`,`version`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='文档聚类schema表' shardkey = app_biz_id

CREATE TABLE `t_knowledge_schema` (
    `id` bigint NOT NULL AUTO_INCREMENT,
    `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
    `app_biz_id` bigint NOT NULL COMMENT '应用业务ID',
    `version` bigint NOT NULL COMMENT '版本id，对应任务表的自增id',
    `item_type` tinyint NOT NULL DEFAULT 0 COMMENT '物料类型,1:文档 2:文档聚类',
    `item_biz_id` bigint NOT NULL COMMENT '物料ID：文档业务ID或文档聚类业务ID',
    `name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '文档或者文档聚类名',
    `summary` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '文档或者文档聚类摘要',
    `is_deleted` tinyint NOT NULL DEFAULT 0 COMMENT '是否删除',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (`id`,`app_biz_id`),
    KEY `idx_corp_app_biz_id` (`corp_biz_id`,`app_biz_id`,`version`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='知识库schema表' shardkey = app_biz_id

CREATE TABLE `t_knowledge_schema_prod` (
    `id` bigint NOT NULL AUTO_INCREMENT,
    `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
    `app_biz_id` bigint NOT NULL COMMENT '应用业务ID',
    `version` bigint NOT NULL COMMENT '版本id，对应任务表的自增id',
    `item_type` tinyint NOT NULL DEFAULT 0 COMMENT '物料类型,1:文档 2:文档聚类',
    `item_biz_id` bigint NOT NULL COMMENT '物料ID：文档业务ID或文档聚类业务ID',
    `name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '文档或者文档聚类名',
    `summary` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '文档或者文档聚类摘要',
    `is_deleted` tinyint NOT NULL DEFAULT 0 COMMENT '是否删除',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (`id`,`app_biz_id`),
    KEY `idx_corp_app_biz_id` (`corp_biz_id`,`app_biz_id`,`version`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='知识库schema已发布数据表' shardkey = app_biz_id