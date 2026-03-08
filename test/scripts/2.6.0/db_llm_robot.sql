
-- randalchen start --
-- 审核表，添加索引
ALTER TABLE `t_audit` ADD INDEX  idx_relateid_type(corp_id,robot_id,relate_id,type);

-- 文档问答对表，修改字段注释
ALTER TABLE  `t_doc_qa` MODIFY COLUMN `release_status` int NOT NULL COMMENT '发布状态(2待发布 3发布中 4已发布 5发布失败 6不采纳 7审核中 8审核失败 9人工审核中 10人工审核通过 11人工审核不通过 12已过期 13超量失效 14超量失效恢复 15人工审核不通过-超量失效 16人工审核不通过-超量失效恢复 17审核失败-超量失效 18审核失败-超量失效恢复 19学习中 20学习失败 21学习失败-超量失效 22学习失败-超量失效恢复)';

-- 文档表，修改字段注释
ALTER TABLE  `t_doc` MODIFY COLUMN `status` int NOT NULL COMMENT '状态(1未生成 2生成中 3生成成功 4生成失败 5删除中 6删除成功 7审核中 8审核失败 9审核通过 10待发布 11发布中 12发布成功 13学习中 14学习失败 15更新中，合并到学习中 16更新失败，合并到学习失败 17解析中 18解析失败 19导入失败 20已过期 21超量失效 22超量失效恢复 23导入失败-超量失效 24审核失败-超量失效 25更新失败-超量失效 26创建索引失败-超量失效 27导入失败-超量失效恢复 28审核失败-超量失效恢复 29更新失败-超量失效恢复 30创建索引失败-超量失效恢复 31已过期-超量失效 32已过期-超量失效恢复 33人工申诉中 34人工申诉失败 35人工申诉失败-超量失效 36人工申诉失败-超量失效恢复)';

-- 向量同步表，添加索引
ALTER TABLE `t_vector_sync` ADD INDEX idx_relateid_type(relate_id,type);

-- randalchen end --


-- halelv 问答增加问题描述 --

ALTER TABLE `t_doc_qa`
    ADD COLUMN `question_desc` varchar(4096) NOT NULL DEFAULT '' COMMENT '问题描述' AFTER `custom_param`;

ALTER TABLE `t_release_qa`
    ADD COLUMN `question_desc` varchar(4096) NOT NULL DEFAULT '' COMMENT '问题描述' AFTER `custom_param`;

-- halelv 问答增加问题描述 --




-- zrwang start --


-- 评测任务结果表 添加消息记录关联字段
ALTER TABLE t_evaluate_test_record
    ADD COLUMN record_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL  DEFAULT '' COMMENT '记录ID',
    ADD COLUMN related_record_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL  DEFAULT '' COMMENT '关联记录ID';


-- 评测任务结果表 添加索引
ALTER TABLE t_evaluate_test_record
    ADD INDEX idx_set_id (set_id);
-- zrwang end --



-- richiewfshi/nickzhwang start --

-- 同义词表
CREATE TABLE `t_synonyms` (
    `id` bigint NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    `synonyms_id` bigint NOT NULL COMMENT '对外ID',
    `robot_id` bigint NOT NULL COMMENT '应用ID',
    `corp_id` bigint NOT NULL COMMENT '企业ID',
    `category_id` int NOT NULL COMMENT '分类ID',
    `parent_id` bigint NOT NULL COMMENT '关联的标准词ID',
    `word` varchar(50) COLLATE utf8mb4_bin DEFAULT '' COMMENT '标准词或者同义词',
    `word_md5` CHAR(32) COLLATE utf8mb4_bin NOT NULL COMMENT '标准词或者同义词的MD5',
    `release_status` int NOT NULL COMMENT '发布状态(1 待发布 2 发布中 3 已发布 4 发布失败)',
    `next_action` tinyint NOT NULL COMMENT '最后操作：1新增 2修改 3删除 4发布',
    `is_deleted` bigint NOT NULL DEFAULT '0' COMMENT '是否删除',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`, `robot_id`),
    KEY `idx_synonyms_id` (`synonyms_id`),
    KEY `idx_robot_id` (`robot_id`),
    KEY `idx_robot_parent_id` (`robot_id`,`parent_id`),
    KEY `idx_robot_synonyms_id` (`robot_id`, `synonyms_id`),
    UNIQUE KEY `uk_word_md5` (`robot_id`,`word_md5`,`is_deleted`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMMENT = '同义词词表' /*!50100 PARTITION BY HASH (`robot_id`) PARTITIONS 100 */;

-- 同义词发布表
CREATE TABLE `t_release_synonyms` (
    `id` bigint NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    `synonyms_id` bigint NOT NULL COMMENT '对外ID',
    `robot_id` bigint NOT NULL COMMENT '应用ID',
    `version_id` bigint NOT NULL COMMENT '版本ID',
    `corp_id` bigint NOT NULL COMMENT '企业ID',
    `parent_id` bigint NOT NULL COMMENT '关联的标准词ID',
    `word` varchar(50) COLLATE utf8mb4_bin DEFAULT '' COMMENT '标准词或者同义词',
    `word_md5` CHAR(32) COLLATE utf8mb4_bin NOT NULL COMMENT '标准词或者同义词的MD5',
    `synonyms` text NOT NULL COMMENT '标准词下面的同义词快照，以逗号相隔',
    `release_status` int NOT NULL COMMENT '发布状态(1 待发布 2 发布中 3 已发布 4 发布失败)',
    `action` tinyint NOT NULL COMMENT '操作行为：1新增 2修改 3删除 4发布',
    `message` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '失败原因',
    `is_deleted` bigint NOT NULL DEFAULT '0' COMMENT '是否删除',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`, `robot_id`),
    KEY `idx_synonyms_id` (`synonyms_id`),
    KEY `idx_robot_version_id` (`robot_id`,`version_id`),
    KEY `idx_robot_parent_id` (`robot_id`,`parent_id`),
    KEY `idx_robot_synonyms_id` (`robot_id`, `synonyms_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMMENT = '同义词发布表'
/*!50100 PARTITION BY HASH (`robot_id`) PARTITIONS 100 */;

-- 同义词生产表
CREATE TABLE `t_synonyms_prod` (
   `id` bigint NOT NULL AUTO_INCREMENT COMMENT '自增ID',
   `synonyms_id` bigint NOT NULL COMMENT '对外ID',
   `robot_id` bigint NOT NULL COMMENT '应用ID',
   `corp_id` bigint NOT NULL COMMENT '企业ID',
   `parent_id` bigint NOT NULL COMMENT '关联的标准词ID',
   `word` varchar(50) COLLATE utf8mb4_bin DEFAULT '' COMMENT '标准词或者同义词',
   `word_md5` CHAR(32) COLLATE utf8mb4_bin NOT NULL COMMENT '标准词或者同义词的MD5',
   `is_deleted` bigint NOT NULL DEFAULT '0' COMMENT '是否删除',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   PRIMARY KEY (`id`, `robot_id`),
   KEY `idx_synonyms_id` (`synonyms_id`),
   KEY `idx_robot_id` (`robot_id`),
   KEY `idx_robot_parent_id` (`robot_id`,`parent_id`),
   KEY `idx_robot_synonyms_id` (`robot_id`, `synonyms_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMMENT = '同义词生产词表' /*!50100 PARTITION BY HASH (`robot_id`) PARTITIONS 100 */;

-- 同义词任务表
CREATE TABLE `t_synonyms_task` (
   `id` bigint NOT NULL AUTO_INCREMENT,
   `corp_id` bigint NOT NULL COMMENT '企业ID',
   `robot_id` bigint NOT NULL COMMENT '机器人ID',
   `create_staff_id` bigint NOT NULL COMMENT '员工ID',
   `params` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '任务参数',
   `status` tinyint NOT NULL COMMENT '任务状态(1 未启动 2 流程中 3 任务完成 4 任务失败)',
   `message` varchar(5120) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '任务信息',
   `file_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '文件名称',
   `cos_url` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT 'cos文件地址(客户上传)',
   `error_cos_url` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT 'cos文件地址(错误标注文件)',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间', PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='同义词任务管理表'


-- 新表Doc分类表
CREATE TABLE `t_doc_category` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `business_id` bigint NOT NULL DEFAULT '0' COMMENT '对外ID',
    `robot_id` bigint unsigned NOT NULL COMMENT '企业ID',
    `corp_id` bigint unsigned NOT NULL,
    `name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '名称',
    `order_num` int NOT NULL COMMENT '排序',
    `parent_id` bigint unsigned NOT NULL DEFAULT '0' COMMENT '父级分类 ID',
    `is_deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT '0未删除 1已删除',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uni_business_id` (`business_id`) USING BTREE,
    KEY `idx_corp_id` (`corp_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='文档分类表'

-- 新表Synonyms分类表
CREATE TABLE `t_synonyms_category` (
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `business_id` bigint NOT NULL COMMENT '对外ID',
    `robot_id` bigint unsigned NOT NULL COMMENT '企业ID',
    `corp_id` bigint unsigned NOT NULL,
    `name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '名称',
    `order_num` int NOT NULL COMMENT '排序',
    `parent_id` bigint unsigned NOT NULL DEFAULT '0' COMMENT '父级分类 ID',
    `is_deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT '0未删除 1已删除',
    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uni_business_id` (`business_id`) USING BTREE,
    KEY `idx_corp_id` (`corp_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='同义词分类表'


-- 更新Doc表新增分类ID字段
ALTER TABLE t_doc ADD COLUMN `category_id` bigint DEFAULT 0 COMMENT '分类ID';

-- 刷历史数据
-- 直接使用QA分类里的“未分类”数据初始化Doc分类
insert into t_doc_category (business_id, robot_id, corp_id, name, order_num, parent_id, is_deleted, create_time, update_time)
select A.business_id, A.robot_id, A.corp_id, A.name, A.order_num, A.parent_id, A.is_deleted, A.create_time, A.update_time
from t_doc_qa_category A
         left join t_doc_category B
                   on A.robot_id = B.robot_id AND A.name = B.name AND A.parent_id = B.parent_id AND A.is_deleted = B.is_deleted
where A.name = '未分类' AND A.parent_id = 0 AND A.is_deleted = 0 AND B.robot_id is null -- B中不存在的

-- 直接使用QA分类里的“未分类”数据初始化Synonyms分类
insert into t_synonyms_category (business_id, robot_id, corp_id, name, order_num, parent_id, is_deleted, create_time, update_time)
select A.business_id, A.robot_id, A.corp_id, A.name, A.order_num, A.parent_id, A.is_deleted, A.create_time, A.update_time
from t_doc_qa_category A
         left join t_synonyms_category B
                   on A.robot_id = B.robot_id AND A.name = B.name AND A.parent_id = B.parent_id AND A.is_deleted = B.is_deleted
where A.name = '未分类' AND A.parent_id = 0 AND A.is_deleted = 0 AND B.robot_id is null -- B中不存在的

-- 更新文档表的categor_id，初始化全部的 doc分类 --> 未分类
UPDATE t_doc AS D
JOIN t_doc_category AS C
ON D.robot_id = C.robot_id AND D.corp_id = C.corp_id AND C.name = '未分类' AND C.parent_id = 0 AND C.is_deleted = 0
SET D.category_id = C.id, D.update_time=D.update_time -- 不更新update_time字段
WHERE D.is_deleted = 0

-- 更新同义词表的categor_id，初始化全部的 synonyms分类 --> 未分类
UPDATE t_synonyms AS D
JOIN t_synonyms_category AS C
ON D.robot_id = C.robot_id AND D.corp_id = C.corp_id AND C.name = '未分类' AND C.parent_id = 0 AND C.is_deleted = 0
SET D.category_id = C.id, D.update_time=D.update_time -- 不更新update_time字段
WHERE D.is_deleted = 0

-- 增加索引
ALTER TABLE `t_doc_qa_category` ADD INDEX idx_corp_robot(corp_id, robot_id);
ALTER TABLE `t_doc_category` ADD INDEX idx_corp_robot(corp_id, robot_id);
ALTER TABLE `t_synonyms_category` ADD INDEX idx_corp_robot(corp_id, robot_id);
-- richiewfshi/nickzhwang start --
