-- 3.0 重构，知识库检索配置历史记录表
CREATE TABLE `t_knowledge_config_history` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `corp_biz_id` bigint NOT NULL COMMENT '企业业务ID',
  `knowledge_biz_id` bigint NOT NULL COMMENT '知识库业务ID',
  `app_biz_id` bigint NOT NULL COMMENT '应用ID',
  `type` tinyint NOT NULL COMMENT '类型，0第三方权限接口配置',
  `version_id` bigint NOT NULL COMMENT '版本ID',
  `release_json` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '发布应用配置',
  `is_release` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否发布',
  `is_deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否删除(0未删除 1已删除）',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`,`knowledge_biz_id`),
  KEY `idx_knowledge_config_history` (`corp_biz_id`,`knowledge_biz_id`,`app_biz_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='知识库配置历史记录' shardkey=knowledge_biz_id;

-- 3.0 重构，知识库检索配置表结构变更
ALTER TABLE
  `t_knowledge_config`
MODIFY
  COLUMN `config` longtext CHARACTER SET `utf8mb4` COLLATE `utf8mb4_general_ci` NOT NULL COMMENT '配置json内容'
AFTER
  `type`,
ADD
  COLUMN `app_biz_id` bigint NOT NULL DEFAULT 0 COMMENT '业务ID',
ADD
  COLUMN `preview_config` longtext CHARACTER SET `utf8mb4` COLLATE `utf8mb4_general_ci` NULL COMMENT '预览配置',
  DROP INDEX `uk_biz_type`,
ADD
  UNIQUE `uk_biz_type` USING btree (
    `app_biz_id`,
    `corp_biz_id`,
    `knowledge_biz_id`,
    `type`
  )