-- 文档切片图片表
CREATE TABLE `t_doc_segment_image` (
   `id` bigint NOT NULL AUTO_INCREMENT,
   `image_id` bigint NOT NULL DEFAULT '0' COMMENT '图片ID',
   `segment_id` bigint NOT NULL DEFAULT '0' COMMENT '切片ID',
   `doc_id` bigint NOT NULL COMMENT '文档ID',
   `robot_id` bigint NOT NULL COMMENT '机器人ID',
   `corp_id` bigint NOT NULL COMMENT '企业ID',
   `staff_id` bigint NOT NULL DEFAULT '0' COMMENT '员工ID',
   `original_url` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '原始url',
   `external_url` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '对外url',
   `is_deleted` int NOT NULL DEFAULT '0' COMMENT '1未删除 2已删除',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '响应时间',
   PRIMARY KEY (`id`, `robot_id`),
   KEY `idx_robot_segment_id` (`robot_id`, `segment_id`),
   KEY `idx_robot_doc_id` (`robot_id`, `doc_id`),
   UNIQUE KEY `idx_robot_image_segment_id` (`robot_id`, `image_id`, `segment_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '文档分片图片表'
PARTITION BY HASH (`robot_id`) PARTITIONS 8;

-- 实时文档切片图片表
CREATE TABLE `t_realtime_doc_segment_image` (
   `id` bigint NOT NULL AUTO_INCREMENT,
   `image_id` bigint NOT NULL DEFAULT '0' COMMENT '图片ID',
   `segment_id` bigint NOT NULL DEFAULT '0' COMMENT '切片ID',
   `doc_id` bigint NOT NULL COMMENT '文档ID',
   `robot_id` bigint NOT NULL COMMENT '机器人ID',
   `corp_id` bigint NOT NULL COMMENT '企业ID',
   `staff_id` bigint NOT NULL DEFAULT '0' COMMENT '员工ID',
   `original_url` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '原始url',
   `external_url` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '对外url',
   `is_deleted` int NOT NULL DEFAULT '0' COMMENT '1未删除 2已删除',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '响应时间',
   PRIMARY KEY (`id`, `robot_id`),
   KEY `idx_robot_segment_id` (`robot_id`, `segment_id`),
   KEY `idx_robot_doc_id` (`robot_id`, `doc_id`),
   UNIQUE KEY `idx_robot_image_segment_id` (`robot_id`, `image_id`, `segment_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '实时文档切片图片表'
PARTITION BY HASH (`robot_id`) PARTITIONS 8;