



ALTER TABLE t_doc
ADD COLUMN is_downloadable TINYINT NOT NULL DEFAULT 0 COMMENT '0:不可下载,1:可下载';

