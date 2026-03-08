alter table taskinfo add column `enable_scope` int not null default 1 comment '网页解析任务勾选生效范围';
alter table docinfo add column `enable_scope` int not null default 1 comment '文档生效范围';
