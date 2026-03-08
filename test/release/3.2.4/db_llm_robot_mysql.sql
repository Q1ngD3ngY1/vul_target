-- 3.0 重构
-- 解决 t_knowledge_task 表数据多时查询慢的问题。优化思路是：
-- 以lane_name开头（高选择性等值查询），快速过滤大部分数据。
-- 包含task_mutex（等值）、task_type（IN列表）、next_start_time（范围查询），能高效缩小扫描范围。
-- 包含runner和user_id：runner用于OR条件过滤，user_id覆盖GROUP BY和SELECT，减少回表。
-- 索引覆盖了WHERE条件中的关键列，但retry_times < max_retry_times和runner_instance仍需回表检查（因涉及列较多，索引过长不现实）。
CREATE INDEX idx_lane_task_time_user ON t_knowledge_task (lane_name, task_mutex, task_type, next_start_time, runner, user_id);

