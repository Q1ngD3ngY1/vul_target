package database

import (
	"testing"
)

func Test_addMysqlDefaultLimit(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		defaultLimit int
		want         string
	}{
		{
			name:         "基本SELECT查询-无LIMIT",
			sql:          "SELECT * FROM users",
			defaultLimit: 100,
			want:         "SELECT * FROM users LIMIT 100",
		},
		{
			name:         "SELECT查询-已有LIMIT",
			sql:          "SELECT * FROM users LIMIT 50",
			defaultLimit: 100,
			want:         "SELECT * FROM users LIMIT 50",
		},
		{
			name:         "SELECT查询-已有LIMIT和逗号格式",
			sql:          "SELECT * FROM users LIMIT 10, 20",
			defaultLimit: 100,
			want:         "SELECT * FROM users LIMIT 10, 20",
		},
		{
			name:         "SELECT查询-已有LIMIT和OFFSET",
			sql:          "SELECT * FROM users LIMIT 100 OFFSET 0",
			defaultLimit: 100,
			want:         "SELECT * FROM users LIMIT 100 OFFSET 0",
		},
		{
			name:         "SELECT查询-已有LIMIT和OFFSET（多个空格）",
			sql:          "SELECT * FROM users LIMIT  100  OFFSET  50",
			defaultLimit: 100,
			want:         "SELECT * FROM users LIMIT  100  OFFSET  50",
		},
		{
			name:         "SELECT查询-带WHERE条件-无LIMIT",
			sql:          "SELECT * FROM users WHERE age > 18",
			defaultLimit: 100,
			want:         "SELECT * FROM users WHERE age > 18 LIMIT 100",
		},
		{
			name:         "SELECT查询-带ORDER BY-无LIMIT",
			sql:          "SELECT * FROM users ORDER BY created_at DESC",
			defaultLimit: 100,
			want:         "SELECT * FROM users ORDER BY created_at DESC LIMIT 100",
		},
		{
			name:         "SELECT查询-带WHERE和ORDER BY-无LIMIT",
			sql:          "SELECT * FROM users WHERE status = 'active' ORDER BY name ASC",
			defaultLimit: 100,
			want:         "SELECT * FROM users WHERE status = 'active' ORDER BY name ASC LIMIT 100",
		},
		{
			name:         "SELECT查询-末尾有分号-无LIMIT",
			sql:          "SELECT * FROM users;",
			defaultLimit: 100,
			want:         "SELECT * FROM users LIMIT 100",
		},
		{
			name:         "SELECT查询-末尾有分号和空格-无LIMIT",
			sql:          "SELECT * FROM users ; ",
			defaultLimit: 100,
			want:         "SELECT * FROM users LIMIT 100",
		},
		{
			name:         "SELECT DISTINCT查询-无LIMIT",
			sql:          "SELECT DISTINCT store_name, store_code FROM intable2",
			defaultLimit: 100,
			want:         "SELECT DISTINCT store_name, store_code FROM intable2 LIMIT 100",
		},
		{
			name:         "SELECT DISTINCT查询-带WHERE和ORDER BY-无LIMIT",
			sql:          "SELECT DISTINCT store_name, store_code FROM intable2 WHERE period = 202510 ORDER BY store_name",
			defaultLimit: 100,
			want:         "SELECT DISTINCT store_name, store_code FROM intable2 WHERE period = 202510 ORDER BY store_name LIMIT 100",
		},
		{
			name:         "复杂查询-带子查询-无LIMIT",
			sql:          "SELECT * FROM (SELECT id, name FROM users) AS subquery",
			defaultLimit: 100,
			want:         "SELECT * FROM (SELECT id, name FROM users) AS subquery LIMIT 100",
		},
		{
			name:         "非SELECT查询-INSERT",
			sql:          "INSERT INTO users (name) VALUES ('test')",
			defaultLimit: 100,
			want:         "INSERT INTO users (name) VALUES ('test')",
		},
		{
			name:         "非SELECT查询-UPDATE",
			sql:          "UPDATE users SET name = 'test' WHERE id = 1",
			defaultLimit: 100,
			want:         "UPDATE users SET name = 'test' WHERE id = 1",
		},
		{
			name:         "非SELECT查询-DELETE",
			sql:          "DELETE FROM users WHERE id = 1",
			defaultLimit: 100,
			want:         "DELETE FROM users WHERE id = 1",
		},
		{
			name:         "大小写混合-select",
			sql:          "select * from users",
			defaultLimit: 100,
			want:         "select * from users LIMIT 100",
		},
		{
			name:         "大小写混合-SELECT",
			sql:          "SELECT * FROM users",
			defaultLimit: 100,
			want:         "SELECT * FROM users LIMIT 100",
		},
		{
			name:         "大小写混合-Select",
			sql:          "Select * From users",
			defaultLimit: 100,
			want:         "Select * From users LIMIT 100",
		},
		{
			name:         "已有LIMIT-大小写混合",
			sql:          "SELECT * FROM users Limit 50",
			defaultLimit: 100,
			want:         "SELECT * FROM users Limit 50",
		},
		{
			name:         "已有LIMIT OFFSET-大小写混合",
			sql:          "SELECT * FROM users Limit 100 Offset 20",
			defaultLimit: 100,
			want:         "SELECT * FROM users Limit 100 Offset 20",
		},
		{
			name:         "Badcase修复-重复LIMIT问题",
			sql:          "SELECT DISTINCT store_name, store_code, dish_code, dish_name FROM intable2 WHERE store_code = '115449' AND period = 202510 LIMIT 100 OFFSET 0",
			defaultLimit: 100,
			want:         "SELECT DISTINCT store_name, store_code, dish_code, dish_name FROM intable2 WHERE store_code = '115449' AND period = 202510 LIMIT 100 OFFSET 0",
		},
		{
			name:         "多行SQL-无LIMIT",
			sql:          "SELECT *\nFROM users\nWHERE age > 18\nORDER BY name",
			defaultLimit: 100,
			want:         "SELECT *\nFROM users\nWHERE age > 18\nORDER BY name LIMIT 100",
		},
		{
			name:         "LIMIT在末尾有空格",
			sql:          "SELECT * FROM users LIMIT 50  ",
			defaultLimit: 100,
			want:         "SELECT * FROM users LIMIT 50  ",
		},
		{
			name:         "LIMIT OFFSET在末尾有空格",
			sql:          "SELECT * FROM users LIMIT 100 OFFSET 0  ",
			defaultLimit: 100,
			want:         "SELECT * FROM users LIMIT 100 OFFSET 0  ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := addMysqlDefaultLimit(tt.sql, tt.defaultLimit)
			if got != tt.want {
				t.Errorf("addMysqlDefaultLimit() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_addSqlServerDefaultTop(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		defaultLimit int
		want         string
	}{
		{
			name:         "基本SELECT查询-无TOP",
			sql:          "SELECT * FROM users",
			defaultLimit: 100,
			want:         "SELECT TOP 100 * FROM users",
		},
		{
			name:         "SELECT查询-已有TOP",
			sql:          "SELECT TOP 50 * FROM users",
			defaultLimit: 100,
			want:         "SELECT TOP 50 * FROM users",
		},
		{
			name:         "SELECT DISTINCT查询-无TOP",
			sql:          "SELECT DISTINCT name FROM users",
			defaultLimit: 100,
			want:         "SELECT DISTINCT TOP 100 name FROM users",
		},
		{
			name:         "SELECT DISTINCT查询-已有TOP",
			sql:          "SELECT DISTINCT TOP 50 name FROM users",
			defaultLimit: 100,
			want:         "SELECT DISTINCT TOP 50 name FROM users",
		},
		{
			name:         "非SELECT查询-INSERT",
			sql:          "INSERT INTO users (name) VALUES ('test')",
			defaultLimit: 100,
			want:         "INSERT INTO users (name) VALUES ('test')",
		},
		{
			name:         "大小写混合-select",
			sql:          "select * from users",
			defaultLimit: 100,
			want:         "SELECT TOP 100 * from users",
		},
		{
			name:         "大小写混合-SELECT DISTINCT",
			sql:          "Select Distinct name from users",
			defaultLimit: 100,
			want:         "SELECT DISTINCT TOP 100 name from users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := addSqlServerDefaultTop(tt.sql, tt.defaultLimit)
			if got != tt.want {
				t.Errorf("addSqlServerDefaultTop() = %v, want %v", got, tt.want)
			}
		})
	}
}
