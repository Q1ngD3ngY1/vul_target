package dao

import (
	"testing"
)

func TestAddMysqlDefaultLimit(t *testing.T) {
	tests := []struct {
		name         string
		inputSQL     string
		defaultLimit int
		expectedSQL  string
	}{
		{
			name:         "simple select without limit",
			inputSQL:     "SELECT * FROM users",
			defaultLimit: 100,
			expectedSQL:  "SELECT * FROM users LIMIT 100",
		},
		{
			name:         "select with where clause",
			inputSQL:     "SELECT * FROM users WHERE age > 18",
			defaultLimit: 50,
			expectedSQL:  "SELECT * FROM users WHERE age > 18 LIMIT 50",
		},
		{
			name:         "select with order by",
			inputSQL:     "SELECT * FROM users ORDER BY name",
			defaultLimit: 30,
			expectedSQL:  "SELECT * FROM users ORDER BY name LIMIT 30",
		},
		{
			name:         "select with group by and having",
			inputSQL:     "SELECT department, COUNT(*) FROM users GROUP BY department HAVING COUNT(*) > 5",
			defaultLimit: 200,
			expectedSQL:  "SELECT department, COUNT(*) FROM users GROUP BY department HAVING COUNT(*) > 5 LIMIT 200",
		},
		{
			name:         "select with existing limit",
			inputSQL:     "SELECT * FROM users LIMIT 10",
			defaultLimit: 100,
			expectedSQL:  "SELECT * FROM users LIMIT 10",
		},
		{
			name:         "select with existing limit and offset",
			inputSQL:     "SELECT * FROM users LIMIT 10, 20",
			defaultLimit: 100,
			expectedSQL:  "SELECT * FROM users LIMIT 10, 20",
		},
		{
			name:         "select with trailing semicolon",
			inputSQL:     "SELECT * FROM users;",
			defaultLimit: 100,
			expectedSQL:  "SELECT * FROM users LIMIT 100",
		},
		{
			name:         "select with mixed case keywords",
			inputSQL:     "SeLeCt * FrOm users WhErE age > 18 OrDeR By name",
			defaultLimit: 75,
			expectedSQL:  "SeLeCt * FrOm users WhErE age > 18 OrDeR By name LIMIT 75",
		},
		{
			name:         "non-select statement",
			inputSQL:     "UPDATE users SET name = 'John' WHERE id = 1",
			defaultLimit: 100,
			expectedSQL:  "UPDATE users SET name = 'John' WHERE id = 1",
		},
		{
			name:         "complex select with subquery",
			inputSQL:     "SELECT u.* FROM users u WHERE u.id IN (SELECT user_id FROM orders WHERE amount > 100)",
			defaultLimit: 50,
			expectedSQL:  "SELECT u.* FROM users u WHERE u.id IN (SELECT user_id FROM orders WHERE amount > 100) LIMIT 50",
		},
		{
			name:         "select with join",
			inputSQL:     "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id",
			defaultLimit: 25,
			expectedSQL:  "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id LIMIT 25",
		},
		{
			name:         "select with multiple clauses",
			inputSQL:     "SELECT u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.active = 1 GROUP BY u.name HAVING COUNT(o.id) > 0 ORDER BY COUNT(o.id) DESC",
			defaultLimit: 10,
			expectedSQL:  "SELECT u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.active = 1 GROUP BY u.name HAVING COUNT(o.id) > 0 ORDER BY COUNT(o.id) DESC LIMIT 10",
		},
		{
			name:         "select with comments",
			inputSQL:     "SELECT * FROM users -- this is a comment\nWHERE age > 18",
			defaultLimit: 100,
			expectedSQL:  "SELECT * FROM users -- this is a comment\nWHERE age > 18 LIMIT 100",
		},
		{
			name:         "select with limit in different case",
			inputSQL:     "SELECT * FROM users LiMiT 5",
			defaultLimit: 10,
			expectedSQL:  "SELECT * FROM users LiMiT 5",
		},
		{
			name:         "complex select with subqueries and join",
			inputSQL:     "select case when target_income = 0 then null else shouldincome_after_kh / target_income end as target_income_ratio, case when target_profit = 0 then null else gross_profit_kh / target_profit end as target_income_ratio from (select sum(shouldincome_after_kh) as shouldincome_after_kh, sum(gross_profit_kh) as gross_profit_kh from bnss_bg_incoming_encryption where ftime between 20250101 and 20250301 and prod_tree_bsc = '云智能') as a join (select sum(target_income) as target_income, sum(target_profit) as target_profit from dept_expense_detail where year = '2025' and month in ('01', '02', '03') and `group` = '' and prod_tree_bsc = '云智能' and pay_type = '平账后') as b on 1=1",
			defaultLimit: 100,
			expectedSQL:  "select case when target_income = 0 then null else shouldincome_after_kh / target_income end as target_income_ratio, case when target_profit = 0 then null else gross_profit_kh / target_profit end as target_income_ratio from (select sum(shouldincome_after_kh) as shouldincome_after_kh, sum(gross_profit_kh) as gross_profit_kh from bnss_bg_incoming_encryption where ftime between 20250101 and 20250301 and prod_tree_bsc = '云智能') as a join (select sum(target_income) as target_income, sum(target_profit) as target_profit from dept_expense_detail where year = '2025' and month in ('01', '02', '03') and `group` = '' and prod_tree_bsc = '云智能' and pay_type = '平账后') as b on 1=1 LIMIT 100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := addMysqlDefaultLimit(tt.inputSQL, tt.defaultLimit)
			if got != tt.expectedSQL {
				t.Errorf("AddDefaultLimit() = %v, want %v", got, tt.expectedSQL)
			}
		})
	}
}

func TestAddSqlServerDefaultTop(t *testing.T) {
	tests := []struct {
		name         string
		inputSQL     string
		defaultLimit int
		expectedSQL  string
	}{
		{
			name:         "simple select without top",
			inputSQL:     "SELECT * FROM users",
			defaultLimit: 100,
			expectedSQL:  "SELECT TOP 100 * FROM users",
		},
		{
			name:         "select distinct without top",
			inputSQL:     "SELECT DISTINCT name FROM users",
			defaultLimit: 50,
			expectedSQL:  "SELECT DISTINCT TOP 50 name FROM users",
		},
		{
			name:         "select with existing top",
			inputSQL:     "SELECT TOP 10 * FROM users",
			defaultLimit: 100,
			expectedSQL:  "SELECT TOP 10 * FROM users",
		},
		{
			name:         "select with where clause",
			inputSQL:     "SELECT name, age FROM users WHERE age > 18",
			defaultLimit: 30,
			expectedSQL:  "SELECT TOP 30 name, age FROM users WHERE age > 18",
		},
		{
			name:         "select with order by",
			inputSQL:     "SELECT * FROM users ORDER BY name",
			defaultLimit: 200,
			expectedSQL:  "SELECT TOP 200 * FROM users ORDER BY name",
		},
		{
			name:         "non-select statement",
			inputSQL:     "UPDATE users SET name = 'John' WHERE id = 1",
			defaultLimit: 100,
			expectedSQL:  "UPDATE users SET name = 'John' WHERE id = 1",
		},
		{
			name:         "select with trailing semicolon",
			inputSQL:     "SELECT * FROM users;",
			defaultLimit: 100,
			expectedSQL:  "SELECT TOP 100 * FROM users;",
		},
		{
			name:         "select with join",
			inputSQL:     "SELECT u.id, u.name FROM users u JOIN orders o ON u.id = o.user_id",
			defaultLimit: 25,
			expectedSQL:  "SELECT TOP 25 u.id, u.name FROM users u JOIN orders o ON u.id = o.user_id",
		},
		{
			name:         "select with mixed case keywords",
			inputSQL:     "SeLeCt * FrOm users",
			defaultLimit: 75,
			expectedSQL:  "SeLeCt TOP 75 * FrOm users",
		},
		{
			name:         "select with comments",
			inputSQL:     "SELECT /* comment */ * FROM users",
			defaultLimit: 100,
			expectedSQL:  "SELECT TOP 100 /* comment */ * FROM users",
		},
		{
			name:         "select with cte",
			inputSQL:     "WITH UserCTE AS (SELECT * FROM users) SELECT * FROM UserCTE",
			defaultLimit: 50,
			expectedSQL:  "WITH UserCTE AS (SELECT * FROM users) SELECT TOP 50 * FROM UserCTE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := addSqlServerDefaultTop(tt.inputSQL, tt.defaultLimit)
			if got != tt.expectedSQL {
				t.Errorf("AddDefaultTop() = %v, want %v", got, tt.expectedSQL)
			}
		})
	}
}
