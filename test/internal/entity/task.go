package entity

import "time"

// Task 任务
type Task struct {
	ID             uint64    `db:"id"`              // 任务ID
	UserID         uint64    `db:"user_id"`         // 机器人ID
	TaskType       uint32    `db:"task_type"`       // 任务类型
	TaskMutex      uint32    `db:"task_mutex"`      // 任务互斥
	Params         string    `db:"params"`          // 任务参数
	RetryTimes     uint32    `db:"retry_times"`     // 重试次数
	MaxRetryTimes  uint32    `db:"max_retry_times"` // 最大重试次数
	Timeout        uint32    `db:"timeout"`         // 超时时间(s)
	Runner         string    `db:"runner"`          // 执行器
	RunnerInstance string    `db:"runner_instance"` // 执行器实例
	Result         string    `db:"result"`          // 本地结果
	TraceID        string    `db:"trace_id"`        // trace id
	StartTime      time.Time `db:"start_time"`      // 任务开始执行时间
	EndTime        time.Time `db:"end_time"`        // 任务完成时间
	NextStartTime  time.Time `db:"next_start_time"` // 下次任务开始执行时间
	CreateTime     time.Time `db:"create_time"`     // 创建时间
	UpdateTime     time.Time `db:"update_time"`     // 更新时间
}

// TaskHistory 任务历史
type TaskHistory struct {
	ID             uint64    `db:"id"`              // 任务ID
	UserID         uint64    `db:"user_id"`         // 机器人ID
	TaskType       uint32    `db:"task_type"`       // 任务类型
	TaskMutex      uint32    `db:"task_mutex"`      // 任务互斥
	Params         string    `db:"params"`          // 任务参数
	RetryTimes     uint32    `db:"retry_times"`     // 重试次数
	MaxRetryTimes  uint32    `db:"max_retry_times"` // 最大重试次数
	Timeout        uint32    `db:"timeout"`         // 超时时间(s)
	Runner         string    `db:"runner"`          // 执行器
	RunnerInstance string    `db:"runner_instance"` // 执行器实例
	Result         string    `db:"result"`          // 本地结果
	IsSuccess      bool      `db:"is_success"`      // 是否成功
	TraceID        string    `db:"trace_id"`        // trace id
	StartTime      time.Time `db:"start_time"`      // 任务开始执行时间
	EndTime        time.Time `db:"end_time"`        // 任务完成时间
	NextStartTime  time.Time `db:"next_start_time"` // 下次任务开始执行时间
	CreateTime     time.Time `db:"create_time"`     // 创建时间
}
