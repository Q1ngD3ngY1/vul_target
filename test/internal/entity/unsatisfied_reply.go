package entity

const (
	// UnsatisfiedReplyStatusWait 不满意回复状态-待处理
	UnsatisfiedReplyStatusWait = iota
	// UnsatisfiedReplyStatusReject 不满意回复状态-已拒答
	UnsatisfiedReplyStatusReject
	// UnsatisfiedReplyStatusIgnore 不满意回复状态-已忽略
	UnsatisfiedReplyStatusIgnore
	// UnsatisfiedReplyStatusPass 不满意回复状态-已添加
	UnsatisfiedReplyStatusPass

	// UnsatisfiedReplyStatusSimilar 不满意回复状态-已添加
	UnsatisfiedReplyStatusSimilar
)
