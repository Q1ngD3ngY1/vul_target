package document

import "time"

type TextDiff struct {
	Type int
	Text string
}

type DocDiffData struct { // 文档对比ID
	CorpBizID  uint64    `gorm:"column:corp_biz_id"`  // 企业ID
	RobotBizID uint64    `gorm:"column:robot_biz_id"` // 应用ID
	DiffBizID  uint64    `gorm:"column:diff_biz_id"`  // 文档比对任务id
	DiffIndex  int       `gorm:"column:diff_index"`   // 文档diff片段序号
	DiffData   string    `gorm:"column:diff_data"`    // 文档片段diff详细内容,json格式
	IsDeleted  bool      `gorm:"column:is_deleted"`   // 是否删除
	CreateTime time.Time `gorm:"column:create_time"`  // 创建时间
	UpdateTime time.Time `gorm:"column:update_time"`  // 更新时间
}

const (
	DocDiffDataTblColCorpBizId  = "corp_biz_id"
	DocDiffDataTblColRobotBizId = "robot_biz_id"
	DocDiffDataTblColDiffBizId  = "diff_biz_id"
	DocDiffDataTblColDiffIndex  = "diff_index"
	DocDiffDataTblColDiffData   = "diff_data"
	DocDiffDataTblColIsDeleted  = "is_deleted"
	DocDiffDataTblColCreateTime = "create_time"
	DocDiffDataTblColUpdateTime = "update_time"
	DocDiffDataTableMaxPageSize = 1000
)

type DocDiffDataFilter struct {
	CorpBizId      uint64 // 企业 ID
	RobotBizId     uint64
	DiffBizId      uint64 // 文档对比任务ID
	IsDeleted      *bool
	Offset         int
	Limit          int
	OrderColumn    []string
	OrderDirection []string
}
