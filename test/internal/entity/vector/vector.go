package entity

import (
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
)

const (
	// VectorTypeQA 向量类型问答
	VectorTypeQA = 1
	// VectorTypeSegment 向量类型分片
	VectorTypeSegment = 2
	// VectorTypeRejectedQuestion 向量类型拒答问题
	VectorTypeRejectedQuestion = 3

	// StatusSyncInit 向量待同步状态
	StatusSyncInit = 1
	// StatusSyncing 向量同步中状态
	StatusSyncing = 2
	// StatusSyncSuccess 向量同步成功状态
	StatusSyncSuccess = 3
	// StatusSyncFailed 向量同步失败状态
	StatusSyncFailed = 4

	// MaxTryTimes 最多尝试次数
	MaxTryTimes = 10
)

// VectorSync 向量同步记录
type VectorSync struct {
	ID          uint64    `db:"id"`
	Type        uint32    `db:"type"`
	RelateID    uint64    `db:"relate_id"`     // 关联ID
	Status      uint32    `db:"status"`        // 1待同步2同步中3同步成功4同步失败
	Request     string    `db:"request"`       // 同步内容
	TryTimes    uint32    `db:"try_times"`     // 尝试次数
	MaxTryTimes uint32    `db:"max_try_times"` // 最大尝试次数
	Result      string    `db:"result"`        // result
	UpdateTime  time.Time `db:"update_time"`
	CreateTime  time.Time `db:"create_time"`   // 1是问答2是分片
	ExtendedId  uint64    `db:"extended_id"`   // 扩展id, type为1(QA)时即相似问ID
	WriteSyncId uint64    `db:"write_sync_id"` // 写入时指定的sync_id,用于反查自增ID
}

// IsVectorTypeQA 是否同步问答对
func (v *VectorSync) IsVectorTypeQA() bool {
	if v == nil {
		return false
	}
	return v.Type == VectorTypeQA
}

// IsVectorTypeSegment 是否同步分片
func (v *VectorSync) IsVectorTypeSegment() bool {
	if v == nil {
		return false
	}
	return v.Type == VectorTypeSegment
}

// IsVectorTypeRejectedQuestion 是否同步拒答问题
func (v *VectorSync) IsVectorTypeRejectedQuestion() bool {
	return v.Type == VectorTypeRejectedQuestion
}

// IsAllowSync 是否允许同步
func (v *VectorSync) IsAllowSync() bool {
	if v == nil {
		return false
	}
	return v.Status == StatusSyncing
}

// SetRequest 设置请求内容
func (v *VectorSync) SetRequest(request any) {
	if v == nil {
		return
	}
	str, _ := jsonx.MarshalToString(request)
	v.Request = str
}

// IsStatusFail 同步状态是否为失败
func (v *VectorSync) IsStatusFail() bool {
	if v == nil {
		return false
	}
	return v.Status == StatusSyncFailed
}

// ReachedMaxTryTimes 达到最大同步次数
func (v *VectorSync) ReachedMaxTryTimes() bool {
	if v == nil {
		return false
	}
	return v.TryTimes >= MaxTryTimes
}

type ListVectorSyncReq struct {
	Type         uint32
	RelatedID    uint64
	WriteSyncIDs []uint64
	Status       []uint32
	StatusNotIn  []uint32
	PageSize     int32
	PageNum      int32
}
