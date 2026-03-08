package segment

const (

	// SegmentStatusInit 未处理
	SegmentStatusInit = uint32(1)
	// SegmentStatusDoing 处理中
	SegmentStatusDoing = uint32(2)
	// SegmentStatusDone 处理完成
	SegmentStatusDone = uint32(3)
	// SegmentStatusFail 处理失败
	SegmentStatusFail = uint32(4)
	// SegmentStatusCreatedQa 切片生成问答任务中,标识任务中已完成的切片,任务结束后更新回SegmentStatusDone
	SegmentStatusCreatedQa = uint32(5)

	// SegmentIsNotDeleted 分片未删除
	SegmentIsNotDeleted = 1
	// SegmentIsDeleted 分片删除
	SegmentIsDeleted = 2

	// SegmentImageVectorIsNotDeleted 分片未删除
	SegmentImageVectorIsNotDeleted = 0
	// SegmentImageVectorIsDeleted 分片删除
	SegmentImageVectorIsDeleted = 1

	// SegmentReleaseStatusInit 未发布
	SegmentReleaseStatusInit = uint32(2)
	// SegmentReleaseStatusIng 发布中
	SegmentReleaseStatusIng = uint32(3)
	// SegmentReleaseStatusSuccess 已发布
	SegmentReleaseStatusSuccess = uint32(4)
	// SegmentReleaseStatusFail 发布失败
	SegmentReleaseStatusFail = uint32(5)
	// SegmentReleaseStatusNotRequired 不需要发布
	SegmentReleaseStatusNotRequired = uint32(6)

	// SegmentTypeQA 用于生成QA
	SegmentTypeQA = 1
	// SegmentTypeIndex 用于生成索引
	SegmentTypeIndex = 2
	// SegmentTypeQAAndIndex 用于生成QA和索引
	SegmentTypeQAAndIndex = 3

	// SegmentTypeSegment 文档切片
	SegmentTypeSegment = "segment"
	// SegmentTypeTable 表格切片
	SegmentTypeTable = "table"
	// SegmentTypeText2SQLMeta text2sql 的meta分片
	SegmentTypeText2SQLMeta = "text2sql_meta"
	// SegmentTypeText2SQLContent text2sql 的内容分片
	SegmentTypeText2SQLContent = "text2sql_content"

	// SegNextActionAdd 新增
	SegNextActionAdd = uint32(1)
	// SegNextActionUpdate 更新
	SegNextActionUpdate = uint32(2)
	// SegNextActionDelete 删除
	SegNextActionDelete = uint32(3)
	// SegNextActionPublish 发布
	SegNextActionPublish = uint32(4)
	// SegMaxTimestamp 最大过期时间
	SegMaxTimestamp int64 = 2461420800
)
