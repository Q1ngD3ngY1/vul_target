package segment

type EsDocBigData struct {
	RobotID   uint64 `json:"robot_id"`    // 机器人ID
	DocID     uint64 `json:"doc_id"`      // 业务的文档ID
	BigDataID string `json:"big_data_id"` // BigData的ID
	BigStart  int32  `json:"big_start"`   // BigData 分片起始索引
	BigEnd    int32  `json:"big_end"`     // BigData 分片结束索引
	BigString string `json:"big_string"`  // BigData的内容
	IsDeleted int    `json:"is_deleted"`  // 标记删除
}
