package dao

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/go-comm/utils"
)

const (
	realtimeDocSegmentFields = `
		id,segment_id,session_id,doc_id,robot_id,corp_id,staff_id,doc_id,file_type,segment_type,
		title,page_content,org_data,split_model,is_sync_knowledge,is_deleted,rich_text_index,start_index,
		end_index,linker_keep,big_data_id, big_start_index, big_end_index, create_time,update_time`

	createRealtimeSegment = `
		INSERT INTO t_realtime_doc_segment (%s)
		VALUES (null,:segment_id,:session_id,:doc_id,:robot_id,:corp_id,:staff_id,:doc_id,:file_type,:segment_type,
		:title,:page_content,:org_data,:split_model,:is_sync_knowledge,:is_deleted,:rich_text_index,:start_index,
		:end_index,:linker_keep,:big_data_id, :big_start_index, :big_end_index, now(),now())`

	// 先简单实现，后续再优化
	getOrgDataByDocID = `
		SELECT 
		    org_data 
		FROM 
		    t_realtime_doc_segment 
		WHERE 
		    doc_id = ? AND is_deleted = 1 
		LIMIT 
		     ?,?
	`

	getCountByDocID = `
		SELECT 
		    count(*) 
		FROM 
		    t_realtime_doc_segment 
		WHERE 
		    doc_id = ? AND is_deleted = 1 
	`
)

// GetCountByDocID 根据DocID 获取 t_realtime_doc_segment 中的文档片段数量
func (d *dao) GetCountByDocID(ctx context.Context, docID uint64) (count int, err error) {
	args := make([]any, 0)
	args = append(args, docID)
	if err := d.db.Get(ctx, &count, getCountByDocID, args...); err != nil {
		log.ErrorContextf(ctx, "GetCountByDocID sql:%s args:%+v err:%+v", getCountByDocID, args, err)
		return count, err
	}
	log.InfoContextf(ctx, "GetCountByDocID sql:%s args:%+v res:%d",
		getCountByDocID, args, count)
	return count, nil

}

// GetOrgDataListByDocID 根据DocID 获取 t_realtime_doc_segment 中的文档片段
func (d *dao) GetOrgDataListByDocID(ctx context.Context,
	docID, offset, limit uint64) ([]string, error) {
	args := make([]any, 0)
	args = append(args, docID, offset, limit)
	res := make([]string, 0)
	if err := d.db.Select(ctx, &res, getOrgDataByDocID, args...); err != nil {
		log.ErrorContextf(ctx, "GetOrgDataListByDocID sql:%s args:%+v err:%+v", getOrgDataByDocID, args, err)
		return res, err
	}
	log.InfoContextf(ctx, "GetOrgDataListByDocID sql:%s args:%+v res:%s",
		getOrgDataByDocID, args, utils.ToJsonString(res))
	return res, nil
}
