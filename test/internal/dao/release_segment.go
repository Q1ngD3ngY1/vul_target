package dao

import (
	"context"
	"fmt"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

const (
	releaseSegmentFields = `
		id,robot_id,corp_id,staff_id,doc_id,segment_id,version_id,file_type,title,
		page_content,org_data,split_model,status,
		release_status,message,is_deleted,action,batch_id,rich_text_index,start_index,end_index,update_time,
		create_time,is_allow_release,attr_labels,expire_time
	`
	createReleaseSegment = `
		INSERT INTO 
		    t_release_segment (%s) 
		VALUES 
		    (null,:robot_id,:corp_id,:staff_id,:doc_id,:segment_id,:version_id,:file_type,:title,
			:page_content,:org_data,:split_model,:status,:release_status,:message,:is_deleted,:action,
		    :batch_id,:rich_text_index,:start_index,:end_index,:update_time,:create_time,:is_allow_release,
		    :attr_labels,:expire_time)
	`
	getReleaseModifySegment = `
		SELECT 
			%s 
		FROM 
		    t_release_segment 
		WHERE 
		    corp_id = ? AND robot_id = ? AND version_id = ? %s  
	`
	getReleaseSegmentCountByVersion = `
		SELECT 
			count(*) 
		FROM 
		    t_release_segment 
		WHERE 
			 robot_id = ? AND version_id = ? %s
	`
	getReleaseSegmentByVersion = `
		SELECT 
			%s 
		FROM 
		    t_release_segment 
		WHERE 
		    robot_id = ? AND version_id = ? %s 
		LIMIT ?,?
	`
	getReleaseSegment = `
		SELECT 
			DISTINCT(doc_id) 
		FROM 
		    t_release_segment 
		WHERE 
		    robot_id = ? AND version_id = ? AND corp_id = ? AND doc_id != 0  
    `
	publishReleaseSeg = `
		UPDATE 
			t_release_segment 
		SET 
		    update_time = :update_time, 
		    release_status = :release_status, 
		    message = :message 
		WHERE 
		    id = :id
	`
)

// GetReleaseModifySegment 获取版本改动的segment
func (d *dao) GetReleaseModifySegment(ctx context.Context, release *model.Release,
	segments []*model.DocSegmentExtend) (
	map[uint64]*model.ReleaseSegment, error) {
	args := make([]any, 0, 3+len(segments))
	args = append(args, release.CorpID, release.RobotID, release.ID)
	condition := "AND 1=1"
	if len(segments) > 0 {
		condition = fmt.Sprintf("AND segment_id IN (%s)", placeholder(len(segments)))
		for _, segment := range segments {
			args = append(args, segment.ID)
		}
	}
	querySQL := fmt.Sprintf(getReleaseModifySegment, releaseSegmentFields, condition)
	list := make([]*model.ReleaseSegment, 0)
	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取版本改动的segment失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	modifySegment := make(map[uint64]*model.ReleaseSegment, 0)
	for _, item := range list {
		modifySegment[item.SegmentID] = item
	}
	return modifySegment, nil
}

// GetModifySegmentCount 获取版本改动segment数量
func (d *dao) GetModifySegmentCount(ctx context.Context, robotID, versionID uint64, action uint32) (uint64, error) {
	args := make([]any, 0, 3)
	args = append(args, robotID, versionID)
	condition := ""
	if action != 0 {
		condition = fmt.Sprintf("%s%s", condition, " AND action = ? ")
		args = append(args, action)
	}
	var total uint64
	querySQL := fmt.Sprintf(getReleaseSegmentCountByVersion, condition)
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取版本改动segment数量失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, err
	}
	return total, nil
}

// GetModifySegmentList 获取版本改动segment范围
func (d *dao) GetModifySegmentList(ctx context.Context, robotID, versionID uint64, actions []uint32,
	page, pageSize uint32) ([]*model.ReleaseSegment, error) {
	args := make([]any, 0, 5)
	args = append(args, robotID, versionID)
	condition := ""
	if len(actions) > 0 {
		condition = fmt.Sprintf("%s AND action IN (%s)", condition, placeholder(len(actions)))
		for _, action := range actions {
			args = append(args, action)
		}
	}
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	querySQL := fmt.Sprintf(getReleaseSegmentByVersion, releaseSegmentFields, condition)
	modifySegments := make([]*model.ReleaseSegment, 0)
	if err := d.db.QueryToStructs(ctx, &modifySegments, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取版本改动segment范围失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return modifySegments, nil
}
