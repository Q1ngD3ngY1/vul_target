package dao

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"gorm.io/gorm"
)

var globalDocSegmentDao *DocSegmentDao

const (
	DocSegmentTblColID            = "id"
	DocSegmentTblColBusinessID    = "business_id"
	DocSegmentTblColRobotID       = "robot_id"
	DocSegmentTblColCorpID        = "corp_id"
	DocSegmentTblColStaffId       = "staff_id"
	DocSegmentTblColDocId         = "doc_id"
	DocSegmentTblColFileType      = "file_type"
	DocSegmentTblColTitle         = "title"
	DocSegmentTblColPageContent   = "page_content"
	DocSegmentTblColOrgData       = "org_data"
	DocSegmentTblColOrgDataBizID  = "org_data_biz_id"
	DocSegmentTblColOutputs       = "outputs"
	DocSegmentTblColCostTime      = "cost_time"
	DocSegmentTblColSplitModel    = "split_model"
	DocSegmentTblColStatus        = "status"
	DocSegmentTblColReleaseStatus = "release_status"
	DocSegmentTblColMessage       = "message"
	DocSegmentTblColIsDeleted     = "is_deleted"
	DocSegmentTblColType          = "type"
	DocSegmentTblColNextAction    = "next_action"
	DocSegmentTblColBatchID       = "batch_id"
	DocSegmentTblColRichTextIndex = "rich_text_index"
	DocSegmentTblColStartIndex    = "start_index"
	DocSegmentTblColEndIndex      = "end_index"
	DocSegmentTblColLinkerKeep    = "linker_keep"
	DocSegmentTblColUpdateTime    = "update_time"
	DocSegmentTblColCreateTime    = "create_time"
	DocSegmentTblColBigDataID     = "big_data_id"
	DocSegmentTblColBigStartIndex = "big_start_index"
	DocSegmentTblColBigEndIndex   = "big_end_index"
	DocSegmentTblColSegmentType   = "segment_type"

	docSegmentMaxPageSize = 1000
)

var DocSegmentTblColList = []string{
	DocSegmentTblColID,
	DocSegmentTblColBusinessID,
	DocSegmentTblColRobotID,
	DocSegmentTblColCorpID,
	DocSegmentTblColStaffId,
	DocSegmentTblColDocId,
	DocSegmentTblColFileType,
	DocSegmentTblColTitle,
	DocSegmentTblColPageContent,
	DocSegmentTblColOrgData,
	DocSegmentTblColOrgDataBizID,
	DocSegmentTblColOutputs,
	DocSegmentTblColCostTime,
	DocSegmentTblColSplitModel,
	DocSegmentTblColStatus,
	DocSegmentTblColReleaseStatus,
	DocSegmentTblColMessage,
	DocSegmentTblColIsDeleted,
	DocSegmentTblColType,
	DocSegmentTblColNextAction,
	DocSegmentTblColBatchID,
	DocSegmentTblColRichTextIndex,
	DocSegmentTblColStartIndex,
	DocSegmentTblColEndIndex,
	DocSegmentTblColLinkerKeep,
	DocSegmentTblColUpdateTime,
	DocSegmentTblColCreateTime,
	DocSegmentTblColBigDataID,
	DocSegmentTblColBigStartIndex,
	DocSegmentTblColBigEndIndex,
	DocSegmentTblColSegmentType,
}

type DocSegmentDao struct {
	BaseDao
	tableName string
}

// GetDocSegmentDao 获取全局的数据操作对象
func GetDocSegmentDao() *DocSegmentDao {
	if globalDocSegmentDao == nil {
		globalDocSegmentDao = &DocSegmentDao{*globalBaseDao, docSegmentTableName}
	}
	return globalDocSegmentDao
}

type DocSegmentFilter struct {
	IDs             []uint64
	BusinessIDs     []uint64
	CorpID          uint64
	RobotID         uint64
	DocID           uint64
	IsDeleted       *int
	SegmentType     string
	ReleaseStatuses []uint32
	NextActions     []uint32
	Offset          uint32
	Limit           uint32
	OrderColumn     []string
	OrderDirection  []string
	RouterAppBizID  uint64
}

// 生成查询条件，必须按照索引的顺序排列
func (d *DocSegmentDao) generateCondition(ctx context.Context, session *gorm.DB, filter *DocSegmentFilter) {
	if filter.CorpID != 0 {
		session.Where(DocSegmentTblColCorpID+sqlEqual, filter.CorpID)
	}
	if filter.RobotID != 0 {
		session.Where(DocSegmentTblColRobotID+sqlEqual, filter.RobotID)
	}
	if filter.DocID != 0 {
		session.Where(DocSegmentTblColDocId+sqlEqual, filter.DocID)
	}
	if len(filter.IDs) != 0 {
		session.Where(DocSegmentTblColID+sqlIn, filter.IDs)
	}
	if len(filter.BusinessIDs) != 0 {
		session.Where(DocSegmentTblColBusinessID+sqlIn, filter.BusinessIDs)
	}
	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session.Where(DocSegmentTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}
	if filter.SegmentType != "" {
		session.Where(DocSegmentTblColSegmentType+sqlEqual, filter.SegmentType)
	}
	if len(filter.ReleaseStatuses) != 0 {
		session.Where(DocSegmentTblColReleaseStatus+sqlIn, filter.ReleaseStatuses)
	}
	if len(filter.NextActions) != 0 {
		session.Where(DocSegmentTblColNextAction+sqlIn, filter.NextActions)
	}
}

// getDocSegmentList 获取文档片段列表
func (d *DocSegmentDao) getDocSegmentList(ctx context.Context, selectColumns []string,
	filter *DocSegmentFilter) ([]*model.DocSegment, error) {
	DocSegmentList := make([]*model.DocSegment, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return DocSegmentList, nil
	}
	if filter.Limit > DefaultMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		log.ErrorContextf(ctx, "getDocSegmentList err: %+v", err)
		return DocSegmentList, err
	}
	db, err := GetGorm(ctx, filter.RouterAppBizID, d.tableName)
	if err != nil {
		log.ErrorContextf(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, filter.RouterAppBizID)
		return DocSegmentList, err
	}
	session := db.WithContext(ctx).Table(d.tableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			log.ErrorContextf(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&DocSegmentList)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return DocSegmentList, res.Error
	}
	return DocSegmentList, nil
}

// GetDocSegmentCount 获取文档片段总数
func (d *DocSegmentDao) GetDocSegmentCount(ctx context.Context, selectColumns []string,
	filter *DocSegmentFilter) (int64, error) {
	session := d.tdsqlGormDB.WithContext(ctx).Table(d.tableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// GetDocSegmentCountAndList 获取文档片段总数和分页列表
func (d *DocSegmentDao) GetDocSegmentCountAndList(ctx context.Context, selectColumns []string,
	filter *DocSegmentFilter) ([]*model.DocSegment, int64, error) {
	count, err := d.GetDocSegmentCount(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	list, err := d.GetDocSegmentList(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	return list, count, nil
}

// GetDocSegmentList 获取所有文档片段列表，使用logic层中GetDocSegmentList方法，可同时获取到org_data
func (d *DocSegmentDao) GetDocSegmentList(ctx context.Context, selectColumns []string,
	filter *DocSegmentFilter) ([]*model.DocSegment, error) {
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := uint32(0)
	wantedCount := filter.Limit
	allDocSegmentList := make([]*model.DocSegment, 0)
	for {
		filter.Offset = offset
		filter.Limit = CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		DocSegmentList, err := d.getDocSegmentList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetDocSegmentList failed, err: %+v", err)
			return nil, err
		}
		allDocSegmentList = append(allDocSegmentList, DocSegmentList...)
		if uint32(len(DocSegmentList)) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	log.DebugContextf(ctx, "GetDocSegmentList count:%d cost:%dms",
		len(allDocSegmentList), time.Since(beginTime).Milliseconds())
	return allDocSegmentList, nil
}

// GetSegmentByDocID 批量获取单文档未删除的segment
func (d *DocSegmentDao) GetSegmentByDocID(ctx context.Context, robotID, docID, startID, count uint64, selectColumns []string) (
	[]*model.DocSegmentExtend, uint64, error) {
	db, err := knowClient.GormClient(ctx, d.tableName, robotID, 0, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "GetSegmentByDocID get GormClient err:%v,robotID:%v", err, robotID)
		return nil, 0, err
	}
	segs, limit, lastID := make([]*model.DocSegmentExtend, 0, count), 500, startID
	for {
		var tmp []*model.DocSegmentExtend
		err := db.WithContext(ctx).Table(d.tableName).Select(selectColumns).
			Where(DocSegmentTblColIsDeleted+sqlEqual, model.SegmentIsNotDeleted).
			Where(DocSegmentTblColRobotID+sqlEqual, robotID).Where(DocSegmentTblColDocId+sqlEqual, docID).
			Where(DocSegmentTblColID+sqlMore, lastID). //避免深分页
			Limit(limit).Find(&tmp).Error
		if err != nil {
			log.ErrorContextf(ctx, "GetSegmentByDocID get err:%v,robotID:%v,docID:%v", err, robotID, docID)
			return nil, 0, err
		}
		if len(tmp) != 0 {
			segs = append(segs, tmp...)
			lastID = tmp[len(tmp)-1].ID
		}
		if len(tmp) < int(limit) || len(segs) >= int(count) {
			break
		}
	}
	return segs, lastID, nil
}

// UpdateDocSegment 更新文档片段
func (d *DocSegmentDao) UpdateDocSegment(ctx context.Context, updateColumns []string, filter *DocSegmentFilter,
	docSegment *model.DocSegment) (int64, error) {
	db, err := knowClient.GormClient(ctx, d.tableName, filter.RobotID, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "get GormClient failed, err: %+v", err)
		return 0, err
	}
	session := db.WithContext(ctx).Table(d.tableName).Select(updateColumns)
	d.generateCondition(ctx, session, filter)
	res := session.Updates(docSegment)
	if res.Error != nil {
		log.ErrorContextf(ctx, "UpdateDoc failed docSegment:%v, err: %+v", docSegment, res.Error)
		return 0, res.Error
	}
	log.DebugContextf(ctx, "update docSegment record: %v", res.RowsAffected)
	return res.RowsAffected, nil
}
