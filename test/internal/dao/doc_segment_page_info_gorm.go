package dao

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"gorm.io/gorm"
)

var globalDocSegmentPageInfoDao *DocSegmentPageInfoDao

const (
	DocSegmentPageInfoTblColID          = "id"
	DocSegmentPageInfoTblPageInfoID     = "page_info_id"
	DocSegmentPageInfoTblSegmentID      = "segment_id"
	DocSegmentPageInfoTblColDocId       = "doc_id"
	DocSegmentPageInfoTblColRobotID     = "robot_id"
	DocSegmentPageInfoTblColCorpID      = "corp_id"
	DocSegmentPageInfoTblColStaffId     = "staff_id"
	DocSegmentPageInfoTblOrgPageNumbers = "org_page_numbers"
	DocSegmentPageInfoTblBigPageNumbers = "big_page_numbers"
	DocSegmentPageInfoTblSheetData      = "sheet_data"
	DocSegmentPageInfoTblColIsDeleted   = "is_deleted"
	DocSegmentPageInfoTblColUpdateTime  = "update_time"
	DocSegmentPageInfoTblColCreateTime  = "create_time"
)

type DocSegmentPageInfoDao struct {
	BaseDao
	tableName string
}

// GetDocSegmentPageInfoDao 获取全局的数据操作对象
func GetDocSegmentPageInfoDao() *DocSegmentPageInfoDao {
	if globalDocSegmentPageInfoDao == nil {
		globalDocSegmentPageInfoDao = &DocSegmentPageInfoDao{*globalBaseDao, docSegmentPageInfoTableName}
	}
	return globalDocSegmentPageInfoDao
}

type DocSegmentPageInfoFilter struct {
	RouterAppBizId uint64
	IDs            []uint64
	CorpID         uint64
	AppID          uint64
	DocID          uint64
	SegmentIDs     []uint64
	IsDeleted      *int
	Offset         uint32
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string
}

// 生成查询条件，必须按照索引的顺序排列
func (d *DocSegmentPageInfoDao) generateCondition(ctx context.Context, session *gorm.DB, filter *DocSegmentPageInfoFilter) {
	if len(filter.IDs) != 0 {
		session.Where(DocSegmentPageInfoTblColID+sqlIn, filter.IDs)
	}
	if filter.CorpID != 0 {
		session.Where(DocSegmentPageInfoTblColCorpID+sqlEqual, filter.CorpID)
	}
	if filter.AppID != 0 {
		session.Where(DocSegmentPageInfoTblColRobotID+sqlEqual, filter.AppID)
	}
	if filter.DocID != 0 {
		session.Where(DocSegmentPageInfoTblColDocId+sqlEqual, filter.DocID)
	}
	if len(filter.SegmentIDs) != 0 {
		session.Where(DocSegmentPageInfoTblSegmentID+sqlIn, filter.SegmentIDs)
	}
	if filter.IsDeleted != nil {
		session.Where(DocSegmentPageInfoTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}
}

// getDocSegmentPageInfoList 获取文档片段页码列表
func (d *DocSegmentPageInfoDao) getDocSegmentPageInfoList(ctx context.Context, selectColumns []string,
	filter *DocSegmentPageInfoFilter) ([]*model.DocSegmentPageInfo, error) {
	DocSegmentPageInfoList := make([]*model.DocSegmentPageInfo, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return DocSegmentPageInfoList, nil
	}
	if filter.Limit > DefaultMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		log.ErrorContextf(ctx, "getDocSegmentPageInfoList err: %+v", err)
		return DocSegmentPageInfoList, err
	}
	db, err := GetGorm(ctx, filter.RouterAppBizId, d.tableName)
	if err != nil {
		log.ErrorContextf(ctx, "getDocSegmentPageInfoList get GormClient err:%v,robotID:%v", err, filter.RouterAppBizId)
		return DocSegmentPageInfoList, err
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
	res := session.Find(&DocSegmentPageInfoList)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return DocSegmentPageInfoList, res.Error
	}
	return DocSegmentPageInfoList, nil
}

// GetDocSegmentPageInfoList 获取所有文档片段页码信息
func (d *DocSegmentPageInfoDao) GetDocSegmentPageInfoList(ctx context.Context, selectColumns []string,
	filter *DocSegmentPageInfoFilter) ([]*model.DocSegmentPageInfo, error) {
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := uint32(0)
	wantedCount := filter.Limit
	allDocSegmentPageInfoList := make([]*model.DocSegmentPageInfo, 0)
	for {
		filter.Offset = offset
		filter.Limit = CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		DocSegmentPageInfoList, err := d.getDocSegmentPageInfoList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetDocSegmentPageInfoList failed, err: %+v", err)
			return nil, err
		}
		allDocSegmentPageInfoList = append(allDocSegmentPageInfoList, DocSegmentPageInfoList...)
		if uint32(len(DocSegmentPageInfoList)) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	log.DebugContextf(ctx, "GetDocSegmentPageInfoList count:%d cost:%dms",
		len(allDocSegmentPageInfoList), time.Since(beginTime).Milliseconds())
	return allDocSegmentPageInfoList, nil
}
