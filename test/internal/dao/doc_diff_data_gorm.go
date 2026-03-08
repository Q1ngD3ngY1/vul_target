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

var globalDocDiffDataDao *docDiffDataDao

const (
	docDiffDataTableName = "t_doc_diff_data"

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

var docDiffDataTblColList = []string{DocDiffDataTblColRobotBizId, DocDiffDataTblColCorpBizId,
	DocDiffDataTblColIsDeleted, DocDiffDataTblColCreateTime, DocDiffDataTblColUpdateTime}

type docDiffDataDao struct {
	BaseDao
}

// GetDocDiffDataDao 获取全局的数据操作对象
func GetDocDiffDataDao() *docDiffDataDao {
	if globalDocDiffDataDao == nil {
		globalDocDiffDataDao = &docDiffDataDao{*globalBaseDao}
	}
	return globalDocDiffDataDao
}

type DocDiffDataFilter struct {
	CorpBizId      uint64 // 企业 ID
	RobotBizId     uint64
	DiffBizId      uint64 // 文档对比任务ID
	IsDeleted      *int
	Offset         uint32
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string
}

// CreateDocDiffData 新建文档对比结果记录
func (d *docDiffDataDao) CreateDocDiffData(ctx context.Context, docDiffData *model.DocDiffData) error {
	selectColumns := []string{DocDiffDataTblColCorpBizId, DocDiffDataTblColRobotBizId, DocDiffDataTblColDiffBizId,
		DocDiffDataTblColDiffData, DocDiffDataTblColDiffIndex}
	res := d.tdsqlGormDB.WithContext(ctx).Table(docDiffDataTableName).Select(selectColumns).Create(docDiffData)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return res.Error
	}
	return nil
}

// 生成查询条件，必须按照索引的顺序排列
func generateDocDiffDataCondition(ctx context.Context, session *gorm.DB, filter *DocDiffDataFilter) {
	if filter.CorpBizId != 0 {
		session = session.Where(DocDiffDataTblColCorpBizId+sqlEqual, filter.CorpBizId)
	}
	if filter.RobotBizId != 0 {
		session = session.Where(DocDiffDataTblColRobotBizId+sqlEqual, filter.RobotBizId)
	}
	if filter.DiffBizId != 0 {
		session = session.Where(DocDiffDataTblColDiffBizId+sqlEqual, filter.DiffBizId)
	}

	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(DocDiffDataTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}
}

// getDocDiffDataList 获取文档比对结果列表
func (d *docDiffDataDao) getDocDiffDataList(ctx context.Context, selectColumns []string,
	filter *DocDiffDataFilter) ([]*model.DocDiffData, error) {
	docDiffDataList := make([]*model.DocDiffData, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return docDiffDataList, nil
	}
	if filter.Limit > DocDiffDataTableMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		log.ErrorContextf(ctx, "getDocDiffDataList err: %+v", err)
		return docDiffDataList, err
	}
	session := d.tdsqlGormDB.WithContext(ctx).Table(docDiffDataTableName).Select(selectColumns)
	generateDocDiffDataCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			log.ErrorContextf(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&docDiffDataList)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return docDiffDataList, res.Error
	}
	return docDiffDataList, nil
}

// GetDocDiffDataCount 获取文档比对结果总数
func (d *docDiffDataDao) GetDocDiffDataCount(ctx context.Context, selectColumns []string,
	filter *DocDiffDataFilter) (int64, error) {
	session := d.tdsqlGormDB.WithContext(ctx).Table(docDiffDataTableName).Select(selectColumns)
	generateDocDiffDataCondition(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// GetDocDiffDataCountAndList 获取文档比对结果总数和分页列表
func (d *docDiffDataDao) GetDocDiffDataCountAndList(ctx context.Context, selectColumns []string,
	filter *DocDiffDataFilter) ([]*model.DocDiffData, int64, error) {
	count, err := d.GetDocDiffDataCount(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	list, err := d.GetDocDiffDataList(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	return list, count, nil
}

// GetDocDiffDataList 获取所有文档比对结果列表
func (d *docDiffDataDao) GetDocDiffDataList(ctx context.Context, selectColumns []string,
	filter *DocDiffDataFilter) ([]*model.DocDiffData, error) {
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := uint32(0)
	wantedCount := filter.Limit
	allDocDiffDataList := make([]*model.DocDiffData, 0)
	for {
		filter.Offset = offset
		filter.Limit = CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		docDiffDataList, err := d.getDocDiffDataList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetDocDiffDataList failed, err: %+v", err)
			return nil, err
		}
		allDocDiffDataList = append(allDocDiffDataList, docDiffDataList...)
		if uint32(len(docDiffDataList)) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	log.DebugContextf(ctx, "GetDocDiffDataList count:%d cost:%dms",
		len(allDocDiffDataList), time.Since(beginTime).Milliseconds())
	return allDocDiffDataList, nil
}

// UpdateDocDiffData 更新对比详情数据指定字段
func (d *docDiffDataDao) UpdateDocDiffData(ctx context.Context, tx *gorm.DB, updateColumns []string,
	corpBizId uint64, robotBizId uint64, diffBizIds []uint64, docDiffData *model.DocDiffData) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	session := tx.WithContext(ctx).Table(docDiffDataTableName).Select(updateColumns).
		Where(DocDiffDataTblColCorpBizId+sqlEqual, corpBizId).
		Where(DocDiffDataTblColRobotBizId+sqlEqual, robotBizId).
		Where(DocDiffDataTblColDiffBizId+sqlIn, diffBizIds)
	res := session.Updates(docDiffData)
	if res.Error != nil {
		log.ErrorContextf(ctx, "UpdateDocDiffData failed for diffBizIds: %+v, err: %+v", diffBizIds, res.Error)
		return res.Error
	}
	return nil
}

// DeleteDocDiffData 删除对比详情数据
func (d *docDiffDataDao) DeleteDocDiffData(ctx context.Context, tx *gorm.DB, corpBizId uint64, robotBizId uint64,
	diffBizIds []uint64) error {
	if tx == nil {
		tx = d.tdsqlGormDB
	}
	updateColumns := []string{DocDiffDataTblColIsDeleted, DocDiffDataTblColUpdateTime}
	docDiffData := &model.DocDiffData{
		IsDeleted:  IsDeleted,  // 是否删除
		UpdateTime: time.Now(), // 更新时间
	}
	return d.UpdateDocDiffData(ctx, tx, updateColumns, corpBizId, robotBizId, diffBizIds, docDiffData)
}
