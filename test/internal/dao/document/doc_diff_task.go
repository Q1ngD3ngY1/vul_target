package document

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/gox/boolx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"gorm.io/gorm"
)

// CreateDocDiff 创建对比任务
func (d *daoImpl) CreateDocDiff(ctx context.Context, docDiff *docEntity.DocDiff) error {
	tbl := d.TdsqlQuery().TDocDiffTask
	tableName := tbl.TableName()
	tDocDiff := ConvertDocDiffTaskDO2PO(docDiff)
	res := tbl.WithContext(ctx).UnderlyingDB().
		Table(tableName).Create(tDocDiff)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return res.Error
	}
	return nil
}

// UpdateDocDiffTasks 更新对比任务指定字段
func (d *daoImpl) UpdateDocDiffTasks(ctx context.Context, updateColumns []string,
	corpBizId uint64, robotBizId uint64, businessIds []uint64, docDiff *docEntity.DocDiff) error {
	tbl := d.TdsqlQuery().TDocDiffTask
	tableName := tbl.TableName()
	session := tbl.WithContext(ctx).UnderlyingDB().Table(tableName).Select(updateColumns).
		Where(tbl.CorpBizID.ColumnName().String()+util.SqlEqual, corpBizId).
		Where(tbl.RobotBizID.ColumnName().String()+util.SqlEqual, robotBizId).
		Where(tbl.BusinessID.ColumnName().String()+util.SqlIn, businessIds)
	res := session.Updates(docDiff)
	if res.Error != nil {
		logx.E(ctx, "UpdateDocDiffTasks failed for businessIds: %+v, err: %+v", businessIds, res.Error)
		return res.Error
	}
	return nil
}

// DeleteDocDiffTasks 删除对比任务
func (d *daoImpl) DeleteDocDiffTasks(ctx context.Context, corpBizId uint64, robotBizId uint64,
	businessIds []uint64) error {
	tbl := d.TdsqlQuery().TDocDiffTask
	updateColumns := []string{tbl.IsDeleted.ColumnName().String(), tbl.UpdateTime.ColumnName().String()}
	docDiff := &docEntity.DocDiff{
		IsDeleted:  true,       // 是否删除
		UpdateTime: time.Now(), // 更新时间
	}
	if err := d.UpdateDocDiffTasks(ctx, updateColumns, corpBizId, robotBizId, businessIds, docDiff); err != nil {
		logx.E(ctx, "DeleteDocDiffTasks failed for businessIds: %+v, err: %+v", businessIds, err)
		return err
	}
	return nil
}

// GetDocDiffTask 获取单个文档对比任务
func (d *daoImpl) GetDocDiffTask(ctx context.Context, selectColumns []string, corpBizId,
	robotBizId, diffId uint64) (*docEntity.DocDiff, error) {
	tbl := d.TdsqlQuery().TDocDiffTask
	tableName := tbl.TableName()
	tDocDiff := &model.TDocDiffTask{}
	session := tbl.WithContext(ctx).UnderlyingDB().Table(tableName).Select(selectColumns)
	session = session.
		Where(tbl.CorpBizID.ColumnName().String()+util.SqlEqual, corpBizId).
		Where(tbl.RobotBizID.ColumnName().String()+util.SqlEqual, robotBizId).
		Where(tbl.BusinessID.ColumnName().String()+util.SqlEqual, diffId).
		Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, boolx.FalseNumber)
	res := session.Take(&tDocDiff)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			logx.W(ctx, "doc diff task not exist err: %+v", res.Error)
			return nil, errs.ErrHandleDocDiffNotFound
		}
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return ConvertDocDiffTaskPO2DO(tDocDiff), nil
}

// getDocDiffTaskList 获取文档对比任务
func (d *daoImpl) getDocDiffTaskList(ctx context.Context, selectColumns []string, filter *docEntity.DocDiffTaskFilter) (
	[]*docEntity.DocDiff, error) {
	tbl := d.TdsqlQuery().TDocDiffTask
	tableName := tbl.TableName()
	docDiffs := make([]*model.TDocDiffTask, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return BatchConvertDocDiffTaskPO2DO(docDiffs), nil
	}
	if filter.Limit > docEntity.DocDiffTaskTableMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		logx.E(ctx, "getDocDiffTaskList err: %+v", err)
		return BatchConvertDocDiffTaskPO2DO(docDiffs), err
	}
	session := tbl.WithContext(ctx).UnderlyingDB().
		Table(tableName).Select(selectColumns)
	session = d.generateDocDiffTaskCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != util.SqlOrderByAsc && filter.OrderDirection[i] != util.SqlOrderByDesc {
			logx.E(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session = session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&docDiffs)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return BatchConvertDocDiffTaskPO2DO(docDiffs), res.Error
	}
	return BatchConvertDocDiffTaskPO2DO(docDiffs), nil
}

// GetDocDiffTaskCount 获取文档比对任务总数
func (d *daoImpl) GetDocDiffTaskCount(ctx context.Context, selectColumns []string,
	filter *docEntity.DocDiffTaskFilter) (int64, error) {
	tbl := d.TdsqlQuery().TDocDiffTask
	tableName := tbl.TableName()
	session := tbl.WithContext(ctx).UnderlyingDB().
		Table(tableName).Select(selectColumns)
	session = d.generateDocDiffTaskCondition(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// GetDocDiffTaskCountAndList 获取文档对比任务列表
func (d *daoImpl) GetDocDiffTaskCountAndList(ctx context.Context, selectColumns []string,
	filter *docEntity.DocDiffTaskFilter) ([]*docEntity.DocDiff, int64, error) {
	count, err := d.GetDocDiffTaskCount(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	list, err := d.GetDocDiffTaskList(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	return list, count, nil
}

// GetDocDiffTaskList 获取所有文档对比任务
func (d *daoImpl) GetDocDiffTaskList(ctx context.Context, selectColumns []string,
	filter *docEntity.DocDiffTaskFilter) ([]*docEntity.DocDiff, error) {
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := 0
	wantedCount := filter.Limit
	allDocDiffTasks := make([]*docEntity.DocDiff, 0)
	for {
		filter.Offset = offset
		filter.Limit = util.CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		docDiffTasks, err := d.getDocDiffTaskList(ctx, selectColumns, filter)
		if err != nil {
			logx.E(ctx, "GetDocDiffTaskList failed, err: %+v", err)
			return nil, err
		}
		allDocDiffTasks = append(allDocDiffTasks, docDiffTasks...)
		if len(docDiffTasks) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	logx.D(ctx, "GetDocDiffTaskList count:%d cost:%dms",
		len(allDocDiffTasks), time.Since(beginTime).Milliseconds())
	return allDocDiffTasks, nil
}

// 生成查询条件，必须按照索引的顺序排列
func (d *daoImpl) generateDocDiffTaskCondition(ctx context.Context, session *gorm.DB,
	filter *docEntity.DocDiffTaskFilter) *gorm.DB {
	tbl := d.TdsqlQuery().TDocDiffTask
	if filter.CorpBizId != 0 {
		session = session.Where(tbl.CorpBizID.ColumnName().String()+util.SqlEqual, filter.CorpBizId)
	}
	// RobotBizIds和RobotBizId互斥，优先使用RobotBizIds
	if len(filter.RobotBizIds) > 0 {
		session = session.Where(tbl.RobotBizID.ColumnName().String()+util.SqlIn, filter.RobotBizIds)
	} else if filter.RobotBizId != 0 {
		session = session.Where(tbl.RobotBizID.ColumnName().String()+util.SqlEqual, filter.RobotBizId)
	}
	if len(filter.BusinessIds) > 0 {
		session = session.Where(tbl.BusinessID.ColumnName().String()+util.SqlIn, filter.BusinessIds)
	}

	if len(filter.Statuses) > 0 {
		session = session.Where(tbl.Status.ColumnName().String()+util.SqlIn, filter.Statuses)
	}

	if len(filter.InNewOldDocId) > 0 {
		session = session.Where("("+tbl.NewDocBizID.ColumnName().String()+util.SqlIn, filter.InNewOldDocId).Or(
			tbl.OldDocBizID.ColumnName().String()+util.SqlIn+")", filter.InNewOldDocId)
	}

	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, *filter.IsDeleted)
	}
	return session
}
