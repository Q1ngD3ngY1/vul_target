package document

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"gorm.io/gen/field"
	"gorm.io/gorm"
)

func (d *daoImpl) CreateDocDiffData(ctx context.Context, docDiffData *docEntity.DocDiffData) error {
	tbl := d.tdsql.TDocDiffDatum

	po := ConvertDocDiffDataDO2PO(docDiffData)

	selectColumns := []field.Expr{
		tbl.CorpBizID, tbl.RobotBizID,
		tbl.DiffBizID, tbl.DiffData,
		tbl.DiffIndex}

	err := tbl.WithContext(ctx).Debug().Select(selectColumns...).Create(po)
	if err != nil {
		logx.E(ctx, "Failed top CreateDocDiffData. execute sql failed, err: %+v", err)
		return err
	}
	return nil
}

// UpdateDocDiffData 更新对比详情数据指定字段
func (d *daoImpl) UpdateDocDiffData(ctx context.Context, updateColumns []string,
	corpBizId uint64, robotBizId uint64, diffBizIds []uint64, docDiffData *docEntity.DocDiffData) error {
	tbl := d.tdsql.TDocDiffDatum
	session := tbl.WithContext(ctx).UnderlyingDB().
		Table(tbl.TableName()).Select(updateColumns).
		Where(tbl.CorpBizID.ColumnName().String()+util.SqlEqual, corpBizId).
		Where(tbl.RobotBizID.ColumnName().String()+util.SqlEqual, robotBizId).
		Where(tbl.DiffBizID.ColumnName().String()+util.SqlIn, diffBizIds)
	res := session.Updates(docDiffData)
	if res.Error != nil {
		logx.E(ctx, "UpdateDocDiffData failed for diffBizIds: %+v, err: %+v",
			diffBizIds, res.Error)
		return res.Error
	}
	return nil
}

// DeleteDocDiffData 删除对比详情数据
func (d *daoImpl) DeleteDocDiffData(ctx context.Context, corpBizId uint64, robotBizId uint64,
	diffBizIds []uint64) error {
	tbl := d.tdsql.TDocDiffDatum
	updateColumns := []string{
		tbl.IsDeleted.ColumnName().String(),
		tbl.UpdateTime.ColumnName().String()}
	docDiffData := &docEntity.DocDiffData{
		IsDeleted:  true,       // 是否删除
		UpdateTime: time.Now(), // 更新时间
	}
	return d.UpdateDocDiffData(ctx, updateColumns, corpBizId, robotBizId, diffBizIds, docDiffData)
}

// 生成查询条件，必须按照索引的顺序排列
func (d *daoImpl) generateDocDiffDataCondition(ctx context.Context,
	session *gorm.DB, filter *docEntity.DocDiffDataFilter) *gorm.DB {
	tbl := d.tdsql.TDocDiffDatum
	if filter.CorpBizId != 0 {
		session = session.Where(tbl.CorpBizID.ColumnName().String()+util.SqlEqual, filter.CorpBizId)
	}
	if filter.RobotBizId != 0 {
		session = session.Where(tbl.RobotBizID.ColumnName().String()+util.SqlEqual, filter.RobotBizId)
	}
	if filter.DiffBizId != 0 {
		session = session.Where(tbl.DiffBizID.ColumnName().String()+util.SqlEqual, filter.DiffBizId)
	}

	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, *filter.IsDeleted)
	}
	return session
}

// getDocDiffDataList 获取文档比对结果列表
func (d *daoImpl) getDocDiffDataList(ctx context.Context, selectColumns []string,
	filter *docEntity.DocDiffDataFilter) ([]*docEntity.DocDiffData, error) {
	docDiffDataList := make([]*model.TDocDiffDatum, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return BatchConvertDocDiffDataPO2DO(docDiffDataList), nil
	}
	if filter.Limit > docEntity.DocDiffDataTableMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		logx.E(ctx, "getDocDiffDataList err: %+v", err)
		return BatchConvertDocDiffDataPO2DO(docDiffDataList), err
	}
	tbl := d.tdsql.TDocDiffDatum
	tableName := tbl.TableName()
	session := tbl.WithContext(ctx).UnderlyingDB().Table(tableName).Select(selectColumns)
	session = d.generateDocDiffDataCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != util.SqlOrderByAsc && filter.OrderDirection[i] != util.SqlOrderByDesc {
			logx.E(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&docDiffDataList)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return BatchConvertDocDiffDataPO2DO(docDiffDataList), res.Error
	}
	return BatchConvertDocDiffDataPO2DO(docDiffDataList), nil
}

// GetDocDiffDataCount 获取文档比对结果总数
func (d *daoImpl) GetDocDiffDataCount(ctx context.Context, selectColumns []string,
	filter *docEntity.DocDiffDataFilter) (int64, error) {
	tbl := d.tdsql.TDocDiffDatum
	tableName := tbl.TableName()
	session := tbl.WithContext(ctx).UnderlyingDB().Table(tableName).Select(selectColumns)
	session = d.generateDocDiffDataCondition(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// GetDocDiffDataCountAndList 获取文档比对结果总数和分页列表
func (d *daoImpl) GetDocDiffDataCountAndList(ctx context.Context, selectColumns []string,
	filter *docEntity.DocDiffDataFilter) ([]*docEntity.DocDiffData, int64, error) {
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
func (d *daoImpl) GetDocDiffDataList(ctx context.Context, selectColumns []string,
	filter *docEntity.DocDiffDataFilter) ([]*docEntity.DocDiffData, error) {
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := 0
	wantedCount := filter.Limit
	allDocDiffDataList := make([]*docEntity.DocDiffData, 0)
	for {
		filter.Offset = offset
		filter.Limit = util.CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		docDiffDataList, err := d.getDocDiffDataList(ctx, selectColumns, filter)
		if err != nil {
			logx.E(ctx, "GetDocDiffDataList failed, err: %+v", err)
			return nil, err
		}
		allDocDiffDataList = append(allDocDiffDataList, docDiffDataList...)
		if len(docDiffDataList) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	logx.D(ctx, "GetDocDiffDataList count:%d cost:%dms",
		len(allDocDiffDataList), time.Since(beginTime).Milliseconds())
	return allDocDiffDataList, nil
}
