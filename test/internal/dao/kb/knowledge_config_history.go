package kb

import (
	"context"
	"time"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"gorm.io/gen"

	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
)

func knowledgeConfigHistoryDO2PO(do *entity.KnowledgeConfigHistory) *model.TKnowledgeConfigHistory {
	po := &model.TKnowledgeConfigHistory{
		ID:             do.ID,
		CorpBizID:      do.CorpBizID,
		KnowledgeBizID: do.KnowledgeBizID,
		AppBizID:       do.AppBizID,
		Type:           do.Type,
		VersionID:      do.VersionID,
		ReleaseJSON:    do.ReleaseJSON,
		IsRelease:      do.IsRelease,
		IsDeleted:      do.IsDeleted,
		CreateTime:     do.CreateTime,
		UpdateTime:     do.UpdateTime,
	}
	return po
}

func knowledgeConfigHistoriesDO2PO(dos []*entity.KnowledgeConfigHistory) []*model.TKnowledgeConfigHistory {
	return slicex.Map(dos, func(do *entity.KnowledgeConfigHistory) *model.TKnowledgeConfigHistory {
		return knowledgeConfigHistoryDO2PO(do)
	})
}

func KnowledgeConfigHistoryPO2DO(po *model.TKnowledgeConfigHistory) *entity.KnowledgeConfigHistory {
	if po == nil {
		return nil
	}
	return &entity.KnowledgeConfigHistory{
		ID:             po.ID,
		CorpBizID:      po.CorpBizID,
		KnowledgeBizID: po.KnowledgeBizID,
		AppBizID:       po.AppBizID,
		Type:           po.Type,
		VersionID:      po.VersionID,
		ReleaseJSON:    po.ReleaseJSON,
		IsRelease:      po.IsRelease,
		IsDeleted:      po.IsDeleted,
		CreateTime:     po.CreateTime,
		UpdateTime:     po.UpdateTime,
	}
}

func KnowledgeConfigHistoriesPO2DO(pos []*model.TKnowledgeConfigHistory) []*entity.KnowledgeConfigHistory {
	return slicex.Map(pos, func(po *model.TKnowledgeConfigHistory) *entity.KnowledgeConfigHistory {
		return KnowledgeConfigHistoryPO2DO(po)
	})
}

func (d *daoImpl) generationCondition(filter *entity.KnowledgeConfigHistoryFilter) []gen.Condition {
	queryCond := []gen.Condition{
		d.tdsql.TKnowledgeConfigHistory.IsDeleted.Is(false),
	}
	if filter.ID > 0 {
		queryCond = append(queryCond, d.tdsql.TKnowledgeConfigHistory.ID.Eq(filter.ID))
	}
	if filter.CorpBizID > 0 {
		queryCond = append(queryCond, d.tdsql.TKnowledgeConfigHistory.CorpBizID.Eq(filter.CorpBizID))
	}
	if filter.KnowledgeBizID > 0 {
		queryCond = append(queryCond, d.tdsql.TKnowledgeConfigHistory.KnowledgeBizID.Eq(filter.KnowledgeBizID))
	}
	if filter.AppBizID > 0 {
		queryCond = append(queryCond, d.tdsql.TKnowledgeConfigHistory.AppBizID.Eq(filter.AppBizID))
	}
	if filter.Type > 0 {
		queryCond = append(queryCond, d.tdsql.TKnowledgeConfigHistory.Type.Eq(filter.Type))
	}
	if filter.VersionID > 0 {
		queryCond = append(queryCond, d.tdsql.TKnowledgeConfigHistory.VersionID.Eq(filter.VersionID))
	}
	if filter.IsRelease != nil {
		queryCond = append(queryCond, d.tdsql.TKnowledgeConfigHistory.IsRelease.Is(*filter.IsRelease))
	}
	return queryCond
}

func (d *daoImpl) DescribeKnowledgeConfigHistoryList(ctx context.Context,
	filter *entity.KnowledgeConfigHistoryFilter) ([]*entity.KnowledgeConfigHistory, error) {
	res, err := d.tdsql.TKnowledgeConfigHistory.WithContext(ctx).
		Where(d.generationCondition(filter)...).Find()
	return KnowledgeConfigHistoriesPO2DO(res), err
}

func (d *daoImpl) DescribeKnowledgeConfigHistory(ctx context.Context,
	filter *entity.KnowledgeConfigHistoryFilter) (*entity.KnowledgeConfigHistory, error) {
	res, err := d.tdsql.TKnowledgeConfigHistory.WithContext(ctx).
		Where(d.generationCondition(filter)...).First()
	return KnowledgeConfigHistoryPO2DO(res), err
}

func (d *daoImpl) CreateKnowledgeConfigHistory(ctx context.Context,
	do *kbe.KnowledgeConfigHistory, tx *tdsqlquery.Query) error {
	db := d.tdsql.TKnowledgeConfigHistory.WithContext(ctx)
	if tx != nil {
		db = tx.TKnowledgeConfigHistory.WithContext(ctx)
	}
	return db.Create(knowledgeConfigHistoryDO2PO(do))
}

func (d *daoImpl) DeleteKnowledgeConfigHistory(ctx context.Context,
	filter *entity.KnowledgeConfigHistoryFilter) error {
	_, err := d.tdsql.TKnowledgeConfigHistory.WithContext(ctx).
		Where(d.generationCondition(filter)...).
		Updates(map[string]interface{}{
			d.tdsql.TKnowledgeConfigHistory.IsDeleted.ColumnName().String(): "True",
		})
	return err
}

func (d *daoImpl) ModifyKnowledgeConfigHistory(ctx context.Context,
	filter *entity.KnowledgeConfigHistoryFilter, do *entity.KnowledgeConfigHistory, tx *tdsqlquery.Query) error {
	updateFields := map[string]interface{}{
		d.tdsql.TKnowledgeConfigHistory.UpdateTime.ColumnName().String(): time.Now(),
	}
	if do.Type > 0 {
		updateFields[d.tdsql.TKnowledgeConfigHistory.Type.ColumnName().String()] = do.Type
	}
	if do.IsRelease {
		updateFields[d.tdsql.TKnowledgeConfigHistory.IsRelease.ColumnName().String()] = do.IsRelease
	}
	if len(do.ReleaseJSON) > 0 {
		updateFields[d.tdsql.TKnowledgeConfigHistory.ReleaseJSON.ColumnName().String()] = do.ReleaseJSON
	}

	db := d.tdsql.TKnowledgeConfigHistory.WithContext(ctx)
	if tx != nil {
		db = tx.TKnowledgeConfigHistory.WithContext(ctx)
	}
	_, err := db.
		Where(d.generationCondition(filter)...).
		Updates(updateFields)
	return err
}
