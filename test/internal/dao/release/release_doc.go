package release

import (
	"context"

	"gorm.io/gorm/clause"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"gorm.io/gen"
	"gorm.io/gorm"
)

func (d *daoImpl) getReleaseDocGormDB(ctx context.Context, tx *gorm.DB) *gorm.DB {
	if tx != nil {
		return mysqlquery.Use(tx).TReleaseDoc.WithContext(ctx).UnderlyingDB()
	}
	db := d.mysql.TReleaseDoc.WithContext(ctx).Debug().UnderlyingDB()
	return db
}

func (d *daoImpl) IsExistReleaseDoc(ctx context.Context, filter *releaseEntity.ReleaseDocFilter) (bool, error) {
	/*
		`
				SELECT
					COUNT(1)
				FROM
					t_release_doc
				WHERE
					robot_id = ? AND version_id = ? AND doc_id = ?
			`
	*/
	tbl := d.mysql.TReleaseDoc
	db := tbl.WithContext(ctx).Debug()
	cond := []gen.Condition{
		tbl.RobotID.Eq(filter.RobotID),
		tbl.VersionID.Eq(filter.VersionId),
		tbl.DocID.Eq(filter.DocID),
	}

	count, err := db.Where(cond...).Count()
	if err != nil {
		logx.E(ctx, "IsExistReleaseDoc data req:(robotId:%v, versionId:%v, docId:%v), error:%v",
			filter.RobotID, filter.VersionId, filter.DocID, err)
		return false, err
	}
	return count > 0, nil
}

func (d *daoImpl) CreateReleaseDocRecords(ctx context.Context, releaseDocs []*releaseEntity.ReleaseDoc, tx *gorm.DB) error {

	if len(releaseDocs) == 0 {
		logx.I(ctx, "no release doc record to create")
		return nil
	}
	db := d.getReleaseDocGormDB(ctx, tx)
	tbl := d.mysql.TReleaseDoc
	total := len(releaseDocs)

	toCreateDocs := BatchConvertReleaseDocPOToDO(releaseDocs)
	logx.I(ctx, "CreateReleaseDocRecord data %d releaseDocs Records",
		total)

	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: tbl.VersionID.ColumnName().String()},
			{Name: tbl.DocID.ColumnName().String()},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			tbl.Source.ColumnName().String(),
			tbl.FileName.ColumnName().String(),
			tbl.FileType.ColumnName().String(),
			tbl.FileSize.ColumnName().String(),
			tbl.CosHash.ColumnName().String(),
			tbl.Status.ColumnName().String(),
			tbl.IsCreatingQa.ColumnName().String(),
			tbl.IsCreatingIndex.ColumnName().String(),
			tbl.Action.ColumnName().String(),
			tbl.UpdateTime.ColumnName().String(),
			tbl.IsDeleted.ColumnName().String(),
		}), // 冲突时更新 question,answer,similar_status,accept_status,action,attr_labels,release_status,update_time,is_deleted 字段
	}).CreateInBatches(toCreateDocs, 100).Error; err != nil {
		logx.E(ctx, "CreateRelaseDocRecord data %d releaseDocs, error:%v",
			len(toCreateDocs), err)
		return err
	}

	return nil

}

func (d *daoImpl) UpdateReleaseDocRecords(ctx context.Context, releaseDocs []*releaseEntity.ReleaseDoc, tx *gorm.DB) error {
	if len(releaseDocs) == 0 {
		logx.I(ctx, "no release doc record to update")
		return nil
	}

	db := d.getReleaseDocGormDB(ctx, tx)

	releaseDocDos := BatchConvertReleaseDocPOToDO(releaseDocs)

	if err := db.Transaction(func(tx *gorm.DB) error {
		for _, v := range releaseDocDos {
			if err := tx.
				Model(&model.TReleaseDoc{}).
				Updates(v).
				Error; err != nil {
				return err
			}
		}
		tx.Commit()
		return nil
	}); err != nil {
		logx.E(ctx, "UpdateReleaseDocRecord failed, error:%v", err)
		return err
	}

	logx.I(ctx, "UpdateReleaseDocRecord data %d releaseDocs Records",
		len(releaseDocs))
	return nil
}

func (d *daoImpl) UpdateReleaseDocRecord(ctx context.Context, updateColumns map[string]any, filter *releaseEntity.ReleaseDocFilter, tx *gorm.DB) error {
	if len(updateColumns) == 0 {
		logx.I(ctx, "no release doc record to update")
		return nil
	}

	db := d.getReleaseDocGormDB(ctx, tx)
	if err := db.
		Model(&model.TReleaseDoc{}).
		Where("robot_id = ? AND version_id = ? AND doc_id = ?", filter.RobotID, filter.VersionId, filter.DocID).
		Updates(updateColumns).
		Error; err != nil {
		return err
	}
	logx.I(ctx, "UpdateReleaseDocRecord data %d releaseDocs Records",
		len(updateColumns))
	return nil
}

func (d *daoImpl) GetReleaseDocIdMap(ctx context.Context, corpId, robotId, versionId uint64,
	docIds []uint64) (map[uint64]struct{}, error) {
	releaseDocIdMap := make(map[uint64]struct{}, 0)
	if len(docIds) == 0 {
		return releaseDocIdMap, nil
	}

	releaseDocs := make([]*model.TReleaseDoc, 0)

	// TODO: need a provider for different db distinginuish with different staff and group
	db, err := knowClient.GormClient(ctx, model.TableNameTReleaseDoc, robotId, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return nil, err
	}

	docQuery := mysqlquery.Use(db)

	queryCond := []gen.Condition{
		docQuery.TReleaseDoc.CorpID.Eq(corpId),
		docQuery.TReleaseDoc.RobotID.Eq(robotId),
		docQuery.TReleaseDoc.VersionID.Eq(versionId),
		docQuery.TReleaseDoc.DocID.In(docIds...),
	}

	releaseDocs, err = docQuery.TReleaseDoc.
		WithContext(ctx).
		Debug().
		Where(queryCond...).Find()
	if err != nil {
		logx.E(ctx, "GetReleaseDocIdMap data req:(corpId:%v, robotId:%v, versionId:%v, docIds:%v), error:%v",
			corpId, robotId, versionId, docIds, err)
		return nil, err
	}
	for _, releaseDoc := range releaseDocs {
		releaseDocIdMap[releaseDoc.DocID] = struct{}{}
	}
	return releaseDocIdMap, nil

}

// GetModifyDocCount 获取版本改动文档数量
func (d *daoImpl) GetModifyDocCount(ctx context.Context, robotID, versionID uint64,
	fileName string, actions []uint32, statuses []uint32) (uint64, error) {
	/*
		 `
			SELECT
				count(*)
			FROM
			    t_release_doc
			WHERE
				 robot_id = ? AND version_id = ? %s
		`
	*/
	tbl := d.mysql.TReleaseDoc
	db := tbl.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		tbl.RobotID.Eq(robotID),
		tbl.VersionID.Eq(versionID),
	}

	if fileName != "" {
		queryCond = append(queryCond, tbl.FileName.Like(special.Replace(fileName)))
	}
	if len(actions) > 0 {
		queryCond = append(queryCond, tbl.Action.In(actions...))
	}
	if len(statuses) > 0 {
		queryCond = append(queryCond, tbl.Status.In(statuses...))
	}

	count, err := db.Where(queryCond...).Count()
	if err != nil {
		logx.E(ctx, "GetModifyDocCount data req:(robotId:%v, versionId:%v, fileName:%v, actions:%v, statuses:%v), error:%v",
			robotID, versionID, fileName, actions, statuses, err)
		return 0, err
	}
	return uint64(count), nil
}

// GetModifyDocList 获取版本改动文档范围
func (d *daoImpl) GetModifyDocList(ctx context.Context, req *releaseEntity.ListReleaseDocReq) (
	[]*releaseEntity.ReleaseDoc, error) {
	/*
		 `
				SELECT
					%s
				FROM
				    t_release_doc
				WHERE
				    robot_id = ? AND version_id = ? %s
				LIMIT ?,?
			`
	*/
	db := d.mysql.TReleaseDoc.WithContext(ctx).Debug()

	queryCond := []gen.Condition{
		d.mysql.TReleaseDoc.RobotID.Eq(req.RobotID),
		d.mysql.TReleaseDoc.VersionID.Eq(req.VersionId),
	}

	if req.FileName != "" {
		queryCond = append(queryCond, d.mysql.TReleaseDoc.FileName.Like(special.Replace(req.FileName)))
	}
	if len(req.Actions) > 0 {
		queryCond = append(queryCond, d.mysql.TReleaseDoc.Action.In(req.Actions...))
	}
	offset, limit := utilx.Page(req.Page, req.PageSize)
	releaseDocs, err := db.Where(queryCond...).
		Order(d.mysql.TReleaseDoc.ID.Desc()).
		Offset(offset).
		Limit(limit).
		Find()
	if err != nil {
		logx.E(ctx, "GetModifyDocList data req:%+v, error:%v", req, err)
		return nil, err
	}
	return BatchConvertRelaseDocDoToPo(releaseDocs), nil
}
