package third_doc_dao

import (
	"context"

	"git.woa.com/adp/common/x/gox/mapx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	query "git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	knowledge "git.woa.com/adp/pb-go/kb/kb_config"
	"gorm.io/gorm/clause"

	"git.woa.com/adp/kb/kb-config/internal/dao/types"
)

type OnedriveDao struct {
	tdsql *query.Query
}

func NewOnedriveDao(mysqlDB types.TDSQLDB) *OnedriveDao {
	return &OnedriveDao{
		tdsql: query.Use(mysqlDB),
	}
}

// Migrate 发起迁移任务
func (d *OnedriveDao) Migrate(ctx context.Context, platformID int32, appBizID uint64, operationAndFileIDMap map[string]uint64) error {
	insertData := make([]*model.TThirdDocMigrateProgress, 0)
	// 迁移任务分为两步， 1. 写入迁移进度库； 2. 开启异步消费任务； 以上两步必须同时成功， 否则会导致数据不一致
	for fileID, operationID := range operationAndFileIDMap {
		insertData = append(insertData, &model.TThirdDocMigrateProgress{
			OperationID: operationID,
			Platform:    platformID,
			FileID:      fileID,
			Status:      int32(knowledge.MigrateStatus_MIGRATE_STATUS_IN_PROGRESS),
		})
	}
	err := d.tdsql.TThirdDocMigrateProgress.WithContext(ctx).Create(insertData...)
	// err := d.gormDB.WithContext(ctx).Model(&thirdDocEntity.ThirdDocMigrateProgress{}).Create(insertData).Error
	if err != nil {
		logx.ErrorContextf(ctx, "create migrate progress failed: %v", err)
		return err
	}

	return nil
}

func (d *OnedriveDao) GetMigrateProgress(ctx context.Context, operationIDs []uint64) ([]*model.TThirdDocMigrateProgress, error) {
	queryData, err := d.tdsql.TThirdDocMigrateProgress.WithContext(ctx).Where(d.tdsql.TThirdDocMigrateProgress.OperationID.In(operationIDs...)).Find()
	// err := d.gormDB.WithContext(ctx).Model(&thirdDocEntity.ThirdDocMigrateProgress{}).Where("operation_id IN ?", operationIDs).Find(&queryData).Error
	if err != nil {
		logx.ErrorContextf(ctx, "get migrate progress failed: err is %v, operations is %+v", err, operationIDs)
		return nil, err
	}
	return queryData, nil
}

func (d *OnedriveDao) UpdateMigrateProgress(ctx context.Context, success, fail map[uint64]*model.TThirdDocMigrateProgress) error {
	err := d.tdsql.Transaction(func(tx *query.Query) error {
		var err error
		if len(success) > 0 {
			err = tx.TThirdDocMigrateProgress.WithContext(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "operation_id"}},
				DoUpdates: clause.AssignmentColumns([]string{"status", "cos_e_tag", "file_size", "cos_hash", "cos_url"}),
			}).Create(mapx.Values(success)...)
			if err != nil {
				return err
			}
		}
		if len(fail) > 0 {
			err = tx.TThirdDocMigrateProgress.WithContext(ctx).Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "operation_id"}},
				DoUpdates: clause.AssignmentColumns([]string{"status"}),
			}).Create(mapx.Values(fail)...)
			if err != nil {
				return err
			}
		}
		return err
	})
	if err != nil {
		logx.ErrorContextf(ctx, "update migrate progress failed: %v", err)
		return err
	}
	return nil
}
