package export

import (
	"context"
	"fmt"
	"time"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/kb/kb-config/internal/entity"
)

func exportPO2DO(po *model.TExport) *entity.Export {
	if po == nil {
		return nil
	}
	return &entity.Export{
		ID:            po.ID,
		CorpID:        po.CorpID,
		RobotID:       po.RobotID,
		CreateStaffID: po.CreateStaffID,
		TaskType:      po.TaskType,
		Name:          po.Name,
		Params:        po.Params,
		Status:        po.Status,
		Result:        po.Result,
		Bucket:        po.Bucket,
		CosURL:        po.CosURL,
		UpdateTime:    po.UpdateTime,
		CreateTime:    po.CreateTime,
	}
}

func exportDO2PO(do *entity.Export) *model.TExport {
	if do == nil {
		return nil
	}
	return &model.TExport{
		ID:            do.ID,
		CorpID:        do.CorpID,
		RobotID:       do.RobotID,
		CreateStaffID: do.CreateStaffID,
		TaskType:      do.TaskType,
		Name:          do.Name,
		Params:        do.Params,
		Status:        do.Status,
		Result:        do.Result,
		Bucket:        do.Bucket,
		CosURL:        do.CosURL,
		UpdateTime:    do.UpdateTime,
		CreateTime:    do.CreateTime,
	}
}

func (d *daoImpl) CreateExportTask(ctx context.Context, export *entity.Export) (uint64, error) {
	po := exportDO2PO(export)
	po.CreateTime = time.Now()
	po.UpdateTime = time.Now()
	err := d.mysql.TExport.WithContext(ctx).Create(po)
	if err != nil {
		return 0, fmt.Errorf("CreateExport err:%+v", err)
	}
	return po.ID, nil
}

func (d *daoImpl) ModifyExportTask(ctx context.Context, export *entity.Export) error {
	updateFields := map[string]any{
		d.mysql.TExport.Status.ColumnName().String():     export.Status,
		d.mysql.TExport.Result.ColumnName().String():     export.Result,
		d.mysql.TExport.Bucket.ColumnName().String():     export.Bucket,
		d.mysql.TExport.CosURL.ColumnName().String():     export.CosURL,
		d.mysql.TExport.UpdateTime.ColumnName().String(): time.Now(),
	}
	db := d.mysql.TExport.WithContext(ctx).Where(d.mysql.TExport.ID.Eq(export.ID))
	_, err := db.Updates(updateFields)
	return err
}

func (d *daoImpl) DescribeExportTask(ctx context.Context, taskID, corpID, robotID uint64) (*entity.Export, error) {
	db := d.mysql.TExport.WithContext(ctx)
	if taskID != 0 {
		db = db.Where(d.mysql.TExport.ID.Eq(taskID))
	}
	if corpID != 0 {
		db = db.Where(d.mysql.TExport.CorpID.Eq(corpID))
	}
	if robotID != 0 {
		db = db.Where(d.mysql.TExport.RobotID.Eq(robotID))
	}
	qs, err := db.Find()
	if err != nil {
		return nil, fmt.Errorf("DescribeExportTask err:%+v", err)
	}
	if len(qs) == 0 {
		return nil, nil
	}
	return exportPO2DO(qs[0]), nil
}
