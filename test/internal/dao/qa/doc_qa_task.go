package qa

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"gorm.io/gorm"
)

// 生成查询条件，必须按照索引的顺序排列
func (d *daoImpl) generateDocQaTaskCondition(ctx context.Context, session *gorm.DB, filter *entity.DocQaTaskFilter) *gorm.DB {
	tbl := d.Query().TDocQaTask

	if filter.ID != 0 {
		session = session.Where(tbl.ID.ColumnName().String()+util.SqlEqual, filter.ID)
	}

	if filter.CorpId != 0 {
		session = session.Where(tbl.CorpID.ColumnName().String()+util.SqlEqual, filter.CorpId)
	}
	if filter.RobotId != 0 {
		session = session.Where(tbl.RobotID.ColumnName().String()+util.SqlEqual, filter.RobotId)
	}
	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(tbl.IsDeleted.ColumnName().String()+util.SqlEqual, *filter.IsDeleted)
	}

	if len(filter.DocId) != 0 {
		session = session.Where(tbl.DocID.ColumnName().String()+util.SqlIn, filter.DocId)
	}
	if len(filter.Status) != 0 {
		session = session.Where(tbl.Status.ColumnName().String()+util.SqlIn, filter.Status)
	}

	if filter.BusinessId != 0 {
		// 业务Id为该表唯一索引
		session = session.Where(tbl.BusinessID.ColumnName().String()+util.SqlEqual, filter.BusinessId)
	}
	if len(filter.BusinessIds) != 0 {
		// 业务Id为该表唯一索引
		session = session.Where(tbl.BusinessID.ColumnName().String()+util.SqlIn, filter.BusinessIds)
	}
	return session
}

// GetDocQaTasks 获取文档生成问答任务集合
func (d *daoImpl) GetDocQaTaskListCount(ctx context.Context, selectColumns []string, filter *entity.DocQaTaskFilter) (
	int64, error) {
	/*
			`
			SELECT
				count(*)
			FROM
			    t_doc_qa_task
			WHERE
			    xxxxx
		`
	*/
	tbl := d.Query().TDocQaTask
	docQaTaskTableName := tbl.TableName()
	session := tbl.WithContext(ctx).UnderlyingDB().Table(docQaTaskTableName).Select(selectColumns)
	session = d.generateDocQaTaskCondition(ctx, session, filter)

	var count int64
	res := session.Count(&count)
	if res.Error != nil {
		logx.E(ctx, "GetDocQaTaskListCount failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// GetDocQaTasks 获取文档生成问答任务集合
func (d *daoImpl) GetDocQaTaskList(ctx context.Context, selectColumns []string, filter *entity.DocQaTaskFilter) (
	[]*entity.DocQATask, error) {
	/*
				`
				SELECT
		    		%s
				FROM
				    t_doc_qa_task
				WHERE
				    corp_id = ? AND robot_id = ? AND is_deleted = ?
				ORDER BY
				    xxxxx
				LIMIT ?,?
				`
	*/
	tbl := d.Query().TDocQaTask
	docQaTaskTableName := tbl.TableName()
	docQATasks := make([]*model.TDocQaTask, 0)
	session := tbl.WithContext(ctx).UnderlyingDB().Table(docQaTaskTableName).Select(selectColumns)
	session = d.generateDocQaTaskCondition(ctx, session, filter)
	// if filter.Limit == 0 || filter.Limit > entity.DocQaTaskTableMaxPageSize {
	// 	filter.Limit = entity.DocQaTaskTableMaxPageSize
	// }
	// session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))

	if filter.PageNo != 0 || filter.PageSize != 0 {
		offset, limit := utilx.Page(filter.PageNo, filter.PageSize)
		session = session.Offset(offset).Limit(limit)
	} else {
		if filter.Limit == 0 || filter.Limit > entity.DocQaTaskTableMaxPageSize {
			filter.Limit = entity.DocQaTaskTableMaxPageSize
		}
		session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	}

	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != util.SqlOrderByAsc && filter.OrderDirection[i] != util.SqlOrderByDesc {
			logx.E(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&docQATasks)
	if res.Error != nil {
		logx.E(ctx, "GetDocQaTaskList failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return BatchConvertTDocQATaskPO2DO(docQATasks), nil
}

func (d *daoImpl) ListDocQaTasks(ctx context.Context, selectColumns []string, filter *entity.DocQaTaskFilter) (uint64, []*entity.DocQATask, error) {
	count, err := d.GetDocQaTaskListCount(ctx, selectColumns, filter)
	if err != nil {
		return 0, nil, err
	}
	docQATasks, err := d.GetDocQaTaskList(ctx, selectColumns, filter)
	if err != nil {
		return 0, nil, err
	}
	return uint64(count), docQATasks, nil
}

func (d *daoImpl) GetDocQaTaskByFilter(ctx context.Context, selectColumns []string, filter *entity.DocQaTaskFilter) (*entity.DocQATask, error) {
	docQATasks, err := d.GetDocQaTaskList(ctx, selectColumns, filter)
	if err != nil {
		return nil, err
	}
	if len(docQATasks) == 0 {
		return nil, errs.ErrDocQaTaskNotFound
	}
	return docQATasks[0], nil
}

// GetDocQATaskGeneratingMaps 查询文档生成问答进行中的任务集合
func (d *daoImpl) GetDocQATaskGeneratingMaps(ctx context.Context, corpID, robotID uint64, docID []uint64) (
	map[uint64]*entity.DocQATask, error) {
	if len(docID) == 0 || corpID == 0 || robotID == 0 {
		return nil, errs.ErrDocQaTaskNotFound
	}
	generatingStatus := []int{entity.DocQATaskStatusGenerating, entity.DocQATaskStatusPause,
		entity.DocQATaskStatusResource, entity.DocQATaskStatusFail}

	notDeleted := docEntity.DocIsNotDeleted
	filter := &entity.DocQaTaskFilter{
		CorpId:    corpID,
		RobotId:   robotID,
		IsDeleted: &notDeleted,
		Status:    generatingStatus,
		DocId:     docID,
	}
	docQATaskMap := make(map[uint64]*entity.DocQATask)
	docQATasks, err := d.GetDocQaTaskList(ctx, entity.DocQaTaskTblColList, filter)
	if err != nil {
		logx.E(ctx, "获取文档生成问答进行中的任务集合失败 GetDocQaTasks err: %+v", err)
		return docQATaskMap, err
	}
	if len(docQATasks) == 0 {
		return docQATaskMap, nil
	}
	for _, dqt := range docQATasks {
		docQATaskMap[dqt.DocID] = dqt
	}
	return docQATaskMap, nil
}

// CreateDocQATask 创建doc qa task
func (d *daoImpl) CreateDocQATask(ctx context.Context, docQaTask *entity.DocQATask, tx *gorm.DB) error {
	tDocQaTask := ConvertTDocQATaskDO2PO(docQaTask)
	tbl := d.Query().TDocQaTask
	docQaTaskTableName := tbl.TableName()
	session := tx
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}
	res := session.Table(docQaTaskTableName).Create(tDocQaTask)
	if res.Error != nil {
		logx.E(ctx, "CreateDocQATask failed, param: %+v, err: %+v", docQaTask, res.Error)
		return res.Error
	}
	docQaTask.ID = tDocQaTask.ID
	return nil
}

// UpdateDocQATasks 更新doc qa tasks
func (d *daoImpl) UpdateDocQATasks(ctx context.Context, updateColumns []string, filter *entity.DocQaTaskFilter,
	docQaTask *entity.DocQATask, tx *gorm.DB) error {
	tDocQaTask := ConvertTDocQATaskDO2PO(docQaTask)
	tbl := d.Query().TDocQaTask
	docQaTaskTableName := tbl.TableName()
	session := tx
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}
	session = session.Table(docQaTaskTableName)
	session = d.generateDocQaTaskCondition(ctx, session, filter)
	res := session.Select(updateColumns).Updates(tDocQaTask)
	if res.Error != nil {
		logx.E(ctx, "UpdateDocQATasks failed, col: %v, param: %+v, qa: %+v, err: %+v",
			updateColumns, filter, docQaTask, res.Error)
		return res.Error
	}
	return nil
}

func (d *daoImpl) BatchUpdateDocQATasks(ctx context.Context, filter *entity.DocQaTaskFilter, updatedFieleds map[string]any, tx *gorm.DB) error {
	tbl := d.Query().TDocQaTask
	docQaTaskTableName := tbl.TableName()
	session := tx
	if session == nil {
		session = tbl.WithContext(ctx).UnderlyingDB()
	}
	session = session.Table(docQaTaskTableName)
	session = d.generateDocQaTaskCondition(ctx, session, filter)
	res := session.Updates(updatedFieleds)
	if res.Error != nil {
		logx.E(ctx, "BatchUpdateDocQATasks failed, param: %+v, err: %+v", filter, res.Error)
		return res.Error
	}
	return nil
}
