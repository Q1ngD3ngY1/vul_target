package database

import (
	"context"
	"errors"
	"time"
	"unicode/utf8"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

func (l *Logic) DescribeColumn(ctx context.Context, filter *entity.ColumnFilter) (*entity.Column, error) {
	c, err := l.dao.DescribeColumn(ctx, filter)
	if err != nil {
		if errors.Is(err, errx.ErrNotFound) {
			return &entity.Column{}, nil
		}
		return nil, err
	}
	return c, nil
}

func (l *Logic) DescribeColumnList(ctx context.Context, filter *entity.ColumnFilter) ([]*entity.Column, int64, error) {
	return l.dao.DescribeColumnList(ctx, filter)
}

func (l *Logic) ListPreviewData(ctx context.Context, connDbSource entity.DatabaseConn, table string, page, pageSize int,
	columnName string, columnValue string, timeout time.Duration) (columns []string, rows []*pb.RowData, total int64,
	err error) {
	return l.dao.ListPreviewData(ctx, connDbSource, table, page, pageSize, columnName, columnValue, timeout)
}

// UpdateDbTableAndColumns 更新表和列数据
func (l *Logic) UpdateDbTableAndColumns(ctx context.Context, req *pb.UpdateDbTableAndColumnsReq) error {
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	timeNow := time.Now()

	if utf8.RuneCountInString(req.GetTable().GetAliasName()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		entity.MaxDbTableAliasNameLength) || utf8.RuneCountInString(req.GetTable().
		GetDescription()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		entity.MaxDbTableDescriptionLength) {
		return errs.ErrDbSourceInputExtraLong
	}

	tableFilter := entity.TableFilter{
		CorpBizID:    corpBizID,
		AppBizID:     req.AppBizId,
		DBTableBizID: req.Table.DbTableBizId,
	}
	dbTable, err := l.dao.DescribeTable(ctx, &tableFilter)
	// dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetTable().GetDbTableBizId())
	if err != nil {
		logx.E(ctx, "get db table by biz id failed, %v", err)
		return err
	}
	if dbTable.LearnStatus == entity.LearnStatusLearning {
		return errs.ErrDbTableStatus
	}

	err = l.rpc.InfoSec.CheckDbSourceField(ctx, req.GetAppBizId(), req.GetTable().GetDbTableBizId(),
		releaseEntity.AuditDbSourceName, req.GetTable().GetAliasName()+req.GetTable().GetDescription())
	if err != nil {
		return err
	}

	auditData := ""
	for _, column := range req.GetColumns() {
		if utf8.RuneCountInString(column.GetAliasName()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
			entity.MaxDbColumnAliasNameLength) ||
			utf8.RuneCountInString(req.GetTable().GetDescription()) > i18n.CalculateExpandedLength(ctx,
				i18n.UserInputCharType, entity.MaxDbColumnDescriptionLength) ||
			utf8.RuneCountInString(column.GetUnit()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
				entity.MaxDbColumnAliasDataTypeLength) {
			return errs.ErrDbSourceInputExtraLong
		}
		auditData += column.AliasName + "," + column.Description + "," + column.Unit + ","
	}
	err = l.rpc.InfoSec.CheckDbSourceField(ctx, req.GetAppBizId(), req.GetTable().GetDbTableBizId(),
		releaseEntity.AuditDbSourceDesc, auditData)
	if err != nil {
		return err
	}

	updates := map[string]any{
		"alias_name":          req.Table.AliasName,
		"description":         req.Table.Description,
		"release_status":      releaseEntity.ReleaseStatusInit,
		"staff_id":            contextx.Metadata(ctx).StaffID(),
		"table_modified_time": timeNow,
		"update_time":         timeNow,
		"learn_status":        entity.LearnStatusLearning,
	}
	EnableScope := dbTable.EnableScope
	if req.Table.GetEnableScope() != pb.RetrievalEnableScope_ENABLE_SCOPE_TYPE_UNKNOWN &&
		EnableScope != uint32(req.Table.GetEnableScope()) {
		EnableScope = uint32(req.Table.GetEnableScope())
		updates["enable_scope"] = EnableScope
	}

	err = l.dao.ModifyTable(ctx, &tableFilter, updates)
	if err != nil {
		return err
	}

	dbTableColumns := make([]*entity.Column, 0, len(req.Columns))

	for _, column := range req.Columns {
		dbTableColumn := &entity.Column{
			DBTableColumnBizID: column.DbTableColumnBizId,
			AliasName:          column.AliasName,
			Description:        column.Description,
			UpdateTime:         timeNow,
			Unit:               column.Unit,
			IsIndexed:          column.IsIndexed,
		}
		dbTableColumns = append(dbTableColumns, dbTableColumn)
	}

	_, err = l.dao.BatchUpdateByBizID(ctx, corpBizID, req.AppBizId,
		[]string{"alias_name", "description", "is_indexed", "update_time", "unit"}, dbTableColumns)
	if err != nil {
		return err
	}

	appDB, err := l.rpc.AppAdmin.DescribeAppById(ctx, req.GetAppBizId())
	if err != nil {
		return err
	}
	robotId := appDB.PrimaryId
	if err != nil {
		return err
	}

	dbFilter := entity.DatabaseFilter{
		CorpBizID:     corpBizID,
		AppBizID:      req.GetAppBizId(),
		DBSourceBizID: dbTable.DBSourceBizID,
	}
	dbSource, err := l.dao.DescribeDatabase(ctx, &dbFilter)
	// dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), dbTable.DBSourceBizID)
	if err != nil {
		logx.E(ctx, "get db source by biz id failed, %v", err)
		return err
	}

	err = l.CreateDbTableLearnTask(ctx, dbSource, robotId, dbTable.DBTableBizID)
	if err != nil {
		logx.E(ctx, "CreateDbTableLearnTask failed, table:%v err:%v", dbTable.DBTableBizID, err)
		return err
	}

	return nil
}

func (l *Logic) BatchUpdateByBizID(ctx context.Context, corpBizID, appBizID uint64, cols []string,
	dbTableColumns []*entity.Column) (int64, error) {
	return l.dao.BatchUpdateByBizID(ctx, corpBizID, appBizID, cols, dbTableColumns)
}

func (l *Logic) SoftDeleteByTableBizID(ctx context.Context, filter *entity.ColumnFilter) error {
	return l.dao.SoftDeleteByTableBizID(ctx, filter)
}
