package service

import (
	"context"
	"time"
	"unicode/utf8"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/db_source"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// ListDbTableColumn 根据 db_table_biz_id 批量获取列列表
func (s *Service) ListDbTableColumn(ctx context.Context, req *pb.ListDbTableColumnReq) (*pb.ListDbTableColumnRsp, error) {
	corpBizID := pkg.CorpBizID(ctx)
	// 1. 获取表信息
	dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, corpBizID, req.AppBizId, req.DbTableBizId)
	if err != nil {
		return nil, err
	}

	// 2. 判断同步时间，如果长时间没有异步则刷新。
	go func() {
		syncCtx := pkg.NewCtxWithTraceID(ctx)
		syncCtx, cancel := context.WithTimeout(syncCtx, 3*time.Minute)
		defer cancel()
		_, err = db_source.FlashTableAndColumn(syncCtx, dbTable, true, s.dao)
		if err != nil {
			log.ErrorContextf(ctx, "FlashTableAndColumn failed: %v", err)
		}
	}()

	// 4. 根据表名， 获取本地最新的列信息
	dbTableColumns, err := dao.GetDBTableColumnDao().GetByTableBizID(ctx, corpBizID, req.AppBizId, dbTable.DBTableBizID)
	if err != nil {
		return nil, err
	}
	dbTableColumnViews, err := db_source.DBTableColumnsToViews(dbTableColumns)
	if err != nil {
		return nil, err
	}

	return &pb.ListDbTableColumnRsp{
		Columns: dbTableColumnViews,
	}, nil
}

// UpdateDbTableAndColumns 更新表和列数据
func (s *Service) UpdateDbTableAndColumns(ctx context.Context, req *pb.UpdateDbTableAndColumnsReq) (*pb.UpdateDbTableAndColumnsResp, error) {
	corpBizID := pkg.CorpBizID(ctx)
	timeNow := time.Now()

	dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetTable().GetDbTableBizId())
	if err != nil {
		log.ErrorContextf(ctx, "get db table by biz id failed, %v", err)
		return nil, err
	}
	if dbTable.LearnStatus == model.LearnStatusLearning {
		return nil, errs.ErrDbTableStatus
	}

	if utf8.RuneCountInString(req.GetTable().GetAliasName()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		model.MaxDbTableAliasNameLength) || utf8.RuneCountInString(req.GetTable().
		GetDescription()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		model.MaxDbTableDescriptionLength) {
		return nil, errs.ErrDbSourceInputExtraLong
	}

	err = dao.GetDBSourceDao().CheckDbSourceField(ctx, req.GetAppBizId(), req.GetTable().GetDbTableBizId(),
		model.AuditDbSourceName, req.GetTable().GetAliasName()+req.GetTable().GetDescription(), s.dao)
	if err != nil {
		return nil, err
	}

	auditData := ""
	for _, column := range req.GetColumns() {
		if utf8.RuneCountInString(column.GetAliasName()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
			model.MaxDbColumnAliasNameLength) ||
			utf8.RuneCountInString(req.GetTable().GetDescription()) > i18n.CalculateExpandedLength(ctx,
				i18n.UserInputCharType, model.MaxDbColumnDescriptionLength) ||
			utf8.RuneCountInString(column.GetUnit()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
				model.MaxDbColumnAliasDataTypeLength) {
			return nil, errs.ErrDbSourceInputExtraLong
		}
		auditData += column.AliasName + "," + column.Description + "," + column.Unit + ","
	}
	err = dao.GetDBSourceDao().CheckDbSourceField(ctx, req.GetAppBizId(), req.GetTable().GetDbTableBizId(),
		model.AuditDbSourceDesc, auditData, s.dao)
	if err != nil {
		return nil, err
	}

	if req.Table != nil {
		dbTable.AliasName = req.Table.AliasName
		dbTable.Description = req.Table.Description
		dbTable.TableModifiedTime = timeNow
		dbTable.UpdateTime = timeNow
		dbTable.StaffID = pkg.StaffID(ctx)
		dbTable.ReleaseStatus = model.ReleaseStatusUnreleased

		cols := []string{"alias_name", "description", "table_modified_time", "release_status", "update_time", "staff_id"}
		originTable, err := dao.GetDBTableDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), req.Table.DbTableBizId)
		if err != nil {
			return nil, err
		}
		if originTable.NextAction != model.ReleaseActionAdd {
			dbTable.NextAction = model.ReleaseActionUpdate
			cols = append(cols, "next_action")
		}

		err = dao.GetDBTableDao().UpdateByBizID(ctx, corpBizID, req.AppBizId, dbTable.DBTableBizID, cols, dbTable)
		if err != nil {
			return nil, err
		}
	}

	dbTableColumns := make([]*model.DBTableColumn, 0, len(req.Columns))

	for _, column := range req.Columns {
		dbTableColumn := &model.DBTableColumn{
			DBTableColumnBizID: column.DbTableColumnBizId,
			AliasName:          column.AliasName,
			Description:        column.Description,
			UpdateTime:         timeNow,
			Unit:               column.Unit,
			IsIndexed:          column.IsIndexed,
		}
		dbTableColumns = append(dbTableColumns, dbTableColumn)
	}

	_, err = dao.GetDBTableColumnDao().BatchUpdateByBizID(ctx, corpBizID, req.AppBizId,
		[]string{"alias_name", "description", "is_indexed", "update_time", "unit"}, dbTableColumns)
	if err != nil {
		return nil, err
	}

	robotId, err := db_source.GetRobotIdByAppBizId(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}

	dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), dbTable.DBSourceBizID)
	if err != nil {
		log.ErrorContextf(ctx, "get db source by biz id failed, %v", err)
		return nil, err
	}

	err = db_source.CreateDbTableLearnTask(ctx, robotId, corpBizID, req.AppBizId, dbTable.DBTableBizID, dbSource, s.dao)
	if err != nil {
		log.ErrorContextf(ctx, "CreateDbTableLearnTask failed, table:%v err:%v", dbTable.DBTableBizID, err)
		return nil, err
	}

	return &pb.UpdateDbTableAndColumnsResp{}, nil
}
