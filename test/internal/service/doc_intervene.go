package service

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/db_source"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc_intervene"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// Text2SqlPreviewTable 预览外部数据库表格下的列数据
func (s *Service) Text2SqlPreviewTable(ctx context.Context, req *pb.Text2SqlPreviewTableReq) (*pb.Text2SqlPreviewTableRsp, error) {
	log.InfoContextf(ctx, "Text2SqlPreviewTable: %v", req)
	sheets, err := doc_intervene.PreviewText2sqlData(ctx, req.GetAppBizId(), req.GetDocBizId(), int(req.GetPageNumber()), int(req.GetPageSize()), s.dao)
	if err != nil {
		return nil, err
	}

	return &pb.Text2SqlPreviewTableRsp{
		SheetData: sheets,
	}, nil
}

// Text2SqlGetColumns text2sql 获取text2sql 列描述
func (s *Service) Text2SqlGetColumns(ctx context.Context, req *pb.GetText2SqlColumnsReq) (*pb.GetText2SqlColumnsRsp, error) {
	log.InfoContext(ctx, "Text2SqlGetColumns: %v", req)
	corpBizID := pkg.CorpBizID(ctx)
	// 1. 获取表信息
	dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDocSheetBizId())
	if err != nil {
		return nil, err
	}

	// 2. 根据表名， 获取本地最新的列信息
	dbTableColumns, err := dao.GetDBTableColumnDao().GetByTableBizID(ctx, corpBizID, req.GetAppBizId(), dbTable.DBTableBizID)
	if err != nil {
		return nil, err
	}
	// 3. 过滤列名为 "@id", "@segment_id" 的列
	dbTableColumnsFiltered := make([]*model.DBTableColumn, 0, len(dbTableColumns))

	for _, column := range dbTableColumns {
		if column.ColumnName != "@id" && column.ColumnName != "@segment_id" {
			dbTableColumnsFiltered = append(dbTableColumnsFiltered, column)
		}
	}
	dbTableColumnViews, err := db_source.Text2sqlColumnsToViews(dbTableColumnsFiltered)
	if err != nil {
		return nil, err
	}

	return &pb.GetText2SqlColumnsRsp{
		Description: dbTable.Description,
		ColumnViews: dbTableColumnViews,
	}, nil
}

// UpdateText2SqlColumns text2sql 更新列描述数据
func (s *Service) UpdateText2SqlColumns(ctx context.Context, req *pb.UpdateText2SqlColumnsReq) (*pb.UpdateText2SqlColumnsRsp, error) {
	log.InfoContextf(ctx, "UpdateText2SqlColumns: %v", req)
	corpBizID := pkg.CorpBizID(ctx)
	dbTableColumns := make([]*model.DBTableColumn, 0, len(req.Columns))

	for _, column := range req.Columns {
		dbTableColumn := &model.DBTableColumn{
			DBTableColumnBizID: column.DbTableColumnBizId,
			AliasName:          column.AliasName,
			Description:        column.Description,
			UpdateTime:         time.Now(),
			Unit:               column.Unit,
		}
		dbTableColumns = append(dbTableColumns, dbTableColumn)
	}

	dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDocSheetBizId())
	if err != nil {
		return nil, err
	}

	cols := []string{"description", "update_time", "release_status"}
	dbTable.ReleaseStatus = model.ReleaseStatusUnreleased
	if dbTable.NextAction != model.ReleaseActionAdd {
		dbTable.NextAction = model.ReleaseActionUpdate
		cols = append(cols, "next_action")
	}
	dbTable.Description = req.GetDescription()
	dbTable.UpdateTime = time.Now()
	err = dao.GetDBTableDao().UpdateByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDocSheetBizId(),
		cols, dbTable)
	if err != nil {
		return nil, err
	}

	_, err = dao.GetDBTableColumnDao().BatchUpdateByBizID(ctx, corpBizID, req.GetAppBizId(),
		[]string{"alias_name", "description", "unit"}, dbTableColumns)
	if err != nil {
		return nil, err
	}

	return &pb.UpdateText2SqlColumnsRsp{}, nil
}
