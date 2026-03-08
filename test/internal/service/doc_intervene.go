package service

import (
	"context"
	"errors"
	"time"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	dbEntity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// Text2SqlPreviewTable 预览外部数据库表格下的列数据
func (s *Service) Text2SqlPreviewTable(ctx context.Context, req *pb.Text2SqlPreviewTableReq) (*pb.Text2SqlPreviewTableRsp, error) {
	logx.I(ctx, "Text2SqlPreviewTable: %v", req)
	sheets, err := s.docLogic.PreviewText2sqlData(ctx, req.GetAppBizId(), req.GetDocBizId(), int(req.GetPageNumber()), int(req.GetPageSize()))
	if err != nil {
		return nil, err
	}

	return &pb.Text2SqlPreviewTableRsp{
		SheetData: sheets,
	}, nil
}

// Text2SqlGetColumns text2sql 获取text2sql 列描述
func (s *Service) Text2SqlGetColumns(ctx context.Context, req *pb.GetText2SqlColumnsReq) (*pb.GetText2SqlColumnsRsp, error) {
	logx.I(ctx, "Text2SqlGetColumns: %v", req)
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	// 1. 获取表信息
	tableFilter := dbEntity.TableFilter{
		CorpBizID:    corpBizID,
		AppBizID:     req.GetAppBizId(),
		DBTableBizID: req.GetDocSheetBizId(),
	}
	dbTable, err := s.dbLogic.DescribeTable(ctx, &tableFilter)
	// dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDocSheetBizId())
	if err != nil {
		return nil, err
	}

	// 2. 根据表名， 获取本地最新的列信息
	columnFilter := dbEntity.ColumnFilter{
		CorpBizID:    corpBizID,
		AppBizID:     req.GetAppBizId(),
		DBTableBizID: req.GetDocSheetBizId(),
	}
	dbTableColumns, _, err := s.dbLogic.DescribeColumnList(ctx, &columnFilter)
	// dbTableColumns, err := dao.GetDBTableColumnDao().GetByTableBizID(ctx, corpBizID, req.GetAppBizId(), dbTable.DBTableBizID)
	if err != nil {
		return nil, err
	}
	// 3. 过滤列名为 "@id", "@segment_id" 的列
	dbTableColumnsFiltered := make([]*dbEntity.Column, 0, len(dbTableColumns))

	for _, column := range dbTableColumns {
		if column.ColumnName != "@id" && column.ColumnName != "@segment_id" {
			dbTableColumnsFiltered = append(dbTableColumnsFiltered, column)
		}
	}
	dbTableColumnViews, err := text2sqlColumnsToViews(dbTableColumnsFiltered)
	if err != nil {
		return nil, err
	}

	return &pb.GetText2SqlColumnsRsp{
		Description: dbTable.Description,
		ColumnViews: dbTableColumnViews,
	}, nil
}

func text2sqlTableColumnsToView(m *dbEntity.Column) (*pb.Text2SqlTableColumnView, error) {
	if m == nil {
		return nil, errors.New("model is nil")
	}
	return &pb.Text2SqlTableColumnView{
		DbTableColumnBizId: m.DBTableColumnBizID,
		ColumnName:         m.ColumnName,
		AliasName:          m.AliasName,
		Description:        m.Description,
		DataType:           m.DataType,
		Unit:               m.Unit,
	}, nil
}

func text2sqlColumnsToViews(models []*dbEntity.Column) ([]*pb.Text2SqlTableColumnView, error) {
	result := make([]*pb.Text2SqlTableColumnView, 0, len(models))
	for _, m := range models {
		if v, err := text2sqlTableColumnsToView(m); v != nil {
			result = append(result, v)
			if err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

// UpdateText2SqlColumns text2sql 更新列描述数据
func (s *Service) UpdateText2SqlColumns(ctx context.Context, req *pb.UpdateText2SqlColumnsReq) (*pb.UpdateText2SqlColumnsRsp, error) {
	logx.I(ctx, "UpdateText2SqlColumns: %v", req)
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	dbTableColumns := make([]*dbEntity.Column, 0, len(req.Columns))

	for _, column := range req.Columns {
		dbTableColumn := &dbEntity.Column{
			DBTableColumnBizID: column.DbTableColumnBizId,
			AliasName:          column.AliasName,
			Description:        column.Description,
			UpdateTime:         time.Now(),
			Unit:               column.Unit,
		}
		dbTableColumns = append(dbTableColumns, dbTableColumn)
	}

	tableFilter := dbEntity.TableFilter{
		CorpBizID:    corpBizID,
		AppBizID:     req.GetAppBizId(),
		DBTableBizID: req.GetDocSheetBizId(),
	}
	dbTable, err := s.dbLogic.DescribeTable(ctx, &tableFilter)
	// dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDocSheetBizId())
	if err != nil {
		return nil, err
	}

	tableData := map[string]any{
		"description":    req.GetDescription(),
		"update_time":    time.Now(),
		"release_status": releaseEntity.ReleaseStatusInit,
	}
	if dbTable.NextAction != releaseEntity.ReleaseActionAdd {
		tableData["next_action"] = releaseEntity.ReleaseActionUpdate
	}
	err = s.dbLogic.ModifyTable(ctx, &tableFilter, tableData)
	// err = dao.GetDBTableDao().UpdateByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDocSheetBizId(), cols, dbTable)
	if err != nil {
		return nil, err
	}

	_, err = s.dbLogic.BatchUpdateByBizID(ctx, corpBizID, req.GetAppBizId(), []string{"alias_name", "description", "unit"}, dbTableColumns)
	if err != nil {
		return nil, err
	}

	return &pb.UpdateText2SqlColumnsRsp{}, nil
}
