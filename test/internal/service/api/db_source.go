package api

import (
	"fmt"
	"strings"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/db_source"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/common/v3/utils"
	sql_parser "git.woa.com/dialogue-platform/go-comm/sql-parser"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"golang.org/x/net/context"
)

// ExecuteSqlForDbSource 针对用户添加的数据库执行SQL
func (s *Service) ExecuteSqlForDbSource(ctx context.Context, req *pb.ExecuteSqlForDbSourceReq) (
	*pb.ExecuteSqlForDbSourceRsp, error) {
	// 1. 获取应用环境信息
	envType := getEnvType(req.GetEnvType())
	// 2. 获取应用信息
	app, err := client.GetAppInfo(ctx, req.GetAppBizId(), envType)
	if err != nil {
		log.ErrorContext(ctx, "ExecuteSqlForDbSource GetAppInfo fail, err=%+v", err)
		return nil, err
	}
	// 3. 验证SQL表权限
	executeTables, err := s.validateSqlTables(ctx, app, req)
	if err != nil {
		return nil, err
	}
	// 4. 执行SQL并获取结果
	columns, data, effCnt, message, err := db_source.RunSql(ctx, req.GetAppBizId(), req.GetDbSourceBizId(),
		req.GetSqlToExecute(), req.GetSqlParams(), s.dao)
	if err != nil {
		log.ErrorContext(ctx, "ExecuteSqlForDbSource run sql failed", "err", err)
		return nil, err
	}
	// 5. 构建列详细信息
	columnInfos, err := s.buildColumnInfos(ctx, app, req, executeTables, columns)
	if err != nil {
		return nil, err
	}
	// 6. 构建返回结果
	return &pb.ExecuteSqlForDbSourceRsp{
		PreviewTableRsp: &pb.PreviewTableRsp{
			Columns:     columns,
			Rows:        data,
			Total:       int32(len(data)),
			ColumnsInfo: columnInfos,
		},
		EffectRowCount: int32(effCnt),
		ErrorMessage:   message,
	}, nil
}

// getEnvType 获取环境类型
func getEnvType(envType pb.EnvType) uint32 {
	if envType == pb.EnvType_Prod {
		return model.AppReleaseScenes
	}
	return model.AppTestScenes
}

// validateSqlTables 验证SQL表权限
func (s *Service) validateSqlTables(ctx context.Context, app *admin.GetAppInfoRsp,
	req *pb.ExecuteSqlForDbSourceReq) (map[uint64]*model.DBTable, error) {
	executeTableInfos := make(map[uint64]*model.DBTable)
	// 获取用户选择的表信息
	dbTables, err := dao.GetDBTableDao().BatchGetByBizIDs(ctx, app.GetCorpBizId(), req.GetAppBizId(),
		req.GetDbSourceTableBizId())
	if err != nil {
		log.ErrorContext(ctx, "ExecuteSqlForDbSource GetDBTables fail", "err", err)
		return executeTableInfos, err
	}
	log.InfoContextf(ctx, "ExecuteSqlForDbSource user Tables:%s", utils.Any2String(dbTables))
	// 解析SQL中的表
	sqlTables, err := getTablesFromSql(strings.ToLower(req.GetDbType()), req.GetSqlToExecute())
	if err != nil {
		log.ErrorContext(ctx, "ExecuteSqlForDbSource get execute table fail", "err", err)
		return executeTableInfos, err
	}
	log.InfoContextf(ctx, "ExecuteSqlForDbSource execute Tables:%s", utils.Any2String(sqlTables))
	// 构建用户表名集合
	userTables := make(map[string]*model.DBTable, len(dbTables))
	for _, table := range dbTables {
		userTables[table.Name] = table
	}
	// 验证SQL表是否在允许范围内
	for _, table := range sqlTables {
		info, exists := userTables[table]
		if !exists {
			return executeTableInfos, fmt.Errorf(
				"SQL statement references table '%s' not included in the allowed data table range", table)
		}
		executeTableInfos[info.DBTableBizID] = info
	}
	return executeTableInfos, nil
}

// buildColumnInfos 构建列详细信息
func (s *Service) buildColumnInfos(ctx context.Context, app *admin.GetAppInfoRsp, req *pb.ExecuteSqlForDbSourceReq,
	executeTables map[uint64]*model.DBTable, columns []string) ([]*pb.DbTableColumnView, error) {
	// 1. 获取所有表的列信息
	tableColumns, err := s.getTableColumns(ctx, app, req, executeTables)
	if err != nil {
		log.ErrorContext(ctx, "Failed to get table columns", "err", err)
		return nil, err
	}
	log.InfoContext(ctx, "ExecuteSqlForDbSource tableColumns:", utils.Any2String(tableColumns))
	// 2. 构建列名到列信息的映射
	columnMap := s.buildColumnMap(executeTables, tableColumns)
	log.InfoContext(ctx, "ExecuteSqlForDbSource columnMap:", utils.Any2String(columnMap))
	// 3. 构建列详细信息
	columnInfos := make([]*pb.DbTableColumnView, 0, len(columns))
	for _, column := range columns {
		columnInfo := s.buildColumnInfo(ctx, column, columnMap)
		columnInfos = append(columnInfos, columnInfo)
	}
	return columnInfos, nil
}

// buildColumnMap 构建列名到列信息的映射
func (s *Service) buildColumnMap(executeTables map[uint64]*model.DBTable,
	tableColumns map[uint64][]*model.DBTableColumn,
) map[string][]*pb.DbTableColumnView {
	columnMap := make(map[string][]*pb.DbTableColumnView)
	for tableBizID, columns := range tableColumns {
		_, exists := executeTables[tableBizID]
		if !exists {
			continue
		}
		for _, col := range columns {
			meta := &pb.DbTableColumnView{
				ColumnName:  col.ColumnName,
				DataType:    col.DataType,
				Description: col.ColumnComment,
				Unit:        col.Unit,
			}
			columnMap[col.ColumnName] = append(columnMap[col.ColumnName], meta)
		}
	}
	return columnMap
}

// buildColumnInfo 构建单个列信息
func (s *Service) buildColumnInfo(ctx context.Context, column string,
	columnMap map[string][]*pb.DbTableColumnView) *pb.DbTableColumnView {
	columnInfo := &pb.DbTableColumnView{
		ColumnName: column,
	}
	// 查找对应的列元数据
	cols, exists := columnMap[column]
	if !exists {
		log.WarnContextf(ctx, "Column '%s' not found in table", column)
		return columnInfo
	}
	// 处理同名列（联表查询场景）
	if len(cols) > 1 { // 联表场景：默认取第一个,这里预留分支，后续联表场景可能需要更复杂的处理逻辑
		col := cols[0]
		log.WarnContextf(ctx, "Column '%s' has more than one column", column)
		columnInfo = col
	} else { // 单表场景
		columnInfo = cols[0]
	}
	return columnInfo
}

// getTableColumns 获取所有表的列信息
func (s *Service) getTableColumns(ctx context.Context, app *admin.GetAppInfoRsp, req *pb.ExecuteSqlForDbSourceReq,
	executeTables map[uint64]*model.DBTable) (map[uint64][]*model.DBTableColumn, error) {
	// 获取执行表的列详情
	executeTableBizIds := make([]uint64, 0, len(executeTables))
	for tableBizID := range executeTables {
		executeTableBizIds = append(executeTableBizIds, tableBizID)
	}
	return dao.GetDBTableColumnDao().BatchGetByTableBizID(ctx, app.CorpBizId, app.AppBizId, executeTableBizIds)
}

// getTablesFromSql 解析sql中涉及到的表名
func getTablesFromSql(dbName string, sql string) ([]string, error) {
	var dbType sql_parser.DatabaseType
	switch dbName {
	case "mysql":
		dbType = sql_parser.MySQL
	case "sqlserver":
		dbType = sql_parser.SQLServer
	default:
		return nil, fmt.Errorf("unsupported database type")
	}
	tables, err := sql_parser.ParseSingleTables(dbType, sql)
	if err != nil {
		return nil, err
	}
	return tables, nil
}

// TextToSQLFromKnowledge 根据用户提问生成 SQL
func (s *Service) TextToSQLFromKnowledge(ctx context.Context, req *pb.TextToSQLFromKnowledgeReq) (
	*pb.TextToSQLFromKnowledgeRsp, error) {
	return db_source.TextToSQLFromKnowledge(ctx, req, s.dao)
}

// ListDbSourceBizIDsWithTableBizIDs 获取数据库源和数据表业务ID
func (s *Service) ListDbSourceBizIDsWithTableBizIDs(ctx context.Context, req *pb.ListDbSourceBizIDsWithTableBizIDsReq) (
	*pb.ListDbSourceBizIDsWithTableBizIDsRsp, error) {
	if req.GetDbSourceBizId() == 0 {
		// 如果传入了appBizID，则查询该应用下的数据库源
		dbSources, total, err := db_source.ListDbSourcesWithTables(ctx, req.GetAppBizId(), req.GetPageSize(),
			req.GetPageNumber())
		if err != nil {
			log.ErrorContextf(ctx, "ListDbSourceBizIDsWithTableBizIDs|batch get fail: %v, appBizID:%v", err, req.GetAppBizId())
			return nil, err
		}
		rsp := &pb.ListDbSourceBizIDsWithTableBizIDsRsp{
			DbSources: dbSources,
			Total:     int32(total),
		}
		return rsp, nil
	} else {
		// 否则查询单一数据库源
		dbSources, err := db_source.GetDbSourcesWithTables(ctx, req.GetDbSourceBizId())
		if err != nil {
			log.ErrorContextf(ctx, "ListDbSourceBizIDsWithTableBizIDs|single get failed: %v, dbSourceBizID:%v", err,
				req.GetDbSourceBizId())
			return nil, err
		}
		rsp := &pb.ListDbSourceBizIDsWithTableBizIDsRsp{
			DbSources: dbSources,
			Total:     int32(len(dbSources)),
		}
		return rsp, nil
	}
}
