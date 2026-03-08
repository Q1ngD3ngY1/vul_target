package service

import (
	"context"
	"slices"
	"time"
	"unicode/utf8"

	"git.woa.com/adp/common/x/encodingx/jsonx"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/logx/auditx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"github.com/spf13/cast"
)

// TestDbSourceConnection 测试链接并且获取数据库名称
func (s *Service) TestDbSourceConnection(ctx context.Context,
	req *pb.DbSourceConnectionReq) (*pb.TestDbSourceConnectionRsp, error) {
	logx.I(ctx, "TestDbSourceConnection: %v", req)

	password, err := entity.DecodePassword(req.Password)
	if err != nil {
		logx.W(ctx, "decode password failed: conn: %v err: %v", req, err)
		return nil, err
	}
	now := time.Now()
	connDbSource := entity.DatabaseConn{
		DbType:     req.DbType,
		Host:       req.Host,
		DbName:     req.DbName,
		Username:   req.Username,
		Password:   password,
		Port:       req.Port,
		CreateTime: &now,
		Uin:        contextx.Metadata(ctx).Uin(),
	}
	dbList, err := s.dbLogic.ShowDatabases(ctx, connDbSource)
	if err != nil {
		return nil, err
	}
	return &pb.TestDbSourceConnectionRsp{DbNames: dbList}, nil
}

// ListDBSourceSchema 列举数据源模式
func (s *Service) ListDBSourceSchema(ctx context.Context, req *pb.ListSchemaReq) (*pb.ListSchemaRsp, error) {
	start := time.Now()

	var err error
	rsp := new(pb.ListSchemaRsp)

	logx.I(ctx, "ListDBSourceSchema, request: %+v", req)
	defer func() {
		logx.I(ctx, "ListDBSourceSchema, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()

	if req.DbConn == nil {
		return nil, errs.ErrParameterInvalid
	}

	// 禁止添加系统库
	err = checkDatabaseName(req.GetDbConn().GetDbType(), req.GetDbConn().GetDbName())
	if err != nil {
		logx.E(ctx, "ListDBSourceSchema, checkDatabaseName failed, error: %+v", err)
		return nil, err
	}

	password, err := entity.DecodePassword(req.DbConn.Password)
	if err != nil {
		logx.W(ctx, "ListDBSourceSchema, DecodePassword failed, conn: %+v error: %+v", req.DbConn, err)
		return nil, err
	}

	now := time.Now()
	conn := entity.DatabaseConn{
		DbType:     req.DbConn.DbType,
		Host:       req.DbConn.Host,
		DbName:     req.DbConn.DbName,
		Username:   req.DbConn.Username,
		Password:   password,
		Port:       req.DbConn.Port,
		CreateTime: &now,
		Uin:        contextx.Metadata(ctx).Uin(),
	}

	schemas, err := s.dbLogic.QueryDBSchemas(ctx, conn)
	if err != nil {
		logx.E(ctx, "QueryDBSchemas failed, error: %+v", err)
		return nil, err
	}

	rsp.Schemas = schemas
	return rsp, nil
}

func checkDatabaseName(dbType, dbName string) error {
	if dbType == entity.DBTypeSQLServer {
		if slices.Contains(config.App().Database.ForbiddenDatabaseNames.SQLServer, dbName) {
			return errs.ErrDbNameBanned
		}
	}
	return nil
}

// ListSourceTableNames 获取外部数据库，下的表
func (s *Service) ListSourceTableNames(ctx context.Context, req *pb.ListTablesReq) (*pb.ListTablesRsp, error) {

	// 禁止添加系统库
	err := checkDatabaseName(req.GetDbConn().GetDbType(), req.GetDbConn().GetDbName())
	if err != nil {
		logx.E(ctx, "check db name is banned failed: %v", err)
		return nil, err
	}

	password, err := entity.DecodePassword(req.DbConn.Password)
	if err != nil {
		logx.W(ctx, "decode password failed: conn: %v err: %v", req.DbConn, err)
		return nil, err
	}

	if req.DbConn == nil {
		return nil, errs.ErrParameterInvalid
	}
	now := time.Now()
	conn := entity.DatabaseConn{
		DbType:     req.DbConn.DbType,
		Host:       req.DbConn.Host,
		DbName:     req.DbConn.DbName,
		Username:   req.DbConn.Username,
		Password:   password,
		Port:       req.DbConn.Port,
		SchemaName: req.DbConn.SchemaName,
		CreateTime: &now,
		Uin:        contextx.Metadata(ctx).Uin(),
	}
	// 1. 获取 db_source 信息，建立数据库连接, 获取对应表的数据
	tables, total, err := s.dbLogic.GetDBTableList(ctx, conn, int(req.PageNumber), int(req.PageSize))
	if err != nil {
		logx.E(ctx, "get db table list failed: %v", err)
		return nil, err
	}

	// 3. 将获取到的表数据，然后将表结果返还给前端
	var tableInfo []*pb.TableInfo
	for _, table := range tables {
		tableInfo = append(tableInfo, &pb.TableInfo{
			TableName: table,
		})
	}

	return &pb.ListTablesRsp{
		List:  tableInfo,
		Total: int32(total),
	}, nil
}

// AddDbSource 添加数据库
func (s *Service) AddDbSource(ctx context.Context, req *pb.AddDbSourceReq) (*pb.AddDbSourceRsp, error) {
	logx.I(ctx, "AddDbSource req:%+s", jsonx.MustMarshalToString(req))
	if len(req.TableNames) > config.App().DbSource.MaxTableNumOnce || len(req.TableNames) == 0 {
		return nil, errs.ErrWrapf(errs.ErrDbTableNumIsInvalid, i18n.Translate(ctx, i18nkey.KeySingleAddTableRange),
			config.App().DbSource.MaxTableNumOnce)
	}

	if utf8.RuneCountInString(req.GetAliasName()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		entity.MaxDbSourceAliasNameLength) || utf8.RuneCountInString(req.GetDescription()) > i18n.CalculateExpandedLength(ctx,
		i18n.UserInputCharType, entity.MaxDbSourceDescriptionLength) {
		return nil, errs.ErrDbSourceInputExtraLong
	}

	app, err := s.DescribeAppAndCheckCorp(ctx, cast.ToString(req.GetAppBizId()))
	if err != nil {
		logx.W(ctx, "DescribeAppAndCheckCorp failed: %v", err)
		return nil, err
	}

	enableScope := checkAndGetEnableScope(ctx, app, req.GetEnableScope())

	// 禁止添加系统库
	err = checkDatabaseName(req.GetDbType(), req.GetDbName())
	if err != nil {
		logx.W(ctx, "check db name is banned failed: %v", err)
		return nil, err
	}

	password, err := entity.DecodePassword(req.Password)
	if err != nil {
		logx.W(ctx, "decode password failed: req:%v err:%v", req, err)
		return nil, err
	}

	db := entity.Database{
		CorpBizID:   contextx.Metadata(ctx).CorpBizID(),
		AppBizID:    req.GetAppBizId(),
		DBName:      req.GetDbName(),
		AliasName:   req.GetAliasName(),
		Description: req.GetDescription(),
		DBType:      req.GetDbType(),
		Host:        req.GetHost(), // 存储原始域名或IP
		Port:        req.GetPort(),
		Username:    req.GetUsername(),
		Password:    password,
		SchemaName:  req.GetSchemaName(),
		EnableScope: uint32(enableScope),
	}
	source, err := s.dbLogic.AddDbSource(ctx, &db, req.GetTableNames())
	if err != nil {
		return nil, err
	}
	source.TableNames = req.GetTableNames()

	auditx.Create(auditx.BizDatabase).App(req.GetAppBizId()).
		Space(app.SpaceId).
		Log(ctx, source.DBSourceBizID, source.AliasName)

	return &pb.AddDbSourceRsp{DbSource: databaseDO2PB(source)}, nil
}

func databaseDO2PB(db *entity.Database) *pb.DbSourceView {
	if db == nil {
		return nil
	}
	return &pb.DbSourceView{
		DbSourceBizId: db.DBSourceBizID,
		DbName:        db.DBName,
		AliasName:     db.AliasName,
		Description:   db.Description,
		DbType:        db.DBType,
		Host:          db.Host,
		Port:          db.Port,
		Username:      db.Username,
		Password:      "",
		TableNames:    db.TableNames,
		Alive:         db.Alive,
		LastSyncTime:  db.LastSyncTime.Unix(), // 转换为Unix时间戳(int64)
		CreateTime:    db.CreateTime.Unix(),   // 转换为Unix时间戳(int64)
		Status:        db.ReleaseStatus,
		IsEnabled:     db.IsIndexed,
		StaffName:     db.StaffName,
		TableNum:      int32(len(db.Tables)),
		SchemaName:    db.SchemaName,
		EnableScope:   pb.RetrievalEnableScope(db.EnableScope),
	}
}

func databasesDO2PB(dbs []*entity.Database) []*pb.DbSourceView {
	result := make([]*pb.DbSourceView, 0, len(dbs))
	for _, db := range dbs {
		result = append(result, databaseDO2PB(db))
	}
	return result
}

// DeleteDbSource 删除数据库
func (s *Service) DeleteDbSource(ctx context.Context, req *pb.DeleteDbSourceReq) (*pb.DeleteDbSourceRsp, error) {
	logx.I(ctx, "DeleteDbSource: %v", req)
	app, err := s.DescribeAppAndCheckCorp(ctx, cast.ToString(req.GetAppBizId()))
	if err != nil {
		logx.W(ctx, "DescribeAppAndCheckCorp failed: %v", err)
		return nil, err
	}
	err = s.dbLogic.DeleteDatabase(ctx, req.AppBizId, req.DbSourceBizId)
	if err != nil {
		logx.E(ctx, "delete db source failed: %v", err)
		return nil, err
	}
	auditx.Delete(auditx.BizDatabase).App(req.GetAppBizId()).Space(app.SpaceId).Log(ctx, req.GetDbSourceBizId())

	return &pb.DeleteDbSourceRsp{}, nil
}

// UpdateDbSource 更新数据库
// TODO(ericjwang): 这里只处理了新增的表，那删除的表是在哪里处理的？
func (s *Service) UpdateDbSource(ctx context.Context, req *pb.UpdateDbSourceReq) (*pb.UpdateDbSourceRsp, error) {
	logx.I(ctx, "UpdateDbSource: %v", req)
	if len(req.TableNames) == 0 {
		return nil, errs.ErrWrapf(errs.ErrDbTableNumIsInvalid, i18n.Translate(ctx, i18nkey.KeySingleAddTableRange),
			config.App().DbSource.MaxTableNumOnce)
	}

	if utf8.RuneCountInString(req.GetAliasName()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		entity.MaxDbSourceAliasNameLength) || utf8.RuneCountInString(
		req.GetDescription()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		entity.MaxDbSourceDescriptionLength) {
		return nil, errs.ErrDbSourceInputExtraLong
	}

	app, err := s.DescribeAppAndCheckCorp(ctx, cast.ToString(req.GetAppBizId()))
	if err != nil {
		logx.W(ctx, "DescribeAppAndCheckCorp failed: %v", err)
		return nil, err
	}

	db, err := s.dbLogic.ModifyDatabase(ctx, req)
	if err != nil {
		logx.E(ctx, "UpdateDbSource: update entity error: %v", err)
		return nil, err
	}
	db.TableNames = req.GetTableNames()
	auditx.Modify(auditx.BizDatabase).App(req.GetAppBizId()).Space(app.SpaceId).
		Log(ctx, req.GetDbSourceBizId(), db.AliasName)
	return &pb.UpdateDbSourceRsp{DbSource: databaseDO2PB(db)}, nil
}

// ListDbSource 数据库列表
func (s *Service) ListDbSource(ctx context.Context, req *pb.ListDbSourceReq) (*pb.ListDbSourceRsp, error) {
	if req.PageNumber < 1 || req.PageSize < 1 {
		return nil, errs.ErrPageNumberInvalid
	}

	describeDatabaseListFilter := entity.DatabaseFilter{
		CorpBizID:     contextx.Metadata(ctx).CorpBizID(),
		AppBizID:      req.GetAppBizId(),
		PageNumber:    ptrx.Uint32(req.GetPageNumber()),
		PageSize:      req.GetPageSize(),
		DBNameLike:    ptrx.String(req.GetFilterDbName()),
		WithSyncAlive: true,
		WithTable:     true,
		WithStaffName: true,
	}
	for _, filter := range req.GetFilters() {
		if filter.FilterKey == "IsEnable" {
			if slices.Contains(filter.FilterValue, "true") {
				describeDatabaseListFilter.IsEnable = ptrx.Bool(true)
			} else {
				describeDatabaseListFilter.IsEnable = ptrx.Bool(false)
			}
		}

		if filter.FilterKey == "EnableScope" && len(filter.FilterValue) > 0 {
			ScopeVal := cast.ToUint32(filter.FilterValue[0])
			describeDatabaseListFilter.EnableScope = ptrx.Uint32(ScopeVal)
		}
	}
	dbSources, total, err := s.dbLogic.DescribeDatabaseList(ctx, &describeDatabaseListFilter)
	if err != nil {
		logx.E(ctx, "list db source failed: %v", err)
		return nil, err
	}

	return &pb.ListDbSourceRsp{
		List:  databasesDO2PB(dbSources),
		Total: int32(total),
	}, nil
}

// GetDbSource 查询单一数据源数据
func (s *Service) GetDbSource(ctx context.Context, req *pb.GetDbSourceReq) (*pb.GetDbSourceRsp, error) {
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	dbFilter := entity.DatabaseFilter{
		CorpBizID:     corpBizID,
		AppBizID:      req.GetAppBizId(),
		DBSourceBizID: req.GetDbSourceBizId(),
	}
	db, err := s.dbLogic.DescribeDatabase(ctx, &dbFilter)
	if err != nil {
		logx.E(ctx, "GetDbSource req:%s, error:%v", req, err)
		return nil, err
	}

	tableFilter := entity.TableFilter{
		CorpBizID:     corpBizID,
		AppBizID:      req.GetAppBizId(),
		DBSourceBizID: req.GetDbSourceBizId(),
	}
	tables, _, err := s.dbLogic.DescribeTableList(ctx, &tableFilter)
	if err != nil {
		logx.E(ctx, "GetDbSource DescribeTableList req:%s, error:%v", req, err)
		return nil, err
	}
	db.TableNames = slicex.Pluck(tables, func(t *entity.Table) string { return t.Name })

	return &pb.GetDbSourceRsp{DbSource: databaseDO2PB(db)}, err
}

// BatchGetDbSources 批量查询数据源数据
func (s *Service) BatchGetDbSources(ctx context.Context, req *pb.BatchGetDbSourcesReq) (*pb.BatchGetDbSourcesRsp,
	error) {
	filter := entity.DatabaseFilter{
		CorpBizID:      contextx.Metadata(ctx).CorpBizID(),
		AppBizID:       req.GetAppBizId(),
		DBSourceBizIDs: req.GetDbSourceBizId(),
	}
	dbs, _, err := s.dbLogic.DescribeDatabaseList(ctx, &filter)
	if err != nil {
		logx.E(ctx, "BatchGetDbSources DescribeTableList req:%s, error:%v", req, err)
		return nil, err
	}
	return &pb.BatchGetDbSourcesRsp{DbSources: databasesDO2PB(dbs)}, nil
}

// PreviewTable 预览表数据
func (s *Service) PreviewTable(ctx context.Context, req *pb.PreviewTableReq) (*pb.PreviewTableRsp, error) {
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	dbFilter := entity.DatabaseFilter{
		CorpBizID:     corpBizID,
		AppBizID:      req.GetAppBizId(),
		DBSourceBizID: req.GetDbSourceBizId(),
	}
	dbSource, err := s.dbLogic.DescribeDatabase(ctx, &dbFilter)
	// dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, corpBizID, req.AppBizId, req.DbSourceBizId)
	if err != nil {
		return nil, err
	}
	password, err := util.Decrypt(dbSource.Password, config.App().DbSource.Salt)
	if err != nil {
		return nil, err
	}
	dbConn := entity.DatabaseConn{
		DbType:     dbSource.DBType,
		Host:       dbSource.Host,
		DbName:     dbSource.DBName,
		Username:   dbSource.Username,
		Password:   password,
		Port:       dbSource.Port,
		SchemaName: dbSource.SchemaName,
		CreateTime: &dbSource.CreateTime, // 使用数据库的创建时间
		Uin:        contextx.Metadata(ctx).Uin(),
	}

	tableFilter := entity.TableFilter{
		CorpBizID:     corpBizID,
		AppBizID:      req.GetAppBizId(),
		DBSourceBizID: req.GetDbSourceBizId(),
		Name:          ptrx.String(req.GetTableName()),
	}
	dbTable, err := s.dbLogic.DescribeTable(ctx, &tableFilter)
	// dbTable, err := dao.GetDBTableDao().GetByBizIDAndTableName(ctx, corpBizID, req.AppBizId, req.DbSourceBizId, req.TableName)
	if err != nil || dbTable == nil {
		return nil, errs.ErrWrapf(errs.ErrDbSourceTableColumnNotExist, i18n.Translate(ctx, i18nkey.KeyDataTableNotExist),
			req.TableName)
	}

	columnFilter := entity.ColumnFilter{
		CorpBizID:    corpBizID,
		AppBizID:     req.GetAppBizId(),
		DBTableBizID: dbTable.DBTableBizID,
		Name:         ptrx.String(req.GetFilterColumn()),
	}
	dbTableColumn, err := s.dbLogic.DescribeColumn(ctx, &columnFilter)
	// dbTableColumn, err := dao.GetDBTableColumnDao().GetByColumnByTableBizIDAndColumnName(ctx, corpBizID, req.AppBizId, dbTable.DBTableBizID, req.FilterColumn)
	if err != nil {
		return nil, errs.ErrWrapf(errs.ErrDbSourceTableColumnNotExist, i18n.Translate(ctx, i18nkey.KeyDataColumnNotExist),
			req.FilterColumn)
	}

	// columns, rows, total, err := dao.GetDBSourceDao().ListPreviewData(ctx, dbConn, dbTable.Name, int(req.PageNumber), int(req.PageSize), dbTableColumn.ColumnName, req.FilterValue, config.App().DbSource.ReadConnTimeout)
	columns, rows, total, err := s.dbLogic.ListPreviewData(ctx, dbConn, dbTable.Name, int(req.PageNumber),
		int(req.PageSize), dbTableColumn.ColumnName, req.FilterValue, config.App().DbSource.ReadConnTimeout)
	if err != nil {
		logx.W(ctx, "list preview data failed: %v", err)
		return nil, err
	}

	return &pb.PreviewTableRsp{
		Columns: columns,
		Rows:    rows,
		Total:   int32(total),
	}, nil
}

// ListReleaseDb 发布数据库查看
func (s *Service) ListReleaseDb(ctx context.Context, req *pb.ListReleaseDbReq) (*pb.ListReleaseDbRsp, error) {
	logx.I(ctx, "ListReleaseDbReq: %v", req)
	return &pb.ListReleaseDbRsp{}, nil
	//return s.dbLogic.ListReleaseDb(ctx, req)
}

func (s *Service) GetDbSourcePublicKey(ctx context.Context,
	req *pb.GetDbSourcePublicKeyReq) (*pb.GetDbSourcePublicKeyRsp, error) {
	logx.I(ctx, "GetDbSourcePublicKeyReq: %v", req)
	privateKey, err := util.GetDbSourcePrivateKey()
	if err != nil {
		logx.E(ctx, "GetDbSourcePublicKey failed: %v", err)
		return nil, err
	}
	// 生成公钥
	publicKeyPEM, err := util.GeneratePublicKeyPEMByPrivateKey(privateKey)
	if err != nil {
		logx.E(ctx, "GetDbSourcePublicKey failed: %v", err)
		return nil, err
	}
	return &pb.GetDbSourcePublicKeyRsp{PublicKey: publicKeyPEM}, nil
}

// TextToSQLFromKnowledge 工作流根据用户提问生成 SQL
func (s *Service) TextToSQLFromKnowledge(ctx context.Context,
	req *pb.TextToSQLFromKnowledgeReq) (*pb.TextToSQLFromKnowledgeRsp, error) {
	return s.dbLogic.TextToSQLFromKnowledge(ctx, req)
}

// BatchGetDbSourcesWithTables 批量获取数据库源信息和表信息
func (s *Service) BatchGetDbSourcesWithTables(ctx context.Context,
	req *pb.BatchGetDbSourcesWithTablesReq) (*pb.BatchGetDbSourcesWithTablesRsp, error) {
	if req == nil || (len(req.GetDbSourceBizId()) == 0 && len(req.GetDbTableBizIds()) == 0) {
		return nil, errs.ErrParameterInvalid
	}

	filter := entity.DatabaseFilter{
		CorpBizID:      contextx.Metadata(ctx).CorpBizID(),
		AppBizID:       req.GetAppBizId(),
		DBSourceBizIDs: req.GetDbSourceBizId(),
		DBTableBizIDs:  req.GetDbTableBizIds(),
		WithTable:      true,
	}
	if len(req.GetDbSourceBizId()) > 0 { // 存在同时查默认知识库+共享智库，需重置appBizId
		filter.AppBizID = 0
	}
	dbs, total, err := s.dbLogic.DescribeDatabaseList(ctx, &filter)
	if err != nil {
		logx.E(ctx, "list db source failed: %v", err)
		return nil, err
	}

	rsp := &pb.BatchGetDbSourcesWithTablesRsp{Total: int32(total)}
	for _, db := range dbs {
		rsp.DbSourcesWithTables = append(rsp.DbSourcesWithTables, &pb.DbSourceWithTables{
			DbSource: databaseDO2PB(db),
			DbTables: tablesDO2PB(db.Tables, false, db),
		})
	}
	return rsp, nil
}

func tableDO2PB(t *entity.Table, isShared bool) *pb.DbTableView {
	if t == nil {
		return nil
	}
	status := t.ReleaseStatus
	if t.LearnStatus == entity.LearnStatusLearning {
		status = entity.FaceStatusLearning
	}
	if t.LearnStatus == entity.LearnStatusFailed {
		status = entity.FaceStatusLearnFailed
	}
	if isShared && t.LearnStatus == entity.LearnStatusLearned {
		status = uint32(entity.FaceStatusLearnSuccess)
	}
	return &pb.DbTableView{
		DbTableBizId:      t.DBTableBizID,
		TableName:         t.Name,
		TableSchema:       t.TableSchema,
		AliasName:         t.AliasName,
		Description:       t.Description,
		RowCount:          int32(t.RowCount),
		ColumnCount:       int32(t.ColumnCount),
		TableAddedTime:    t.TableAddedTime.Unix(),
		TableModifiedTime: t.UpdateTime.Unix(),
		Status:            status,
		IsEnabled:         t.IsIndexed,
		IsDeleted:         !t.Alive,
		EnableScope:       pb.RetrievalEnableScope(t.EnableScope),
	}
}

func tablesDO2PB(tables []*entity.Table, isShared bool, db *entity.Database) []*pb.DbTableView {
	result := make([]*pb.DbTableView, 0, len(tables))
	for _, t := range tables {
		tablePB := tableDO2PB(t, isShared)
		tablePB.DbEnableScope = pb.RetrievalEnableScope(db.EnableScope)
		result = append(result, tablePB)
	}
	return result
}

// BatchGetTablesWithColumns 批量获取表信息和列信息
func (s *Service) BatchGetTablesWithColumns(ctx context.Context,
	req *pb.BatchGetTablesWithColumnsReq) (*pb.BatchGetTablesWithColumnsRsp, error) {
	if req == nil || len(req.GetDbSourceTableBizId()) == 0 {
		return nil, errs.ErrParameterInvalid
	}

	tableFilter := entity.TableFilter{
		CorpBizID:     contextx.Metadata(ctx).CorpBizID(),
		AppBizID:      req.GetAppBizId(),
		DBSourceBizID: req.GetDbSourceBizId(),
		DBTableBizIDs: req.GetDbSourceTableBizId(),
		WithColumn:    true,
	}
	tables, total, err := s.dbLogic.DescribeTableList(ctx, &tableFilter)
	if err != nil {
		logx.E(ctx, "BatchGetTablesWithColumns req:%s error:%v", req, err)
		return nil, err
	}

	rsp := &pb.BatchGetTablesWithColumnsRsp{Total: int32(total)}
	for _, t := range tables {
		rsp.DbTablesWithColumns = append(rsp.DbTablesWithColumns, &pb.DbTableWithColumns{
			DbTable:   tableDO2PB(t, false),
			DbColumns: columnsDO2PB(t.Columns),
		})
	}
	return rsp, nil
}

func columnDO2PB(c *entity.Column) *pb.DbTableColumnView {
	if c == nil {
		return nil
	}
	return &pb.DbTableColumnView{
		DbTableColumnBizId: c.DBTableColumnBizID,
		ColumnName:         c.ColumnName,
		DataType:           c.DataType,
		AliasName:          c.AliasName,
		Description:        c.Description,
		Unit:               c.Unit,
		IsIndexed:          c.IsIndexed,
	}
}

func columnsDO2PB(columns []*entity.Column) []*pb.DbTableColumnView {
	result := make([]*pb.DbTableColumnView, 0, len(columns))
	for _, c := range columns {
		result = append(result, columnDO2PB(c))
	}
	return result
}

// DeleteDbTable 删除单一表数据
func (s *Service) DeleteDbTable(ctx context.Context, req *pb.DeleteDbTableReq) (*pb.DeleteDbTableRsp, error) {
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	// 获取 robotId
	appDB, err := s.rpc.AppAdmin.DescribeAppById(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	// 获取table记录
	// TODO(ericjwang): 为什么要查一下？
	tableFilter := entity.TableFilter{
		CorpBizID:    corpBizID,
		AppBizID:     req.GetAppBizId(),
		DBTableBizID: req.GetDbTableBizId(),
		RobotID:      appDB.PrimaryId,
	}
	table, err := s.dbLogic.DescribeTable(ctx, &tableFilter)
	// table, err := dao.GetDBTableDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDbTableBizId())
	if err != nil {
		logx.E(ctx, "get table err:%v,req:%+v", err, req)
		return nil, err
	}
	// 删除table记录
	err = s.dbLogic.DeleteTable(ctx, &tableFilter)
	// err = db_source.DeleteTableAndColumn(ctx, corpBizID, robotId, req.GetAppBizId(), req.DbTableBizId, s.dao)
	if err != nil {
		logx.E(ctx, "soft delete db table failed: %v", err)
		return nil, err
	}
	// 更新数据库修改人
	dbFilter := entity.DatabaseFilter{
		CorpBizID:     corpBizID,
		AppBizID:      req.GetAppBizId(),
		DBSourceBizID: table.DBSourceBizID,
	}
	err = s.dbLogic.ModifyDatabaseSimple(ctx, &dbFilter, map[string]any{"staff_id": contextx.Metadata(ctx).StaffID()})
	// err = dao.GetDBSourceDao().UpdateByBizID(ctx, corpBizID, req.GetAppBizId(), table.DBSourceBizID, []string{"staff_id"}, &entity.Database{StaffID: contextx.Metadata(ctx).StaffID()})
	if err != nil {
		logx.E(ctx, "update db source failed: %v", err)
		return nil, err
	}
	auditx.Delete(auditx.BizDatabaseTable).App(req.GetAppBizId()).Space(appDB.SpaceId).
		Log(ctx, table.DBSourceBizID, req.GetDbTableBizId(), table.AliasName)
	return &pb.DeleteDbTableRsp{}, nil
}

// GetDbTable 查询单一表数据
func (s *Service) GetDbTable(ctx context.Context, req *pb.GetDbTableReq) (*pb.GetDbTableRsp, error) {
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	tableFilter := entity.TableFilter{
		CorpBizID:    corpBizID,
		AppBizID:     req.GetAppBizId(),
		DBTableBizID: req.GetDbTableBizId(),
	}
	dbTable, err := s.dbLogic.DescribeTable(ctx, &tableFilter)
	// dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, corpBizID, req.AppBizId, req.DbTableBizId)
	if err != nil {
		return nil, err
	}
	return &pb.GetDbTableRsp{DbTable: tableDO2PB(dbTable, false)}, nil
}

// ListDbTable 分页查询表数据
func (s *Service) ListDbTable(ctx context.Context, req *pb.ListDbTableReq) (*pb.ListDbTableRsp, error) {
	describeTableListFilter := entity.TableFilter{
		CorpBizID:     contextx.Metadata(ctx).CorpBizID(),
		AppBizID:      req.GetAppBizId(),
		DBSourceBizID: req.GetDbSourceBizId(),
		PageNumber:    ptrx.Uint32(req.GetPageNumber()),
		PageSize:      req.GetPageSize(),
		NameLike:      ptrx.String(req.GetFilterTableName()),
		WithDatabase:  true,
		WithStaffName: true,
	}
	for _, filter := range req.GetFilters() {
		if filter.FilterKey == "EnableScope" && len(filter.FilterValue) > 0 {
			describeTableListFilter.EnableScope = ptrx.Uint32(cast.ToUint32(filter.FilterValue[0]))
		}
	}
	tables, total, err := s.dbLogic.DescribeTableList(ctx, &describeTableListFilter)
	if err != nil {
		logx.E(ctx, "list db table failed: %v", err)
		return nil, err
	}
	var db *entity.Database
	if len(tables) > 0 {
		db = tables[0].Database
	} else {
		dbFilter := &entity.DatabaseFilter{
			DBSourceBizID: req.GetDbSourceBizId(),
		}
		db, err = s.dbLogic.DescribeDatabase(ctx, dbFilter)
		if err != nil {
			return nil, err
		}
	}
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, db.AppBizID)
	if err != nil {
		return nil, err
	}
	return &pb.ListDbTableRsp{
		DbName: db.DBName,
		List:   tablesDO2PB(tables, app.IsShared, db),
		Alive:  db.Alive,
		Total:  int32(total),
	}, nil
}

// ListReleaseDbTable 发布数据表查看
func (s *Service) ListReleaseDbTable(ctx context.Context, req *pb.ListReleaseDbDbTableReq) (*pb.ListReleaseDbDbTableRsp, error) {
	return &pb.ListReleaseDbDbTableRsp{}, nil
	//return s.dbLogic.ListReleaseDbTable(ctx, req)
}

func (s *Service) UpdateDbTableEnabled(ctx context.Context,
	req *pb.UpdateDbTableEnabledReq) (*pb.UpdateDbTableEnabledRsq, error) {
	log.InfoContextf(ctx, "UpdateDbTableEnabled req: %+v", req)
	appDB, err := s.rpc.AppAdmin.DescribeAppById(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	resp := &pb.UpdateDbTableEnabledRsq{}
	if err := s.dbLogic.UpdateDbTableEnabled(ctx, req); err != nil {
		log.ErrorContextf(ctx, "UpdateDbTableEnabled err: %+v", err)
		return resp, err
	}

	if req.IsEnable {
		auditx.Enable(auditx.BizDatabaseTable).App(req.GetAppBizId()).Space(appDB.SpaceId).
			Log(ctx, req.GetDbSourceBizId(), req.GetDbTableBizId())
	} else {
		auditx.Disable(auditx.BizDatabaseTable).App(req.GetAppBizId()).Space(appDB.SpaceId).
			Log(ctx, req.GetDbSourceBizId(), req.GetDbTableBizId())
	}

	return resp, nil
}

// ListDbTableColumn 根据 db_table_biz_id 批量获取列列表
func (s *Service) ListDbTableColumn(ctx context.Context, req *pb.ListDbTableColumnReq) (*pb.ListDbTableColumnRsp,
	error) {
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	// 1. 获取表信息
	tableFilter := entity.TableFilter{
		CorpBizID:    corpBizID,
		AppBizID:     req.AppBizId,
		DBTableBizID: req.DbTableBizId,
	}
	dbTable, err := s.dbLogic.DescribeTable(ctx, &tableFilter)
	if err != nil {
		return nil, err
	}

	// 2. 判断同步时间，如果长时间没有同步则刷新。
	flag, err := s.dbLogic.FlashTableAndColumn(ctx, dbTable)
	if err != nil {
		return nil, err
	}
	if flag {
		log.InfoContext(ctx, "flash table and column success")
	}

	// 4. 根据表名， 获取本地最新的列信息
	columnFilter := entity.ColumnFilter{
		CorpBizID:    corpBizID,
		AppBizID:     req.AppBizId,
		DBTableBizID: dbTable.DBTableBizID,
	}
	dbTableColumns, _, err := s.dbLogic.DescribeColumnList(ctx, &columnFilter)
	if err != nil {
		return nil, err
	}

	return &pb.ListDbTableColumnRsp{Columns: columnsDO2PB(dbTableColumns)}, nil
}

// UpdateDbTableAndColumns 更新表和列数据
func (s *Service) UpdateDbTableAndColumns(ctx context.Context,
	req *pb.UpdateDbTableAndColumnsReq) (*pb.UpdateDbTableAndColumnsResp, error) {
	return &pb.UpdateDbTableAndColumnsResp{}, s.dbLogic.UpdateDbTableAndColumns(ctx, req)
}

// DescribeDbConfig 查询数据库配置
func (s *Service) DescribeDbConfig(ctx context.Context, req *pb.DescribeDbConfigReq) (*pb.DescribeDbConfigRsp, error) {
	start := time.Now()

	var err error
	rsp := new(pb.DescribeDbConfigRsp)

	logx.I(ctx, "DescribeDbConfig, request: %+v", req)
	defer func() {
		logx.I(ctx, "DescribeDbConfig, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()

	rsp.DbTypes, err = s.dbLogic.DescribeAvailableDBTypes(ctx, contextx.Metadata(ctx).CorpBizID(), req.GetAppBizId())
	if err != nil {
		return nil, err
	}

	return rsp, nil
}
