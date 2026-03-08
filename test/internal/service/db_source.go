package service

import (
	"context"
	"slices"
	"time"
	"unicode/utf8"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/db_source"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	errgroup "git.woa.com/dialogue-platform/common/v3/sync/errgroupx"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"google.golang.org/protobuf/proto"
)

func decodePassword(string2 string) (string, error) {
	priv, err := util.GetDbSourcePrivateKey()
	if err != nil {
		return "", err
	}
	password, err := util.DecryptWithPrivateKeyPEM(priv, string2)
	if err != nil {
		return "", errs.ErrPasswordDecodeFail
	}
	return password, nil
}

// TestDbSourceConnection 测试链接并且获取数据库名称
func (s *Service) TestDbSourceConnection(ctx context.Context, req *pb.DbSourceConnectionReq) (
	*pb.TestDbSourceConnectionRsp, error) {
	log.InfoContextf(ctx, "TestDbSourceConnection: %v", req)

	// 检查是否有权限访问内网数据库，并获取解析后的IP
	resolvedHost, err := db_source.CheckInternalDBAccess(ctx, req.Host, 0)
	if err != nil {
		return nil, err
	}

	password, err := decodePassword(req.Password)
	if err != nil {
		log.WarnContextf(ctx, "decode password failed: conn: %v err: %v", req, err)
		return nil, err
	}

	connDbSource := model.ConnDbSource{
		DbType:   req.DbType,
		Host:     resolvedHost, // 使用解析后的IP
		DbName:   "",
		Username: req.Username,
		Password: password,
		Port:     int(req.Port),
	}
	dbList, err := dao.GetDBSourceDao().GetDBList(ctx, connDbSource)
	if err != nil {
		return nil, err
	}
	return &pb.TestDbSourceConnectionRsp{DbNames: dbList}, nil
}

// ListSourceTableNames 获取外部数据库，下的表
func (s *Service) ListSourceTableNames(ctx context.Context, req *pb.ListTablesReq) (*pb.ListTablesRsp, error) {

	var tableInfo []*pb.TableInfo

	// 检查是否有权限访问内网数据库，并获取解析后的IP（这里没有appBizID，使用0表示全局白名单）
	resolvedHost, err := db_source.CheckInternalDBAccess(ctx, req.GetDbConn().GetHost(), 0)
	if err != nil {
		return nil, err
	}

	// 禁止添加系统库
	err = dao.GetDBSourceDao().CheckDbNameIsBanned(req.GetDbConn().GetDbName(), req.GetDbConn().GetDbType())
	if err != nil {
		log.ErrorContextf(ctx, "check db name is banned failed: %v", err)
		return nil, err
	}

	password, err := decodePassword(req.DbConn.Password)
	if err != nil {
		log.WarnContextf(ctx, "decode password failed: conn: %v err: %v", req.DbConn, err)
		return nil, err
	}

	if req.DbConn == nil {
		return nil, errs.ErrParameterInvalid
	}
	conn := model.ConnDbSource{
		DbType:   req.DbConn.DbType,
		Host:     resolvedHost, // 使用解析后的IP
		DbName:   req.DbConn.DbName,
		Username: req.DbConn.Username,
		Password: password,
		Port:     int(req.DbConn.Port),
	}
	// 1. 获取 db_source 信息，建立数据库连接, 获取对应表的数据
	tables, total, err := dao.GetDBSourceDao().GetDBTableList(ctx, conn, int(req.PageNumber), int(req.PageSize))

	if err != nil {
		log.ErrorContextf(ctx, "get db table list failed: %v", err)
		return nil, err
	}

	// 3. 将获取到的表数据，然后将表结果返还给前端
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
	// 检查是否有权限访问内网数据库，并获取解析后的IP
	resolvedHost, err := db_source.CheckInternalDBAccess(ctx, req.Host, req.GetAppBizId())
	if err != nil {
		return nil, err
	}

	if len(req.TableNames) > config.App().DbSource.MaxTableNumOnce || len(req.TableNames) == 0 {
		return nil, errs.ErrWrapf(errs.ErrDbTableNumIsInvalid, i18n.Translate(ctx, i18nkey.KeySingleAddTableRange),
			config.App().DbSource.MaxTableNumOnce)
	}

	if utf8.RuneCountInString(req.GetAliasName()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		model.MaxDbSourceAliasNameLength) || utf8.RuneCountInString(req.GetDescription()) > i18n.CalculateExpandedLength(ctx,
		i18n.UserInputCharType, model.MaxDbSourceDescriptionLength) {
		return nil, errs.ErrDbSourceInputExtraLong
	}

	// 禁止添加系统库
	err = dao.GetDBSourceDao().CheckDbNameIsBanned(req.GetDbName(), req.GetDbType())
	if err != nil {
		log.WarnContextf(ctx, "check db name is banned failed: %v", err)
		return nil, err
	}

	password, err := decodePassword(req.Password)
	if err != nil {
		log.WarnContextf(ctx, "decode password failed: req:%v err:%v", req, err)
		return nil, err
	}
	connDbSource := model.ConnDbSource{
		DbType:   req.DbType,
		Host:     resolvedHost, // 使用解析后的IP
		DbName:   req.DbName,
		Username: req.Username,
		Password: password,
		Port:     int(req.Port),
	}

	source, err := db_source.AddDbSource(ctx, req.AppBizId, req.AliasName, req.Description, connDbSource, req.TableNames,
		s.dao)
	if err != nil {
		return nil, err
	}

	dbSourceView, err := db_source.DBSourceModelToViewProto(source, req.TableNames, 0)
	if err != nil {
		log.ErrorContextf(ctx, "convert db source failed: %v", err)
		return nil, err
	}
	log.InfoContextf(ctx, "add db source: %v", dbSourceView)
	return &pb.AddDbSourceRsp{
		DbSource: dbSourceView,
	}, nil

}

// DeleteDbSource 删除数据库
func (s *Service) DeleteDbSource(ctx context.Context, req *pb.DeleteDbSourceReq) (*pb.DeleteDbSourceRsp, error) {
	log.InfoContextf(ctx, "DeleteDbSource: %v", req)
	err := db_source.DeleteDbSourceAndTableCol(ctx, req.AppBizId, req.DbSourceBizId, s.dao)
	if err != nil {
		log.ErrorContextf(ctx, "delete db source failed: %v", err)
		return nil, err
	}
	return &pb.DeleteDbSourceRsp{}, nil
}

// UpdateDbSource 更新数据库
func (s *Service) UpdateDbSource(ctx context.Context, req *pb.UpdateDbSourceReq) (*pb.UpdateDbSourceRsp, error) {
	log.InfoContextf(ctx, "UpdateDbSource: %v", req)
	if len(req.TableNames) == 0 {
		return nil, errs.ErrWrapf(errs.ErrDbTableNumIsInvalid, i18n.Translate(ctx, i18nkey.KeySingleAddTableRange),
			config.App().DbSource.MaxTableNumOnce)
	}

	if utf8.RuneCountInString(req.GetAliasName()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		model.MaxDbSourceAliasNameLength) || utf8.RuneCountInString(
		req.GetDescription()) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType,
		model.MaxDbSourceDescriptionLength) {
		return nil, errs.ErrDbSourceInputExtraLong
	}

	copBizId := pkg.CorpBizID(ctx)

	// 1. 获取原始数据库
	dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, copBizId, req.GetAppBizId(), req.GetDbSourceBizId())
	if err != nil {
		log.ErrorContextf(ctx, "get db source failed: %v", err)
		return nil, errs.ErrUpdateDbSourceGetFail
	}

	if dbSource.DBName != req.GetDbName() {
		return nil, errs.ErrWrapf(errs.ErrDbNameIsInvalid, i18n.Translate(ctx, i18nkey.KeyDatabaseNameNotAllowedModify))
	}

	// 1.1 判断数据库名称是否被禁用
	err = dao.GetDBSourceDao().CheckDbSourceField(ctx, req.GetAppBizId(), dbSource.DBSourceBizID, model.AuditDbSourceName,
		req.GetAliasName(), s.dao)
	if err != nil {
		return nil, err
	}
	// 1.2 判断数据库描述是否存在敏感词
	err = dao.GetDBSourceDao().CheckDbSourceField(ctx, req.GetAppBizId(), dbSource.DBSourceBizID, model.AuditDbSourceDesc,
		req.GetDescription(), s.dao)
	if err != nil {
		return nil, err
	}

	// 2. 判断数据库是否已经存在
	dbTableList, err := dao.GetDBTableDao().ListAllByDBSourceBizID(ctx, copBizId, req.GetAppBizId(),
		req.GetDbSourceBizId())
	if err != nil {
		log.ErrorContextf(ctx, "get db table failed: %v", err)
		return nil, errs.ErrUpdateDbSourceGetFail
	}

	localDbTables := make(map[string]*model.DBTable)
	for _, dbTable := range dbTableList {
		localDbTables[dbTable.Name] = dbTable
	}

	newTable := make([]string, 0)
	for _, tableName := range req.GetTableNames() {
		if _, ok := localDbTables[tableName]; !ok {
			newTable = append(newTable, tableName)
		}
	}

	if len(newTable)+len(dbTableList) > config.App().DbSource.MaxTableNum {
		return nil, errs.ErrWrapf(errs.ErrDbTableNumIsInvalid, i18n.Translate(ctx, i18nkey.KeyDatabaseMaxAddAmount),
			config.App().DbSource.MaxTableNum)
	}

	// 2. 批量创建数据表和列
	dbTables, err := db_source.BatchCreateDbTableAndColumn(ctx, dbSource, newTable, s.dao)
	if err != nil {
		log.ErrorContextf(ctx, "batch create db table and column failed: %v", err)
		return nil, err
	}

	// 3. 获取 robotId
	robotId, err := db_source.GetRobotIdByAppBizId(ctx, req.GetAppBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}

	wg, wgCtx := errgroup.WithContext(ctx)
	wg.SetLimit(5)
	// 4. 保存到 es
	for _, value := range dbTables {
		dbTable := value
		wg.Go(func() (err error) {
			err = db_source.CreateDbTableLearnTask(ctx, robotId, copBizId, req.GetAppBizId(), dbTable.DBTableBizID, dbSource,
				s.dao)
			if err != nil {
				log.ErrorContextf(wgCtx, "CreateDbTableLearnTask failed, table:%v err:%v", dbTable.DBTableBizID, err)
				return errs.ErrWriteIntoEsFail
			}
			return nil
		})
	}
	if err = wg.Wait(); err != nil {
		log.ErrorContextf(ctx, "UpdateDbSource|AddDbTableData2ES1 dbSource %v; dbTable %v; failed: %v", dbSource, dbTables,
			err)
		return nil, err
	}

	password, err := decodePassword(req.GetPassword())
	if err != nil {
		log.WarnContextf(ctx, "decode password failed: req: %v err:%v", req, err)
		return nil, err
	}

	// 5. 更新数据库
	encryptedPassword, err := util.Encrypt(password)
	if err != nil {
		log.ErrorContextf(ctx, "encrypt password failed: %v", err)
		return nil, err
	}
	dbSource.Password = encryptedPassword
	dbSource.Description = req.GetDescription()
	dbSource.AliasName = req.GetAliasName()
	dbSource.Username = req.GetUsername()
	dbSource.StaffID = pkg.StaffID(ctx)
	dbSource.LastSyncTime = time.Now()

	dbSource.ReleaseStatus = model.ReleaseStatusUnreleased
	if dbSource.NextAction != model.ReleaseActionAdd {
		dbSource.NextAction = model.ReleaseActionUpdate
	}
	err = dao.GetDBSourceDao().UpdateByBizID(ctx, copBizId, req.GetAppBizId(), req.GetDbSourceBizId(),
		[]string{"alias_name", "description", "username", "password", "staff_id", "release_status", "last_sync_time"},
		dbSource)
	if err != nil {
		log.ErrorContextf(ctx, "update db source failed: %v", err)
		return nil, errs.ErrUpdateDbSourceCreateFail
	}

	dbSourceView, err := db_source.DBSourceModelToViewProto(dbSource, req.TableNames, 0)
	if err != nil {
		log.ErrorContextf(ctx, "convert db source failed: %v", err)
		return nil, errs.ErrUpdateDbSourceCreateFail
	}

	return &pb.UpdateDbSourceRsp{
		DbSource: dbSourceView,
	}, nil
}

// ListDbSource 数据库列表
func (s *Service) ListDbSource(ctx context.Context, req *pb.ListDbSourceReq) (*pb.ListDbSourceRsp, error) {
	if req.PageNumber < 1 || req.PageSize < 1 {
		return nil, errs.ErrPageNumberInvalid
	}

	var (
		dbSources []*model.DBSource
		cnt       int64
		err       error
	)

	copBizId := pkg.CorpBizID(ctx)

	opt := &dao.ListDBSourcesOption{
		CorpBizID: copBizId,
		AppBizID:  req.AppBizId,
		IDOrName:  req.FilterDbName, // 如果为空则不添加DB名过滤
		Page:      int(req.PageNumber),
		PageSize:  int(req.PageSize),
	}

	for _, filter := range req.GetFilters() {
		if filter.FilterKey == "IsEnable" {
			if slices.Contains(filter.FilterValue, "true") {
				opt.IsEnable = proto.Bool(true)
			} else {
				opt.IsEnable = proto.Bool(false)
			}
		}
	}
	dbSources, cnt, err = dao.GetDBSourceDao().ListByOption(ctx, opt)
	if err != nil {
		log.ErrorContextf(ctx, "list db source failed: %v", err)
		return nil, err
	}
	staffIDs := make([]uint64, 0, len(dbSources))

	dbSourcesBizIDs := make([]uint64, 0, len(dbSources))
	// 判断 db_source 状态是否正常
	for _, dbSource := range dbSources {
		staffIDs = append(staffIDs, dbSource.StaffID)
		err := db_source.UpdateDbSourceState(ctx, dbSource)
		if err != nil {
			log.ErrorContextf(ctx, "update db source [%d] failed: %v", dbSource.ID, err)
			return nil, err
		}
		dbSourcesBizIDs = append(dbSourcesBizIDs, dbSource.DBSourceBizID)
	}
	// 如果数据表存在学习中，则将数据库的状态修改为学习中
	numOfLearning, err := dao.GetDBSourceDao().CountByBizIDsAndStatus(ctx, copBizId, req.GetAppBizId(), dbSourcesBizIDs)
	if err != nil {
		log.WarnContextf(ctx, "count db source learning failed: %v", err)
	}
	if numOfLearning != nil {
		for _, dbSource := range dbSources {
			if numOfLearning[dbSource.DBSourceBizID] > 0 {
				dbSource.ReleaseStatus = model.FaceStatusLearning
			}
		}
	}
	dbSourcesBizID2TableNum := make(map[uint64]int32)
	if len(dbSourcesBizIDs) > 200 {
		batchSize := 200
		for i := 0; i < len(dbSourcesBizIDs); i += batchSize {
			end := i + batchSize
			if end > len(dbSourcesBizIDs) {
				end = len(dbSourcesBizIDs)
			}
			batch := dbSourcesBizIDs[i:end]
			batchResult, err := dao.GetDBTableDao().GetCountByDbSourceBizIDs(ctx, copBizId, batch)
			if err != nil {
				log.WarnContextf(ctx, "count db table failed:dbSource:%v, err:%v", batch, err)
				continue
			}
			for k, v := range batchResult {
				dbSourcesBizID2TableNum[k] = v
			}
		}
	} else {
		dbSourcesBizID2TableNum, err = dao.GetDBTableDao().GetCountByDbSourceBizIDs(ctx, copBizId, dbSourcesBizIDs)
		if err != nil {
			log.WarnContextf(ctx, "count db table failed:dbSource:%v, err:%v", dbSourcesBizIDs, err)
		}
	}

	// 获取员工名称
	staffByID, err := client.ListCorpStaffByIds(ctx, copBizId, staffIDs)
	if err != nil { // 失败降级为返回员工ID
		log.ErrorContextf(ctx, "ListDbSource get staff name err:%v,staffIDs:%v", err, staffIDs)
	}
	dbSourceView, err := db_source.DBSourceModelsToViews(dbSources, staffByID, dbSourcesBizID2TableNum)
	if err != nil {
		log.ErrorContextf(ctx, "convert db source failed: %v", err)
		return nil, errs.ErrTypeConvertFail
	}
	return &pb.ListDbSourceRsp{
		List:  dbSourceView,
		Total: int32(cnt),
	}, nil
}

// GetDbSource 查询单一数据源数据
func (s *Service) GetDbSource(ctx context.Context, req *pb.GetDbSourceReq) (*pb.GetDbSourceRsp, error) {
	copBizId := pkg.CorpBizID(ctx)

	source, err := dao.GetDBSourceDao().GetByBizID(ctx, copBizId, req.AppBizId, req.DbSourceBizId)
	if err != nil {
		log.ErrorContextf(ctx, "get db source failed: %v", err)
		return nil, err
	}

	tableNames, err := dao.GetDBTableDao().ListAllTableNameByDBSourceBizID(ctx, copBizId, req.AppBizId, req.DbSourceBizId)
	if err != nil {
		log.ErrorContextf(ctx, "get db table failed: %v", err)
		return nil, err
	}
	dbSourceView, err := db_source.DBSourceModelToViewProto(source, tableNames, 0)
	if err != nil {
		log.ErrorContextf(ctx, "convert db source failed: %v", err)
		return nil, errs.ErrTypeConvertFail
	}
	return &pb.GetDbSourceRsp{
		DbSource: dbSourceView,
	}, err
}

// BatchGetDbSources 批量查询数据源数据
func (s *Service) BatchGetDbSources(ctx context.Context, req *pb.BatchGetDbSourcesReq) (*pb.BatchGetDbSourcesRsp,
	error) {
	var dbSourceViews []*pb.DbSourceView
	copBizId := pkg.CorpBizID(ctx)
	for _, dbSourceBizID := range req.DbSourceBizId {
		source, err := dao.GetDBSourceDao().GetByBizID(ctx, copBizId, req.AppBizId, dbSourceBizID)
		if err != nil {
			log.ErrorContextf(ctx, "get db source failed: %v", err)
			return nil, err
		}
		dbSourceView, err := db_source.DBSourceModelToViewProto(source, nil, 0)
		if err != nil {
			log.ErrorContextf(ctx, "convert db source failed: %v", err)
			return nil, errs.ErrTypeConvertFail
		}
		dbSourceViews = append(dbSourceViews, dbSourceView)
	}
	return &pb.BatchGetDbSourcesRsp{
		DbSources: dbSourceViews,
	}, nil
}

// PreviewTable 预览表数据
func (s *Service) PreviewTable(ctx context.Context, req *pb.PreviewTableReq) (*pb.PreviewTableRsp, error) {
	copBizId := pkg.CorpBizID(ctx)
	dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, copBizId, req.AppBizId, req.DbSourceBizId)
	if err != nil {
		return nil, err
	}
	password, err := util.Decrypt(dbSource.Password)
	if err != nil {
		return nil, err
	}
	dbConn := model.ConnDbSource{
		DbType:   dbSource.DBType,
		Host:     dbSource.Host,
		DbName:   dbSource.DBName,
		Username: dbSource.Username,
		Password: password,
		Port:     dbSource.Port,
	}

	dbTable, err := dao.GetDBTableDao().GetByBizIDAndTableName(ctx, copBizId, req.AppBizId, req.DbSourceBizId,
		req.TableName)
	if err != nil || dbTable == nil {
		return nil, errs.ErrWrapf(errs.ErrDbSourceTableColumnNotExist, i18n.Translate(ctx, i18nkey.KeyDataTableNotExist),
			req.TableName)
	}

	dbTableColumn, err := dao.GetDBTableColumnDao().GetByColumnByTableBizIDAndColumnName(ctx, copBizId, req.AppBizId,
		dbTable.DBTableBizID, req.FilterColumn)
	if err != nil {
		return nil, errs.ErrWrapf(errs.ErrDbSourceTableColumnNotExist, i18n.Translate(ctx, i18nkey.KeyDataColumnNotExist),
			req.FilterColumn)
	}

	columns, rows, total, err := dao.GetDBSourceDao().
		ListPreviewData(ctx, dbConn, dbTable.Name, int(req.PageNumber), int(req.PageSize), dbTableColumn.ColumnName,
			req.FilterValue, config.App().DbSource.ReadConnTimeout)
	if err != nil {
		log.WarnContextf(ctx, "list preview data failed: %v", err)
		return nil, err
	}

	// 更新本地表信息，更新失败不影响结果
	if dbTable.RowCount != total || dbTable.ColumnCount != len(columns) {
		dbTable.RowCount = total
		dbTable.ColumnCount = len(columns)
		dbTable.Alive = true
		dbTable.LastSyncTime = time.Now()
		dbTable.UpdateTime = time.Now()
		_ = dao.GetDBTableDao().UpdateByBizID(ctx, dbTable.CorpBizID, dbTable.AppBizID, dbTable.DBTableBizID,
			[]string{"row_count", "column_count", "last_sync_time", "alive", "update_time"}, dbTable)
	}

	return &pb.PreviewTableRsp{
		Columns: columns,
		Rows:    rows,
		Total:   int32(total),
	}, nil
}

// ListReleaseDb 发布数据库查看
func (s *Service) ListReleaseDb(ctx context.Context, req *pb.ListReleaseDbReq) (*pb.ListReleaseDbRsp, error) {
	var list []*pb.ReleaseDb
	corpBizID := pkg.CorpBizID(ctx)
	if req.GetReleaseBizId() == 0 {
		var startTime, endTime time.Time
		if req.GetStartTime() != 0 {
			startTime = time.Unix(req.GetStartTime(), 0)
		}
		if req.GetEndTime() != 0 {
			endTime = time.Unix(req.GetEndTime(), 0)
		}

		dbSources, err := dao.GetDBSourceDao().FindUnReleaseDBSourceByConditions(ctx, corpBizID, req.GetAppBizId(),
			req.GetQuery(),
			startTime, endTime, req.GetActions(), req.GetPageNumber(), req.GetPageSize())
		if err != nil {
			return nil, err
		}

		for _, release := range dbSources {
			list = append(list, &pb.ReleaseDb{
				DbSourceBizId: release.DBSourceBizID,
				DbName:        release.DBName,
				UpdateTime:    uint64(release.UpdateTime.Unix()),
				Action:        uint32(release.NextAction),
				ActionDesc:    model.ActionDesc(ctx, uint32(release.NextAction)),
			})
		}
	} else {
		releaseDBSource, err := dao.GetDBSourceDao().GetAllReleaseDBSources(ctx, req.GetAppBizId(), req.GetReleaseBizId())
		if err != nil {
			return nil, err
		}

		for _, release := range releaseDBSource {
			list = append(list, &pb.ReleaseDb{
				DbSourceBizId: release.DBSourceBizID,
				DbName:        release.DBName,
				UpdateTime:    uint64(release.UpdateTime.Unix()),
				Action:        uint32(release.Action),
				ActionDesc:    model.ActionDesc(ctx, uint32(release.Action)),
			})
		}
	}

	rsp := &pb.ListReleaseDbRsp{
		Total: int32(len(list)),
		List:  list,
	}

	return rsp, nil
}

// GetDbSourcePublicKey TODO
func (s *Service) GetDbSourcePublicKey(ctx context.Context, req *pb.GetDbSourcePublicKeyReq) (
	*pb.GetDbSourcePublicKeyRsp, error) {
	log.InfoContextf(ctx, "GetDbSourcePublicKeyReq: %v", req)
	privateKey, err := util.GetDbSourcePrivateKey()
	if err != nil {
		log.ErrorContextf(ctx, "GetDbSourcePublicKey failed: %v", err)
		return nil, err
	}

	// 生成公钥
	publicKeyPEM, err := util.GeneratePublicKeyPEMByPrivateKey(privateKey)
	if err != nil {
		log.ErrorContextf(ctx, "GetDbSourcePublicKey failed: %v", err)
		return nil, err
	}

	return &pb.GetDbSourcePublicKeyRsp{
		PublicKey: publicKeyPEM,
	}, nil
}

// TextToSQLFromKnowledge 工作流根据用户提问生成 SQL
func (s *Service) TextToSQLFromKnowledge(ctx context.Context, req *pb.TextToSQLFromKnowledgeReq) (
	*pb.TextToSQLFromKnowledgeRsp, error) {
	return db_source.TextToSQLFromKnowledge(ctx, req, s.dao)
}

// BatchGetDbSourcesWithTables 批量获取数据库源信息和表信息
func (s *Service) BatchGetDbSourcesWithTables(ctx context.Context, req *pb.BatchGetDbSourcesWithTablesReq) (
	*pb.BatchGetDbSourcesWithTablesRsp, error) {
	if req == nil || len(req.GetDbSourceBizId()) == 0 {
		return nil, errs.ErrParameterInvalid
	}
	dbSourceWithTables, err := db_source.BatchGetDbSourcesWithTables(ctx, req.GetDbSourceBizId())
	if err != nil {
		return nil, err
	}
	rsp := &pb.BatchGetDbSourcesWithTablesRsp{
		DbSourcesWithTables: dbSourceWithTables,
		Total:               int32(len(dbSourceWithTables)),
	}
	return rsp, nil
}

// BatchGetTablesWithColumns 批量获取表信息和列信息
func (s *Service) BatchGetTablesWithColumns(ctx context.Context, req *pb.BatchGetTablesWithColumnsReq) (
	*pb.BatchGetTablesWithColumnsRsp, error) {
	if req == nil || len(req.GetDbSourceTableBizId()) == 0 {
		return nil, errs.ErrParameterInvalid
	}
	dbSourceWithTables, err := db_source.BatchGetTablesWithColumns(ctx, req.GetAppBizId(), req.GetDbSourceTableBizId())
	if err != nil {
		return nil, err
	}
	rsp := &pb.BatchGetTablesWithColumnsRsp{
		DbTablesWithColumns: dbSourceWithTables,
		Total:               int32(len(dbSourceWithTables)),
	}
	return rsp, nil
}
