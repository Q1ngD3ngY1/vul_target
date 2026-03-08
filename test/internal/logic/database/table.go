package database

import (
	"bytes"
	"context"
	"fmt"
	"time"
	"unicode/utf8"

	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	pm "git.woa.com/adp/pb-go/platform/platform_manager"
	"github.com/spf13/cast"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	entity0 "git.woa.com/adp/kb/kb-config/internal/entity"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

const (
	blankStr        = ""
	replacementChar = "?"
)

// BatchCreateDbTableAndColumn 新建表数据和列数据，保存到数据库
func (l *Logic) BatchCreateDbTableAndColumn(ctx context.Context, dbSource *entity.Database,
	tableNames []string) ([]*entity.Table, error) {
	logx.I(ctx, "BatchCreateDbTableAndColumn start, db source: %v, table names: %v", dbSource, tableNames)
	decrypt, err := util.Decrypt(dbSource.Password, config.App().DbSource.Salt)
	if err != nil {
		return nil, err
	}
	uin, err := l.dao.GetDBUin(ctx, dbSource.CorpBizID)
	if err != nil {
		return nil, err
	}
	timeNow := time.Now()
	dbTableList := make([]*entity.Table, len(tableNames))
	dbConn := entity.DatabaseConn{
		DbType:     dbSource.DBType,
		Host:       dbSource.Host,
		DbName:     dbSource.DBName,
		Username:   dbSource.Username,
		Password:   decrypt,
		Port:       dbSource.Port,
		SchemaName: dbSource.SchemaName,
		CreateTime: &dbSource.CreateTime,
		Uin:        uin,
	}

	wg, wgCtx := errgroupx.WithContext(ctx)
	wg.SetLimit(5)
	// 1. 将获取到的表数据，存入 db_table 表中
	for idx, tableName := range tableNames {
		currentIdx := idx
		wg.Go(func() (err error) {
			// 1.1 拉取表单的具体数据
			tableInfo, err := l.dao.GetTableInfo(wgCtx, dbConn, tableName)
			if err != nil {
				logx.W(wgCtx, "conn: %v, get table info failed: %v", dbConn.Host, err)
				return err
			}

			dbTable := &entity.Table{
				CorpBizID:         dbSource.CorpBizID,
				AppBizID:          dbSource.AppBizID,
				DBSourceBizID:     dbSource.DBSourceBizID,
				DBTableBizID:      idgen.GetId(),
				Source:            entity.TableSourceDB,
				Name:              tableName,
				TableSchema:       dbSource.DBName,
				TableComment:      tableInfo.TableComment,
				AliasName:         blankStr,
				Description:       blankStr,
				RowCount:          tableInfo.RowCount,
				ColumnCount:       tableInfo.ColumnCount,
				TableAddedTime:    timeNow,
				TableModifiedTime: timeNow,
				ReleaseStatus:     releaseEntity.ReleaseStatusInit,
				NextAction:        releaseEntity.ReleaseActionAdd,
				Alive:             true,
				IsIndexed:         true,
				IsDeleted:         false,
				LearnStatus:       entity.LearnStatusLearning,
				LastSyncTime:      timeNow,
				CreateTime:        timeNow,
				UpdateTime:        timeNow,
				StaffID:           contextx.Metadata(ctx).StaffID(),
				EnableScope:       dbSource.EnableScope,
			}
			dbTableList[currentIdx] = dbTable

			// 2.2. 将获取到的列数据，存入 db_table_column 表中
			// err = dao.GetDBTableColumnDao().AddColumns(wgCtx, tableInfo, dbTable, getDao)
			err = l.CreateColumnList(ctx, dbTable.CorpBizID, dbTable.AppBizID, dbTable.DBTableBizID,
				tableInfo.ColumnInfo)
			if err != nil {
				return err
			}
			return nil
		})
	}
	if err = wg.Wait(); err != nil {
		logx.W(ctx, "BatchCreateDbTableAndColumn failed: %v", err)
		return nil, err
	}
	// 3. 批量创建数据库表
	err = l.dao.CreateTableList(ctx, dbTableList)
	if err != nil {
		logx.E(ctx, "create db table failed: %v", err)
		return nil, err
	}
	return dbTableList, nil
}

// DeleteTablesAndColByDbSourceBizID 根据 数据源id， 删除数据源下的表和列
func (l *Logic) deleteTablesAndColByDbSourceBizID(ctx context.Context,
	corpBizID, appBizID, dbSourceBizID uint64) error {
	logx.I(ctx, "deleteTablesAndColByDbSourceBizID start, corpBizID: %v, appBizID: %v, dbSourceBizID: %v",
		corpBizID, appBizID, dbSourceBizID)
	tableFilter := entity.TableFilter{
		CorpBizID:     corpBizID,
		AppBizID:      appBizID,
		DBSourceBizID: dbSourceBizID,
	}
	dbTableList, _, err := l.dao.DescribeTableList(ctx, &tableFilter)
	// dbTableList, err := l.dao.ListAllByDBSourceBizID(ctx, corpBizID, appBizID, dbSourceBizID)
	if err != nil {
		return err
	}

	// 获取 robotId
	appDB, err := l.rpc.AppAdmin.DescribeAppById(ctx, appBizID)
	if err != nil {
		return err
	}
	robotId := appDB.PrimaryId

	dbTableBizIDs := make([]uint64, 0)
	for _, dbTable := range dbTableList {
		tableFilter := entity.TableFilter{
			CorpBizID:    corpBizID,
			AppBizID:     appBizID,
			DBTableBizID: dbTable.DBTableBizID,
			RobotID:      robotId,
		}
		err = l.DeleteTable(ctx, &tableFilter)
		if err != nil {
			return err
		}
		dbTableBizIDs = append(dbTableBizIDs, dbTable.DBTableBizID)
	}

	err = l.userLogic.DeleteKnowledgeRoleDbTables(ctx, appBizID, dbTableBizIDs, 200, 10000)
	if err != nil {
		logx.E(ctx, "deleteTablesAndColByDbSourceBizID failed, delete knowledge role db tables failed: %v", err)
		return err
	}
	return nil
}

// DeleteTable 根据 表ID， 删除表和列
// DeleteTableAndColumn
func (l *Logic) DeleteTable(ctx context.Context, filter *entity.TableFilter) error {
	err := l.rpc.RetrievalDirectIndex.DeleteDBText2SQL(ctx, filter.RobotID, []uint64{filter.DBTableBizID}, retrieval.EnvType_Test)
	if err != nil {
		return err
	}
	embeddingVersion, embeddingName, err := l.GetAppEmbeddingInfoById(ctx, filter.AppBizID)
	if err != nil {
		logx.E(ctx, "GetAppEmbeddingInfoById failed, get app embedding version failed: %v", err)
		return err
	}

	err = l.DeleteDbTableVdb(ctx, filter.RobotID, filter.CorpBizID, filter.AppBizID, filter.DBTableBizID,
		embeddingVersion, embeddingName, retrieval.EnvType_Test)
	if err != nil {
		logx.E(ctx, "DeleteDbTableVdb failed, delete db table vdb failed, dbTableBizID:%v err:%v",
			filter.DBTableBizID, err)
		return err
	}

	err = l.dao.DeleteTable(ctx, filter)
	if err != nil {
		return err
	}

	columnFilter := entity.ColumnFilter{
		CorpBizID:    filter.CorpBizID,
		AppBizID:     filter.AppBizID,
		DBTableBizID: filter.DBTableBizID,
	}
	err = l.dao.SoftDeleteByTableBizID(ctx, &columnFilter)
	// err = dao.GetDBTableColumnDao().SoftDeleteByTableBizID(ctx, corpBizID, appBizID, dbTableBizID)
	if err != nil {
		return err
	}
	return nil
}

func (l *Logic) FlashTableAndColumn(ctx context.Context, dbTable *entity.Table) (bool, error) {
	if time.Since(dbTable.LastSyncTime).Minutes() < 30 {
		return false, nil
	}
	// 0. 判断是否需要同步
	logx.I(ctx, "FlashTableAndColumn start, dbTable: %v", dbTable)
	dbFilter := entity.DatabaseFilter{
		CorpBizID:     dbTable.CorpBizID,
		AppBizID:      dbTable.AppBizID,
		DBSourceBizID: dbTable.DBSourceBizID,
	}
	dbSource, err := l.dao.DescribeDatabase(ctx, &dbFilter)
	if err != nil {
		return false, err
	}
	password, err := util.Decrypt(dbSource.Password, config.App().DbSource.Salt)
	if err != nil {
		return false, err
	}
	uin, err := l.dao.GetDBUin(ctx, dbSource.CorpBizID)
	if err != nil {
		return false, err
	}
	dbConn := entity.DatabaseConn{
		DbType:     dbSource.DBType,
		Host:       dbSource.Host,
		DbName:     dbSource.DBName,
		Username:   dbSource.Username,
		Password:   password,
		Port:       dbSource.Port,
		SchemaName: dbSource.SchemaName,
		CreateTime: &dbSource.CreateTime,
		Uin:        uin,
	}

	// 1. 根据数据源， 获取远端最新的表信息
	tableFilter := entity.TableFilter{
		CorpBizID:    dbTable.CorpBizID,
		AppBizID:     dbTable.AppBizID,
		DBTableBizID: dbTable.DBTableBizID,
	}
	tableInfo, err := l.dao.GetTableInfo(ctx, dbConn, dbTable.Name)
	if err != nil {
		if errx.Code(err) == errx.Code(errs.ErrDbTableIsNotExist) {
			logx.W(ctx, "FlashTableAndColumn|dbTable %v err %v", dbTable, err)
			dbTable.Alive = false
			err = l.dao.ModifyTable(ctx, &tableFilter, map[string]any{
				"last_sync_time": dbTable.LastSyncTime,
				"alive":          dbTable.Alive,
			})
			// err = l.dao.UpdateByBizID(ctx, dbTable.CorpBizID, dbTable.AppBizID, dbTable.DBTableBizID, []string{"last_sync_time", "alive"}, dbTable)
			if err != nil {
				return false, err
			}
			return false, errs.ErrDbTableIsNotExist
		}
		return false, err
	}

	if len(tableInfo.ColumnInfo) == 0 {
		logx.W(ctx, "FlashTableAndColumn|table info is empty, table: %v", dbTable)
		dbTable.Alive = false
	} else {
		dbTable.Alive = true
	}

	// 2. 更新本地表信息
	dbTable.RowCount = tableInfo.RowCount
	dbTable.ColumnCount = tableInfo.ColumnCount
	dbTable.LastSyncTime = time.Now()
	err = l.dao.ModifyTable(ctx, &tableFilter, map[string]any{
		"last_sync_time": dbTable.LastSyncTime,
		"alive":          dbTable.Alive,
		"row_count":      dbTable.RowCount,
		"column_count":   dbTable.ColumnCount,
	})
	// err = dao.GetDBTableDao().UpdateByBizID(ctx, dbTable.CorpBizID, dbTable.AppBizID, dbTable.DBTableBizID, []string{"row_count", "column_count", "last_sync_time", "alive"}, dbTable)
	if err != nil {
		return false, err
	}

	// 3. 获取现有数据库中的列信息, 与远端表信息进行对比， 将不存在的列添加到数据库中
	columnFilter := entity.ColumnFilter{
		CorpBizID:    dbTable.CorpBizID,
		AppBizID:     dbTable.AppBizID,
		DBTableBizID: dbTable.DBTableBizID,
	}
	columns, _, err := l.dao.DescribeColumnList(ctx, &columnFilter)
	// columns, err := dao.GetDBTableColumnDao().GetByTableBizID(ctx, dbTable.CorpBizID, dbTable.AppBizID, dbTable.DBTableBizID)
	if err != nil {
		return false, err
	}

	addColumns := make([]*entity.ColumnInfo, 0)
	localColumns := make(map[string]struct{})
	remoteColumns := make(map[string]struct{})
	delColumnIds := make([]uint64, 0)

	for _, columnInfo := range tableInfo.ColumnInfo {
		remoteColumns[columnInfo.ColumnName] = struct{}{}
	}

	for _, column := range columns {
		localColumns[column.ColumnName] = struct{}{}
		if _, ok := remoteColumns[column.ColumnName]; !ok {
			delColumnIds = append(delColumnIds, column.DBTableColumnBizID)
		}
	}

	for _, columnInfo := range tableInfo.ColumnInfo {
		if _, ok := localColumns[columnInfo.ColumnName]; !ok {
			addColumns = append(addColumns, columnInfo)
		}
	}

	flag := false
	if len(addColumns) != 0 {
		flag = true
		// newTableInfo := &entity.TableInfo{
		// 	ColumnInfo:  addColumns,
		// 	RowCount:    tableInfo.RowCount,
		// 	ColumnCount: tableInfo.ColumnCount,
		// }
		// err = dao.GetDBTableColumnDao().AddColumns(ctx, newTableInfo, dbTable, d)
		err = l.CreateColumnList(ctx, dbTable.CorpBizID, dbTable.AppBizID, dbTable.DBTableBizID, addColumns)
		if err != nil {
			return false, err
		}
	}

	// 4. 将不存在的列删除
	if len(delColumnIds) != 0 {
		flag = true
		cnt, err := l.dao.DeleteByTableBizID(ctx, delColumnIds)
		if err != nil {
			return false, err
		}
		if cnt > 0 {
			logx.I(ctx, "delete db table columns success, count: %d", cnt)
			flag = true
		}
	}

	return flag, nil
}

func (l *Logic) CreateColumnList(ctx context.Context, corpID, appID, tableID uint64,
	columns []*entity.ColumnInfo) error {
	var createColumns []*entity.Column
	now := time.Now()
	for _, c := range columns {
		createColumns = append(createColumns, &entity.Column{
			CorpBizID:          corpID,
			AppBizID:           appID,
			DBTableBizID:       tableID,
			DBTableColumnBizID: idgen.GetId(),
			ColumnName:         c.ColumnName,
			DataType:           c.DataType,
			ColumnComment:      c.ColComment,
			AliasName:          "",
			Description:        "",
			Unit:               "",
			IsIndexed:          true,
			IsDeleted:          false,
			CreateTime:         now,
			UpdateTime:         now,
		})
	}
	return l.dao.CreateColumnList(ctx, createColumns)
}

// AddDbTableData2ES1 将远端数据库中的数据，添加到es1中
// 评测端不需要传标签，函数中会取标签
func (l *Logic) AddDbTableData2ES1(ctx context.Context, dbSource *entity.Database, robotId, dbTableBizID uint64,
	envType retrieval.EnvType) error {

	logx.I(ctx, "AddDbTableData2ES1, table: %v, env: %v", dbTableBizID, envType.String())

	// dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, dbSource.CorpBizID, dbSource.AppBizID, dbTableBizID)
	tableFilter := entity.TableFilter{
		CorpBizID:    dbSource.CorpBizID,
		AppBizID:     dbSource.AppBizID,
		DBTableBizID: dbTableBizID,
	}
	dbTable, err := l.dao.DescribeTable(ctx, &tableFilter)
	if err != nil {
		logx.E(ctx, "get db table by biz id failed, %v", err)
		return err
	}

	columnFilter := entity.ColumnFilter{
		CorpBizID:    dbSource.CorpBizID,
		AppBizID:     dbSource.AppBizID,
		DBTableBizID: dbTableBizID,
	}
	columns, _, err := l.dao.DescribeColumnList(ctx, &columnFilter)
	// columns, err := dao.GetDBTableColumnDao().GetByTableBizID(ctx, dbSource.CorpBizID, dbSource.AppBizID, dbTable.DBTableBizID)
	if err != nil {
		logx.E(ctx, "get db table columns by table biz id failed, %v", err)
		return err
	}

	indexColumnName := make(map[string]struct{})
	for _, column := range columns {
		if column.IsIndexed {
			indexColumnName[column.ColumnName] = struct{}{}
		}
	}

	password, err := util.Decrypt(dbSource.Password, config.App().DbSource.Salt)
	if err != nil {
		return err
	}
	uin, err := l.dao.GetDBUin(ctx, dbSource.CorpBizID)
	if err != nil {
		return err
	}
	dbConn := entity.DatabaseConn{
		DbType:     dbSource.DBType,
		Host:       dbSource.Host,
		DbName:     dbSource.DBName,
		Username:   dbSource.Username,
		Password:   password,
		Port:       dbSource.Port,
		SchemaName: dbSource.SchemaName,
		CreateTime: &dbSource.CreateTime,
		Uin:        uin,
	}

	colNames, rows, total, err := l.dao.ListPreviewData(ctx, dbConn, dbTable.Name, 1,
		config.App().DbSource.EsInputSize, blankStr, blankStr, config.App().DbSource.EsInsertTimeOut)
	logx.I(ctx, "colNames: %v, rowLen: %v", colNames, total)
	if err != nil {
		logx.E(ctx, "AddDbTableData2ES1|get db table preview data failed, dbSource %v err: %v", dbSource,
			err)
		return err
	}
	if total == 0 {
		logx.W(ctx, "get db table preview data total is 0")
		return nil
	}

	colName2Model := make(map[string]*entity.Column)
	for _, column := range columns {
		colName2Model[column.ColumnName] = column
	}

	dbRowData := make([]*retrieval.DBRowData, 0)

	// 获取配置的单元格值最大长度
	maxCellValueLength := config.App().DbSource.EsCellValueMaxLength

	for rowIdx, row := range rows {
		dbCells := make([]*retrieval.DBCell, 0)
		for idx, name := range colNames {
			if _, ok := indexColumnName[name]; !ok {
				continue
			}
			col := colName2Model[name]
			if col == nil {
				logx.E(ctx, "colName2Model not found col, %v, table biz id %v", name, dbTableBizID)
				col = &entity.Column{}
			}
			colDesc := col.ColumnComment + "|" + col.AliasName + "|" + col.Description + "|" + col.Unit
			var cell *retrieval.DBCell

			// 截断单元格值到配置的最大长度
			cellValue := row.Values[idx]
			if maxCellValueLength > 0 && len(cellValue) > maxCellValueLength {
				cellValue = cellValue[:maxCellValueLength]
			}

			// 第一列保存全量数据，其他列没有全量数据，减少数据发送量，在 检索 端会进行恢复。
			if rowIdx == 0 {
				cell = &retrieval.DBCell{
					ColumnName:      ensureValidUTF8(name),
					ColumnAliasName: ensureValidUTF8(col.AliasName),
					ColumnDesc:      ensureValidUTF8(colDesc),
					DataType:        ensureValidUTF8(col.DataType),
					Value:           ensureValidUTF8(cellValue),
				}
			} else {
				cell = &retrieval.DBCell{
					Value: ensureValidUTF8(cellValue),
				}
			}
			dbCells = append(dbCells, cell)
		}
		dbRowData = append(dbRowData, &retrieval.DBRowData{
			Cells: dbCells,
		})
	}

	// 库描述信息，格式：库名｜库别名｜库描述
	dbDesc := dbSource.DBName + "|" + dbSource.AliasName + "|" + dbSource.Description

	// 表描述信息，格式：表名｜表注释｜表别名｜表描述
	tableDesc := dbTable.Name + "|" + dbTable.TableComment + "|" + dbTable.AliasName + "|" + dbTable.Description

	// labels, err := dao.GetRoleLabelsOfDBTable(ctx, dbSource.AppBizID, dbTableBizID)
	// if err != nil {
	// 	return err
	// }
	// 上面几行替换成了下面获取 labels 的几行：

	// 根据数据库业务id获取被引用的角色业务id
	roleBizIDS, err := l.userLogic.DescribeRoleByDbBiz(ctx, dbSource.AppBizID, dbTableBizID, 10000)
	if err != nil {
		logx.E(ctx, "getAllLabelsOfDBTable err:%v,dbTableBizID:%+v", err, dbTableBizID)
		return err
	}
	labels := make([]*retrieval.VectorLabel, 0, len(roleBizIDS))
	for _, v := range roleBizIDS {
		labels = append(labels, &retrieval.VectorLabel{
			Name:  config.GetMainConfig().Permissions.RoleRetrievalKey, // 角色向量统一key
			Value: cast.ToString(v),
		})
	}
	labels = append(labels, &retrieval.VectorLabel{ // 表的业务id统一写标签
		Name:  entity.LabelDBTableBizID,
		Value: cast.ToString(dbTableBizID),
	})

	labels = append(
		labels, &retrieval.VectorLabel{
			Name:  entity0.EnableScopeAttr,
			Value: cast.ToString(entity0.EnableScopeDb2Label[dbTable.EnableScope]),
		})

	req := &retrieval.AddDBText2SQLReq{
		RobotId:       robotId,
		DbSourceBizId: dbSource.DBSourceBizID,
		DbDesc:        dbDesc,
		DbTableBizId:  dbTableBizID,
		TableDesc:     tableDesc,
		Rows:          dbRowData,
		EnvType:       envType,
		Labels:        labels,
	}

	if dbSource.DBType == entity.DBTypeMySQL {
		req.DbType = retrieval.DBType_MYSQL
	} else if dbSource.DBType == entity.DBTypeSQLServer {
		req.DbType = retrieval.DBType_SQL_SERVER
	} else if dbSource.DBType == entity.DBTypeOracle {
		req.DbType = retrieval.DBType_ORACLE
	} else if dbSource.DBType == entity.DBTypePostgreSQL {
		req.DbType = retrieval.DBType_POSTGRES
	} else {
		return errs.ErrDbSourceTypeNotSupport
	}

	// 添加之前先全量删除，AddDBText2SQL接口支持全量覆盖，不过为了防止存入的数据行数配置项变化导致删除残留
	// 比如之前存了100行，后面修改到只存10行，不能完全覆盖之前的100行
	err = l.rpc.RetrievalDirectIndex.DeleteDBText2SQL(ctx, robotId, []uint64{dbTableBizID}, envType)
	if err != nil {
		logx.E(ctx, "delete db text2sql to es1 failed, dbTableBizId:%v err:%v", dbTableBizID, err)
		return err
	}
	if len(dbRowData) == 0 {
		logx.E(ctx, "no data in db table %v, no need to add to text2sql", req.DbTableBizId)
		return nil
	}

	logx.I(ctx, "AddDBText2SQL, req:%v", req)
	_, err = retrieval.NewDirectIndexClientProxy().AddDBText2SQL(ctx, req)
	if err != nil {
		logx.E(ctx, "add db text2sql to es1 failed, dbTableBizID:%v, err:%v", req.DbTableBizId, err)
		return err
	}

	return nil
}

// ensureValidUTF8 确保字符串是有效的UTF-8字符串
func ensureValidUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	// 最后用 replacement 替换非法字节
	return string(bytes.ToValidUTF8([]byte(s), []byte(replacementChar)))
}

// ChangeDbTableEnable 修改数据表的启用状态
func (l *Logic) ChangeDbTableEnable(ctx context.Context, robotID, corpBizID, appBizId, dbTableBizId uint64,
	isEnable bool) error {
	tableFilter := entity.TableFilter{
		CorpBizID:    corpBizID,
		AppBizID:     appBizId,
		DBTableBizID: dbTableBizId,
	}
	table, err := l.dao.DescribeTable(ctx, &tableFilter)
	if err != nil {
		return fmt.Errorf("logic:ChangeDbTableEnable filter:%+v, error:%w", tableFilter, err)
	}

	// // 1.获取db table
	// dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, corpBizID, appBizId, dbTableBizId)
	// if err != nil {
	// 	logx.E(ctx, "ChangeDbTableEnable| appBizId %v, dbTableBizId %v, get db table by biz id failed, %v", appBizId, dbTableBizId, err)
	// 	return err
	// }

	// 2 关闭删除es1数据
	err = l.rpc.RetrievalDirectIndex.DeleteDBText2SQL(ctx, robotID, []uint64{dbTableBizId}, retrieval.EnvType_Test)
	if err != nil {
		logx.E(ctx, "DeleteDBText2SQL failed, %v", err)
		return err
	}

	embeddingVersion, embeddingName, err := l.GetAppEmbeddingInfoById(ctx, appBizId)
	if err != nil {
		logx.E(ctx, "GetAppEmbeddingInfoById failed, get app embedding version failed: %v", err)
		return err
	}

	err = l.DeleteDbTableVdb(ctx, robotID, corpBizID, appBizId, dbTableBizId,
		embeddingVersion, embeddingName, retrieval.EnvType_Test)
	if err != nil {
		logx.E(ctx, "DeleteDbTableVdb failed, delete db table vdb failed, dbTableBizID:%v err:%v",
			dbTableBizId, err)
		return err
	}

	// 3.根据是否开启进行处理
	if isEnable {
		// 2.2 开启添加es1数据
		dbFilter := entity.DatabaseFilter{
			CorpBizID:     corpBizID,
			AppBizID:      appBizId,
			DBSourceBizID: table.DBSourceBizID,
		}
		dbSource, err := l.dao.DescribeDatabase(ctx, &dbFilter)
		if err != nil {
			logx.E(ctx, "get db source by biz id failed, %v", err)
			return err
		}

		err = l.AddDbTableData2ES1(ctx, dbSource, robotID, dbTableBizId, retrieval.EnvType_Test)
		if err != nil {
			logx.E(ctx, "add db source table data to es1 failed: %v", err)
			return err
		}

		err = l.dao.GetTopNValueV2(ctx, dbSource, robotID, dbTableBizId, embeddingVersion, embeddingName)
		if err != nil {
			logx.E(ctx,
				"ChangeDbTableEnable|get top n value for mysql failed: robotID:%v, dbTableBizId:%v, err:%v", robotID,
				dbTableBizId, err)
			return err
		}
	}

	// cols := []string{"is_indexed", "update_time", "release_status", "staff_id", "learn_status", "table_modified_time"}
	// table.ReleaseStatus = releaseEntity.ReleaseStatusInit
	// if table.NextAction != releaseEntity.ReleaseActionAdd {
	// 	table.NextAction = releaseEntity.ReleaseActionUpdate
	// 	cols = append(cols, "next_action")
	// }
	// table.IsIndexed = convx.BoolToInt[uint32](isEnable)
	// table.UpdateTime = time.Now()
	// table.StaffID = contextx.Metadata(ctx).StaffID()
	// table.TableModifiedTime = time.Now()
	// table.LearnStatus = entity.LearnStatusLearned
	// err = dao.GetDBTableDao().UpdateByBizID(ctx, corpBizID, appBizId, dbTableBizId, cols, dbTable)
	tableData := map[string]any{
		"is_indexed":          convx.BoolToInt[uint32](isEnable),
		"update_time":         time.Now(),
		"release_status":      releaseEntity.ReleaseStatusInit,
		"staff_id":            contextx.Metadata(ctx).StaffID(),
		"learn_status":        entity.LearnStatusLearned,
		"table_modified_time": time.Now(),
	}
	if table.NextAction != releaseEntity.ReleaseActionAdd {
		tableData["next_action"] = releaseEntity.ReleaseActionUpdate
	}
	err = l.dao.ModifyTable(ctx, &tableFilter, tableData)
	if err != nil {
		logx.E(ctx, "ChangeDbTableEnable failed, %v", err)
		return err
	}
	return nil
}

// CreateDbTableLearnTask 创建db table 学习任务
func (l *Logic) CreateDbTableLearnTask(ctx context.Context, db *entity.Database, robotID, dbTableBizID uint64) error {
	embeddingVersion, embeddingName, err := l.GetAppEmbeddingInfoById(ctx, db.AppBizID)
	if err != nil {
		logx.E(ctx, "GetAppEmbeddingInfoById failed: %v", err)
		return err
	}
	_, err = scheduler.NewAddTableTask(ctx, &entity0.LearnDBTableParams{
		Name:             "",
		RobotID:          robotID,
		CorpBizID:        db.CorpBizID,
		AppBizID:         db.AppBizID,
		DBTableBizID:     dbTableBizID,
		DBSource:         db,
		EmbeddingVersion: embeddingVersion,
		EmbeddingName:    embeddingName,
	})
	if err != nil {
		logx.E(ctx,
			"CreateDbTableLearnTask| robotID: %v, corpBizID: %v, appBizID: %v, dbTableBizID: %v, embeddingVersion:%v, dbSource: %v,  failed, %v",
			robotID, db.CorpBizID, db.AppBizID, dbTableBizID, embeddingVersion, db, err)
		return err
	}
	return nil
}

// UpsertDbTable2Vdb 更新外部数据库的值到 vdb 中
func (l *Logic) UpsertDbTable2Vdb(ctx context.Context,
	robotID, corpBizID, appBizID, dbTableBizID, embeddingVersion uint64, embeddingName string) error {
	logx.I(ctx,
		"UpsertDbTable2Vdb| robotID: %v, corpBizID: %v, appBizID: %v, dbTableBizID: %v, embeddingVersion:%v", robotID,
		corpBizID, appBizID, dbTableBizID, embeddingVersion)
	maxID := uint64(0)
	for {
		topValues, err := l.dao.GetTopValuesPageByDbTableBizID(ctx, corpBizID, appBizID, dbTableBizID, maxID, 50)
		if err != nil {
			return err
		}
		if len(topValues) == 0 {
			break
		}
		maxID = topValues[len(topValues)-1].ID
		logx.D(ctx, "UpsertDbTable2Vdb|dbTableBizID: %v, maxID: %v", dbTableBizID, maxID)
		for _, value := range topValues {
			content := fmt.Sprintf("%v;%v;%v", value.ColumnName, value.ColumnComment, value.ColumnValue)
			err = l.dao.AddVector(ctx, robotID, appBizID, dbTableBizID, value.BusinessID, embeddingVersion, embeddingName,
				entity0.EnableScopePublish, content,
				retrieval.EnvType_Prod)
			if err != nil {
				logx.E(ctx,
					"UpsertDbTable2Vdb| robotID: %v, corpBizID: %v, appBizID: %v, dbTableBizID: %v, embeddingVersion:%v, content: %v, failed, %v",
					robotID, corpBizID, appBizID, dbTableBizID, embeddingVersion, content, err)
				return err
			}
		}
	}
	return nil
}

// DeleteDbTableVdb 删除外部数据库的值到 vdb 中
func (l *Logic) DeleteDbTableVdb(ctx context.Context,
	robotID, corpBizID, appBizID, dbTableBizID, embeddingVersion uint64, embeddingName string, envType retrieval.EnvType) error {
	logx.I(ctx,
		"DeleteDbTableVdb| robotID: %v, corpBizID: %v, appBizID: %v, dbTableBizID: %v, embeddingVersion:%v, "+
			"embeddingName: %s", robotID, corpBizID, appBizID, dbTableBizID, embeddingVersion, embeddingName)
	t0 := time.Now()
	maxID := uint64(0)
	cnt := 0
	for {
		var topValueBizIDs []uint64
		var topValues []*entity.TableTopValue
		var err error
		if envType == retrieval.EnvType_Test {
			topValues, err = l.dao.GetTopValuesPageByDbTableBizID(ctx, corpBizID, appBizID, dbTableBizID, maxID, 200)
			if err != nil {
				return err
			}
		} else {
			topValues, err = l.dao.GetDeletedTopValuesPageByDbTableBizID(ctx, corpBizID, appBizID, dbTableBizID, maxID,
				200)
			if err != nil {
				return err
			}
		}

		logx.D(ctx, "DeleteDbTableVdb|dbTableBizID: %+v, maxID: %v, topValues :%v", dbTableBizID, maxID,
			topValues)
		if len(topValues) == 0 {
			break
		}
		maxID = topValues[len(topValues)-1].ID
		for _, value := range topValues {
			cnt++
			topValueBizIDs = append(topValueBizIDs, value.BusinessID)
		}
		err = l.dao.DeleteVector(ctx, robotID, appBizID, embeddingVersion, embeddingName, topValueBizIDs, envType)
		if err != nil {
			logx.E(ctx, "DeleteDbTableVdb| appBizID: %v, dbTableBizID: %v, embeddingVersion:%v, failed, %v", appBizID, dbTableBizID, embeddingVersion, err)
			return err
		}
		if envType == retrieval.EnvType_Test {
			err = l.dao.BatchSoftDeleteTopValuesByBizID(ctx, topValueBizIDs)
			if err != nil {
				logx.E(ctx,
					"BatchSoftDeleteTopValuesByBizID| appBizID: %v, dbTableBizID: %v, embeddingVersion:%v, failed, %v",
					appBizID, dbTableBizID, embeddingVersion, err)
				return err
			}
		} else {
			err = l.dao.BatchDeleteTopValuesByBizID(ctx, topValueBizIDs)
			if err != nil {
				logx.E(ctx,
					"BatchDeleteTopValuesByBizID| appBizID: %v, dbTableBizID: %v, embeddingVersion:%v, failed, %v",
					appBizID, dbTableBizID, embeddingVersion, err)
				return err
			}
		}
	}
	if cnt > 0 {
		logx.D(ctx, "DeleteDbTableVdb|dbTableBizID  %v, cnt: %v", dbTableBizID, cnt)
		if envType == retrieval.EnvType_Test {
			err := l.dao.BatchSoftDeleteTopValuesByDbTableBizID(ctx, corpBizID, appBizID, dbTableBizID)
			if err != nil {
				logx.E(ctx,
					"DeleteDbTableVdb| appBizID: %v, dbTableBizID: %v, embeddingVersion:%v, failed, %v", appBizID,
					dbTableBizID, embeddingVersion, err)
				return err
			}
		} else {
			err := l.dao.BatchCleanDeletedTopValuesByDbTableBizID(ctx, corpBizID, appBizID, dbTableBizID)
			if err != nil {
				logx.E(ctx,
					"DeleteDbTableVdb| appBizID: %v, dbTableBizID: %v, embeddingVersion:%v, failed, %v", appBizID,
					dbTableBizID, embeddingVersion, err)
				return err
			}
		}
	} else {
		logx.W(ctx, "DeleteDbTableVdb|dbTableBizID: %v, cnt: %v, del vdb zero", dbTableBizID, cnt)
	}
	logx.I(ctx, "DeleteDbTableVdb|dbTableBizID: %v, cnt: %v, cost: %vms", dbTableBizID, cnt,
		time.Now().Sub(t0).Milliseconds())
	return nil
}

func (l *Logic) DescribeTable(ctx context.Context, filter *entity.TableFilter) (*entity.Table, error) {
	table, err := l.dao.DescribeTable(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("logic:DescribeTable error: %w", err)
	}
	return table, nil
}

// DescribeTableList 查询表列表，支持查询数据库详情和员工详情
func (l *Logic) DescribeTableList(ctx context.Context, filter *entity.TableFilter) ([]*entity.Table, int64, error) {
	tables, total, err := l.dao.DescribeTableList(ctx, filter)
	if err != nil {
		return nil, 0, fmt.Errorf("logic:DescribeTableList error: %w", err)
	}

	if filter.WithColumn {

		DBTableBizIDs := make([]uint64, 0)
		for _, table := range tables {
			DBTableBizIDs = append(DBTableBizIDs, table.DBTableBizID)
		}

		columnFilter := entity.ColumnFilter{
			CorpBizID:     filter.CorpBizID,
			AppBizID:      filter.AppBizID,
			DBTableBizIDs: DBTableBizIDs,
			IsDeleted:     ptrx.Bool(false),
		}
		columns, _, err := l.dao.DescribeColumnList(ctx, &columnFilter)
		if err != nil {
			return nil, 0, fmt.Errorf("logic:DescribeTableList error: %w", err)
		}
		columnMap := make(map[uint64][]*entity.Column)
		for _, column := range columns {
			if _, ok := columnMap[column.DBTableBizID]; !ok {
				columnMap[column.DBTableBizID] = make([]*entity.Column, 0)
			}
			columnMap[column.DBTableBizID] = append(columnMap[column.DBTableBizID], column)
		}
		for _, table := range tables {
			if cols, ok := columnMap[table.DBTableBizID]; ok {
				table.Columns = cols
			} else {
				table.Columns = make([]*entity.Column, 0)
			}
		}
	}
	// 如果需要查询数据库详情
	dbSourceMap := make(map[uint64]*entity.Database)
	if filter.WithDatabase {
		// 获取所有相关的数据源ID
		dbSourceBizIDs := slicex.Pluck(tables, func(t *entity.Table) uint64 { return t.DBSourceBizID })
		dbSourceBizIDs = slicex.Unique(dbSourceBizIDs)

		if len(dbSourceBizIDs) > 0 {
			dbFilter := entity.DatabaseFilter{
				CorpBizID:      filter.CorpBizID,
				AppBizID:       filter.AppBizID,
				DBSourceBizIDs: dbSourceBizIDs,
			}
			databases, _, err := l.dao.DescribeDatabaseList(ctx, &dbFilter)
			if err != nil {
				logx.W(ctx, "DescribeTableList get database info failed: %v", err)
			} else {
				// 构建数据源映射
				for _, db := range databases {
					dbSourceMap[db.DBSourceBizID] = db
				}
				// 为每个表关联数据库信息
				for _, table := range tables {
					if db, ok := dbSourceMap[table.DBSourceBizID]; ok {
						table.Database = db
					}
				}
			}
		}
	}

	// 如果查询员工详情
	if filter.WithStaffName {
		// 获取所有员工ID
		staffIDs := slicex.Pluck(tables, func(t *entity.Table) uint64 { return t.StaffID })
		staffIDs = slicex.Unique(staffIDs)

		if len(staffIDs) > 0 {
			staffs, err := l.rpc.PlatformAdmin.DescribeStaffList(ctx, &pm.DescribeStaffListReq{
				StaffIds: staffIDs,
			})
			if err != nil {
				logx.W(ctx, "DescribeTableList get staff name failed: %v", err)
			} else {
				// 为每个表设置员工名称
				for _, table := range tables {
					if staff, ok := staffs[table.StaffID]; ok {
						table.StaffName = staff.GetNickName()
					} else {
						table.StaffName = fmt.Sprintf("%d", table.StaffID)
					}
				}
			}
		}
	}
	return tables, total, nil
}

// DescribeTableProdList 查询发布域表列表
func (l *Logic) DescribeTableProdList(ctx context.Context, filter *entity.TableFilter) ([]*entity.TableProd, int64, error) {
	tables, total, err := l.dao.DescribeTableProdList(ctx, filter)
	if err != nil {
		return nil, 0, fmt.Errorf("logic:DescribeTableProdList error: %w", err)
	}
	return tables, total, nil
}

func (l *Logic) ModifyTable(ctx context.Context, filter *entity.TableFilter, updateMap map[string]any) error {
	return l.dao.ModifyTable(ctx, filter, updateMap)
}

// ListReleaseDbTable 发布数据表查看
func (l *Logic) ListReleaseDbTable(ctx context.Context, req *pb.ListReleaseDbDbTableReq) (*pb.ListReleaseDbDbTableRsp, error) {
	var list []*pb.ReleaseDbTable
	logx.D(ctx, "ListReleaseDbTable, req: %+v", req)
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	if req.GetReleaseBizId() == 0 {
		var startTime, endTime time.Time
		if req.GetStartTime() != 0 {
			startTime = time.Unix(req.GetStartTime(), 0)
		}
		if req.GetEndTime() != 0 {
			endTime = time.Unix(req.GetEndTime(), 0)
		}
		tables, err := l.dao.DescribeUnreleaseTableListByConds(ctx, corpBizID, req.GetAppBizId(), req.GetQuery(),
			startTime, endTime, req.GetActions(), req.GetPageNumber(), req.GetPageSize())
		if err != nil {
			return nil, err
		}

		for _, table := range tables {
			name := table.Name
			dbName := table.TableSchema
			if table.Source == entity.TableSourceDoc {
				// 不能把内部的库表名称暴露出去
				name = table.AliasName
				dbName = ""
			}
			list = append(list, &pb.ReleaseDbTable{
				DbTableBizId: table.DBTableBizID,
				TableName:    name,
				DbName:       dbName,
				UpdateTime:   uint64(table.UpdateTime.Unix()),
				Action:       table.NextAction,
				ActionDesc:   i18n.Translate(ctx, docEntity.DocActionDesc[table.NextAction]),
			})
		}
	} else {
		releaseTable, err := l.dao.GetAllReleaseDBTables(ctx, req.GetAppBizId(), req.GetReleaseBizId(), false)
		if err != nil {
			return nil, err
		}

		for _, release := range releaseTable {
			name := release.Name
			dbName := release.TableSchema
			if release.Source == entity.TableSourceDoc {
				name = release.AliasName
				dbName = ""
			}
			list = append(list, &pb.ReleaseDbTable{
				DbTableBizId: release.DBTableBizID,
				TableName:    name,
				DbName:       dbName,
				UpdateTime:   uint64(release.TableModifiedTime.Unix()),
				Action:       release.Action,
				ActionDesc:   i18n.Translate(ctx, docEntity.DocActionDesc[release.Action]),
			})
		}
	}
	rsp := &pb.ListReleaseDbDbTableRsp{
		Total: int32(len(list)),
		List:  list,
	}

	return rsp, nil
}

func (l *Logic) UpdateDbTableEnabled(ctx context.Context, req *pb.UpdateDbTableEnabledReq) error {
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	appDB, err := l.rpc.AppAdmin.DescribeAppById(ctx, req.GetAppBizId())
	if err != nil {
		return err
	}
	robotId := appDB.PrimaryId
	isIndexed := req.GetIsEnable()
	staffID := contextx.Metadata(ctx).StaffID()
	var dbTablesBizIDs []uint64
	cols := []string{"learn_status", "staff_id", "is_indexed"}
	if req.GetDbTableBizId() > 0 {
		tableFilter := entity.TableFilter{
			CorpBizID:    corpBizID,
			AppBizID:     req.GetAppBizId(),
			DBTableBizID: req.GetDbTableBizId(),
		}
		dbTable, err := l.dao.DescribeTable(ctx, &tableFilter)
		// dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDbTableBizId())
		if err != nil {
			return err
		}
		if dbTable.IsIndexed == req.GetIsEnable() {
			return nil
		}
		if dbTable.LearnStatus == releaseEntity.ReleaseStatusInit {
			return errs.ErrDbTableStatus
		}
		dbFilter := entity.DatabaseFilter{
			CorpBizID:     corpBizID,
			AppBizID:      req.GetAppBizId(),
			DBSourceBizID: dbTable.DBSourceBizID,
		}
		dbSource, err := l.dao.DescribeDatabase(ctx, &dbFilter)
		// dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), dbTable.DBSourceBizID)
		if err != nil {
			return err
		}
		if !dbSource.IsIndexed {
			return errs.ErrDbSourceNotEnabled
		}
		err = l.dao.ModifyTable(ctx, &tableFilter, map[string]any{
			"learn_status": releaseEntity.ReleaseStatusInit,
			"staff_id":     staffID,
			"is_indexed":   isIndexed,
		})
		// err = dao.GetDBTableDao().UpdateByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDbTableBizId(), cols, dbTable)
		if err != nil {
			logx.E(ctx, "ChangeDbTableEnable failed, %v", err)
			return err
		}
		dbTablesBizIDs = append(dbTablesBizIDs, req.GetDbTableBizId())
	} else if req.GetDbSourceBizId() > 0 {
		dbFilter := entity.DatabaseFilter{
			CorpBizID:     corpBizID,
			AppBizID:      req.GetAppBizId(),
			DBSourceBizID: req.GetDbSourceBizId(),
		}
		dbSource, err := l.dao.DescribeDatabase(ctx, &dbFilter)
		// dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDbSourceBizId())
		if err != nil {
			logx.E(ctx, "EnableDbSourceScheduler|Prepare| get db source %v, err: %v", req.GetDbSourceBizId(),
				err)
			return err
		}
		if dbSource.IsIndexed == req.GetIsEnable() {
			return errs.ErrDbSourceStatusForChangeEnable
		}

		tableFilter := entity.TableFilter{
			CorpBizID:     corpBizID,
			AppBizID:      req.GetAppBizId(),
			DBSourceBizID: req.GetDbSourceBizId(),
			LearnStatus:   ptrx.Uint32(entity.LearnStatusLearning),
		}
		cnt, err := l.dao.CountTable(ctx, &tableFilter)
		// cnt, err := dao.GetDBSourceDao().CountByBizIDAndStatus(ctx, corpBizID, req.GetAppBizId(), req.GetDbSourceBizId())
		if err != nil {
			return err
		}
		if cnt > 0 {
			return errs.ErrDbSourceStatusForChangeEnable
		}
		tableFilter = entity.TableFilter{
			CorpBizID:     corpBizID,
			AppBizID:      req.GetAppBizId(),
			DBSourceBizID: req.GetDbSourceBizId(),
		}
		dbTables, _, err := l.dao.DescribeTableList(ctx, &tableFilter)
		// dbTables, err := dao.GetDBTableDao().ListAllByDBSourceBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDbSourceBizId())
		if err != nil {
			logx.E(ctx, "EnableDbSourceScheduler|Prepare| get db table by db source biz id %v, err: %v",
				req.GetDbSourceBizId(), err)
			return err
		}
		// 如果数据库关闭，则将之前表的状态记录
		if !req.GetIsEnable() {
			cols = append(cols, "prev_is_indexed")
			for _, table := range dbTables {
				table.PrevIsIndexed = table.IsIndexed
			}
		}
		for _, t := range dbTables {
			if t.LearnStatus == entity.LearnStatusLearning || t.IsIndexed == req.GetIsEnable() {
				continue
			}
			if (!req.GetIsEnable() && t.IsIndexed) || (req.GetIsEnable() && t.PrevIsIndexed) {
				// 如果数据库关，则需要将所有表的状态改为关闭
				// 如果数据库开启，则将之前用户设置为开的表进行开启
				dbTablesBizIDs = append(dbTablesBizIDs, t.DBTableBizID)
				t.LearnStatus = entity.LearnStatusLearning
				t.StaffID = staffID
				t.IsIndexed = isIndexed
			}
		}
		err = l.dao.BatchUpsertByBizID(ctx, cols, dbTables)
		if err != nil {
			logx.E(ctx, "BatchUpsertByBizID failed, %v", err)
			return err
		}

		dbFilter = entity.DatabaseFilter{
			CorpBizID:     corpBizID,
			AppBizID:      req.GetAppBizId(),
			DBSourceBizID: req.GetDbSourceBizId(),
		}
		now := time.Now()
		err = l.dao.ModifyDatabaseSimple(ctx, &dbFilter, map[string]any{
			"is_indexed":     isIndexed,
			"release_status": releaseEntity.ReleaseStatusInit,
			"staff_id":       staffID,
			"update_time":    now,
			"last_sync_time": now,
		})
		// err = dao.GetDBSourceDao().UpdateByBizID(ctx, corpBizID, req.GetAppBizId(), req.GetDbSourceBizId(), []string{"is_indexed", "update_time", "release_status", "staff_id", "last_sync_time"}, dbSource)
		if err != nil {
			logx.E(ctx, "EnableDbSourceScheduler|Prepare| update db source %v, err: %v",
				req.GetDbSourceBizId(), err)
			return err
		}
	}
	if len(dbTablesBizIDs) == 0 {
		return nil
	}
	for _, tableBizID := range dbTablesBizIDs {
		_, err = scheduler.NewEnableDbSourceTask(ctx, &entity0.EnableDBSourceParams{
			Name:          "",
			RobotID:       robotId,
			CorpBizID:     corpBizID,
			AppBizID:      req.GetAppBizId(),
			DbTableBizID:  tableBizID,
			DbSourceBizID: req.GetDbSourceBizId(),
			Enable:        req.GetIsEnable(),
			StaffID:       staffID,
		})
		if err != nil {
			logx.E(ctx, "NewEnableDbSourceTask failed, req:%v err:%v", req, err)
			return err
		}
	}
	return nil
}

func (l *Logic) CountTable(ctx context.Context, filter *entity.TableFilter) (int64, error) {
	return l.dao.CountTable(ctx, filter)
}

func (l *Logic) GetUnreleasedDBTable(ctx context.Context, appBizID uint64) ([]*entity.Table, error) {
	return l.dao.DescribeUnreleasedTableList(ctx, appBizID)
}

func (l *Logic) CollectUnreleasedDBTable(ctx context.Context, appBizID, releaseBizID uint64) error {
	return l.dao.CollectUnreleasedDBTable(ctx, appBizID, releaseBizID)
}

func (l *Logic) GetAllReleaseDBTables(ctx context.Context, appBizID, releaseBizID uint64,
	onlyBizID bool) ([]*entity.TableRelease, error) {
	return l.dao.GetAllReleaseDBTables(ctx, appBizID, releaseBizID, onlyBizID)
}

func (l *Logic) GetReleaseDBTable(ctx context.Context, appBizID uint64, releaseBizID uint64,
	dbTableBizID uint64) (entity.TableRelease, error) {
	return l.dao.GetReleaseDBTable(ctx, appBizID, releaseBizID, dbTableBizID)
}

func (l *Logic) ReleaseDBTableToProd(ctx context.Context, releaseDBTable entity.TableRelease) error {
	return l.dao.ReleaseDBTableToProd(ctx, releaseDBTable)
}
