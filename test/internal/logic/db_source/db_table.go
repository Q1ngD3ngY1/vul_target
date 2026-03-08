package db_source

import (
	"bytes"
	"context"
	"fmt"
	"time"
	"unicode/utf8"

	terrs "git.code.oa.com/trpc-go/trpc-go/errs"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	errgroup "git.woa.com/dialogue-platform/common/v3/sync/errgroupx"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
)

const (
	blankStr        = ""
	replacementChar = "?"
)

// BatchCreateDbTableAndColumn 新建表数据和列数据，保存到数据库
func BatchCreateDbTableAndColumn(ctx context.Context, dbSource *model.DBSource, tableNames []string, getDao dao.Dao) ([]*model.DBTable, error) {
	log.InfoContextf(ctx, "BatchCreateDbTableAndColumn start, db source: %v, table names: %v", dbSource, tableNames)
	decrypt, err := util.Decrypt(dbSource.Password)
	if err != nil {
		return nil, err
	}
	timeNow := time.Now()
	dbTableList := make([]*model.DBTable, len(tableNames))
	dbConn := model.ConnDbSource{
		DbType:   dbSource.DBType,
		Host:     dbSource.Host,
		DbName:   dbSource.DBName,
		Username: dbSource.Username,
		Password: decrypt,
		Port:     dbSource.Port,
	}

	wg, wgCtx := errgroup.WithContext(ctx)
	wg.SetLimit(5)
	// 1. 将获取到的表数据，存入 db_table 表中
	for idx, tableName := range tableNames {
		currentIdx := idx
		wg.Go(func() (err error) {
			// 1.1 拉取表单的具体数据
			tableInfo, err := dao.GetDBSourceDao().GetTableInfo(wgCtx, dbConn, tableName)
			if err != nil {
				log.WarnContextf(wgCtx, "conn : %v, get table info failed: %v", dbConn, err)
				return err
			}

			dbTable := &model.DBTable{
				CorpBizID:         dbSource.CorpBizID,
				AppBizID:          dbSource.AppBizID,
				DBSourceBizID:     dbSource.DBSourceBizID,
				DBTableBizID:      getDao.GenerateSeqID(),
				Source:            model.TableSourceDB,
				Name:              tableName,
				TableSchema:       dbSource.DBName,
				TableComment:      tableInfo.TableComment,
				AliasName:         blankStr,
				Description:       blankStr,
				RowCount:          tableInfo.RowCount,
				ColumnCount:       tableInfo.ColumnCount,
				TableAddedTime:    timeNow,
				TableModifiedTime: timeNow,
				ReleaseStatus:     model.ReleaseStatusUnreleased,
				NextAction:        model.ReleaseActionAdd,
				Alive:             true,
				IsIndexed:         true,
				IsDeleted:         false,
				LearnStatus:       model.LearnStatusLearning,
				LastSyncTime:      timeNow,
				CreateTime:        timeNow,
				UpdateTime:        timeNow,
				StaffID:           pkg.StaffID(ctx),
			}
			dbTableList[currentIdx] = dbTable

			// 2.2. 将获取到的列数据，存入 db_table_column 表中
			err = dao.GetDBTableColumnDao().AddColumns(wgCtx, tableInfo, dbTable, getDao)
			if err != nil {
				return err
			}
			return nil
		})
	}
	if err = wg.Wait(); err != nil {
		log.WarnContextf(ctx, "BatchCreateDbTableAndColumn failed: %v", err)
		return nil, err
	}
	// 3. 批量创建数据库表
	err = dao.GetDBTableDao().BatchCreate(ctx, dbTableList)
	if err != nil {
		log.ErrorContextf(ctx, "create db table failed: %v", err)
		return nil, err
	}
	return dbTableList, nil
}

// DeleteTablesAndColByDbSourceBizID 根据 数据源id， 删除数据源下的表和列
func DeleteTablesAndColByDbSourceBizID(ctx context.Context, corpBizID, appBizID, dbSourceBizID uint64, getDao dao.Dao) error {
	log.InfoContextf(ctx, "DeleteTablesAndColByDbSourceBizID start, corpBizID: %v, appBizID: %v, dbSourceBizID: %v", corpBizID, appBizID, dbSourceBizID)
	dbTableList, err := dao.GetDBTableDao().ListAllByDBSourceBizID(ctx, corpBizID, appBizID, dbSourceBizID)
	if err != nil {
		return err
	}

	// 获取 robotId
	robotId, err := GetRobotIdByAppBizId(ctx, appBizID)
	if err != nil {
		return err
	}

	dbTableBizIDs := make([]uint64, 0)
	for _, dbTable := range dbTableList {
		dbTableBizIDs = append(dbTableBizIDs, dbTable.DBTableBizID)
		err = DeleteTableAndColumn(ctx, corpBizID, robotId, appBizID, dbTable.DBTableBizID, getDao)
		if err != nil {
			return err
		}
	}

	err = dao.GetRoleDao(nil).DeleteKnowledgeRoleDbTables(ctx, appBizID, dbTableBizIDs)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteTablesAndColByDbSourceBizID failed, delete knowledge role db tables failed: %v", err)
		return err
	}
	return nil
}

// DeleteTableAndColumn 根据 表ID， 删除表和列
func DeleteTableAndColumn(ctx context.Context, corpBizID, robotID, appBizID, dbTableBizID uint64, getDao dao.Dao) error {
	err := DeleteDBText2SQL(ctx, robotID, []uint64{dbTableBizID}, retrieval.EnvType_Test)
	if err != nil {
		return err
	}
	embeddingVersion, err := GetAppEmbeddingVersionById(ctx, robotID, getDao)
	if err != nil {
		log.ErrorContextf(ctx, "GetAppEmbeddingVersionById failed, get app embedding version failed: %v", err)
		return err
	}

	err = DeleteDbTableVdb(ctx, robotID, corpBizID, appBizID, dbTableBizID, embeddingVersion, retrieval.EnvType_Test)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteDbTableVdb failed, delete db table vdb failed, dbTableBizID:%v err:%v", dbTableBizID, err)
		return err
	}

	err = dao.GetDBTableDao().SoftDeleteByBizID(ctx, corpBizID, appBizID, dbTableBizID)
	if err != nil {
		return err
	}
	err = dao.GetDBTableColumnDao().SoftDeleteByTableBizID(ctx, corpBizID, appBizID, dbTableBizID)
	if err != nil {
		return err
	}
	return nil
}

// FlashTableAndColumn 同步用户数据表信息，以及列信息
func FlashTableAndColumn(ctx context.Context, dbTable *model.DBTable, isFlashColumn bool, d dao.Dao) (bool, error) {
	if int(time.Since(dbTable.LastSyncTime).Seconds()) < config.App().DbSource.SyncTimeS {
		return false, nil
	}
	// 0. 判断是否需要同步
	log.InfoContextf(ctx, "FlashTableAndColumn start, dbTable: %v", dbTable)
	dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, dbTable.CorpBizID, dbTable.AppBizID, dbTable.DBSourceBizID)
	if err != nil {
		return false, err
	}
	password, err := util.Decrypt(dbSource.Password)
	if err != nil {
		return false, err
	}
	dbConn := model.ConnDbSource{
		DbType:   dbSource.DBType,
		Host:     dbSource.Host,
		DbName:   dbSource.DBName,
		Username: dbSource.Username,
		Password: password,
		Port:     dbSource.Port,
	}

	// 1. 根据数据源， 获取远端最新的表信息
	tableInfo, err := dao.GetDBSourceDao().GetTableInfo(ctx, dbConn, dbTable.Name)
	if err != nil {
		if terrs.Code(err) == terrs.Code(errs.ErrDbTableIsNotExist) {
			log.WarnContextf(ctx, "FlashTableAndColumn|dbTable %v err %v", dbTable, err)
			dbTable.Alive = false
			err = dao.GetDBTableDao().UpdateByBizID(ctx, dbTable.CorpBizID, dbTable.AppBizID, dbTable.DBTableBizID,
				[]string{"last_sync_time", "alive"}, dbTable)
			if err != nil {
				return false, err
			}
			return false, errs.ErrDbTableIsNotExist
		}
		return false, err
	}

	if len(tableInfo.ColumnInfo) == 0 {
		log.WarnContextf(ctx, "FlashTableAndColumn|table info is empty, table: %v", dbTable)
		dbTable.Alive = false
	} else {
		dbTable.Alive = true
	}

	// 2. 更新本地表信息
	dbTable.RowCount = tableInfo.RowCount
	dbTable.ColumnCount = tableInfo.ColumnCount
	dbTable.LastSyncTime = time.Now()
	dbTable.UpdateTime = time.Now()
	err = dao.GetDBTableDao().UpdateByBizID(ctx, dbTable.CorpBizID, dbTable.AppBizID, dbTable.DBTableBizID,
		[]string{"row_count", "column_count", "last_sync_time", "alive", "update_time"}, dbTable)
	if err != nil {
		return false, err
	}
	if !isFlashColumn {
		return true, nil
	}

	// 3. 获取现有数据库中的列信息, 与远端表信息进行对比， 将不存在的列添加到数据库中
	columns, err := dao.GetDBTableColumnDao().GetByTableBizID(ctx, dbTable.CorpBizID, dbTable.AppBizID, dbTable.DBTableBizID)
	if err != nil {
		return false, err
	}

	addColumns := make([]*model.ColumnInfo, 0)
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
		newTableInfo := &model.TableInfo{
			ColumnInfo:  addColumns,
			RowCount:    tableInfo.RowCount,
			ColumnCount: tableInfo.ColumnCount,
		}
		err = dao.GetDBTableColumnDao().AddColumns(ctx, newTableInfo, dbTable, d)
		if err != nil {
			return false, err
		}
	}

	// 4. 将不存在的列删除
	if len(delColumnIds) != 0 {
		flag = true
		cnt, err := dao.GetDBTableColumnDao().DeleteByTableBizID(ctx, delColumnIds)
		if err != nil {
			return false, err
		}
		if cnt > 0 {
			log.InfoContextf(ctx, "delete db table columns success, count: %d", cnt)
			flag = true
		}
	}

	return flag, nil
}

// AddDbTableData2ES1 将远端数据库中的数据，添加到es1中
// 评测端不需要传标签，函数中会取标签
func AddDbTableData2ES1(ctx context.Context, dbSource *model.DBSource, robotId, dbTableBizID uint64,
	envType retrieval.EnvType) error {

	log.InfoContextf(ctx, "AddDbTableData2ES1, table: %v, env: %v", dbTableBizID, envType.String())

	dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, dbSource.CorpBizID, dbSource.AppBizID, dbTableBizID)
	if err != nil {
		log.ErrorContextf(ctx, "get db table by biz id failed, %v", err)
		return err
	}

	columns, err := dao.GetDBTableColumnDao().GetByTableBizID(ctx, dbSource.CorpBizID, dbSource.AppBizID, dbTable.DBTableBizID)
	if err != nil {
		log.ErrorContextf(ctx, "get db table columns by table biz id failed, %v", err)
		return err
	}

	indexColumnName := make(map[string]struct{})
	for _, column := range columns {
		if column.IsIndexed {
			indexColumnName[column.ColumnName] = struct{}{}
		}
	}

	password, err := util.Decrypt(dbSource.Password)
	if err != nil {
		return err
	}
	dbConn := model.ConnDbSource{
		DbType:   dbSource.DBType,
		Host:     dbSource.Host,
		DbName:   dbSource.DBName,
		Username: dbSource.Username,
		Password: password,
		Port:     dbSource.Port,
	}

	colNames, rows, total, err := dao.GetDBSourceDao().ListPreviewData(ctx, dbConn, dbTable.Name, 1,
		config.App().DbSource.EsInputSize, blankStr, blankStr, config.App().DbSource.EsInsertTimeOut)
	log.InfoContextf(ctx, "colNames: %v, rowLen: %v", colNames, total)
	if err != nil {
		log.ErrorContextf(ctx, "AddDbTableData2ES1|get db table preview data failed, dbSource %v err: %v", dbSource, err)
		return err
	}
	if total == 0 {
		log.WarnContextf(ctx, "get db table preview data total is 0")
		return nil
	}

	colName2Model := make(map[string]*model.DBTableColumn)
	for _, column := range columns {
		colName2Model[column.ColumnName] = column
	}

	dbRowData := make([]*retrieval.DBRowData, 0)

	for rowIdx, row := range rows {
		dbCells := make([]*retrieval.DBCell, 0)
		for idx, name := range colNames {
			if _, ok := indexColumnName[name]; !ok {
				continue
			}
			col := colName2Model[name]
			if col == nil {
				log.ErrorContextf(ctx, "colName2Model not found col, %v, table biz id %v", name, dbTableBizID)
				col = &model.DBTableColumn{}
			}
			colDesc := col.ColumnComment + "|" + col.AliasName + "|" + col.Description + "|" + col.Unit
			var cell *retrieval.DBCell
			// 第一列保存全量数据，其他列没有全量数据，减少数据发送量，在 检索 端会进行恢复。
			if rowIdx == 0 {
				cell = &retrieval.DBCell{
					ColumnName:      ensureValidUTF8(name),
					ColumnAliasName: ensureValidUTF8(col.AliasName),
					ColumnDesc:      ensureValidUTF8(colDesc),
					DataType:        ensureValidUTF8(col.DataType),
					Value:           ensureValidUTF8(row.Values[idx]),
				}
			} else {
				cell = &retrieval.DBCell{
					Value: ensureValidUTF8(row.Values[idx]),
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

	labels, err := dao.GetRoleLabelsOfDBTable(ctx, dbSource.AppBizID, dbTableBizID)
	if err != nil {
		return err
	}

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

	if dbSource.DBType == dao.DbTypeMysql {
		req.DbType = retrieval.DBType_MYSQL
	} else if dbSource.DBType == dao.DbTypeSqlserver {
		req.DbType = retrieval.DBType_SQL_SERVER
	} else {
		return errs.ErrDbSourceTypeNotSupport
	}

	// 添加之前先全量删除，AddDBText2SQL接口支持全量覆盖，不过为了防止存入的数据行数配置项变化导致删除残留
	// 比如之前存了100行，后面修改到只存10行，不能完全覆盖之前的100行
	err = DeleteDBText2SQL(ctx, robotId, []uint64{dbTableBizID}, envType)
	if err != nil {
		log.ErrorContextf(ctx, "delete db text2sql to es1 failed, dbTableBizId:%v err:%v", dbTableBizID, err)
		return err
	}

	_, err = retrieval.NewDirectIndexClientProxy().AddDBText2SQL(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "add db text2sql to es1 failed, dbTableBizID:%v, err:%v", req.DbTableBizId, err)
		return err
	}

	return nil
}

func DeleteDBText2SQL(ctx context.Context, robotId uint64, dbTableBizIDs []uint64, envType retrieval.EnvType) error {
	_, err := retrieval.NewDirectIndexClientProxy().DeleteDBText2SQL(ctx, &retrieval.DeleteDBText2SQLReq{
		RobotId:      robotId,
		DbTableBizId: dbTableBizIDs,
		EnvType:      envType,
	})
	if err != nil {
		log.ErrorContextf(ctx, "DeleteDBText2SQL table: %+v, env %v", dbTableBizIDs, envType.String())
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

func GetRobotIdByAppBizId(ctx context.Context, appBizId uint64) (uint64, error) {
	id, err := dao.GetAppIDByAppBizID(ctx, appBizId)
	if err != nil {
		log.ErrorContextf(ctx, "GetAppIDByAppBizID| appBizId %v, get app id by app biz id failed RobotId:%v, %v", appBizId, id, err)
		return 0, err
	}
	return id, nil
}

// ChangeDbTableEnable 修改数据表的启用状态
func ChangeDbTableEnable(ctx context.Context, robotID, corpBizID, appBizId, dbTableBizId uint64, isEnable bool, getDao dao.Dao) error {

	// 1.获取db table
	dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, corpBizID, appBizId, dbTableBizId)
	if err != nil {
		log.ErrorContextf(ctx, "ChangeDbTableEnable| appBizId %v, dbTableBizId %v, get db table by biz id failed, %v", appBizId, dbTableBizId, err)
		return err
	}

	// 2 关闭删除es1数据
	err = DeleteDBText2SQL(ctx, robotID, []uint64{dbTableBizId}, retrieval.EnvType_Test)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteDBText2SQL failed, %v", err)
		return err
	}

	embeddingVersion, err := GetAppEmbeddingVersionById(ctx, robotID, getDao)
	if err != nil {
		log.ErrorContextf(ctx, "GetAppEmbeddingVersionById failed, get app embedding version failed: %v", err)
		return err
	}

	err = DeleteDbTableVdb(ctx, robotID, corpBizID, appBizId, dbTableBizId, embeddingVersion, retrieval.EnvType_Test)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteDbTableVdb failed, delete db table vdb failed, dbTableBizID:%v err:%v", dbTableBizId, err)
		return err
	}

	// 3.根据是否开启进行处理
	if isEnable {
		// 2.2 开启添加es1数据
		dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, corpBizID, appBizId, dbTable.DBSourceBizID)
		if err != nil {
			log.ErrorContextf(ctx, "get db source by biz id failed, %v", err)
			return err
		}

		err = AddDbTableData2ES1(ctx, dbSource, robotID, dbTableBizId, retrieval.EnvType_Test)
		if err != nil {
			log.ErrorContextf(ctx, "add db source table data to es1 failed: %v", err)
			return err
		}

		err = dao.GetDBSourceDao().GetTopNValueV2(ctx, dbSource, robotID, dbTableBizId, embeddingVersion, getDao)
		if err != nil {
			log.ErrorContextf(ctx, "ChangeDbTableEnable|get top n value for mysql failed: robotID:%v, dbTableBizId:%v, err:%v", robotID, dbTableBizId, err)
			return err
		}
	}

	cols := []string{"is_indexed", "update_time", "release_status", "staff_id", "learn_status", "table_modified_time"}
	dbTable.ReleaseStatus = model.ReleaseStatusUnreleased
	if dbTable.NextAction != model.ReleaseActionAdd {
		dbTable.NextAction = model.ReleaseActionUpdate
		cols = append(cols, "next_action")
	}
	dbTable.IsIndexed = isEnable
	dbTable.UpdateTime = time.Now()
	dbTable.StaffID = pkg.StaffID(ctx)
	dbTable.TableModifiedTime = time.Now()
	dbTable.LearnStatus = model.LearnStatusLearned
	err = dao.GetDBTableDao().UpdateByBizID(ctx, corpBizID, appBizId, dbTableBizId,
		cols, dbTable)
	if err != nil {
		log.ErrorContextf(ctx, "ChangeDbTableEnable failed, %v", err)
		return err
	}
	return nil
}

// CreateDbTableLearnTask 创建db table 学习任务
func CreateDbTableLearnTask(ctx context.Context, robotID, corpBizID, appBizID, dbTableBizID uint64, dbSource *model.DBSource, getDao dao.Dao) error {
	embeddingVersion, err := GetAppEmbeddingVersionById(ctx, robotID, getDao)
	if err != nil {
		log.ErrorContextf(ctx, "GetAppEmbeddingVersionById failed: %v", err)
		return err
	}
	_, err = dao.NewAddTableTask(ctx, &model.LearnDBTableParams{
		Name:             "",
		RobotID:          robotID,
		CorpBizID:        corpBizID,
		AppBizID:         appBizID,
		DBTableBizID:     dbTableBizID,
		DBSource:         dbSource,
		EmbeddingVersion: embeddingVersion,
	})
	if err != nil {
		log.ErrorContextf(ctx, "CreateDbTableLearnTask| robotID: %v, corpBizID: %v, appBizID: %v, dbTableBizID: %v, embeddingVersion:%v, dbSource: %v,  failed, %v",
			robotID, corpBizID, appBizID, dbTableBizID, embeddingVersion, dbSource, err)
		return err
	}
	return nil
}

// UpsertDbTable2Vdb 更新外部数据库的值到 vdb 中
func UpsertDbTable2Vdb(ctx context.Context, robotID, corpBizID, appBizID, dbTableBizID, embeddingVersion uint64) error {
	log.InfoContextf(ctx, "UpsertDbTable2Vdb| robotID: %v, corpBizID: %v, appBizID: %v, dbTableBizID: %v, embeddingVersion:%v", robotID, corpBizID, appBizID, dbTableBizID, embeddingVersion)
	maxID := uint64(0)
	for {
		topValues, err := dao.GetDBSourceDao().GetTopValuesPageByDbTableBizID(ctx, corpBizID, appBizID, dbTableBizID, maxID, 50)
		if err != nil {
			return err
		}
		if len(topValues) == 0 {
			break
		}
		maxID = topValues[len(topValues)-1].ID
		log.DebugContextf(ctx, "UpsertDbTable2Vdb|dbTableBizID: %v, maxID: %v", dbTableBizID, maxID)
		for _, value := range topValues {
			content := fmt.Sprintf("%v;%v;%v", value.ColumnName, value.ColumnComment, value.ColumnValue)
			err = dao.GetDBSourceDao().AddVdb(ctx, robotID, appBizID, value.BusinessID, embeddingVersion, content, retrieval.EnvType_Prod)
			if err != nil {
				log.ErrorContextf(ctx, "UpsertDbTable2Vdb| robotID: %v, corpBizID: %v, appBizID: %v, dbTableBizID: %v, embeddingVersion:%v, content: %v, failed, %v",
					robotID, corpBizID, appBizID, dbTableBizID, embeddingVersion, content, err)
				return err
			}
		}
	}
	return nil
}

// DeleteDbTableVdb 删除外部数据库的值到 vdb 中
func DeleteDbTableVdb(ctx context.Context, robotID, corpBizID, appBizID, dbTableBizID, embeddingVersion uint64, envType retrieval.EnvType) error {
	log.InfoContextf(ctx, "DeleteDbTableVdb| robotID: %v, corpBizID: %v, appBizID: %v, dbTableBizID: %v, embeddingVersion:%v", robotID, corpBizID, appBizID, dbTableBizID, embeddingVersion)
	t0 := time.Now()
	maxID := uint64(0)
	cnt := 0
	for {
		var topValueBizIDs []uint64
		var topValues []*model.DbTableTopValue
		var err error
		if envType == retrieval.EnvType_Test {
			topValues, err = dao.GetDBSourceDao().GetTopValuesPageByDbTableBizID(ctx, corpBizID, appBizID, dbTableBizID, maxID, 200)
			if err != nil {
				return err
			}
		} else {
			topValues, err = dao.GetDBSourceDao().GetDeletedTopValuesPageByDbTableBizID(ctx, corpBizID, appBizID, dbTableBizID, maxID, 200)
			if err != nil {
				return err
			}
		}

		log.DebugContextf(ctx, "DeleteDbTableVdb|dbTableBizID: %+v, maxID: %v, topValues :%v", dbTableBizID, maxID, topValues)
		if len(topValues) == 0 {
			break
		}
		maxID = topValues[len(topValues)-1].ID
		for _, value := range topValues {
			cnt++
			topValueBizIDs = append(topValueBizIDs, value.BusinessID)
		}
		err = dao.GetDBSourceDao().DelVdb(ctx, robotID, appBizID, embeddingVersion, topValueBizIDs, envType)
		if err != nil {
			log.ErrorContextf(ctx, "DeleteDbTableVdb| appBizID: %v, dbTableBizID: %v, embeddingVersion:%v, failed, %v", appBizID, dbTableBizID, embeddingVersion, err)
			return err
		}

		if envType == retrieval.EnvType_Test {
			err = dao.GetDBSourceDao().BatchSoftDeleteTopValuesByBizID(ctx, topValueBizIDs)
			if err != nil {
				log.ErrorContextf(ctx, "BatchSoftDeleteTopValuesByBizID| appBizID: %v, dbTableBizID: %v, embeddingVersion:%v, failed, %v", appBizID, dbTableBizID, embeddingVersion, err)
				return err
			}
		} else {
			err = dao.GetDBSourceDao().BatchHardDeleteTopValuesByBizID(ctx, topValueBizIDs)
			if err != nil {
				log.ErrorContextf(ctx, "BatchHardDeleteTopValuesByBizID| appBizID: %v, dbTableBizID: %v, embeddingVersion:%v, failed, %v", appBizID, dbTableBizID, embeddingVersion, err)
				return err
			}
		}
	}
	if cnt > 0 {
		log.DebugContextf(ctx, "DeleteDbTableVdb|dbTableBizID  %v, cnt: %v", dbTableBizID, cnt)
		if envType == retrieval.EnvType_Test {
			err := dao.GetDBSourceDao().BatchSoftDeleteTopValuesByDbTableBizID(ctx, corpBizID, appBizID, dbTableBizID)
			if err != nil {
				log.ErrorContextf(ctx, "DeleteDbTableVdb| appBizID: %v, dbTableBizID: %v, embeddingVersion:%v, failed, %v", appBizID, dbTableBizID, embeddingVersion, err)
				return err
			}
		} else {
			err := dao.GetDBSourceDao().BatchCleanDeletedTopValuesByDbTableBizID(ctx, corpBizID, appBizID, dbTableBizID)
			if err != nil {
				log.ErrorContextf(ctx, "DeleteDbTableVdb| appBizID: %v, dbTableBizID: %v, embeddingVersion:%v, failed, %v", appBizID, dbTableBizID, embeddingVersion, err)
				return err
			}
		}
	} else {
		log.WarnContextf(ctx, "DeleteDbTableVdb|dbTableBizID: %v, cnt: %v, del vdb zero", dbTableBizID, cnt)
	}
	log.InfoContextf(ctx, "DeleteDbTableVdb|dbTableBizID: %v, cnt: %v, cost: %vms", dbTableBizID, cnt, time.Now().Sub(t0).Milliseconds())
	return nil
}

// BatchGetTablesWithColumns 批量获取表和列
func BatchGetTablesWithColumns(ctx context.Context, appBizID uint64, dbTableBizIDs []uint64) ([]*pb.DbTableWithColumns, error) {
	corpBizID := pkg.CorpBizID(ctx)
	dbTables, err := dao.GetDBTableDao().BatchGetByBizIDs(ctx, corpBizID, appBizID, dbTableBizIDs)
	if err != nil {
		log.ErrorContextf(ctx, "BatchGetTablesWithColumns|failed:dbTableBizIDs:%v, err:%v", dbTableBizIDs, err)
		return nil, err
	}
	dbTableWithColumns := make([]*pb.DbTableWithColumns, 0, len(dbTables))
	if len(dbTables) == 0 {
		log.WarnContextf(ctx, "BatchGetTablesWithColumns|dbTableBizIDs:%v, dbTables is empty", dbTableBizIDs)
		return dbTableWithColumns, nil
	}
	dbColumns, err := dao.GetDBTableColumnDao().BatchGetByTableBizID(ctx, corpBizID, appBizID, dbTableBizIDs)
	if err != nil {
		log.ErrorContextf(ctx, "BatchGetTablesWithColumns|failed:dbTableBizIDs:%v, err:%v", dbTableBizIDs, err)
		return nil, err
	}
	if dbColumns == nil {
		dbColumns = make(map[uint64][]*model.DBTableColumn)
	}
	for _, dbTable := range dbTables {
		if dbTable == nil {
			continue
		}
		view := DbTableWithColumnsToView(dbTable, dbColumns[dbTable.DBTableBizID])
		dbTableWithColumns = append(dbTableWithColumns, view)
	}
	return dbTableWithColumns, nil
}
