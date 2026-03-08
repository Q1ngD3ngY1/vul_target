package doc_intervene

import (
	"context"
	"encoding/json"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/db_source"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
)

const (
	blankStr       = ""
	batchSizeForDB = 500
	batchSizeForEs = 100
)

func PreviewText2sqlData(ctx context.Context, appBizId, docBizId uint64, pageNum, pageSize int, getDao dao.Dao) ([]*bot_knowledge_config_server.Text2SqlPreviewSheet, error) {

	roBotId, err := db_source.GetRobotIdByAppBizId(ctx, appBizId)
	if err != nil {
		log.ErrorContextf(ctx, "GetRobotIdByAppBizId err: %v", err)
		return nil, err
	}

	metaMappings, err := dao.GetDocMetaDataDao().GetDocMetaDataByDocId(ctx, docBizId, roBotId)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocMetaDataByDocId err: %v", err)
		return nil, err
	}

	var sheetList []*bot_knowledge_config_server.Text2SqlPreviewSheet
	for _, metaMapping := range metaMappings {
		// 1. 判断 table 数据是否添加
		dbTable, err := GetSheetTableColumn(ctx, appBizId, metaMapping, getDao)
		if err != nil || dbTable == nil {
			log.ErrorContextf(ctx, "GetSheetTableColumn err: %v", err)
			return nil, err
		}

		// 2. 获取数据
		lastID := (pageNum - 1) * pageSize
		col, rows, total, err := dao.GetDocMetaDataDao().GetText2sqlDataPreview(ctx, metaMapping.DbName, metaMapping.MappingTableName, false, lastID, pageSize)
		if err != nil {
			return nil, err
		}

		sheetData := &bot_knowledge_config_server.Text2SqlPreviewSheet{
			SheetName:     dbTable.AliasName,
			DocSheetBizId: dbTable.DBTableBizID,
			Columns:       col,
			Rows:          rows,
			Total:         int32(total),
		}
		sheetList = append(sheetList, sheetData)
	}
	return sheetList, nil
}

func GetSheetTableColumn(ctx context.Context, appBizId uint64, metaMapping model.Text2sqlMetaMappingPreview, getDao dao.Dao) (*model.DBTable, error) {
	log.InfoContextf(ctx, "GetSheetTableColumn| metaMapping BizID %v, DBName %v,  MappingTableName %v ", metaMapping.BusinessID, metaMapping.DbName, metaMapping.MappingTableName)
	corpBizId := pkg.CorpBizID(ctx)
	isExist, err := dao.GetDBTableDao().Text2sqlExistsByDbSourceBizID(ctx, corpBizId, appBizId, metaMapping.BusinessID)
	if err != nil {
		return nil, err
	}
	if isExist {
		log.InfoContextf(ctx, "GetSheetTableColumn metaMapping: %v, already exist", metaMapping)
		dbTable, err := dao.GetDBTableDao().Text2sqlGetByDbSourceBizID(ctx, corpBizId, appBizId, metaMapping.BusinessID)
		if err != nil {
			return nil, err
		}
		return dbTable, nil
	}

	// 1. 获取数据库表信息
	tableInfo, err := dao.GetDocMetaDataDao().GetText2sqlTableInfo(ctx, metaMapping.DbName, metaMapping.MappingTableName)
	if err != nil {
		log.ErrorContextf(ctx, "GetText2sqlTableInfo err: %v", err)
		return nil, err
	}

	var data map[string]interface{}
	err = json.Unmarshal([]byte(metaMapping.Mapping), &data)
	if err != nil {
		log.ErrorContextf(ctx, "GetSheetTableColumn json.Unmarshal err: %v", err)
		return nil, err
	}

	tableName, ok := data["table_name"].(map[string]interface{})
	if !ok {
		return nil, err
	}
	rawValue, ok := tableName["raw"].(string)
	if !ok {
		return nil, err
	}

	timeNow := time.Now()
	dbTable := &model.DBTable{
		CorpBizID:         corpBizId,
		AppBizID:          appBizId,
		DBSourceBizID:     metaMapping.BusinessID,
		DBTableBizID:      getDao.GenerateSeqID(),
		Source:            model.TableSourceDoc,
		Name:              metaMapping.MappingTableName,
		TableSchema:       metaMapping.DbName,
		TableComment:      tableInfo.TableComment,
		AliasName:         rawValue,
		Description:       blankStr,
		RowCount:          tableInfo.RowCount,
		ColumnCount:       tableInfo.ColumnCount,
		TableAddedTime:    timeNow,
		TableModifiedTime: timeNow,
		// 特殊逻辑，对于从解析干预页面创建的db_table，如果不做干预，没有任何有效信息，设置特殊状态字段，不需要发布
		// 否在在点击进入页面之后，会有待发布的表格
		ReleaseStatus: model.ReleaseStatus(0),
		NextAction:    model.ReleaseActionAdd,
		Alive:         true,
		IsIndexed:     true,
		IsDeleted:     false,
		LastSyncTime:  timeNow,
		CreateTime:    timeNow,
		UpdateTime:    timeNow,
	}

	// 2. 将获取到的列数据，存入 db_table_column 表中
	err = dao.GetDBTableColumnDao().AddColumns(ctx, tableInfo, dbTable, getDao)
	if err != nil {
		log.ErrorContextf(ctx, "add db table columns failed: %v", err)
		return nil, err
	}

	// 3. 批量创建数据库表
	err = dao.GetDBTableDao().BatchCreate(ctx, []*model.DBTable{dbTable})
	if err != nil {
		log.ErrorContextf(ctx, "create db table failed: %v", err)
		return nil, err
	}
	return dbTable, nil
}

func getSheetName(ctx context.Context, metaMapping model.Text2sqlMetaMappingPreview) (string, error) {
	var data map[string]interface{}
	err := json.Unmarshal([]byte(metaMapping.Mapping), &data)
	if err != nil {
		log.ErrorContextf(ctx, "GetSheetTableColumn json.Unmarshal err: %v", err)
		return "", err
	}

	tableName, ok := data["table_name"].(map[string]interface{})
	if !ok {
		return "", err
	}
	rawValue, ok := tableName["raw"].(string)
	if !ok {
		return "", err
	}
	return rawValue, nil
}

// AddSheetTableData2es1 将text2sql数据库中的数据，添加到es1中
func AddSheetTableData2es1(ctx context.Context, robotId, appBizId uint64, metaMapping model.Text2sqlMetaMappingPreview) error {
	log.InfoContextf(ctx, "AddSheetTableData2es1|metaMapping: %v", metaMapping)

	corpBizID := pkg.CorpBizID(ctx)
	dbTable, err := dao.GetDBTableDao().Text2sqlGetByDbSourceBizID(ctx, corpBizID, appBizId, metaMapping.BusinessID)
	if err != nil {
		log.ErrorContextf(ctx, "get db table by biz id failed, %v", err)
		return err
	}

	// 更新 table 的同步时间 和 更新时间， 确保两者一致，用于特别判断 解析干预 中 结构化数据 用户自定义是否发生了变更。
	timeNow := time.Now()
	dbTable.LastSyncTime = timeNow
	dbTable.UpdateTime = timeNow
	err = dao.GetDBTableDao().UpdateByBizID(ctx, corpBizID, dbTable.AppBizID, dbTable.DBTableBizID, []string{"last_sync_time", "update_time"}, dbTable)
	if err != nil {
		log.ErrorContextf(ctx, "AddSheetTableData2es1|update db table failed, %v", err)
		return err
	}

	columns, err := dao.GetDBTableColumnDao().GetByTableBizID(ctx, corpBizID, appBizId, dbTable.DBTableBizID)
	if err != nil {
		log.ErrorContextf(ctx, "get db table columns by table biz id failed, %v", err)
		return err
	}

	maxId, err := dao.GetDocMetaDataDao().GetText2sqlMaxId(ctx, metaMapping.DbName, metaMapping.MappingTableName)
	if err != nil {
		log.ErrorContextf(ctx, "get text2sql max id failed, %v", err)
		return err
	}

	batchSize := batchSizeForDB
	var colNames []string
	var rows []*bot_knowledge_config_server.RowData
	var total int64
	for lastId := 0; lastId <= maxId; lastId += batchSize {
		colNames, rows, total, err = dao.GetDocMetaDataDao().GetText2sqlDataPreview(ctx, metaMapping.DbName, metaMapping.MappingTableName, true, lastId, batchSize)
		if err != nil {
			log.ErrorContextf(ctx, "get db table preview data failed, %v", err)
			return err
		}

		if total == 0 {
			log.WarnContextf(ctx, "get db table preview data total is 0")
			break
		}

		for _, batchRows := range slicex.Chunk(rows, batchSizeForEs) {
			err = updateText2sqlEs(ctx, dbTable, columns, colNames, batchRows, robotId)
			if err != nil {
				log.ErrorContextf(ctx, "update text2sql es failed, %v", err)
				return err
			}
		}

		if len(rows) < batchSize {
			break
		}
	}

	return nil
}

func updateText2sqlEs(ctx context.Context, dbTable *model.DBTable, columns []*model.DBTableColumn,
	colNames []string, rows []*bot_knowledge_config_server.RowData, robotId uint64) error {
	log.InfoContextf(ctx, "PreviewText2sqlData preview: col %v", colNames)
	colName2Model := make(map[string]*model.DBTableColumn)
	for _, column := range columns {
		colName2Model[column.ColumnName] = column
	}

	dbRowData := make([]*bot_retrieval_server.DBRowData, 0)

	for rowIdx, row := range rows {
		dbCells := make([]*bot_retrieval_server.DBCell, 0)
		for idx, name := range colNames {
			col := colName2Model[name]
			if col == nil {
				log.ErrorContextf(ctx, "colName2Model not found col, %v, table biz id %v", name, dbTable)
				col = &model.DBTableColumn{}
			}
			colDesc := col.ColumnComment + "|" + col.Description + "|" + col.DataType + "|" + col.Unit
			var cell *bot_retrieval_server.DBCell
			if rowIdx == 0 {
				cell = &bot_retrieval_server.DBCell{
					ColumnName:      name,
					ColumnAliasName: col.AliasName,
					ColumnDesc:      colDesc,
					DataType:        col.DataType,
					Value:           row.Values[idx],
				}
			} else {
				cell = &bot_retrieval_server.DBCell{
					Value: row.Values[idx],
				}
			}
			dbCells = append(dbCells, cell)
		}
		dbRowData = append(dbRowData, &bot_retrieval_server.DBRowData{
			Cells: dbCells,
		})
	}

	// 表描述信息，格式：表名｜表注释｜表别名｜表描述
	tableDesc := dbTable.Name + "|" + dbTable.TableComment + "|" + dbTable.AliasName + "|" + dbTable.Description

	req := &bot_retrieval_server.AddDBText2SQLReq{
		RobotId:       robotId,
		DbSourceBizId: dbTable.DBSourceBizID,
		DbType:        bot_retrieval_server.DBType_Text_To_SQL,
		DbDesc:        "",
		DbTableBizId:  dbTable.DBTableBizID,
		TableDesc:     tableDesc,
		Rows:          dbRowData,
		EnvType:       bot_retrieval_server.EnvType_Test,
	}
	// 文档写入es1数据是增量，标签数据不变，不需要传
	log.InfoContextf(ctx, "AddSheetTableData2es1|DbSourceBizId: %v,DbType:%v, DbTableBizId: %v, EnvType: %v, TableDesc: %v",
		dbTable.DBSourceBizID, req.DbType, dbTable.DBTableBizID, req.EnvType, tableDesc)

	_, err := bot_retrieval_server.NewDirectIndexClientProxy().AddDBText2SQL(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "add db text2sql to es1 failed, %v", err)
		return err
	}
	return nil
}

// UpdateSheet2Knowledge 解析干预 text2sql， 更新结构化数据注释后，更新 es 数据
func UpdateSheet2Knowledge(ctx context.Context, appBizId, docBizId uint64) error {

	roBotId, err := db_source.GetRobotIdByAppBizId(ctx, appBizId)
	if err != nil {
		log.ErrorContextf(ctx, "GetRobotIdByAppBizId err: %v", err)
		return err
	}

	metaMappings, err := dao.GetDocMetaDataDao().GetDocMetaDataByDocId(ctx, docBizId, roBotId)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocMetaDataByDocId err: %v", err)
		return err
	}

	for _, metaMapping := range metaMappings {
		err = AddSheetTableData2es1(ctx, roBotId, appBizId, metaMapping)
		if err != nil {
			log.ErrorContextf(ctx, "AddSheetTableData2es1 err: %v", err)
			return err
		}
	}
	return nil
}

// CheckTextToSqlTableIsChanged 解析干预 text2sql， 判断用户是否修改结构化数据注释
func CheckTextToSqlTableIsChanged(ctx context.Context, appBizId, docBizId uint64) (bool, error) {
	log.InfoContextf(ctx, "CheckTextToSqlTableIsChanged check table is changed,  doc biz id: %v", docBizId)
	roBotId, err := db_source.GetRobotIdByAppBizId(ctx, appBizId)
	if err != nil {
		log.ErrorContextf(ctx, "GetRobotIdByAppBizId err: %v", err)
		return false, err
	}

	metaMappings, err := dao.GetDocMetaDataDao().GetDocMetaDataByDocId(ctx, docBizId, roBotId)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocMetaDataByDocId err: %v", err)
		return false, err
	}

	corpBizID := pkg.CorpBizID(ctx)
	for _, metaMapping := range metaMappings {
		dbTable, err := dao.GetDBTableDao().Text2sqlGetByDbSourceBizID(ctx, corpBizID, appBizId, metaMapping.BusinessID)
		if err != nil {
			return false, err
		}
		if dbTable.LastSyncTime != dbTable.UpdateTime {
			return true, nil
		}
	}
	return false, nil
}

// DelText2SqlTablesByDocId 解析干预 text2sql， 根据 docId， 批量删除用户注释
func DelText2SqlTablesByDocId(ctx context.Context, appBizId, docBizId uint64) error {
	roBotId, err := db_source.GetRobotIdByAppBizId(ctx, appBizId)
	if err != nil {
		log.ErrorContextf(ctx, "GetRobotIdByAppBizId err: %v", err)
		return err
	}
	metaMappings, err := dao.GetDocMetaDataDao().GetDocMetaDataByDocId(ctx, docBizId, roBotId)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocMetaDataByDocId err: %v", err)
		return err
	}
	dbTableSourceBizIds := make([]uint64, 0)

	for _, metaMapping := range metaMappings {
		dbTableSourceBizIds = append(dbTableSourceBizIds, metaMapping.BusinessID)
	}

	err = dao.GetDBTableDao().BatchSoftDeleteByDBSourceBizID(ctx, pkg.CorpBizID(ctx), appBizId, dbTableSourceBizIds)
	if err != nil {
		log.ErrorContextf(ctx, "BatchSoftDeleteByDBSourceBizID err: %v", err)
		return err
	}
	return nil
}

// DelText2SqlTablesByDocIdAndSheetName  解析干预 text2sql， 根据 docId， sheetName， 删除结构化数据用户注释
func DelText2SqlTablesByDocIdAndSheetName(ctx context.Context, appBizId, docBizId uint64, sheetName string) error {

	roBotId, err := db_source.GetRobotIdByAppBizId(ctx, appBizId)
	if err != nil {
		log.ErrorContextf(ctx, "GetRobotIdByAppBizId err: %v", err)
		return err
	}
	metaMappings, err := dao.GetDocMetaDataDao().GetDocMetaDataByDocId(ctx, docBizId, roBotId)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocMetaDataByDocId err: %v", err)
		return err
	}

	for _, metaMapping := range metaMappings {
		name, err := getSheetName(ctx, metaMapping)
		if err != nil {
			log.ErrorContextf(ctx, "getSheetName err: %v", err)
			return err
		}
		if name == sheetName {
			err := dao.GetDBTableDao().BatchSoftDeleteByDBSourceBizID(ctx, pkg.CorpBizID(ctx), appBizId, []uint64{metaMapping.BusinessID})
			if err != nil {
				log.ErrorContextf(ctx, "BatchSoftDeleteByDBSourceBizID err: %v", err)
				return err
			}
		}
	}
	return nil
}
