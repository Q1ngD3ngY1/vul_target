package document

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	dbEntity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

// 文档干预逻辑

const (
	blankStr       = ""
	batchSizeForDB = 500
	batchSizeForEs = 100
)

func (l *Logic) GetDocMetaDataByDocId(ctx context.Context, docBizId, roBotId uint64) (
	[]*docEntity.Text2sqlMetaMappingPreview, error) {
	docId, err := l.docDao.GetDocIdByDocBizId(ctx, docBizId, roBotId)
	if err != nil {
		return nil, err
	}
	return l.docDao.GetDocMetaDataByDocId(ctx, docId, roBotId)
}

func (l *Logic) PreviewText2sqlData(ctx context.Context, appBizId, docBizId uint64, pageNum, pageSize int) (
	[]*pb.Text2SqlPreviewSheet, error) {

	appDB, err := l.rpc.AppAdmin.DescribeAppById(ctx, appBizId)
	if err != nil {
		logx.E(ctx, "DescribeAppById err: %v", err)
		return nil, err
	}
	robotID := appDB.PrimaryId
	metaMappings, err := l.GetDocMetaDataByDocId(ctx, docBizId, robotID)
	if err != nil {
		logx.E(ctx, "GetDocMetaDataByDocId err: %v", err)
		return nil, err
	}

	var sheetList []*pb.Text2SqlPreviewSheet
	for _, metaMapping := range metaMappings {
		// 1. 判断 table 数据是否添加
		dbTable, err := l.GetSheetTableColumn(ctx, appBizId, metaMapping)
		if err != nil || dbTable == nil {
			logx.E(ctx, "GetSheetTableColumn err: %v", err)
			return nil, err
		}

		// 2. 获取数据
		lastID := (pageNum - 1) * pageSize
		col, rows, total, err := l.docDao.GetText2sqlDataPreview(ctx, metaMapping.DbName, metaMapping.MappingTableName,
			false, lastID, pageSize)
		if err != nil {
			return nil, err
		}

		sheetData := &pb.Text2SqlPreviewSheet{
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

func (l *Logic) GetSheetTableColumn(ctx context.Context, appBizId uint64,
	metaMapping *docEntity.Text2sqlMetaMappingPreview) (*dbEntity.Table, error) {
	logx.I(ctx, "GetSheetTableColumn| metaMapping BizID %v, DBName %v,  MappingTableName %v ",
		metaMapping.BusinessID, metaMapping.DbName, metaMapping.MappingTableName)
	corpBizId := contextx.Metadata(ctx).CorpBizID()
	tableFilter := dbEntity.TableFilter{
		CorpBizID:     corpBizId,
		AppBizID:      appBizId,
		DBSourceBizID: metaMapping.BusinessID,
	}
	dbTable, err := l.dbDao.DescribeTable(ctx, &tableFilter)
	// isExist, err := l.dbDao.Text2sqlExistsByDbSourceBizID(ctx, corpBizId, appBizId, metaMapping.BusinessID)
	if err != nil && !errors.Is(err, errx.ErrNotFound) {
		return nil, err
	}
	if dbTable != nil {
		return dbTable, nil
	}

	// 1. 获取数据库表信息
	tableInfo, err := l.docDao.GetText2sqlTableInfo(ctx, metaMapping.DbName, metaMapping.MappingTableName)
	if err != nil {
		logx.E(ctx, "GetText2sqlTableInfo err: %v", err)
		return nil, err
	}

	var data map[string]any
	err = json.Unmarshal([]byte(metaMapping.Mapping), &data)
	if err != nil {
		logx.E(ctx, "GetSheetTableColumn json.Unmarshal err: %v", err)
		return nil, err
	}

	tableName, ok := data["table_name"].(map[string]any)
	if !ok {
		return nil, err
	}
	rawValue, ok := tableName["raw"].(string)
	if !ok {
		return nil, err
	}

	timeNow := time.Now()
	dbTable = &dbEntity.Table{
		CorpBizID:         corpBizId,
		AppBizID:          appBizId,
		DBSourceBizID:     metaMapping.BusinessID,
		DBTableBizID:      idgen.GetId(),
		Source:            dbEntity.TableSourceDoc,
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
		ReleaseStatus: 0,
		NextAction:    releaseEntity.ReleaseActionAdd,
		Alive:         true,
		IsIndexed:     true,
		IsDeleted:     false,
		LastSyncTime:  timeNow,
		CreateTime:    timeNow,
		UpdateTime:    timeNow,
	}

	columns := make([]*dbEntity.Column, 0)
	for _, columnInfo := range tableInfo.ColumnInfo {
		dbTableColumn := &dbEntity.Column{
			CorpBizID:          dbTable.CorpBizID,
			AppBizID:           dbTable.AppBizID,
			DBTableBizID:       dbTable.DBTableBizID,
			DBTableColumnBizID: idgen.GetId(),
			ColumnName:         columnInfo.ColumnName,
			DataType:           columnInfo.DataType,
			ColumnComment:      columnInfo.ColComment,
			AliasName:          blankStr,
			Description:        blankStr,
			Unit:               blankStr,
			IsIndexed:          true,
			IsDeleted:          false,
			CreateTime:         time.Now(),
			UpdateTime:         time.Now(),
		}
		columns = append(columns, dbTableColumn)
	}
	dbTable.Columns = columns

	// 2. 将获取到的列数据，存入 db_table_column 表中
	err = l.dbDao.CreateColumnList(ctx, dbTable.Columns)
	// err = dao.GetDBTableColumnDao().AddColumns(ctx, tableInfo, dbTable, getDao)
	if err != nil {
		logx.E(ctx, "add db table columns failed: %v", err)
		return nil, err
	}

	// 3. 批量创建数据库表
	err = l.dbDao.CreateTableList(ctx, []*dbEntity.Table{dbTable})
	// err = dao.GetDBTableDao().BatchCreate(ctx, []*dbEntity.Table{dbTable})
	if err != nil {
		logx.E(ctx, "create db table failed: %v", err)
		return nil, err
	}
	return dbTable, nil
}

// AddSheetTableData2es1 将text2sql数据库中的数据，添加到es1中
func (l *Logic) AddSheetTableData2es1(ctx context.Context, robotId, appBizId uint64,
	metaMapping *docEntity.Text2sqlMetaMappingPreview) error {
	logx.I(ctx, "AddSheetTableData2es1|metaMapping: %v", metaMapping)

	corpBizID := contextx.Metadata(ctx).CorpBizID()
	tableFilter := dbEntity.TableFilter{
		CorpBizID:     corpBizID,
		AppBizID:      appBizId,
		DBSourceBizID: metaMapping.BusinessID,
	}
	dbTable, err := l.dbDao.DescribeTable(ctx, &tableFilter)
	// dbTable, err := l.dbDao.Text2sqlGetByDbSourceBizID(ctx, corpBizID, appBizId, metaMapping.BusinessID)
	if err != nil {
		logx.E(ctx, "get db table by biz id failed, %v", err)
		return err
	}

	// 更新 table 的同步时间 和 更新时间， 确保两者一致，用于特别判断 解析干预 中 结构化数据 用户自定义是否发生了变更。
	timeNow := time.Now()
	dbTable.LastSyncTime = timeNow
	dbTable.UpdateTime = timeNow
	tableFilter = dbEntity.TableFilter{
		CorpBizID:    corpBizID,
		AppBizID:     appBizId,
		DBTableBizID: dbTable.DBTableBizID,
	}
	err = l.dbDao.ModifyTable(ctx, &tableFilter, map[string]any{
		"last_sync_time": dbTable.LastSyncTime,
		"update_time":    dbTable.UpdateTime,
	})
	// err = dao.GetDBTableDao().UpdateByBizID(ctx, corpBizID, dbTable.AppBizID, dbTable.DBTableBizID, []string{"last_sync_time", "update_time"}, dbTable)
	if err != nil {
		logx.E(ctx, "AddSheetTableData2es1|update db table failed, %v", err)
		return err
	}

	columnFilter := dbEntity.ColumnFilter{
		CorpBizID:    corpBizID,
		AppBizID:     appBizId,
		DBTableBizID: dbTable.DBTableBizID,
	}
	columns, _, err := l.dbDao.DescribeColumnList(ctx, &columnFilter)
	// columns, err := dao.GetDBTableColumnDao().GetByTableBizID(ctx, corpBizID, appBizId, dbTable.DBTableBizID)
	if err != nil {
		logx.E(ctx, "get db table columns by table biz id failed, %v", err)
		return err
	}

	maxId, err := l.docDao.GetText2sqlMaxId(ctx, metaMapping.DbName, metaMapping.MappingTableName)
	if err != nil {
		logx.E(ctx, "get text2sql max id failed, %v", err)
		return err
	}

	batchSize := batchSizeForDB
	var colNames []string
	var rows []*pb.RowData
	var total int64
	for lastId := 0; lastId <= maxId; lastId += batchSize {
		colNames, rows, total, err = l.docDao.GetText2sqlDataPreview(ctx, metaMapping.DbName,
			metaMapping.MappingTableName, true, lastId, batchSize)
		if err != nil {
			logx.E(ctx, "get db table preview data failed, %v", err)
			return err
		}

		if total == 0 {
			logx.W(ctx, "get db table preview data total is 0")
			break
		}

		for _, batchRows := range slicex.Chunk(rows, batchSizeForEs) {
			err = updateText2sqlEs(ctx, dbTable, columns, colNames, batchRows, robotId)
			if err != nil {
				logx.E(ctx, "update text2sql es failed, %v", err)
				return err
			}
		}

		if len(rows) < batchSize {
			break
		}
	}

	return nil
}

func updateText2sqlEs(ctx context.Context, dbTable *dbEntity.Table, columns []*dbEntity.Column,
	colNames []string, rows []*pb.RowData, robotId uint64) error {
	logx.I(ctx, "PreviewText2sqlData preview: col %v", colNames)
	colName2Model := make(map[string]*dbEntity.Column)
	for _, column := range columns {
		colName2Model[column.ColumnName] = column
	}

	dbRowData := make([]*retrieval.DBRowData, 0)

	// 获取配置的单元格值最大长度
	maxCellValueLength := config.App().DbSource.EsCellValueMaxLength

	for rowIdx, row := range rows {
		dbCells := make([]*retrieval.DBCell, 0)
		for idx, name := range colNames {
			col := colName2Model[name]
			if col == nil {
				logx.E(ctx, "colName2Model not found col, %v, table biz id %v", name, dbTable)
				col = &dbEntity.Column{}
			}
			colDesc := col.ColumnComment + "|" + col.Description + "|" + col.DataType + "|" + col.Unit
			var cell *retrieval.DBCell

			// 截断单元格值到配置的最大长度
			cellValue := row.Values[idx]
			if maxCellValueLength > 0 && len(cellValue) > maxCellValueLength {
				cellValue = cellValue[:maxCellValueLength]
			}

			if rowIdx == 0 {
				cell = &retrieval.DBCell{
					ColumnName:      name,
					ColumnAliasName: col.AliasName,
					ColumnDesc:      colDesc,
					DataType:        col.DataType,
					Value:           cellValue,
				}
			} else {
				cell = &retrieval.DBCell{
					Value: cellValue,
				}
			}
			dbCells = append(dbCells, cell)
		}
		dbRowData = append(dbRowData, &retrieval.DBRowData{
			Cells: dbCells,
		})
	}

	// 表描述信息，格式：表名｜表注释｜表别名｜表描述
	tableDesc := dbTable.Name + "|" + dbTable.TableComment + "|" + dbTable.AliasName + "|" + dbTable.Description

	req := &retrieval.AddDBText2SQLReq{
		RobotId:       robotId,
		DbSourceBizId: dbTable.DBSourceBizID,
		DbType:        retrieval.DBType_Text_To_SQL,
		DbDesc:        "",
		DbTableBizId:  dbTable.DBTableBizID,
		TableDesc:     tableDesc,
		Rows:          dbRowData,
		EnvType:       retrieval.EnvType_Test,
	}
	// 文档写入es1数据是增量，标签数据不变，不需要传
	logx.I(ctx,
		"AddSheetTableData2es1|DbSourceBizId: %v,DbType:%v, DbTableBizId: %v, EnvType: %v, TableDesc: %v",
		dbTable.DBSourceBizID, req.DbType, dbTable.DBTableBizID, req.EnvType, tableDesc)

	_, err := retrieval.NewDirectIndexClientProxy().AddDBText2SQL(ctx, req)
	if err != nil {
		logx.E(ctx, "add db text2sql to es1 failed, %v", err)
		return err
	}
	return nil
}

// UpdateSheet2Knowledge 解析干预 text2sql， 更新结构化数据注释后，更新 es 数据
func (l *Logic) UpdateSheet2Knowledge(ctx context.Context, appBizId, docBizId uint64) error {
	appDB, err := l.rpc.AppAdmin.DescribeAppById(ctx, appBizId)
	if err != nil {
		logx.E(ctx, "DescribeAppById err: %v", err)
		return err
	}
	robotID := appDB.PrimaryId
	metaMappings, err := l.docDao.GetDocMetaDataByDocId(ctx, docBizId, robotID)
	if err != nil {
		logx.E(ctx, "GetDocMetaDataByDocId err: %v", err)
		return err
	}

	for _, metaMapping := range metaMappings {
		err = l.AddSheetTableData2es1(ctx, robotID, appBizId, metaMapping)
		if err != nil {
			logx.E(ctx, "AddSheetTableData2es1 err: %v", err)
			return err
		}
	}
	return nil
}

// DelText2SqlTablesByDocId 解析干预 text2sql， 根据 docId， 批量删除用户注释
func (l *Logic) DelText2SqlTablesByDocId(ctx context.Context, appBizId, docBizId uint64) error {
	appDB, err := l.rpc.AppAdmin.DescribeAppById(ctx, appBizId)
	if err != nil {
		logx.E(ctx, "DescribeAppById err: %v", err)
		return err
	}
	robotID := appDB.PrimaryId
	metaMappings, err := l.GetDocMetaDataByDocId(ctx, docBizId, robotID)
	if err != nil {
		logx.E(ctx, "GetDocMetaDataByDocId err: %v", err)
		return err
	}
	dbTableSourceBizIds := make([]uint64, 0)

	for _, metaMapping := range metaMappings {
		dbTableSourceBizIds = append(dbTableSourceBizIds, metaMapping.BusinessID)
	}

	err = l.dbDao.BatchSoftDeleteByDBSourceBizID(ctx, contextx.Metadata(ctx).CorpBizID(), appBizId, dbTableSourceBizIds)
	if err != nil {
		logx.E(ctx, "BatchSoftDeleteByDBSourceBizID err: %v", err)
		return err
	}
	return nil
}

func (l *Logic) GetDocMetaDataForSchema(ctx context.Context, docId, robotId uint64, scenes uint32) (
	bool, []docEntity.Text2sqlMetaMappingPreview, error) {
	return l.docDao.GetDocMetaDataForSchema(ctx, docId, robotId, scenes)
}

// CreateDocParsingIntervention 创建解析干预任务
func (l *Logic) CreateDocParsingIntervention(ctx context.Context,
	docCommon *segEntity.DocSegmentCommon, auditFlag uint32, doc *docEntity.Doc) (
	*pb.CreateDocParsingInterventionRsp, error) {
	rsp := new(pb.CreateDocParsingInterventionRsp)
	logx.I(ctx, "CreateDocParsingIntervention|start")
	// 老文档状态等字段更新
	if err := l.UpdateOldDocStatus(ctx, auditFlag, doc); err != nil {
		logx.E(ctx, "CreateDocParsingIntervention|UpdateDocStatus|err:%+v", err)
		return rsp, err
	}
	// 减少已使用的容量
	err := l.financeLogic.UpdateAppCapacityUsage(ctx, entity.CapacityUsage{
		CharSize:          -int64(doc.CharSize),
		StorageCapacity:   gox.IfElse(doc.Source == docEntity.SourceFromCorpCOSDoc, 0, -int64(doc.FileSize)),
		ComputeCapacity:   -int64(doc.FileSize),
		KnowledgeCapacity: -int64(doc.FileSize),
	}, doc.RobotID, doc.CorpID)
	if err != nil {
		logx.E(ctx, "CreateDocParsingIntervention|UpdateAppUsedCharSize|err:%+v", err)
		return rsp, err
	}

	// 触发异步任务
	taskID := idgen.GetId()
	if err := scheduler.NewDocSegInterveneTask(ctx, docCommon.AppID, entity.DocSegInterveneParams{
		CorpID:         docCommon.CorpID,
		CorpBizID:      docCommon.CorpBizID,
		StaffID:        docCommon.StaffID,
		StaffBizID:     docCommon.StaffBizID,
		AppBizID:       docCommon.AppBizID,
		AppID:          docCommon.AppID,
		OriginDocBizID: docCommon.DocBizID,
		TaskID:         taskID,
		FileType:       doc.FileType,
		FileName:       doc.GetDocFileName(),
		SourceEnvSet:   contextx.Metadata(ctx).EnvSet(),
		DataSource:     docCommon.DataSource,
	}); err != nil {
		logx.E(ctx, "CreateDocParsingIntervention|NewDocSegInterveneTask|err:%+v", err)
		return rsp, err
	}
	logx.I(ctx, "CreateDocParsingIntervention|scheduler task running|taskID:%d", taskID)
	return rsp, nil
}
