package document

import (
	"context"
	"database/sql"
	"fmt"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/database"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

func (d *daoImpl) GetDocIdByDocBizId(ctx context.Context, docBizId, robotId uint64) (uint64, error) {
	var docId uint64
	tbl := d.mysql.TDoc
	tblName := tbl.TableName()

	logx.I(ctx, "GetDocIdByDocBizId docBizId: %d, robotId: %d", docBizId, robotId)
	err := tbl.WithContext(ctx).UnderlyingDB().Table(tblName).
		Where("business_id = ? and robot_id = ? AND is_deleted = 0", docBizId, robotId).
		Pluck("id", &docId).
		Error
	if err != nil {
		logx.E(ctx, "GetDocIdByDocBizId err: %v", err)
		return 0, err
	}
	return docId, nil
}

func (d *daoImpl) GetDocMetaDataByDocId(ctx context.Context, docId, robotId uint64) (
	[]*docEntity.Text2sqlMetaMappingPreview, error) {
	// var docId uint64
	tbl := d.mysql.TText2sqlMetaMappingPreview
	tblName := tbl.TableName()

	var records []*model.TText2sqlMetaMappingPreview
	err := tbl.WithContext(ctx).UnderlyingDB().Table(tblName).
		Where("robot_id = ? and doc_id = ? AND is_deleted = 0", robotId, docId).
		Find(&records).Error
	if err != nil {
		logx.E(ctx, "GetDocMetaDataByDocId err: %v", err)
		return nil, err
	}

	return BatchConvertText2sqlMetaMappingPO2DO(records), nil
}

func (d *daoImpl) GetText2sqlMaxId(ctx context.Context, dbName, dbTableName string) (int, error) {
	logx.I(ctx, "GetText2sqlMaxId dbName: %s, dbTableName: %s", dbName, dbTableName)
	tableName := dbName + "." + dbTableName
	var maxId int
	db := d.text2sqlDB
	err := db.Table(tableName).Select("max(`@id`)").Row().Scan(&maxId)
	if err != nil {
		logx.E(ctx, "GetText2sqlMaxId err: %v", err)
		return 0, err
	}
	return maxId, nil
}

// GetText2sqlDataPreview 获取文本转sql预览数据， fetchFull 为true时，返回全量数据，否则只返回预览数据
func (d *daoImpl) GetText2sqlDataPreview(ctx context.Context, dbName, dbTableName string, fetchFull bool, lastId, pageSize int) ([]string, []*pb.RowData, int64, error) {
	logx.I(ctx, "GetText2sqlDataPreview dbName: %s, dbTableName: %s， lastId: %d, pageSize: %d",
		dbName, dbTableName, lastId, pageSize)
	tableName := dbName + "." + dbTableName
	db := d.text2sqlDB.Table(tableName)
	// 1. 获取原始数据
	query := "SELECT * FROM " + tableName + " WHERE `@id` > ? ORDER BY `@id` ASC LIMIT ?"
	rows, err := db.Raw(query, lastId, pageSize).Rows()
	if err != nil {
		logx.E(ctx, "GetText2sqlDataPreview err: %v", err)
		return nil, nil, 0, err
	}
	defer func(rows *sql.Rows) {
		closeErr := rows.Close()
		if closeErr != nil {
			logx.E(ctx, "rows.Close err: %v", closeErr)
		}
	}(rows)

	// 2.获取列名
	columns, err := rows.Columns()
	if err != nil {
		logx.E(ctx, "rows.Columns err: %v", err)
		return nil, nil, 0, err
	}

	if len(columns) < 2 {
		logx.E(ctx, "text2sql rows.Columns is invalid: %v, ", columns)
		return nil, nil, 0, fmt.Errorf("text2sql rows.Columns is invalid")
	}

	var results []*pb.RowData

	// 3. 获取表的总行数
	var totalRows int64
	if err := d.text2sqlDB.Table(tableName).Count(&totalRows).Error; err != nil {
		logx.E(ctx, "获取表总行数失败: %v", err)
		return nil, nil, 0, fmt.Errorf("获取表总行数失败: %w", err)
	}

	// 4. 解析数据行
	for rows.Next() {
		rowData := make([]any, len(columns))
		for i := range rowData {
			rowData[i] = new(sql.NullString)
		}

		if err = rows.Scan(rowData...); err != nil {
			logx.E(ctx, "解析数据行失败: %v", err)
			return nil, nil, 0, fmt.Errorf("解析数据行失败: %w", err)
		}

		row := &pb.RowData{
			Values: make([]string, len(columns)),
		}

		for i, val := range rowData {
			if v, ok := val.(*sql.NullString); ok && v.Valid {
				row.Values[i] = v.String
			} else {
				row.Values[i] = ""
			}
		}

		// text2sql 前两列无需展示
		if !fetchFull {
			row.Values = append([]string{}, row.Values[2:]...)
		}
		results = append(results, row)
	}

	if err = rows.Err(); err != nil {
		logx.E(ctx, "遍历数据行失败: %v", err)
		return nil, nil, 0, fmt.Errorf("遍历数据行失败: %w", err)
	}

	// text2sql 前两列无需展示
	if !fetchFull {
		columns = columns[2:]
	}

	return columns, results, totalRows, nil
}

// GetText2sqlTableInfo 获取 text2sql 数据表的完整信息，包括列名、数据类型、行数和列数
func (d *daoImpl) GetText2sqlTableInfo(ctx context.Context, dbName, dbTableName string) (*database.TableInfo, error) {
	logx.I(ctx, "GetText2sqlTableInfo dbName: %s, dbTableName: %s", dbName, dbTableName)
	db := d.text2sqlDB
	// 1. 获取列信息
	rows, err := db.Raw(
		"SELECT COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ?"+
			" AND TABLE_NAME = ?",
		dbName, dbTableName).Rows()
	if err != nil {
		logx.E(ctx, "Get Column metadata error: %v", err)
		return nil, fmt.Errorf("Get Column metadata error: %w", err)
	}

	defer func(rows *sql.Rows) {
		closeErr := rows.Close()
		if closeErr != nil {
			logx.E(ctx, "关闭列名查询结果失败: %v", closeErr)
		}
	}(rows)

	columnInfo := make([]*database.ColumnInfo, 0)
	// 解析列信息
	for rows.Next() {
		var (
			field    string
			typeInfo string
			comment  sql.NullString
		)
		err = rows.Scan(&field, &typeInfo, &comment)
		if err != nil {
			logx.E(ctx, "Parse Column Information error: %v", err)
			return nil, fmt.Errorf("Parse Column Information error: %w", err)
		}
		columnInfo = append(columnInfo, &database.ColumnInfo{
			ColumnName: field,
			DataType:   typeInfo,
			ColComment: comment.String,
		})
	}

	if err = rows.Err(); err != nil {
		logx.E(ctx, "Iterate columns info error: %v", err)
		return nil, fmt.Errorf("Iterate columns info error: %w", err)
	}

	// 3.获取行数,  获取表注解
	var (
		rowCount     int64
		tableComment sql.NullString
	)

	// tdsqlDB := d.Query().WithContext(ctx).TText2sqlMetaMappingPreview.UnderlyingDB()

	err = d.text2sqlDB.Table("INFORMATION_SCHEMA.TABLES").
		Select("TABLE_ROWS, TABLE_COMMENT").
		Where("TABLE_SCHEMA = ? AND TABLE_NAME = ?", dbName, dbTableName).
		Row().Scan(&rowCount, &tableComment)

	if err != nil {
		logx.E(ctx, "GetRowNum error: %v", err)
		return nil, fmt.Errorf("GetRowNum error: %w", err)
	}

	columnCount := len(columnInfo)

	return &database.TableInfo{
		ColumnInfo:   columnInfo,
		RowCount:     rowCount,
		ColumnCount:  columnCount,
		TableComment: tableComment.String,
	}, nil
}

// GetDocMetaDataForSchema 获取表格文件的元数据，返回所有结构化sheet页的信息
func (d *daoImpl) GetDocMetaDataForSchema(ctx context.Context, docId, robotId uint64, scenes uint32) (
	bool, []docEntity.Text2sqlMetaMappingPreview, error) {
	tableName := d.mysql.TText2sqlMetaMappingPreview.TableName()
	if scenes == entity.RunEnvPRODUCT {
		tableName = d.mysql.TText2sqlMetaMappingProd.TableName()
	}
	tbl := d.mysql.TText2sqlMetaMappingPreview

	cols := []string{tbl.BusinessID.ColumnName().String(), tbl.TableID.ColumnName().String(),
		tbl.FileName.ColumnName().String(), tbl.Mapping.ColumnName().String()}
	var records []docEntity.Text2sqlMetaMappingPreview
	err := tbl.WithContext(ctx).UnderlyingDB().Table(tableName).Select(cols).
		Where("robot_id = ? and doc_id = ?", robotId, docId).Find(&records).Error
	if err != nil {
		logx.E(ctx, "GetDocMetaDataForSchema err: %v", err)
		return false, records, err
	}
	if len(records) == 0 {
		return false, records, nil
	}
	return true, records, nil
}
