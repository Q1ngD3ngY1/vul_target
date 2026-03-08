package dao

import (
	"context"
	"database/sql"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

const (
	metaMappingPreviewTable = "t_text2sql_meta_mapping_preview"
	metaMappingProdTable    = "t_text2sql_meta_mapping_prod"
)

var globalDocMetaDataDao *DocMetaDataDao

type DocMetaDataDao struct {
	BaseDao
}

func GetDocMetaDataDao() *DocMetaDataDao {
	if globalDocMetaDataDao == nil {
		globalDocMetaDataDao = &DocMetaDataDao{*globalBaseDao}
	}
	return globalDocMetaDataDao
}

func (d *DocMetaDataDao) GetDocMetaDataByDocId(ctx context.Context, docBizId, robotId uint64) ([]model.Text2sqlMetaMappingPreview, error) {
	var docId uint64
	log.InfoContextf(ctx, "GetDocMetaDataByDocId docBizId: %d, robotId: %d", docBizId, robotId)
	err := d.gormDB.WithContext(ctx).Table("t_doc").
		Where("business_id = ? and robot_id = ? AND is_deleted = 0", docBizId, robotId).
		Pluck("id", &docId).
		Error
	if err != nil {
		log.ErrorContextf(ctx, "GetDocMetaDataByDocId err: %v", err)
		return nil, err
	}

	var records []model.Text2sqlMetaMappingPreview
	err = d.gormDB.WithContext(ctx).Where("robot_id = ? and doc_id = ? AND is_deleted = 0", robotId, docId).Find(&records).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetDocMetaDataByDocId err: %v", err)
		return nil, err
	}
	return records, nil
}

func (d *DocMetaDataDao) GetText2sqlMaxId(ctx context.Context, dbName, dbTableName string) (int, error) {
	log.InfoContextf(ctx, "GetText2sqlMaxId dbName: %s, dbTableName: %s", dbName, dbTableName)
	tableName := dbName + "." + dbTableName
	var maxId int
	err := d.text2sqlGormDB.WithContext(ctx).Table(tableName).Select("max(`@id`)").Row().Scan(&maxId)
	if err != nil {
		log.ErrorContextf(ctx, "GetText2sqlMaxId err: %v", err)
		return 0, err
	}
	return maxId, nil
}

// GetText2sqlDataPreview 获取文本转sql预览数据， fetchFull 为true时，返回全量数据，否则只返回预览数据
func (d *DocMetaDataDao) GetText2sqlDataPreview(ctx context.Context, dbName, dbTableName string, fetchFull bool, lastId, pageSize int) ([]string, []*pb.RowData, int64, error) {
	log.InfoContextf(ctx, "GetText2sqlDataPreview dbName: %s, dbTableName: %s， lastId: %d, pageSize: %d", dbName, dbTableName, lastId, pageSize)
	tableName := dbName + "." + dbTableName
	// 1. 获取原始数据
	query := "SELECT * FROM " + tableName + " WHERE `@id` > ? ORDER BY `@id` ASC LIMIT ?"
	rows, err := d.text2sqlGormDB.WithContext(ctx).Raw(query, lastId, pageSize).Rows()
	if err != nil {
		log.ErrorContextf(ctx, "GetText2sqlDataPreview err: %v", err)
		return nil, nil, 0, err
	}
	defer func(rows *sql.Rows) {
		closeErr := rows.Close()
		if closeErr != nil {
			log.ErrorContextf(ctx, "rows.Close err: %v", closeErr)
		}
	}(rows)

	// 2.获取列名
	columns, err := rows.Columns()
	if err != nil {
		log.ErrorContextf(ctx, "rows.Columns err: %v", err)
		return nil, nil, 0, err
	}

	if len(columns) < 2 {
		log.ErrorContextf(ctx, "text2sql rows.Columns is invalid: %v, ", columns)
		return nil, nil, 0, fmt.Errorf("text2sql rows.Columns is invalid")
	}

	var results []*pb.RowData

	// 3. 获取表的总行数
	var totalRows int64
	if err := d.text2sqlGormDB.WithContext(ctx).Table(tableName).Count(&totalRows).Error; err != nil {
		log.ErrorContextf(ctx, "获取表总行数失败: %v", err)
		return nil, nil, 0, fmt.Errorf("获取表总行数失败: %w", err)
	}

	// 4. 解析数据行
	for rows.Next() {
		rowData := make([]interface{}, len(columns))
		for i := range rowData {
			rowData[i] = new(sql.NullString)
		}

		if err = rows.Scan(rowData...); err != nil {
			log.ErrorContextf(ctx, "解析数据行失败: %v", err)
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
		log.ErrorContextf(ctx, "遍历数据行失败: %v", err)
		return nil, nil, 0, fmt.Errorf("遍历数据行失败: %w", err)
	}

	// text2sql 前两列无需展示
	if !fetchFull {
		columns = columns[2:]
	}

	return columns, results, totalRows, nil
}

// GetText2sqlTableInfo 获取 text2sql 数据表的完整信息，包括列名、数据类型、行数和列数
func (d *DocMetaDataDao) GetText2sqlTableInfo(ctx context.Context, dbName, dbTableName string) (*model.TableInfo, error) {
	log.InfoContextf(ctx, "GetText2sqlTableInfo dbName: %s, dbTableName: %s", dbName, dbTableName)
	// 1. 获取列信息
	rows, err := d.text2sqlGormDB.WithContext(ctx).Raw("SELECT COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", dbName, dbTableName).Rows()
	if err != nil {
		log.ErrorContextf(ctx, "获取列信息失败: %v", err)
		return nil, fmt.Errorf("获取列信息失败: %w", err)
	}

	defer func(rows *sql.Rows) {
		closeErr := rows.Close()
		if closeErr != nil {
			log.ErrorContextf(ctx, "关闭列名查询结果失败: %v", closeErr)
		}
	}(rows)

	columnInfo := make([]*model.ColumnInfo, 0)
	// 解析列信息
	for rows.Next() {
		var (
			field    string
			typeInfo string
			comment  sql.NullString
		)
		err = rows.Scan(&field, &typeInfo, &comment)
		if err != nil {
			log.ErrorContextf(ctx, "解析列信息失败: %v", err)
			return nil, fmt.Errorf("解析列信息失败: %w", err)
		}
		columnInfo = append(columnInfo, &model.ColumnInfo{
			ColumnName: field,
			DataType:   typeInfo,
			ColComment: comment.String,
		})
	}

	if err = rows.Err(); err != nil {
		log.ErrorContextf(ctx, "遍历列信息失败: %v", err)
		return nil, fmt.Errorf("遍历列信息失败: %w", err)
	}

	// 3.获取行数,  获取表注解
	var (
		rowCount     int64
		tableComment sql.NullString
	)

	err = d.text2sqlGormDB.Table("INFORMATION_SCHEMA.TABLES").
		Select("TABLE_ROWS, TABLE_COMMENT").
		Where("TABLE_SCHEMA = ? AND TABLE_NAME = ?", dbName, dbTableName).
		Row().Scan(&rowCount, &tableComment)

	if err != nil {
		log.ErrorContextf(ctx, "获取行数失败: %v", err)
		return nil, fmt.Errorf("获取行数失败: %w", err)
	}

	columnCount := len(columnInfo)

	return &model.TableInfo{
		ColumnInfo:   columnInfo,
		RowCount:     rowCount,
		ColumnCount:  columnCount,
		TableComment: tableComment.String,
	}, nil
}

// GetDocMetaDataForSchema 获取表格文件的元数据，返回所有结构化sheet页的信息
func (d *DocMetaDataDao) GetDocMetaDataForSchema(ctx context.Context, docId, robotId uint64, scenes uint32) (
	bool, []model.Text2sqlMetaMappingPreview, error) {
	tableName := metaMappingPreviewTable
	if scenes == model.RunEnvPRODUCT {
		tableName = metaMappingProdTable
	}
	cols := []string{"business_id", "table_id", "file_name", "mapping"}
	var records []model.Text2sqlMetaMappingPreview
	err := d.gormDB.WithContext(ctx).Table(tableName).Select(cols).
		Where("robot_id = ? and doc_id = ?", robotId, docId).Find(&records).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetDocMetaDataForSchema err: %v", err)
		return false, records, err
	}
	if len(records) == 0 {
		return false, records, nil
	}
	return true, records, nil
}
