package dao

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
)

var (
	mysqlTopNDataType = map[string]struct{}{
		`varchar`: {},
		`text`:    {},
		`char`:    {},
	}
	sqlServerTopNDataType = map[string]struct{}{
		"varchar":  {},
		"nvarchar": {},
		"char":     {},
		"nchar":    {},
		"text":     {},
		"ntext":    {},
	}
)

// GetTopNValue 获取某个表的某个列的topN值
func (r *DBSourceDao) GetTopNValue(ctx context.Context, dbSource *model.DBSource, robotId, dbTableBizID,
	embeddingVersion uint64, getDao Dao) error {
	log.DebugContextf(ctx, "GetTopNValue|开始获取topN值: dbSource:%v, dbtableBizId:%v", dbSource, dbTableBizID)
	if dbSource == nil {
		log.WarnContextf(ctx, "GetTopNValue|dbSource is nil or dbType is not mysql: %v", dbSource)
		return errs.ErrAddDbSourceFail
	}
	dbTable, err := GetDBTableDao().GetByBizID(ctx, dbSource.CorpBizID, dbSource.AppBizID, dbTableBizID)
	if err != nil {
		log.ErrorContextf(ctx, "GetTopNValue|获取表信息失败: %v", err)
		return err
	}
	if dbTable.RowCount > CountMaxLimit {
		log.WarnContextf(ctx, "GetTopNValue|rowCount:%v 超过限制: %v", dbTable.RowCount, CountMaxLimit)
		return nil
	}
	decrypt, err := util.Decrypt(dbSource.Password)
	if err != nil {
		return err
	}
	connDbSource := model.ConnDbSource{
		DbType:   dbSource.DBType,
		Host:     dbSource.Host,
		DbName:   dbSource.DBName,
		Username: dbSource.Username,
		Password: decrypt,
		Port:     dbSource.Port,
	}
	dbConn, err := r.GetDBConnection(ctx, connDbSource)
	if err != nil {
		log.WarnContextf(ctx, "GetTopNValue|获取数据库连接失败: %v", err)
		return err
	}
	defer func() {
		closeErr := dbConn.Close()
		if closeErr != nil {
			log.WarnContextf(ctx, "GetTopNValue|关闭数据库连接失败: %v", closeErr)
		}
	}()

	dbTableColumns, err := GetDBTableColumnDao().GetByTableBizID(ctx, dbSource.CorpBizID, dbSource.AppBizID, dbTableBizID)
	if err != nil {
		log.ErrorContextf(ctx, "GetTopNValue|获取表列信息失败: %v", err)
		return err
	}

	for _, dbTableColumn := range dbTableColumns {
		log.DebugContextf(ctx, "GetTopNValue|处理列: %v", dbTableColumn)
		var sqlQuery string
		if dbSource.DBType == DbTypeMysql {
			sqlQuery = fmt.Sprintf(
				"SELECT %s, COUNT(*) as frequency FROM %s WHERE %s IS NOT NULL GROUP BY %s ORDER BY frequency DESC LIMIT %v",
				dbTableColumn.ColumnName, dbTable.Name, dbTableColumn.ColumnName, dbTableColumn.ColumnName, config.App().
					DbSource.ValueLinkConfig.MaxTopValueNum)
			if _, ok := mysqlTopNDataType[dbTableColumn.DataType]; !ok {
				continue
			}
		}
		if dbSource.DBType == DbTypeSqlserver {
			sqlQuery = fmt.Sprintf(
				"SELECT TOP %d [%s], COUNT(*) as frequency FROM [%s] WHERE [%s] IS NOT NULL GROUP BY [%s] ORDER BY frequency DESC",
				config.App().DbSource.ValueLinkConfig.MaxTopValueNum, dbTableColumn.ColumnName, dbTable.Name,
				dbTableColumn.ColumnName, dbTableColumn.ColumnName,
			)
			if _, ok := sqlServerTopNDataType[dbTableColumn.DataType]; !ok {
				continue
			}
		}
		rows, err := dbConn.QueryContext(ctx, sqlQuery)
		if err != nil {
			log.WarnContextf(ctx, "GetTopNValue|获取topN值失败: %v", err)
			return err
		}
		topValue := make([]string, 0, config.App().DbSource.ValueLinkConfig.MaxTopValueNum)
		var dbTableTopValues []*model.DbTableTopValue
		for rows.Next() {
			var value string
			var frequency int
			err = rows.Scan(&value, &frequency)
			if err != nil {
				log.WarnContextf(ctx, "GetTopNValue|获取topN值失败: %v", err)
				return err
			}
			topValue = append(topValue, value)
			dbTableTopValue := &model.DbTableTopValue{
				CorpBizID:          dbTableColumn.CorpBizID,
				AppBizID:           dbTableColumn.AppBizID,
				DBSourceBizID:      dbTable.DBSourceBizID,
				DbTableBizID:       dbTableColumn.DBTableBizID,
				DbTableColumnBizID: dbTableColumn.DBTableColumnBizID,
				BusinessID:         getDao.GenerateSeqID(),
				ColumnName:         dbTableColumn.ColumnName,
				ColumnValue:        value,
				ColumnComment:      dbTableColumn.ColumnComment,
				IsDeleted:          false,
				CreateTime:         time.Now(),
				UpdateTime:         time.Now(),
			}
			dbTableTopValues = append(dbTableTopValues, dbTableTopValue)
			content := fmt.Sprintf("%v;%v;%v", dbTableColumn.ColumnName, dbTableColumn.ColumnComment, value)
			err = r.AddVdb(ctx, robotId, dbTableColumn.AppBizID, dbTableTopValue.BusinessID, embeddingVersion, content,
				retrieval.EnvType_Test)
			if err != nil {
				log.ErrorContextf(ctx, "GetTopNValue|input vdb err: %v", err)
				return err
			}
		}

		err = r.CreateTopValue(ctx, dbTableTopValues)
		if err != nil {
			log.ErrorContextf(ctx, "GetTopNValue| value:%v, 新增topN值失败: %v", dbTableTopValues, err)
			return err
		}
	}
	return nil
}

// GetTopNValueV2 TODO
func (r *DBSourceDao) GetTopNValueV2(ctx context.Context, dbSource *model.DBSource, robotId, dbTableBizID,
	embeddingVersion uint64, getDao Dao) error {
	log.DebugContextf(ctx, "GetTopNValueV2|开始获取topN值: dbSource:%v, dbtableBizId:%v", dbSource, dbTableBizID)
	t0 := time.Now()
	if dbSource == nil {
		return errs.ErrAddDbSourceFail
	}
	dbTable, err := GetDBTableDao().GetByBizID(ctx, dbSource.CorpBizID, dbSource.AppBizID, dbTableBizID)
	if err != nil {
		return err
	}
	maxCowSize := min(int(dbTable.RowCount), config.App().DbSource.ValueLinkConfig.MaxTraverseRow)
	decrypt, err := util.Decrypt(dbSource.Password)
	if err != nil {
		return err
	}
	connDbSource := model.ConnDbSource{
		DbType:   dbSource.DBType,
		Host:     dbSource.Host,
		DbName:   dbSource.DBName,
		Username: dbSource.Username,
		Password: decrypt,
		Port:     dbSource.Port,
	}
	dbConn, err := r.GetDBConnection(ctx, connDbSource)
	if err != nil {
		log.WarnContextf(ctx, "GetTopNValue|获取数据库连接失败: %v", err)
		return err
	}
	defer func() {
		closeErr := dbConn.Close()
		if closeErr != nil {
			log.WarnContextf(ctx, "GetTopNValue|关闭数据库连接失败: %v", closeErr)
		}
	}()

	dbTableColumns, err := GetDBTableColumnDao().GetByTableBizID(ctx, dbSource.CorpBizID, dbSource.AppBizID, dbTableBizID)
	if err != nil {
		log.ErrorContextf(ctx, "GetTopNValue|获取表列信息失败: %v", err)
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, config.App().DbSource.ValueLinkTimeout)
	defer cancel()

	// 获取数据表的主键列名
	var primaryKeys []string
	var sqlQuery string
	switch connDbSource.DbType {
	case DbTypeMysql:
		primaryKeys, _ = getMySQLPrimaryKeys(ctx, dbConn, connDbSource.DbName, dbTable.Name)
	case DbTypeSqlserver:
		primaryKeys, _ = getSQLServerPrimaryKeys(ctx, dbConn, dbTable.Name)
	default:
		return errs.ErrDbSourceTypeNotSupport
	}

	primaryColumn := dbTableColumns[0].ColumnName
	if len(primaryKeys) > 0 {
		primaryColumn = primaryKeys[0]
	}
	selectedColumns := make([]string, 0, len(dbTableColumns))
	selectedColumnForQuery := make([]string, 0, len(dbTableColumns))
	for _, col := range dbTableColumns {
		if connDbSource.DbType == DbTypeMysql {
			if _, ok := mysqlTopNDataType[col.DataType]; ok {
				selectedColumns = append(selectedColumns, col.ColumnName)
				selectedColumnForQuery = append(selectedColumnForQuery, fmt.Sprintf("`%s`", col.ColumnName))
			}
		} else if connDbSource.DbType == DbTypeSqlserver {
			if _, ok := sqlServerTopNDataType[col.DataType]; ok {
				selectedColumns = append(selectedColumns, col.ColumnName)
				selectedColumnForQuery = append(selectedColumnForQuery, fmt.Sprintf("[%s]", col.ColumnName))
			}
		}
	}
	if len(selectedColumns) == 0 {
		log.WarnContextf(ctx, "GetTopNValue|没有找到表列信息: dbTable: %v", dbTable)
		return nil
	}

	log.InfoContextf(ctx, "GetTopNValueV2|获取topN值: dbTable: %v, selectedColumns: %v, primaryColumn:%v", dbTable,
		selectedColumns, primaryColumn)
	switch connDbSource.DbType {
	case DbTypeMysql:
		sqlQuery = fmt.Sprintf("SELECT %s FROM `%s` ORDER BY `%s` LIMIT ? OFFSET ?", strings.Join(selectedColumnForQuery,
			","), dbTable.Name, primaryColumn)
	case DbTypeSqlserver:
		sqlQuery = fmt.Sprintf("SELECT %s FROM [%s] ORDER BY [%s] OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY",
			strings.Join(selectedColumnForQuery, ","), dbTable.Name, primaryColumn)
	default:
		return errs.ErrDbSourceTypeNotSupport
	}

	col2ColInfo := make(map[string]*model.DBTableColumn)
	colStats := make(map[string]*TopN)
	for _, col := range dbTableColumns {
		col2ColInfo[col.ColumnName] = col
		colStats[col.ColumnName] = NewTopN(config.App().DbSource.ValueLinkConfig.MaxTopValueNum, config.App().
			DbSource.ValueLinkConfig.TrimKeepSize, config.App().DbSource.ValueLinkConfig.TrimThreshold)
	}

	offset := 0
	for offset < maxCowSize {
		limit := config.App().DbSource.ValueLinkConfig.MaxTraverseRow
		if offset+limit > maxCowSize {
			limit = maxCowSize - offset
		}
		var rows *sql.Rows
		if connDbSource.DbType == DbTypeSqlserver {
			rows, err = dbConn.QueryContext(ctx, sqlQuery, offset, limit)
		} else {
			rows, err = dbConn.QueryContext(ctx, sqlQuery, limit, offset)
		}
		if err != nil {
			log.ErrorContextf(ctx, "GetTopNValue|获取数据失败: %v sqlQuery:%v, limit:%v, offset:%v", err, sqlQuery, limit, offset)
			return err
		}

		columns, err := rows.Columns()
		if err != nil {
			log.ErrorContextf(ctx, "GetTopNValue|获取列名失败: %v", err)
			return err
		}
		rowData := make([]interface{}, len(columns))
		for i := range rowData {
			rowData[i] = new(sql.NullString)
		}

		for rows.Next() {
			if err = rows.Scan(rowData...); err != nil {
				log.ErrorContextf(ctx, "GetTopNValue|扫描数据失败: %v", err)
				return err
			}
			for i, val := range rowData {
				if v, ok := val.(*sql.NullString); ok && v.Valid {
					if utf8.RuneCountInString(v.String) > config.App().DbSource.ValueLinkConfig.MaxValueLen {
						v.String = string([]rune(v.String)[:config.App().DbSource.ValueLinkConfig.MaxValueLen]) + "..."
					}
					colStats[columns[i]].Add(v.String)
				} else {
					continue
				}
			}
		}
		offset += limit
		if rows != nil {
			err = rows.Close()
			if err != nil {
				log.WarnContextf(ctx, "GetTopNValue|关闭数据失败: %v", err)
			}
		}
	}
	t1 := time.Now()
	log.DebugContextf(ctx, "GetTopNValue|get top value over cost:%v， dbTable: %v, offset:%v,  ", t1.Sub(t0).Milliseconds(),
		dbTable, offset)

	for _, colName := range selectedColumns {
		dbTableColumn := col2ColInfo[colName]
		topValue := make([]string, 0, config.App().DbSource.ValueLinkConfig.MaxTopValueNum)
		var dbTableTopValues []*model.DbTableTopValue
		log.DebugContextf(ctx, "GetTopNValue|获取topN值: colName:%+v, value:%+v", colName, colStats[colName].Top())
		for _, values := range colStats[colName].Top() {
			value := values.Value
			topValue = append(topValue, value)
			dbTableTopValue := &model.DbTableTopValue{
				CorpBizID:          dbTableColumn.CorpBizID,
				AppBizID:           dbTableColumn.AppBizID,
				DBSourceBizID:      dbTable.DBSourceBizID,
				DbTableBizID:       dbTableColumn.DBTableBizID,
				DbTableColumnBizID: dbTableColumn.DBTableColumnBizID,
				BusinessID:         getDao.GenerateSeqID(),
				ColumnName:         dbTableColumn.ColumnName,
				ColumnValue:        value,
				ColumnComment:      dbTableColumn.ColumnComment,
				IsDeleted:          false,
				CreateTime:         time.Now(),
				UpdateTime:         time.Now(),
			}
			dbTableTopValues = append(dbTableTopValues, dbTableTopValue)
		}

		err = r.CreateTopValue(ctx, dbTableTopValues)
		if err != nil {
			log.ErrorContextf(ctx, "GetTopNValue| value:%v, 新增topN值失败: %v", dbTableTopValues, err)
			return err
		}
		for _, dbTableTopValue := range dbTableTopValues {
			content := fmt.Sprintf("%v:%v", dbTableColumn.ColumnName, dbTableTopValue.ColumnValue)
			err = r.AddVdb(ctx, robotId, dbTableColumn.AppBizID, dbTableTopValue.BusinessID, embeddingVersion, content,
				retrieval.EnvType_Test)
			if err != nil {
				log.ErrorContextf(ctx, "GetTopNValue|input vdb err: %v", err)
				return err
			}
		}
	}
	t2 := time.Now()
	log.InfoContextf(ctx, "GetTopNValue|add vdb and update: cost: %v dbTable: %v", t2.Sub(t1).Milliseconds(), dbTable)
	return nil
}

// TopNCount TODO
type TopNCount struct {
	Value interface{}
	Count int
}

// ValueCount TODO
type ValueCount struct {
	Value string
	Count int
}

// TopN TODO
type TopN struct {
	N        int
	MaxSize  int
	KeepSize int
	Counts   map[string]int
}

// NewTopN TODO
func NewTopN(n, keepSize, maxSize int) *TopN {
	return &TopN{
		N:        n,
		KeepSize: keepSize,
		MaxSize:  maxSize,
		Counts:   make(map[string]int),
	}
}

// Add TODO
func (t *TopN) Add(val string) {
	t.Counts[val]++
	if len(t.Counts) > t.MaxSize {
		t.trim()
	}
}

func (t *TopN) trim() {
	arr := make([]ValueCount, 0, len(t.Counts))
	for v, c := range t.Counts {
		arr = append(arr, ValueCount{v, c})
	}
	sort.Slice(arr, func(i, j int) bool {
		return arr[i].Count > arr[j].Count
	})
	t.Counts = make(map[string]int)
	for i := 0; i < t.KeepSize && i < len(arr); i++ {
		t.Counts[arr[i].Value] = arr[i].Count
	}
}

// Top TODO
func (t *TopN) Top() []ValueCount {
	arr := make([]ValueCount, 0, len(t.Counts))
	for v, c := range t.Counts {
		arr = append(arr, ValueCount{v, c})
	}
	sort.Slice(arr, func(i, j int) bool {
		return arr[i].Count > arr[j].Count
	})
	if len(arr) > t.N {
		arr = arr[:t.N]
	}
	return arr
}

// CreateTopValue 新增 top value 记录
func (r *DBSourceDao) CreateTopValue(ctx context.Context, cols []*model.DbTableTopValue) error {
	batchSize := 50
	err := r.db.WithContext(ctx).CreateInBatches(cols, batchSize).Error
	if err != nil {
		log.ErrorContextf(ctx, "CreateTopValue|db error: %v", err)
		return err
	}
	return nil
}

// GetTopValuesPageByDbTableBizID 根据 dbTableBizID 获取 top value 记录
func (r *DBSourceDao) GetTopValuesPageByDbTableBizID(ctx context.Context, corpBizID, appBizID, dbTableBizID, ID uint64,
	pageSize int) ([]*model.DbTableTopValue, error) {
	var results []*model.DbTableTopValue
	err := r.db.WithContext(ctx).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ? AND id > ? AND is_deleted = 0", corpBizID, appBizID,
			dbTableBizID, ID).
		Order("id ASC").
		Limit(pageSize).
		Find(&results).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetTopValuesPageByDbTableBizID|查询失败: %v", err)
		return nil, err
	}
	return results, nil
}

// GetTopValuesByDbTableBizID 根据 dbTableBizID 获取全部的 top value 记录
func (r *DBSourceDao) GetTopValuesByDbTableBizID(ctx context.Context, corpBizID, appBizID,
	dbTableBizID uint64) ([]*model.DbTableTopValue, error) {
	var results []*model.DbTableTopValue
	err := r.db.WithContext(ctx).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ? AND is_deleted = 0", corpBizID, appBizID,
			dbTableBizID).
		Order("id ASC").
		Find(&results).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetTopValuesByDbTableBizID|查询失败: %v", err)
		return nil, err
	}
	return results, nil
}

// GetDeletedTopValuesPageByDbTableBizID 根据 dbTableBizID 获取 top value 记录, 获取已经删除的数据
func (r *DBSourceDao) GetDeletedTopValuesPageByDbTableBizID(ctx context.Context, corpBizID, appBizID, dbTableBizID,
	ID uint64,
	pageSize int) ([]*model.DbTableTopValue, error) {
	var results []*model.DbTableTopValue
	err := r.db.WithContext(ctx).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ? AND id > ? AND is_deleted = 1", corpBizID, appBizID,
			dbTableBizID, ID).
		Order("id ASC").
		Limit(pageSize).
		Find(&results).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetTopValuesPageByDbTableBizID|查询失败:dbTableBizID:%v, err:%v", dbTableBizID, err)
		return nil, err
	}
	return results, nil
}

// BatchSoftDeleteTopValuesByDbTableBizID 批量软删除
func (r *DBSourceDao) BatchSoftDeleteTopValuesByDbTableBizID(ctx context.Context, corpBizID, appBizID,
	dbTableBizID uint64) error {
	err := r.db.WithContext(ctx).Model(&model.DbTableTopValue{}).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ? ", corpBizID, appBizID, dbTableBizID).
		Update("is_deleted", true).Error
	if err != nil {
		log.ErrorContextf(ctx, "BatchSoftDeleteTopValues|批量软删除失败: %v", err)
		return err
	}
	return nil
}

// BatchSoftDeleteTopValuesByBizID 批量软删除
func (r *DBSourceDao) BatchSoftDeleteTopValuesByBizID(ctx context.Context, businessIDs []uint64) error {
	batchSize := 200
	for i := 0; i < len(businessIDs); i += batchSize {
		end := i + batchSize
		if end > len(businessIDs) {
			end = len(businessIDs)
		}
		batch := businessIDs[i:end]

		err := r.db.WithContext(ctx).Model(&model.DbTableTopValue{}).
			Where("business_id IN ?", batch).
			Update("is_deleted", true).Error
		if err != nil {
			log.ErrorContextf(ctx, "BatchSoftDeleteTopValues|批量软删除失败: %v", err)
			return err
		}
	}
	return nil
}

// BatchHardDeleteTopValuesByBizID 批量硬删除
func (r *DBSourceDao) BatchHardDeleteTopValuesByBizID(ctx context.Context, businessIDs []uint64) error {
	batchSize := 200
	for i := 0; i < len(businessIDs); i += batchSize {
		end := i + batchSize
		if end > len(businessIDs) {
			end = len(businessIDs)
		}
		batch := businessIDs[i:end]

		err := r.db.WithContext(ctx).Model(&model.DbTableTopValue{}).
			Where("business_id IN ?", batch).
			Delete(&model.DbTableTopValue{}).Error
		if err != nil {
			log.ErrorContextf(ctx, "BatchHardDeleteTopValues|批量硬删除失败: %v", err)
			return err
		}
	}
	return nil
}

// BatchCleanDeletedTopValuesByDbTableBizID 批量清除，已经软删除的数据
func (r *DBSourceDao) BatchCleanDeletedTopValuesByDbTableBizID(ctx context.Context, corpBizID, appBizID,
	dbTableBizID uint64) error {
	err := r.db.WithContext(ctx).Model(&model.DbTableTopValue{}).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ? AND is_deleted = 1", corpBizID, appBizID,
			dbTableBizID).
		Unscoped().Delete(&model.DbTableTopValue{}).Error
	if err != nil {
		log.ErrorContextf(ctx, "BatchHardDeleteTopValues|批量硬删除失败: %v", err)
		return err
	}
	return nil
}

// AddVdb 添加向量
func (r *DBSourceDao) AddVdb(ctx context.Context, robotId, appBizID, businessID, embeddingVersion uint64,
	content string, envType retrieval.EnvType) error {
	if !config.App().DbSource.EnableVdb {
		log.InfoContextf(ctx, "disable DelVdb|robotId:%v, appBizID:%v, businessID:%v, embeddingVersion:%v, envType:%v",
			robotId, appBizID, businessID, embeddingVersion, envType)
		return nil
	}
	req := &retrieval.AddVectorReq{
		RobotId:          robotId,
		IndexId:          model.DbSourceVersionID,
		Id:               businessID,
		PageContent:      content,
		DocType:          model.DocTypeSegment,
		EmbeddingVersion: embeddingVersion,
		Labels:           nil,
		BotBizId:         appBizID,
		EnvType:          envType,
		Type:             0,
	}
	_, err := retrieval.NewDirectIndexClientProxy().AddVector(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "GetTopNValue|req :%v input vdb err: %v", req, err)
		return err
	}
	return nil
}

// DelVdb 添加向量
func (r *DBSourceDao) DelVdb(ctx context.Context, robotId, appBizID, embeddingVersion uint64, businessIDs []uint64,
	envType retrieval.EnvType) error {
	if !config.App().DbSource.EnableVdb {
		log.InfoContextf(ctx, "disable DelVdb|robotId:%v, appBizID:%v, embeddingVersion:%v, envType:%v",
			robotId, appBizID, embeddingVersion, envType)
		return nil
	}
	req := &retrieval.DeleteVectorReq{
		RobotId:          robotId,
		IndexId:          model.DbSourceVersionID,
		Ids:              businessIDs,
		EmbeddingVersion: embeddingVersion,
		BotBizId:         appBizID,
		EnvType:          envType,
	}
	// GroupID:__qd:qbot_dev:rob_1958:typ_12:emb_8
	_, err := retrieval.NewDirectIndexClientProxy().DeleteVector(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "DelVdb|req :%v err: %v", req, err)
		return err
	}
	return nil
}

// QuerySqlForDbSource TODO
func (r *DBSourceDao) QuerySqlForDbSource(ctx context.Context, connDbSource model.ConnDbSource,
	exeSql string, sqlParams []string) ([]string, []*pb.RowData, string, error) {
	log.InfoContextf(ctx, "QuerySqlForDbSource|dbName: %v, sql:%v, sqlParams:%+v, len:%v", connDbSource.DbName, exeSql,
		sqlParams, len(sqlParams))
	dbConn, err := r.GetDBConnection(ctx, connDbSource)
	if err != nil {
		log.ErrorContextf(ctx, "QuerySqlForDbSource|get db connection failed: %v", err)
		return nil, nil, err.Error(), err
	}
	defer func() {
		closeErr := dbConn.Close()
		if closeErr != nil {
			log.WarnContextf(ctx, "关闭数据库连接失败: %v", closeErr)
		}
	}()
	anySlice := make([]any, len(sqlParams))
	for i, v := range sqlParams {
		anySlice[i] = v
	}
	// 查询语句判断是否要加limit，没有需要使用默认limit
	defaultLimit := utils.When(config.App().DbSource.DefaultSelectLimit > 0, config.App().DbSource.DefaultSelectLimit, 100)
	exeSql = strings.TrimRight(strings.TrimSpace(exeSql), ";") // 去除末尾分号
	if connDbSource.DbType == DbTypeMysql {
		exeSql = addMysqlDefaultLimit(exeSql, defaultLimit)
	} else if connDbSource.DbType == DbTypeSqlserver {
		exeSql = addSqlServerDefaultTop(exeSql, defaultLimit)
	}
	log.InfoContextf(ctx, "exeSql:%s", exeSql)
	rows, err := dbConn.QueryContext(ctx, exeSql, anySlice...)
	if err != nil {
		log.WarnContextf(ctx, "QueryContext err: %v sqlQuery:%v, sqlParams:%v, dbSource:%v",
			err, exeSql, sqlParams, connDbSource.DbName)
		return nil, nil, err.Error(), err
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.WarnContextf(ctx, "关闭查询结果集失败: %v", closeErr)
		}
	}()

	columns, err := rows.Columns()
	if err != nil {
		log.ErrorContextf(ctx, "获取列名失败: %v", err)
		return nil, nil, err.Error(), err
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		log.ErrorContextf(ctx, "获取列类型失败: %v", err)
		return nil, nil, err.Error(), err
	}
	var values []*pb.RowData

	for rows.Next() {
		rowData := make([]interface{}, len(columns))
		// 针对 sql server 特殊处理 uniqueidentifier 进行特殊处理
		for i, colType := range columnTypes {
			if colType.DatabaseTypeName() == SqlserverUniqueidentifier {
				rowData[i] = new([]byte)
			} else {
				rowData[i] = new(sql.NullString)
			}
		}
		if err = rows.Scan(rowData...); err != nil {
			log.ErrorContextf(ctx, "Scan error: %v", err)
			return nil, nil, "", err
		}
		row := &pb.RowData{
			Values: make([]string, len(columns)),
		}

		for i, val := range rowData {
			// 针对 sql server 特殊处理 uniqueidentifier 进行特殊处理
			if columnTypes[i].DatabaseTypeName() == SqlserverUniqueidentifier {
				if v, ok := val.(*[]byte); ok && v != nil {
					row.Values[i] = bytesToGUIDString(*v)
				} else {
					row.Values[i] = ""
				}
			} else {
				if v, ok := val.(*sql.NullString); ok && v.Valid {
					row.Values[i] = v.String
				} else {
					row.Values[i] = ""
				}
			}
		}
		values = append(values, row)
	}
	return columns, values, "", nil
}

// RunSqlForDbSource TODO
func (r *DBSourceDao) RunSqlForDbSource(ctx context.Context, connDbSource model.ConnDbSource,
	exeSql string, sqlParams []string) (int64, string, error) {
	log.InfoContextf(ctx, "RunSqlForDbSource|dbName: %v, sql:%v, sqlParams:%+v", connDbSource.DbName, exeSql, sqlParams)
	dbConn, err := r.GetDBConnection(ctx, connDbSource)
	if err != nil {
		log.ErrorContextf(ctx, "RunSqlForDbSource|get db connection failed: %v", err)
		return 0, err.Error(), nil
	}
	defer func() {
		closeErr := dbConn.Close()
		if closeErr != nil {
			log.WarnContextf(ctx, "关闭数据库连接失败: %v", closeErr)
		}
	}()
	anySlice := make([]any, len(sqlParams))
	for i, v := range sqlParams {
		anySlice[i] = v
	}
	result, err := dbConn.ExecContext(ctx, exeSql, anySlice...)
	if err != nil {
		log.WarnContextf(ctx, "ExecContext err: %v sqlQuery:%v, sqlParams:%v, dbSource:%v",
			err, exeSql, sqlParams, connDbSource.DbName)
		return 0, err.Error(), nil
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.WarnContextf(ctx, "获取影响行数失败: %v", err)
		return 0, err.Error(), nil
	}
	return rowsAffected, "", nil
}

// addMysqlDefaultLimit 检查 SQL 查询并添加默认 LIMIT 如果缺少的话
func addMysqlDefaultLimit(sql string, defaultLimit int) string {
	// 转换为小写以进行不区分大小写的匹配
	lowerSQL := strings.ToLower(sql)
	// 检查是否是 SELECT 查询
	if !strings.HasPrefix(lowerSQL, "select") {
		return sql
	}
	// 检查是否已经包含 LIMIT
	limitRegex := regexp.MustCompile(`limit\s+\d+(\s*,\s*\d+)?\s*$`)
	if limitRegex.MatchString(lowerSQL) {
		return sql
	}

	// 先去掉可能的末尾分号
	trimmedSQL := strings.TrimRight(sql, ";")
	trimmedSQL = strings.TrimSpace(trimmedSQL)

	// 使用更精确的正则表达式来找到最外层的ORDER BY子句
	// 这个正则会匹配不在括号内的ORDER BY
	orderByRegex := regexp.MustCompile(`(?i)\s+order\s+by\s+[^)]*$`)
	if orderByRegex.MatchString(trimmedSQL) {
		// 如果有ORDER BY，在其后添加LIMIT
		return trimmedSQL + fmt.Sprintf(" LIMIT %d", defaultLimit)
	}

	// 如果没有ORDER BY，直接在末尾添加LIMIT
	return trimmedSQL + fmt.Sprintf(" LIMIT %d", defaultLimit)
}

// addSqlServerDefaultTop 检查 SQL 查询并添加默认 TOP 如果缺少的话
func addSqlServerDefaultTop(sql string, defaultLimit int) string {
	// 转换为小写以进行不区分大小写的匹配
	lowerSQL := strings.ToLower(sql)
	// 检查是否是 SELECT 查询
	if !strings.HasPrefix(lowerSQL, "select") {
		return sql
	}
	// 检查是否已经包含 TOP
	topRegex := regexp.MustCompile(`top\s+\d+\s`)
	if topRegex.MatchString(lowerSQL) {
		return sql
	}
	// 检查是否是 SELECT DISTINCT 查询
	distinctRegex := regexp.MustCompile(`select\s+distinct\s`)
	hasDistinct := distinctRegex.MatchString(lowerSQL)
	// 在 SELECT 后添加 TOP 子句
	if hasDistinct {
		// 处理 SELECT DISTINCT 情况
		re := regexp.MustCompile(`(?i)select\s+distinct\s`)
		return re.ReplaceAllString(sql, fmt.Sprintf("SELECT DISTINCT TOP %d ", defaultLimit))
	} else {
		// 普通 SELECT 情况
		re := regexp.MustCompile(`(?i)select\s`)
		return re.ReplaceAllString(sql, fmt.Sprintf("SELECT TOP %d ", defaultLimit))
	}
}
