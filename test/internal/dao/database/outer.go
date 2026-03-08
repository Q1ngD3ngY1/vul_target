package database

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	entity0 "git.woa.com/adp/kb/kb-config/internal/entity"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"github.com/spf13/cast"
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
	oracleTopNDataType = map[string]struct{}{
		"CHAR":      {},
		"NCHAR":     {},
		"VARCHAR":   {},
		"VARCHAR2":  {},
		"NVARCHAR2": {},
		"CLOB":      {},
		"NCLOB":     {},
	}

	// 预编译的正则表达式
	mysqlLimitRegex              = regexp.MustCompile(`limit\s+\d+(\s*,\s*\d+|\s+offset\s+\d+)?\s*$`)
	mysqlOrderByRegex            = regexp.MustCompile(`(?i)\s+order\s+by\s+[^)]*$`)
	sqlServerTopRegex            = regexp.MustCompile(`top\s+\d+\s`)
	sqlServerDistinctRegex       = regexp.MustCompile(`select\s+distinct\s`)
	sqlServerSelectRegex         = regexp.MustCompile(`(?i)select\s`)
	sqlServerSelectDistinctRegex = regexp.MustCompile(`(?i)select\s+distinct\s`)
)

type TopNCount struct {
	Value any
	Count int
}

type ValueCount struct {
	Value string
	Count int
}

type TopN struct {
	N        int
	MaxSize  int
	KeepSize int
	Counts   map[string]int
}

func NewTopN(n, keepSize, maxSize int) *TopN {
	return &TopN{
		N:        n,
		KeepSize: keepSize,
		MaxSize:  maxSize,
		Counts:   make(map[string]int),
	}
}

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
func (d *dao) CreateTopValue(ctx context.Context, cols []*entity.TableTopValue) error {
	batchSize := 50
	err := d.tdsql.TDbTableTopValue.WithContext(ctx).UnderlyingDB().CreateInBatches(cols, batchSize).Error
	if err != nil {
		logx.E(ctx, "CreateTopValue|db error: %v", err)
		return err
	}
	return nil
}

// GetTopValuesPageByDbTableBizID 根据 dbTableBizID 获取 top value 记录
func (d *dao) GetTopValuesPageByDbTableBizID(ctx context.Context, corpBizID, appBizID, dbTableBizID, ID uint64, pageSize int) ([]*entity.TableTopValue, error) {
	var results []*entity.TableTopValue
	err := d.tdsql.TDbTableTopValue.WithContext(ctx).UnderlyingDB().
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ? AND id > ? AND is_deleted = 0", corpBizID, appBizID, dbTableBizID, ID).
		Order("id ASC").
		Limit(pageSize).
		Find(&results).Error
	if err != nil {
		logx.E(ctx, "GetTopValuesPageByDbTableBizID|查询失败: %v", err)
		return nil, err
	}
	return results, nil
}

// GetTopValuesByDbTableBizID 根据 dbTableBizID 获取全部的 top value 记录
func (d *dao) GetTopValuesByDbTableBizID(ctx context.Context, corpBizID, appBizID, dbTableBizID uint64) ([]*entity.TableTopValue, error) {
	var results []*entity.TableTopValue
	err := d.tdsql.TDbTableTopValue.WithContext(ctx).UnderlyingDB().
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ? AND is_deleted = 0", corpBizID, appBizID, dbTableBizID).
		Order("id ASC").
		Find(&results).Error
	if err != nil {
		logx.E(ctx, "GetTopValuesByDbTableBizID|查询失败: %v", err)
		return nil, err
	}
	return results, nil
}

// GetDeletedTopValuesPageByDbTableBizID 根据 dbTableBizID 获取 top value 记录, 获取已经删除的数据
func (d *dao) GetDeletedTopValuesPageByDbTableBizID(ctx context.Context, corpBizID, appBizID, dbTableBizID, ID uint64, pageSize int) ([]*entity.TableTopValue, error) {
	var results []*entity.TableTopValue
	err := d.tdsql.TDbTableTopValue.WithContext(ctx).UnderlyingDB().
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ? AND id > ? AND is_deleted = 1", corpBizID, appBizID, dbTableBizID, ID).
		Order("id ASC").
		Limit(pageSize).
		Find(&results).Error
	if err != nil {
		logx.E(ctx, "GetTopValuesPageByDbTableBizID|查询失败:dbTableBizID:%v, err:%v", dbTableBizID, err)
		return nil, err
	}
	return results, nil
}

// BatchSoftDeleteTopValuesByDbTableBizID 批量软删除
func (d *dao) BatchSoftDeleteTopValuesByDbTableBizID(ctx context.Context, corpBizID, appBizID, dbTableBizID uint64) error {
	err := d.tdsql.TDbTableTopValue.WithContext(ctx).UnderlyingDB().Model(&entity.TableTopValue{}).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ? ", corpBizID, appBizID, dbTableBizID).
		Update("is_deleted", true).Error
	if err != nil {
		logx.E(ctx, "BatchSoftDeleteTopValues|批量软删除失败: %v", err)
		return err
	}
	return nil
}

// BatchSoftDeleteTopValuesByBizID 批量软删除
func (d *dao) BatchSoftDeleteTopValuesByBizID(ctx context.Context, businessIDs []uint64) error {
	batchSize := 200
	for i := 0; i < len(businessIDs); i += batchSize {
		end := i + batchSize
		if end > len(businessIDs) {
			end = len(businessIDs)
		}
		batch := businessIDs[i:end]

		err := d.tdsql.TDbTableTopValue.WithContext(ctx).UnderlyingDB().Model(&entity.TableTopValue{}).
			Where("business_id IN ?", batch).
			Update("is_deleted", true).Error
		if err != nil {
			logx.E(ctx, "BatchSoftDeleteTopValues|批量软删除失败: %v", err)
			return err
		}
	}
	return nil
}

// BatchDeleteTopValuesByBizID 批量硬删除
func (d *dao) BatchDeleteTopValuesByBizID(ctx context.Context, businessIDs []uint64) error {
	batchSize := 200
	for i := 0; i < len(businessIDs); i += batchSize {
		end := i + batchSize
		if end > len(businessIDs) {
			end = len(businessIDs)
		}
		batch := businessIDs[i:end]

		err := d.tdsql.TDbTableTopValue.WithContext(ctx).UnderlyingDB().Model(&entity.TableTopValue{}).
			Where("business_id IN ?", batch).
			Delete(&entity.TableTopValue{}).Error
		if err != nil {
			logx.E(ctx, "BatchHardDeleteTopValues|批量硬删除失败: %v", err)
			return err
		}
	}
	return nil
}

// BatchCleanDeletedTopValuesByDbTableBizID 批量清除，已经软删除的数据
func (d *dao) BatchCleanDeletedTopValuesByDbTableBizID(ctx context.Context, corpBizID, appBizID, dbTableBizID uint64) error {
	err := d.tdsql.TDbTableTopValue.WithContext(ctx).UnderlyingDB().Model(&entity.TableTopValue{}).
		Where("corp_biz_id = ? AND app_biz_id = ? AND db_table_biz_id = ? AND is_deleted = 1", corpBizID, appBizID, dbTableBizID).
		Unscoped().Delete(&entity.TableTopValue{}).Error
	if err != nil {
		logx.E(ctx, "BatchHardDeleteTopValues|批量硬删除失败: %v", err)
		return err
	}
	return nil
}

// AddVector 添加向量
func (d *dao) AddVector(ctx context.Context, robotId, appBizID, dbTableBizID, businessID, embeddingVersion uint64,
	embeddingName string, enableScope uint32, content string, envType retrieval.EnvType) error {
	if !config.App().DbSource.EnableVdb {
		logx.I(ctx, "disable AddVector|robotId:%v, appBizID:%v, businessID:%v, embeddingVersion:%v, envType:%v", robotId, appBizID, businessID, embeddingVersion, envType)
		return nil
	}
	labels := make([]*retrieval.VectorLabel, 0)
	labels = append(labels, &retrieval.VectorLabel{ // 表的业务id统一写标签 todo role labels.
		Name:  entity.LabelDBTableBizID,
		Value: cast.ToString(dbTableBizID),
	}, &retrieval.VectorLabel{
		Name:  entity0.EnableScopeAttr,
		Value: entity0.EnableScopeDb2Label[enableScope],
	})
	req := &retrieval.BatchAddKnowledgeReq{
		RobotId:            robotId,
		IndexId:            entity0.DbSourceVersionID,
		DocType:            entity0.DocTypeSegment,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingName,
		BotBizId:           appBizID,
		EnvType:            envType,
		Type:               retrieval.KnowledgeType_KNOWLEDGE,
		IsVector:           true,
		Knowledge: []*retrieval.KnowledgeData{{
			Id:          businessID,
			PageContent: content,
			Labels:      labels,
		}},
	}
	logx.I(ctx, "BatchAddKnowledge|req :%v", req)
	_, err := d.rpc.RetrievalDirectIndex.BatchAddKnowledge(ctx, req)
	if err != nil {
		logx.E(ctx, "AddVector|req :%v input vdb error: %v", req, err)
		return err
	}
	return nil
}

// DeleteVector 添加向量
func (d *dao) DeleteVector(ctx context.Context, robotId, appBizID, embeddingVersion uint64, embeddingName string, businessIDs []uint64, envType retrieval.EnvType) error {
	if !config.App().DbSource.EnableVdb {
		logx.I(ctx, "disable DelVdb|robotId:%v, appBizID:%v, embeddingVersion:%v, envType:%v",
			robotId, appBizID, embeddingVersion, envType)
		return nil
	}
	data := make([]*retrieval.KnowledgeIDType, len(businessIDs))
	for i, id := range businessIDs {
		data[i] = &retrieval.KnowledgeIDType{Id: id}
	}
	// GroupID:__qd:qbot_dev:rob_1958:typ_12:emb_8
	req := retrieval.BatchDeleteKnowledgeReq{
		RobotId:            robotId,
		IndexId:            entity0.DbSourceVersionID,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingName,
		BotBizId:           appBizID,
		EnvType:            envType,
		IsVector:           true,
		Data:               data,
	}
	err := d.rpc.RetrievalDirectIndex.BatchDeleteKnowledge(ctx, &req)
	if err != nil {
		logx.E(ctx, "DeleteVector|req :%v err: %v", &req, err)
		return err
	}
	return nil
}

func (d *dao) RunSqlForDbSource(ctx context.Context, connDbSource entity.DatabaseConn,
	exeSql string, sqlParams []string) (int64, string, error) {
	logx.I(ctx, "RunSqlForDbSource|dbName: %v, sql:%v, sqlParams:%+v", connDbSource.DbName, exeSql, sqlParams)
	dbConn, err := d.GetDBConnection(ctx, connDbSource)
	if err != nil {
		logx.E(ctx, "RunSqlForDbSource|get db connection failed: %v", err)
		return 0, errs.ErrOpenDbSourceFail.Error(), nil
	}
	defer func() {
		closeErr := dbConn.Close()
		if closeErr != nil {
			logx.W(ctx, "关闭数据库连接失败: %v", closeErr)
		}
	}()
	anySlice := make([]any, len(sqlParams))
	for i, v := range sqlParams {
		anySlice[i] = v
	}
	// 使用预处理语句
	stmt, err := dbConn.PrepareContext(ctx, exeSql)
	if err != nil {
		logx.E(ctx, "Prepare SQL failed: %v, sql: %v", err, exeSql)
		return 0, "", fmt.Errorf("SQL预处理失败")
	}
	defer stmt.Close()
	result, err := stmt.ExecContext(ctx, anySlice...)
	if err != nil {
		logx.W(ctx, "ExecContext err: %v sqlQuery:%v, sqlParams:%v",
			err, exeSql, sqlParams)
		return 0, "", fmt.Errorf("SQL执行失败")
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		logx.W(ctx, "获取影响行数失败: %v", err)
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
	// 检查是否已经包含 LIMIT（支持 LIMIT n 或 LIMIT n, m 或 LIMIT n OFFSET m 格式）
	if mysqlLimitRegex.MatchString(lowerSQL) {
		return sql
	}

	// 先去掉可能的末尾空格和分号
	trimmedSQL := strings.TrimSpace(sql)
	trimmedSQL = strings.TrimRight(trimmedSQL, ";")
	trimmedSQL = strings.TrimSpace(trimmedSQL)

	// 使用更精确的正则表达式来找到最外层的ORDER BY子句
	// 这个正则会匹配不在括号内的ORDER BY
	if mysqlOrderByRegex.MatchString(trimmedSQL) {
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
	if sqlServerTopRegex.MatchString(lowerSQL) {
		return sql
	}
	// 检查是否是 SELECT DISTINCT 查询
	hasDistinct := sqlServerDistinctRegex.MatchString(lowerSQL)
	// 在 SELECT 后添加 TOP 子句
	if hasDistinct {
		// 处理 SELECT DISTINCT 情况
		return sqlServerSelectDistinctRegex.ReplaceAllString(sql, fmt.Sprintf("SELECT DISTINCT TOP %d ", defaultLimit))
	} else {
		// 普通 SELECT 情况
		return sqlServerSelectRegex.ReplaceAllString(sql, fmt.Sprintf("SELECT TOP %d ", defaultLimit))
	}
}
