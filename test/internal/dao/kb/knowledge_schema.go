package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

const (
	knowledgeSchemaTaskTableName        = "t_knowledge_schema_task"
	KnowledgeSchemaTaskTblColCorpBizId  = "corp_biz_id" // 企业业务ID
	KnowledgeSchemaTaskTblColAppBizId   = "app_biz_id"  // 应用业务ID
	KnowledgeSchemaTaskTblColBusinessId = "business_id" // 业务ID
	KnowledgeSchemaTaskTblColStatus     = "status"      // 状态(0待处理 1处理中 2处理成功 3处理失败)
	KnowledgeSchemaTaskTblColIsDeleted  = "is_deleted"  // 是否删除
	knowledgeSchemaTableName            = "t_knowledge_schema"
	knowledgeSchemaProdTableName        = "t_knowledge_schema_prod"
	KnowledgeSchemaTblColCorpBizId      = "corp_biz_id" // 企业业务ID
	KnowledgeSchemaTblColAppBizId       = "app_biz_id"  // 应用业务ID
	KnowledgeSchemaTblColIsDeleted      = "is_deleted"  // 是否删除
)

func genKnowledgeSchemaKey(appBizId uint64, envType string) string {
	return fmt.Sprintf("knowledge_schema_%d_%s", appBizId, envType)
}

// genKnowledgeSchemaDocClusterBizIDKey 目录id映射文档自增id的缓存key
func genKnowledgeSchemaDocClusterBizIDKey(appBizId uint64, envType string, docClusterBizId uint64) string {
	prefix := genKnowledgeSchemaDocClusterKeyPrefix(appBizId, envType)
	return fmt.Sprintf("%s%d", prefix, docClusterBizId)
}

// genKnowledgeSchemaDocClusterKeyPrefix 目录id映射文档自增id的缓存key前缀
func genKnowledgeSchemaDocClusterKeyPrefix(appBizId uint64, envType string) string {
	return fmt.Sprintf("knowledge_schema_doc_cluster_%d_%s_", appBizId, envType)
}

// genKnowledgeSchemaDocClusterId2AppBizIdKey 目录id映射文档AppBizId的缓存key
func genKnowledgeSchemaDocClusterId2AppBizIdKey(docClusterBizId uint64, envType string) string {
	return fmt.Sprintf("knowledge_schema_doc_cluster_2_app_biz_id_%d_%s", docClusterBizId, envType)
}

func (d *daoImpl) GetKnowledgeSchema(ctx context.Context, appBizId uint64, envType string) ([]*pb.GetKnowledgeSchemaRsp_SchemaItem, error) {
	key := genKnowledgeSchemaKey(appBizId, envType)
	result, err := d.rdb.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, errx.ErrNotFound
		}
		logx.E(ctx, "GetKnowledgeSchema get redis result fail, err: %+v", err)
		return nil, err
	}
	var schemaItems []*pb.GetKnowledgeSchemaRsp_SchemaItem
	err = json.Unmarshal([]byte(result), &schemaItems)
	if err != nil {
		logx.E(ctx, "GetKnowledgeSchema Unmarshal schemaItems fail, err: %+v", err)
		return nil, err
	}
	logx.InfoContextf(ctx, "GetKnowledgeSchema schemaItems: %+v", schemaItems)
	return schemaItems, nil
}

// SetKnowledgeSchema 设置知识库schema缓存数据
func (d *daoImpl) SetKnowledgeSchema(ctx context.Context, appBizId uint64, envType string, schemaItems []*pb.GetKnowledgeSchemaRsp_SchemaItem) error {
	val, err := json.Marshal(schemaItems)
	if err != nil {
		logx.E(ctx, "SetKnowledgeSchema Marshal schemaItems fail, err: %+v", err)
		return err
	}
	key := genKnowledgeSchemaKey(appBizId, envType)
	if err = d.rdb.Set(ctx, key, val, 0).Err(); err != nil {
		logx.E(ctx, "SetKnowledgeSchema set redis value fail, err: %+v", err)
		return err
	}
	return nil
}

// GetKnowledgeSchemaDocIdByDocClusterId 读取目录id映射文档自增id的缓存，文档自增id对应t_doc表的id字段
func (d *daoImpl) GetKnowledgeSchemaDocIdByDocClusterId(ctx context.Context, appBizId uint64, envType string,
	docClusterBizId uint64) ([]uint64, error) {
	key := genKnowledgeSchemaDocClusterBizIDKey(appBizId, envType, docClusterBizId)
	result, err := d.rdb.Get(ctx, key).Result()
	if err != nil {
		// 如果redis中没有数据，则返回空，不报错
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		logx.E(ctx, "GetKnowledgeSchemaDocIdByDocClusterId get redis result fail, err: %+v", err)
		return nil, err
	}

	var docIds []uint64
	err = json.Unmarshal([]byte(result), &docIds)
	if err != nil {
		logx.E(ctx, "GetKnowledgeSchemaDocIdByDocClusterId Unmarshal docIds fail, err: %+v", err)
		return nil, err
	}
	return docIds, nil
}

// SetKnowledgeSchemaDocIdByDocClusterId 设置目录id映射文档自增id的缓存，文档自增id对应t_doc表的id字段
func (d *daoImpl) SetKnowledgeSchemaDocIdByDocClusterId(ctx context.Context, appBizId uint64, envType string,
	docClusterSchema *docEntity.DocClusterSchema) error {
	if docClusterSchema == nil {
		err := fmt.Errorf("SetKnowledgeSchemaDocIdByDocClusterId docClusterSchema is nil")
		logx.E(ctx, "%+v", err)
		return err
	}
	key := genKnowledgeSchemaDocClusterBizIDKey(appBizId, envType, docClusterSchema.BusinessID)

	if err := d.rdb.Set(ctx, key, docClusterSchema.DocIDs, 0).Err(); err != nil {
		logx.E(ctx, "SetKnowledgeSchemaDocIdByDocClusterId set redis value fail, err: %+v", err)
		return err
	}
	return nil
}

// GetKnowledgeSchemaAppBizIdByDocClusterId 读取目录id映射问文档对应的AppBizId的缓存
func (d *daoImpl) GetKnowledgeSchemaAppBizIdByDocClusterId(ctx context.Context,
	docClusterBizId uint64, envType string) (uint64, error) {
	key := genKnowledgeSchemaDocClusterId2AppBizIdKey(docClusterBizId, envType)
	result, err := d.rdb.Get(ctx, key).Result()
	if err != nil {
		// 如果redis中没有数据，则返回空，不报错
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		logx.E(ctx, "GetKnowledgeSchemaAppBizIdByDocClusterId get redis result fail, err: %+v", err)
		return 0, err
	}
	var appBizId uint64
	err = json.Unmarshal([]byte(result), &appBizId)
	if err != nil {
		logx.E(ctx, "GetKnowledgeSchemaAppBizIdByDocClusterId Unmarshal appBizId fail, err: %+v", err)
		return 0, err
	}
	return appBizId, nil
}

// SetKnowledgeSchemaAppBizIdByDocClusterId 设置目录id映射文档AppBizId的缓存
func (d *daoImpl) SetKnowledgeSchemaAppBizIdByDocClusterId(ctx context.Context,
	docClusterBizId, appBizId uint64, envType string) error {
	if docClusterBizId == 0 || appBizId == 0 {
		err := fmt.Errorf("SetKnowledgeSchemaAppBizIdByDocClusterId docClusterBizId or appBizId is 0")
		logx.E(ctx, "%+v", err)
		return err
	}
	key := genKnowledgeSchemaDocClusterId2AppBizIdKey(docClusterBizId, envType)
	if err := d.rdb.Set(ctx, key, appBizId, 0).Err(); err != nil {
		logx.E(ctx, "SetKnowledgeSchemaDocIdByDocClusterId set redis value fail, err: %+v", err)
		return err
	}
	return nil
}

// UpdateKnowledgeSchemaTask 更新知识库schema任务
func (d *daoImpl) UpdateKnowledgeSchemaTask(ctx context.Context, updateColumns []string, task *kbe.KnowledgeSchemaTask) error {
	q := d.tdsql.TKnowledgeSchemaTask.WithContext(ctx)
	session := q.UnderlyingDB().Table(knowledgeSchemaTaskTableName).Select(updateColumns).
		Where(KnowledgeSchemaTaskTblColCorpBizId+sqlEqual, task.CorpBizId).
		Where(KnowledgeSchemaTaskTblColAppBizId+sqlEqual, task.AppBizId).
		Where(KnowledgeSchemaTaskTblColBusinessId+sqlEqual, task.BusinessID)
	res := session.Updates(task)
	if res.Error != nil {
		logx.E(ctx, "UpdateKnowledgeSchemaTask failed for task businessID: %+v, err: %+v",
			task.BusinessID, res.Error)
		return res.Error
	}
	return nil
}

// GetKnowledgeSchemaTask 获取知识库schema任务
func (d *daoImpl) GetKnowledgeSchemaTask(ctx context.Context, selectColumns []string,
	filter *kbe.KnowledgeSchemaTaskFilter) (*kbe.KnowledgeSchemaTask, error) {
	if len(filter.OrderColumn) != len(filter.OrderDirection) {
		logx.E(ctx, "GetKnowledgeSchemaTask invalid order length, "+
			"len(OrderColumn)=%d, len(OrderDirection)=%d", len(filter.OrderColumn), len(filter.OrderDirection))
		return nil, errors.New("invalid order length")
	}
	q := d.tdsql.TKnowledgeSchemaTask.WithContext(ctx)
	knowledgeSchemaTask := &model.TKnowledgeSchemaTask{}
	session := q.UnderlyingDB().Table(knowledgeSchemaTaskTableName).Select(selectColumns)
	session = generateKnowledgeSchemaTaskCondition(ctx, session, filter)

	res := session.Take(knowledgeSchemaTask)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			logx.W(ctx, "GetKnowledgeSchemaTask task not exist err=%+v", res.Error)
			return nil, errs.ErrKnowledgeSchemaTaskNotFound
		}
		logx.E(ctx, "GetKnowledgeSchemaTask execute sql failed, err=%+v", res.Error)
		return nil, res.Error
	}
	return convertDoToSchemaTask(knowledgeSchemaTask), nil
}

// 生成查询条件，必须按照索引的顺序排列
func generateKnowledgeSchemaTaskCondition(ctx context.Context, session *gorm.DB, filter *kbe.KnowledgeSchemaTaskFilter) *gorm.DB {
	if filter.AppBizId != 0 {
		session = session.Where(KnowledgeSchemaTaskTblColAppBizId+sqlEqual, filter.AppBizId)
	}
	if len(filter.Statuses) > 0 {
		session = session.Where(KnowledgeSchemaTaskTblColStatus+util.SqlIn, filter.Statuses)
	}
	if filter.Limit > 0 {
		session = session.Limit(int(filter.Limit))
	}
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			logx.E(ctx, "generateKnowledgeSchemaTaskCondition invalid order direction: %s",
				filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	// is_deleted必须加入查询条件, 否则默认查询未删除的
	if filter.IsDeleted != nil {
		session = session.Where(KnowledgeSchemaTaskTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	} else {
		session = session.Where(KnowledgeSchemaTaskTblColIsDeleted+sqlEqual, kbe.IsNotDeleted)
	}
	return session
}

func (d *daoImpl) CreateKnowledgeSchemaTask(ctx context.Context, task *kbe.KnowledgeSchemaTask) error {
	q := d.tdsql.TKnowledgeSchemaTask.WithContext(ctx)
	res := q.UnderlyingDB().Table(knowledgeSchemaTaskTableName).Create(convertSchemaTaskToPO(task))
	if res.Error != nil {
		logx.E(ctx, "CreateKnowledgeSchemaTask execute sql failed, err=%+v", res.Error)
		return res.Error
	}
	return nil
}

func convertSchemaTaskToPO(po *kbe.KnowledgeSchemaTask) *model.TKnowledgeSchemaTask {
	if po == nil {
		return nil
	}

	taskDo := &model.TKnowledgeSchemaTask{
		ID:         po.Id,
		CorpBizID:  po.CorpBizId,
		AppBizID:   po.AppBizId,
		BusinessID: po.BusinessID,
		Status:     po.Status,
		StatusCode: po.StatusCode,
		IsDeleted:  po.IsDeleted,
		Message:    po.Message,
		UpdateTime: po.UpdateTime,
		CreateTime: po.CreateTime,
	}
	return taskDo
}

func convertDoToSchemaTask(do *model.TKnowledgeSchemaTask) *kbe.KnowledgeSchemaTask {
	if do == nil {
		return nil
	}
	taskDo := &kbe.KnowledgeSchemaTask{
		Id:         do.ID,
		CorpBizId:  do.CorpBizID,
		AppBizId:   do.AppBizID,
		BusinessID: do.BusinessID,
		Status:     do.Status,
		StatusCode: do.StatusCode,
		Message:    do.Message,
		IsDeleted:  do.IsDeleted,
		CreateTime: do.CreateTime,
		UpdateTime: do.UpdateTime,
	}
	return taskDo
}

// GetKnowledgeSchemaMaxVersion 获取知识库schema最大版本
func (d *daoImpl) GetKnowledgeSchemaMaxVersion(ctx context.Context, appBizId uint64) (uint64, error) {
	q := d.tdsql.TKnowledgeSchema.WithContext(ctx)
	// 使用Select和Row获取最大值
	maxVersion := uint64(0)
	row := q.UnderlyingDB().Table(knowledgeSchemaTableName).
		Select("COALESCE(MAX(version), 0) as max_version").
		Where(KnowledgeSchemaTblColAppBizId+sqlEqual, appBizId).Row()
	if err := row.Scan(&maxVersion); err != nil {
		return 0, err
	}
	return maxVersion, nil
}

// DeleteKnowledgeSchema 硬性删除知识库schema
func (d *daoImpl) DeleteKnowledgeSchema(ctx context.Context, corpBizId, appBizId uint64) error {
	q := d.tdsql.TKnowledgeSchema.WithContext(ctx)
	session := q.UnderlyingDB().Table(knowledgeSchemaTableName).
		Where(KnowledgeSchemaTblColCorpBizId+sqlEqual, corpBizId).
		Where(KnowledgeSchemaTblColAppBizId+sqlEqual, appBizId)
	res := session.Delete(&kbe.KnowledgeSchema{})
	if res.Error != nil {
		logx.E(ctx, "DeleteKnowledgeSchema failed for appBizId: %+v, err: %+v",
			appBizId, res.Error)
		return res.Error
	}
	return nil
}

// FindKnowledgeSchema 获取知识库schema
func (d *daoImpl) FindKnowledgeSchema(ctx context.Context, selectColumns []string,
	filter *kbe.KnowledgeSchemaFilter) ([]*kbe.KnowledgeSchema, error) {
	if len(filter.OrderColumn) != len(filter.OrderDirection) {
		logx.E(ctx, "GetKnowledgeSchema invalid order length, "+
			"len(OrderColumn)=%d, len(OrderDirection)=%d", len(filter.OrderColumn), len(filter.OrderDirection))
		return nil, errors.New("invalid order length")
	}
	q := d.tdsql.TKnowledgeSchema.WithContext(ctx)
	tableName := knowledgeSchemaTableName
	if filter.EnvType == entity.EnvTypeProduct {
		tableName = knowledgeSchemaProdTableName
	}
	var knowledgeSchemaList []*kbe.KnowledgeSchema
	session := q.UnderlyingDB().Table(tableName).Select(selectColumns)
	session = generateKnowledgeSchemaCondition(ctx, session, filter)

	res := session.Find(&knowledgeSchemaList)
	if res.Error != nil {
		logx.E(ctx, "GetKnowledgeSchema execute sql failed, err=%+v", res.Error)
		return knowledgeSchemaList, res.Error
	}
	return knowledgeSchemaList, nil
}

// 生成查询条件，必须按照索引的顺序排列
func generateKnowledgeSchemaCondition(ctx context.Context, session *gorm.DB, filter *kbe.KnowledgeSchemaFilter) *gorm.DB {
	if filter.AppBizId != 0 {
		session = session.Where(KnowledgeSchemaTblColAppBizId+sqlEqual, filter.AppBizId)
	}
	if filter.Limit > 0 {
		session = session.Limit(int(filter.Limit))
	}
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			logx.E(ctx, "generateKnowledgeSchemaCondition invalid order direction: %s",
				filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	// is_deleted必须加入查询条件, 否则默认查询未删除的
	if filter.IsDeleted != nil {
		session = session.Where(KnowledgeSchemaTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	} else {
		session = session.Where(KnowledgeSchemaTblColIsDeleted+sqlEqual, kbe.IsNotDeleted)
	}
	return session
}

// CreateKnowledgeSchema 创建知识库schema
func (d *daoImpl) CreateKnowledgeSchema2(ctx context.Context, knowledgeSchema *kbe.KnowledgeSchema) error {
	q := d.tdsql.TKnowledgeSchema.WithContext(ctx)
	res := q.UnderlyingDB().Table(knowledgeSchemaTableName).Create(knowledgeSchema)
	if res.Error != nil {
		logx.E(ctx, "CreateKnowledgeSchema execute sql failed, err=%+v", res.Error)
		return res.Error
	}
	return nil
}

// CreateKnowledgeSchema 创建知识库schema
func (d *daoImpl) CreateKnowledgeSchema(ctx context.Context, knowledgeSchema *kbe.KnowledgeSchema) error {
	q := d.tdsql.TKnowledgeSchema.WithContext(ctx)
	// 使用Model明确指定结构体
	res := q.UnderlyingDB().
		Model(&kbe.KnowledgeSchema{}). // 明确指定模型
		Table(knowledgeSchemaTableName).
		Create(knowledgeSchema)
	if res.Error != nil {
		logx.E(ctx, "CreateKnowledgeSchemaProd执行失败, err=%+v", res.Error)
		return res.Error
	}
	logx.I(ctx, "创建成功, 影响行数: %d", res.RowsAffected)
	return nil
}
