package dao

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao/redis"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"github.com/spf13/cast"

	"gorm.io/gorm"
)

type RoleDao struct {
	DB *gorm.DB
}

// GetRoleDao 获取全局的数据操作对象
func GetRoleDao(db *gorm.DB) *RoleDao {
	if db == nil {
		db = globalBaseDao.tdsqlGormDB
	}
	return &RoleDao{DB: db}
}

var chunkNumber = 0

// 单测专用
func SetChunkNumber(num int) {
	chunkNumber = num
}

func GetChunkNumber() int {
	if chunkNumber == 0 {
		chunkNumber = config.GetMainConfig().Permissions.ChunkNumber
	}
	if chunkNumber == 0 {
		chunkNumber = 100
	}
	return chunkNumber
}

type KnowledgeBase struct {
	CorpBizID      uint64
	AppBizID       uint64
	IsDeleted      *int
	Offset         int
	Limit          int
	OrderColumn    []string
	OrderDirection uint32 //0升序，1降序
	Fields         []string
}

// KnowledgeRole 角色信息查询结构
type KnowledgeRoleReq struct {
	KnowledgeBase
	BusinessID  uint64 // 角色业务ID
	Name        string // 角色名称
	SearchWord  string // 搜索名字
	Type        int8   // 角色类型(1 预置 2 自定义)
	Description string // 角色描述
	SearchType  int8   // 整体检索范围(1全部知识 2按知识库)
	BizIDs      []uint64
}

// CreateKnowledgeRole 创建角色信息
func (r *RoleDao) CreateKnowledgeRole(ctx context.Context, record *model.KnowledgeRole) error {
	return r.DB.Create(record).Error
}

// UpdateKnowledgeRole 更新角色信息
func (r *RoleDao) UpdateKnowledgeRole(ctx context.Context, record *model.KnowledgeRole) error {
	return r.DB.Model(record).Omit("corp_biz_id", "app_biz_id", "business_id").Updates(record).Error
}

// DeleteKnowledgeRole 删除角色信息(逻辑删除)
func (r *RoleDao) DeleteKnowledgeRole(ctx context.Context, req *KnowledgeRoleReq) error {
	db := r.DB.Model(&model.KnowledgeRole{}).
		Where(model.ColumnCorpBizID+sqlEqual, req.CorpBizID).
		Where(model.ColumnAppBizID+sqlEqual, req.AppBizID).
		Where(model.ColumnAppBizID+sqlEqual, req.AppBizID).
		Where(model.ColumnDeleted+sqlEqual, 0)
	if req.Type != 0 {
		db = db.Where(model.ColumnType+sqlEqual, req.Type)
	}
	if req.BusinessID != 0 {
		db = db.Where(model.ColumnBusinessID+sqlEqual, req.BusinessID)
	}
	return db.Update(model.ColumnDeleted, 1).Error
}

// ListKnowledgeRoles 查询角色信息列表
func (r *RoleDao) ListKnowledgeRoles(ctx context.Context, req *KnowledgeRoleReq) (int64, []*model.KnowledgeRole, error) {
	var records []*model.KnowledgeRole
	db := r.DB.Model(&model.KnowledgeRole{}).
		Where(model.ColumnCorpBizID+sqlEqual, req.CorpBizID).
		Where(model.ColumnAppBizID+sqlEqual, req.AppBizID).
		Where(model.ColumnDeleted+sqlEqual, 0)
	if req.BusinessID != 0 {
		db = db.Where(model.ColumnBusinessID+sqlEqual, req.BusinessID)
	}
	if req.SearchWord != "" {
		newStr := fmt.Sprintf("%%%s%%", fileNameReplacer.Replace(req.SearchWord))
		db = db.Where(model.ColumnName+sqlLike, newStr)
	}
	if req.Name != "" {
		db = db.Where(model.ColumnName+sqlEqual, req.Name)
	}

	if req.Type != 0 {
		db = db.Where(model.ColumnType+sqlEqual, req.Type)
	}
	if len(req.BizIDs) != 0 {
		db = db.Where(model.ColumnBusinessID+sqlIn, req.BizIDs)
	}
	db = db.Order("type asc").Order("update_time desc")
	count := int64(0)
	db.Count(&count)
	if req.Limit == -1 { // -1 表示仅查询总数
		return count, nil, nil
	}
	if req.Limit != 0 {
		db = db.Offset(req.Offset).Limit(req.Limit)
	}
	return count, records, db.Find(&records).Error
}

// GetRoleList 获取角色列表
func (d *RoleDao) GetRoleList(ctx context.Context, selectColumns []string,
	filter *KnowledgeRoleReq) ([]*model.KnowledgeRole, error) {
	if filter.Limit == 0 {
		filter.Limit = 10
	}
	sql := d.DB.WithContext(ctx).Model(&model.KnowledgeRole{}).Select(selectColumns)
	d.generateCondition(ctx, sql, filter)
	sql.Offset(filter.Offset).Limit(filter.Limit)
	dir := "asc"
	if filter.OrderDirection == 1 {
		dir = "desc"
	}
	for _, column := range filter.OrderColumn {
		sql.Order(column + "" + dir)
	}
	var records []*model.KnowledgeRole
	err := sql.Find(&records).Error
	if err != nil {
		log.ErrorContextf(ctx, "GetRoleList err:%v,filter:%+v", err, filter)
		return nil, err
	}
	return records, nil
}

// generateCondition 生成查询条件
func (d *RoleDao) generateCondition(ctx context.Context, sql *gorm.DB, filter *KnowledgeRoleReq) {
	if filter.CorpBizID != 0 {
		sql.Where(model.ColumnCorpBizID+sqlEqual, filter.CorpBizID)
	}
	if filter.AppBizID != 0 {
		sql.Where(model.ColumnAppBizID+sqlEqual, filter.AppBizID)
	}
	if filter.IsDeleted != nil {
		sql.Where(model.ColumnDeleted+sqlEqual, *filter.IsDeleted)
	}
	if len(filter.BizIDs) != 0 {
		sql.Where(model.ColumnBusinessID+sqlIn, filter.BizIDs)
	}
	if filter.Type != 0 {
		sql.Where(model.ColumnType+sqlEqual, filter.Type)
	}
}

type KnowledgeRoleKnowReq struct {
	KnowledgeBase
	RoleBizID       uint64
	KnowledgeBizID  uint64
	KnowledgeBizIDs []uint64
}

// CreateKnowledgeRoleKnow 创建角色知识库关联
func (r *RoleDao) CreateKnowledgeRoleKnow(ctx context.Context, record *model.KnowledgeRoleKnow) error {
	return r.DB.Create(record).Error
}

// BatchCreateKnowledgeRoleKnow 批量创建角色知识库关联
func (r *RoleDao) BatchCreateKnowledgeRoleKnow(ctx context.Context, records []*model.KnowledgeRoleKnow) error {
	return r.DB.CreateInBatches(records, GetChunkNumber()).Error
}

// DeleteKnowledgeRoleKnow 删除角色知识库关联(逻辑删除)
func (r *RoleDao) DeleteKnowledgeRoleKnow(ctx context.Context, req *KnowledgeRoleKnowReq) error {
	db := r.DB.Model(&model.KnowledgeRoleKnow{}).
		Where(model.ColumnCorpBizID+sqlEqual, req.CorpBizID).
		Where(model.ColumnAppBizID+sqlEqual, req.AppBizID).
		Where(model.ColumnDeleted+sqlEqual, 0)
	if req.RoleBizID != 0 {
		db = db.Where(model.ColumnRoleBizID+sqlEqual, req.RoleBizID)
	}
	if req.KnowledgeBizID != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlEqual, req.KnowledgeBizID)
	}
	if len(req.KnowledgeBizIDs) != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlIn, req.KnowledgeBizIDs)
	}
	return db.Update("is_deleted", 1).Error
}

// ListKnowledgeRoleKnowByRoleID 根据角色ID获取关联的知识库列表
func (r *RoleDao) ListKnowledgeRoleKnow(ctx context.Context, req *KnowledgeRoleKnowReq) ([]*model.KnowledgeRoleKnow, error) {
	var records []*model.KnowledgeRoleKnow
	db := r.DB.Model(&model.KnowledgeRoleKnow{}).
		Where(model.ColumnCorpBizID+sqlEqual, req.CorpBizID).
		Where(model.ColumnAppBizID+sqlEqual, req.AppBizID).
		Where(model.ColumnDeleted+sqlEqual, 0)
	if req.RoleBizID != 0 {
		db = db.Where(model.ColumnRoleBizID+sqlEqual, req.RoleBizID)
	}
	if req.KnowledgeBizID != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlEqual, req.KnowledgeBizID)
	}
	if len(req.KnowledgeBizIDs) != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlIn, req.KnowledgeBizIDs)
	}
	return records, db.Find(&records).Error
}

// UpdateKnowledgeCondition 更新客户库逻辑条件
func (r *RoleDao) UpdateKnowledgeCondition(ctx context.Context, req *model.KnowledgeRoleKnow) (int, error) {
	db := r.DB.Model(&model.KnowledgeRoleKnow{}).
		Where(model.ColumnCorpBizID+sqlEqual, req.CorpBizID).
		Where(model.ColumnAppBizID+sqlEqual, req.AppBizID).Where(model.ColumnDeleted+sqlEqual, 0)
	db = db.Where(model.ColumnRoleBizID+sqlEqual, req.RoleBizID)
	db = db.Where(model.ColumnKnowledgeBizID+sqlEqual, req.KnowledgeBizID)
	res := db.Updates(map[string]interface{}{
		model.ColumnKnowledgeType:  req.KnowledgeType,
		model.ColumnSearchType:     req.SearchType,
		model.ColumnLableCondition: req.LabelCondition,
	})
	return int(res.RowsAffected), res.Error
}

// KnowledgeRoleDoc 角色文档权限查询结构
type KnowledgeRoleDocReq struct {
	KnowledgeBase
	RoleBizID       uint64
	KnowledgeBizID  uint64
	KnowledgeBizIDs []uint64
	DocBizID        uint64
	DocBizIDs       []uint64
}

// CreateKnowledgeRoleDoc 创建角色文档关联
func (r *RoleDao) CreateKnowledgeRoleDoc(ctx context.Context, record *model.KnowledgeRoleDoc) error {
	return r.DB.Create(record).Error
}

// BatchCreateKnowledgeRoleDoc 批量创建角色文档关联
func (r *RoleDao) BatchCreateKnowledgeRoleDoc(ctx context.Context, records []*model.KnowledgeRoleDoc) error {
	return r.DB.CreateInBatches(records, GetChunkNumber()).Error
}

// DeleteKnowledgeRoleDoc 删除角色文档关联(逻辑删除)
func (r *RoleDao) DeleteKnowledgeRoleDoc(ctx context.Context, req *KnowledgeRoleDocReq) error {
	deleteIDs := make([][]uint64, 0, len(req.DocBizIDs))
	if len(req.DocBizIDs) != 0 {
		deleteIDs = slicex.Chunk(req.DocBizIDs, GetChunkNumber())
	}
	for _, ids := range deleteIDs {
		db := r.DB.Model(&model.KnowledgeRoleDoc{}).
			Where(model.ColumnCorpBizID+sqlEqual, req.CorpBizID).
			Where(model.ColumnAppBizID+sqlEqual, req.AppBizID).
			Where(model.ColumnDeleted+sqlEqual, 0)
		if req.RoleBizID != 0 {
			db = db.Where(model.ColumnRoleBizID+sqlEqual, req.RoleBizID)
		}
		if req.KnowledgeBizID != 0 {
			db = db.Where(model.ColumnKnowledgeBizID+sqlEqual, req.KnowledgeBizID)
		}
		if len(req.KnowledgeBizIDs) != 0 {
			db = db.Where(model.ColumnKnowledgeBizID+sqlIn, req.KnowledgeBizIDs)
		}
		if req.DocBizID != 0 {
			db = db.Where(model.ColumnDocBizID+sqlEqual, req.DocBizID)
		}
		if len(req.DocBizIDs) != 0 {
			db = db.Where(model.ColumnDocBizID+sqlIn, ids)
		}
		if v := db.Update(model.ColumnDeleted, 1); v.Error != nil {
			log.ErrorContextf(ctx, "feature_permission DeleteKnowledgeRoleDoc err:%v,req:%v", v.Error, req)
			return v.Error
		}
	}

	return nil
}

// BatchDeleteRoleDoc 批量删除角色文档关联(逻辑删除)
func (r *RoleDao) BatchDeleteRoleDoc(ctx context.Context, corpBizId, appBizId, docBizId uint64) error {
	log.DebugContextf(ctx, "feature_permission BatchDeleteRoleDoc corpBizId:%v,appBizId:%v,docBizId:%v",
		corpBizId, appBizId, docBizId)
	for deleteRows := 10000; deleteRows == 10000; {
		res := r.DB.WithContext(ctx).Model(&model.KnowledgeRoleDoc{}).Where(model.ColumnDeleted+sqlEqual, 0).
			Where(model.ColumnCorpBizID+sqlEqual, corpBizId).
			Where(model.ColumnKnowledgeBizID+sqlEqual, appBizId). //兼容共享知识库处理
			Where(model.ColumnDocBizID+sqlEqual, docBizId).Limit(10000).
			Updates(map[string]interface{}{model.ColumnDeleted: 1, model.ColumnUpdateTime: time.Now()})
		if res.Error != nil { //柔性放过
			log.ErrorContextf(ctx, "feature_permission BatchDeleteRoleDoc err:%v,corpBizId:%v,appBizId:%v,docBizId:%v",
				res.Error, corpBizId, appBizId, docBizId)
			return nil
		}
		deleteRows = int(res.RowsAffected)
	}
	return nil
}

// GetRoleByDocBiz 获取文档关联的所有角色业务ids
func (r *RoleDao) GetRoleByDocBiz(ctx context.Context, appBizId, docBizId uint64) ([]uint64, error) {
	log.DebugContextf(ctx, "feature_permission GetRoleByDocBiz appBizId:%v,docBizId:%v", appBizId, docBizId)
	roleBizIds, maxId, selectRow := make([]uint64, 0), 0, 10000 //一次取1万行，因为一个文档可能被无限个角色引用，这边会有耗时问题
	for selectRow == 10000 {
		var roleList []*model.KnowledgeRoleDoc
		err := r.DB.WithContext(ctx).Model(&model.KnowledgeRoleDoc{}).Where(model.ColumnDeleted+sqlEqual, 0).
			Where(model.ColumnKnowledgeBizID+sqlEqual, appBizId). //兼容共享知识库处理
			Where(model.ColumnDocBizID+sqlEqual, docBizId).
			Where(model.ColumnID+sqlMore, maxId). //避免深分页问题
			Select([]string{model.ColumnID, model.ColumnRoleBizID}).Limit(10000).
			Order(model.ColumnID + " " + SqlOrderByAsc).Find(&roleList).Error
		if err != nil {
			log.ErrorContextf(ctx, "feature_permission GetRoleByDocBiz err:%v,appBizId:%v,docBizId:%v",
				err, appBizId, docBizId)
			return nil, err
		}
		for _, role := range roleList {
			roleBizIds = append(roleBizIds, role.RoleBizID)
		}
		selectRow = len(roleList)
		if selectRow != 0 {
			maxId = int(roleList[selectRow-1].ID)
		}
	}
	return roleBizIds, nil
}

// ListKnowledgeRoleDoc 查询角色文档关联列表
func (r *RoleDao) ListKnowledgeRoleDoc(ctx context.Context, req *KnowledgeRoleDocReq) ([]*model.KnowledgeRoleDoc, error) {
	db := r.DB.Model(&model.KnowledgeRoleDoc{}).
		Where(model.ColumnCorpBizID+sqlEqual, req.CorpBizID).
		Where(model.ColumnAppBizID+sqlEqual, req.AppBizID).
		Where(model.ColumnDeleted+sqlEqual, 0)
	if len(req.Fields) != 0 {
		db = db.Select(req.Fields)
	}
	if req.RoleBizID != 0 {
		db = db.Where(model.ColumnRoleBizID+sqlEqual, req.RoleBizID)
	}
	if req.KnowledgeBizID != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlEqual, req.KnowledgeBizID)
	}
	if len(req.KnowledgeBizIDs) != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlIn, req.KnowledgeBizIDs)
	}
	if req.DocBizID != 0 {
		db = db.Where(model.ColumnDocBizID+sqlEqual, req.DocBizID)
	}
	if len(req.DocBizIDs) != 0 {
		db = db.Where(model.ColumnDocBizID+sqlIn, req.DocBizIDs)
	}
	res := make([]*model.KnowledgeRoleDoc, 0, 5)
	var records []*model.KnowledgeRoleDoc
	err := db.Order(model.ColumnID).FindInBatches(&records, GetChunkNumber(), func(tx *gorm.DB, num int) error {
		log.DebugContextf(ctx, "ListKnowledgeRoleDoc, chunk_number:%v res:%d", num, len(records))
		res = append(res, records...)
		return nil
	}).Error
	return res, err
}

// KnowledgeRoleQA 角色问答权限查询结构
type KnowledgeRoleQAReq struct {
	KnowledgeBase
	RoleBizID       uint64
	KnowledgeBizID  uint64
	KnowledgeBizIDs []uint64
	QABizID         uint64
	QABizIDs        []uint64
}

// CreateKnowledgeRoleQA 创建角色问答关联
func (r *RoleDao) CreateKnowledgeRoleQA(ctx context.Context, record *model.KnowledgeRoleQA) error {
	return r.DB.Create(record).Error
}

// BatchCreateKnowledgeRoleQA 批量创建角色问答关联
func (r *RoleDao) BatchCreateKnowledgeRoleQA(ctx context.Context, records []*model.KnowledgeRoleQA) error {
	return r.DB.CreateInBatches(records, GetChunkNumber()).Error
}

// DeleteKnowledgeRoleQA 删除角色问答关联(逻辑删除)
func (r *RoleDao) DeleteKnowledgeRoleQA(ctx context.Context, req *KnowledgeRoleQAReq) error {
	deleteIDs := make([][]uint64, 0, len(req.QABizIDs))
	if len(req.QABizIDs) != 0 {
		deleteIDs = slicex.Chunk(req.QABizIDs, GetChunkNumber())
	}
	for _, ids := range deleteIDs {
		db := r.DB.Model(&model.KnowledgeRoleQA{}).
			Where(model.ColumnCorpBizID+sqlEqual, req.CorpBizID).
			Where(model.ColumnAppBizID+sqlEqual, req.AppBizID).
			Where(model.ColumnDeleted+sqlEqual, 0)
		if req.RoleBizID != 0 {
			db = db.Where(model.ColumnRoleBizID+sqlEqual, req.RoleBizID)
		}
		if req.KnowledgeBizID != 0 {
			db = db.Where(model.ColumnKnowledgeBizID+sqlEqual, req.KnowledgeBizID)
		}
		if len(req.KnowledgeBizIDs) != 0 {
			db = db.Where(model.ColumnKnowledgeBizID+sqlIn, req.KnowledgeBizIDs)
		}
		if req.QABizID != 0 {
			db = db.Where(model.ColumnQABizID+sqlEqual, req.QABizID)
		}
		if len(req.QABizIDs) != 0 {
			db = db.Where(model.ColumnQABizID+sqlIn, ids)
		}
		if err := db.Update(model.ColumnDeleted, 1).Error; err != nil {
			log.ErrorContextf(ctx, "DeleteKnowledgeRoleQA failed, err: %v", err)
			return err
		}
	}
	return nil
}

// ListKnowledgeRoleQA 查询角色问答关联列表
func (r *RoleDao) ListKnowledgeRoleQA(ctx context.Context, req *KnowledgeRoleQAReq) ([]*model.KnowledgeRoleQA, error) {
	db := r.DB.Model(&model.KnowledgeRoleQA{}).
		Where(model.ColumnCorpBizID+sqlEqual, req.CorpBizID).
		Where(model.ColumnAppBizID+sqlEqual, req.AppBizID).
		Where(model.ColumnDeleted+sqlEqual, 0)

	if len(req.Fields) != 0 {
		db = db.Select(req.Fields)
	}

	if req.RoleBizID != 0 {
		db = db.Where(model.ColumnRoleBizID+sqlEqual, req.RoleBizID)
	}
	if req.KnowledgeBizID != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlEqual, req.KnowledgeBizID)
	}
	if len(req.KnowledgeBizIDs) != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlIn, req.KnowledgeBizIDs)
	}
	if req.QABizID != 0 {
		db = db.Where(model.ColumnQABizID+sqlEqual, req.QABizID)
	}
	if len(req.QABizIDs) != 0 {
		db = db.Where(model.ColumnQABizID+sqlIn, req.QABizIDs)
	}
	res := make([]*model.KnowledgeRoleQA, 0, 5)
	var records []*model.KnowledgeRoleQA
	err := db.Order(model.ColumnID).FindInBatches(&records, GetChunkNumber(), func(tx *gorm.DB, num int) error {
		log.DebugContextf(ctx, "ListKnowledgeRoleQa, chunk_number:%v res:%d", num, len(records))
		res = append(res, records...)
		return nil
	}).Error
	return res, err
}

// KnowledgeRoleAttributeLabel 角色标签权限查询结构
type KnowledgeRoleAttributeLabelReq struct {
	KnowledgeBase
	RoleBizID       uint64
	KnowledgeBizID  uint64
	KnowledgeBizIDs []uint64
	AttrBizID       uint64
	AttrBizIDs      []uint64
	LabelBizID      uint64
	LabelBizIDs     []uint64
}

// CreateKnowledgeRoleAttributeLabel 创建角色标签权限关联
func (r *RoleDao) CreateKnowledgeRoleAttributeLabel(ctx context.Context, record *model.KnowledgeRoleAttributeLabel) error {
	return r.DB.Create(record).Error
}

// BatchCreateKnowledgeRoleAttributeLabel 批量创建角色标签权限关联
func (r *RoleDao) BatchCreateKnowledgeRoleAttributeLabel(ctx context.Context, records []*model.KnowledgeRoleAttributeLabel) error {
	return r.DB.CreateInBatches(records, GetChunkNumber()).Error
}

// DeleteKnowledgeRoleAttributeLabel 删除角色标签权限关联(逻辑删除)
func (r *RoleDao) DeleteKnowledgeRoleAttributeLabel(ctx context.Context, req *KnowledgeRoleAttributeLabelReq) (int64, error) {
	db := r.DB.Model(&model.KnowledgeRoleAttributeLabel{}).
		Where(model.ColumnCorpBizID+sqlEqual, req.CorpBizID).
		Where(model.ColumnAppBizID+sqlEqual, req.AppBizID).
		Where(model.ColumnDeleted+sqlEqual, 0)
	if req.RoleBizID != 0 {
		db = db.Where(model.ColumnRoleBizID+sqlEqual, req.RoleBizID)
	}
	if req.KnowledgeBizID != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlEqual, req.KnowledgeBizID)
	}
	if len(req.KnowledgeBizIDs) != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlIn, req.KnowledgeBizIDs)
	}
	if req.AttrBizID != 0 {
		db = db.Where(model.ColumnAttrBizID+sqlEqual, req.AttrBizID)
	}
	if req.LabelBizID != 0 {
		db = db.Where(model.ColumnLabelBizID+sqlEqual, req.LabelBizID)
	}
	if len(req.LabelBizIDs) != 0 {
		db = db.Where(model.ColumnLabelBizID+sqlIn, req.LabelBizIDs)
	}
	if len(req.AttrBizIDs) != 0 {
		db = db.Where(model.ColumnAttrBizID+sqlIn, req.AttrBizIDs)
	}
	if req.Limit != 0 {
		db = db.Limit(req.Limit)
	}
	res := db.Update(model.ColumnDeleted, 1)
	return res.RowsAffected, res.Error
}

// ListKnowledgeRoleAttributeLabel 查询角色标签权限关联列表
func (r *RoleDao) ListKnowledgeRoleAttributeLabel(ctx context.Context, req *KnowledgeRoleAttributeLabelReq) ([]*model.KnowledgeRoleAttributeLabel, error) {
	var records []*model.KnowledgeRoleAttributeLabel
	db := r.DB.Model(&model.KnowledgeRoleAttributeLabel{}).Where(model.ColumnDeleted+sqlEqual, 0)
	if req.CorpBizID != 0 {
		db = db.Where(model.ColumnCorpBizID+sqlEqual, req.CorpBizID)
	}
	if req.AppBizID != 0 {
		db = db.Where(model.ColumnAppBizID+sqlEqual, req.AppBizID)

	}
	if req.RoleBizID != 0 {
		db = db.Where(model.ColumnRoleBizID+sqlEqual, req.RoleBizID)
	}
	if req.KnowledgeBizID != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlEqual, req.KnowledgeBizID)
	}
	if len(req.KnowledgeBizIDs) != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlIn, req.KnowledgeBizIDs)
	}
	if req.AttrBizID != 0 {
		db = db.Where(model.ColumnAttrBizID+sqlEqual, req.AttrBizID)
	}
	if len(req.AttrBizIDs) != 0 {
		db = db.Where(model.ColumnAttrBizID+sqlIn, req.AttrBizIDs)
	}
	if len(req.LabelBizIDs) != 0 {
		db = db.Where(model.ColumnLabelBizID+sqlIn, req.LabelBizIDs)
	}
	if req.LabelBizID != 0 {
		db = db.Where(model.ColumnLabelBizID+sqlEqual, req.LabelBizID)
	}
	return records, db.Find(&records).Error
}

// BatchDeleteRoleLabel 删除角色标签值权限关联(逻辑删除)
func (r *RoleDao) BatchDeleteRoleLabel(ctx context.Context, knowBizID uint64, labelBizIds []uint64) error {
	log.DebugContextf(ctx, "feature_permission BatchDeleteRoleLabel knowBizID:%v,labelBizIds:%v", knowBizID, labelBizIds)

	// 提前获取所属知识库，用于更新缓存
	defer r.UpdateRoleKnowledgeByAttrChange(ctx, knowBizID, nil, labelBizIds)()

	pageSize, length := 200, len(labelBizIds)
	for start := 0; start < length; start += pageSize {
		deleteMaxRow, end := int64(10000), min(start+pageSize, length)
		tmp := labelBizIds[start:end]
		for deleteMaxRow == 10000 { //每次删除一万行
			res := r.DB.Model(&model.KnowledgeRoleAttributeLabel{}).
				Where(model.ColumnKnowledgeBizID+sqlEqual, knowBizID). //兼容共享知识库需求
				Where(model.ColumnLabelBizID+sqlIn, tmp).Limit(10000).
				Update(model.ColumnDeleted, 1)
			if res.Error != nil {
				log.ErrorContextf(ctx, "feature_permission BatchDeleteRoleLabel err:%v,knowBizID:%v,labelBizIds:%v",
					res.Error, knowBizID, labelBizIds)
				return res.Error
			}
			deleteMaxRow = res.RowsAffected
		}
	}
	return nil
}

// BatchDeleteRoleAttribute 删除角色标签权限关联(逻辑删除)
func (r *RoleDao) BatchDeleteRoleAttribute(ctx context.Context, knowBizID uint64, attrBizIds []uint64) error {
	log.DebugContextf(ctx, "feature_permission BatchDeleteRoleLabel knowBizID:%v,labelBizIds:%v", knowBizID, attrBizIds)

	// 标签变更用于更新缓存
	defer r.UpdateRoleKnowledgeByAttrChange(ctx, knowBizID, attrBizIds, nil)()

	pageSize, length := 200, len(attrBizIds) //200个一批
	for start := 0; start < length; start += pageSize {
		deleteMaxRow, end := int64(10000), min(start+pageSize, length)
		tmp := attrBizIds[start:end]
		for deleteMaxRow == 10000 { //每次删除一万行
			res := r.DB.Model(&model.KnowledgeRoleAttributeLabel{}).
				Where(model.ColumnKnowledgeBizID+sqlEqual, knowBizID). //兼容共享知识库需求
				Where(model.ColumnAttrBizID+sqlIn, tmp).Limit(10000).
				Update(model.ColumnDeleted, 1)
			if res.Error != nil {
				log.ErrorContextf(ctx, "feature_permission BatchDeleteRoleAttribute err:%v,knowBizID:%v,attrBizIds:%v",
					res.Error, knowBizID, attrBizIds)
				return res.Error
			}
			deleteMaxRow = res.RowsAffected
		}
	}

	return nil
}

// KnowledgeRoleCateReq 角色分类权限查询结构
type KnowledgeRoleCateReq struct {
	KnowledgeBase
	RoleBizID       uint64
	KnowledgeBizID  uint64
	KnowledgeBizIDs []uint64
	CatType         uint64
	CateBizID       uint64
	CateBizIDs      []uint64
}

// CreateKnowledgeRoleCate 创建角色分类关联
func (r *RoleDao) CreateKnowledgeRoleCate(ctx context.Context, record *model.KnowledgeRoleCate) error {
	return r.DB.Create(record).Error
}

// BatchCreateKnowledgeRoleCate 批量创建角色分类关联
func (r *RoleDao) BatchCreateKnowledgeRoleCate(ctx context.Context, records []*model.KnowledgeRoleCate) error {
	return r.DB.CreateInBatches(records, GetChunkNumber()).Error
}

// DeleteKnowledgeRoleCate 删除角色分类关联(逻辑删除)
func (r *RoleDao) DeleteKnowledgeRoleCate(ctx context.Context, req *KnowledgeRoleCateReq) (int64, error) {

	db := r.DB.Model(&model.KnowledgeRoleCate{}).
		Where(model.ColumnCorpBizID+sqlEqual, req.CorpBizID).
		Where(model.ColumnAppBizID+sqlEqual, req.AppBizID).
		Where(model.ColumnDeleted+sqlEqual, 0)
	if req.RoleBizID != 0 {
		db = db.Where(model.ColumnRoleBizID+sqlEqual, req.RoleBizID)
	}
	if req.KnowledgeBizID != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlEqual, req.KnowledgeBizID)
	}
	if len(req.KnowledgeBizIDs) != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlIn, req.KnowledgeBizIDs)
	}
	if req.CateBizID != 0 {
		db = db.Where(model.ColumnCateBizID+sqlEqual, req.CateBizID)
	}
	if req.CatType != 0 {
		db = db.Where(model.ColumnCateType+sqlEqual, req.CatType)
	}
	if len(req.CateBizIDs) != 0 {
		db = db.Where(model.ColumnCateBizID+sqlIn, req.CateBizIDs)
	}
	if req.Limit != 0 {
		db = db.Limit(req.Limit)
	}
	res := db.Update(model.ColumnDeleted, 1)
	return res.RowsAffected, res.Error
}

// ListKnowledgeRoleCate 查询角色分类关联列表
func (r *RoleDao) ListKnowledgeRoleCate(ctx context.Context, req *KnowledgeRoleCateReq) ([]*model.KnowledgeRoleCate, error) {
	var records []*model.KnowledgeRoleCate
	db := r.DB.Model(&model.KnowledgeRoleCate{}).
		Where(model.ColumnCorpBizID+sqlEqual, req.CorpBizID).
		Where(model.ColumnDeleted+sqlEqual, 0)

	if req.AppBizID != 0 {
		db = db.Where(model.ColumnAppBizID+sqlEqual, req.AppBizID)
	}
	if req.RoleBizID != 0 {
		db = db.Where(model.ColumnRoleBizID+sqlEqual, req.RoleBizID)
	}
	if req.KnowledgeBizID != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlEqual, req.KnowledgeBizID)
	}
	if len(req.KnowledgeBizIDs) != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlIn, req.KnowledgeBizIDs)
	}

	if req.CateBizID != 0 {
		db = db.Where(model.ColumnCateBizID+sqlEqual, req.CateBizID)
	}
	if len(req.CateBizIDs) != 0 {
		db = db.Where(model.ColumnCateBizID+sqlIn, req.CateBizIDs)
	}
	return records, db.Find(&records).Error
}

// BatchDeleteRoleCate 批量删除角色分类关联(逻辑删除)
func (r *RoleDao) BatchDeleteRoleCate(ctx context.Context, knowBizID, cateBizId uint64) error {
	log.DebugContextf(ctx, "feature_permission BatchDeleteRoleCate knowBizID:%v,cateBizId:%v", knowBizID, cateBizId)
	for deleteRows := 10000; deleteRows == 10000; { //每次删除一万行
		res := r.DB.Model(&model.KnowledgeRoleCate{}).
			Where(model.ColumnKnowledgeBizID+sqlEqual, knowBizID). //兼容共享知识库需求
			Where(model.ColumnCateBizID+sqlEqual, cateBizId).Limit(10000).
			Update(model.ColumnDeleted, 1)
		if res.Error != nil {
			log.ErrorContextf(ctx, "feature_permission BatchDeleteRoleCate err:%v,knowBizID:%v,cateBizId:%v",
				res.Error, knowBizID, cateBizId)
			return res.Error
		}
		deleteRows = int(res.RowsAffected)
	}
	return nil
}

// KnowledgeRoleDatabaseReq 角色数据库权限查询结构
type KnowledgeRoleDatabaseReq struct {
	KnowledgeBase
	RoleBizID       uint64
	KnowledgeBizID  uint64
	KnowledgeBizIDs []uint64
	DatabaseBizID   uint64
	DatabaseBizIDs  []uint64
}

// CreateKnowledgeRoleDatabase 创建角色数据库关联
func (r *RoleDao) CreateKnowledgeRoleDatabase(ctx context.Context, record *model.KnowledgeRoleDatabase) error {
	return r.DB.Create(record).Error
}

// BatchCreateKnowledgeRoleDatabase 批量创建角色数据库关联
func (r *RoleDao) BatchCreateKnowledgeRoleDatabase(ctx context.Context, records []*model.KnowledgeRoleDatabase) error {
	return r.DB.CreateInBatches(records, GetChunkNumber()).Error
}

// DeleteKnowledgeRoleDatabase 删除角色数据库关联(逻辑删除)
func (r *RoleDao) DeleteKnowledgeRoleDatabase(ctx context.Context, req *KnowledgeRoleDatabaseReq) (uint64, error) {
	deleteIDs := make([][]uint64, 0, len(req.DatabaseBizIDs))
	if len(req.DatabaseBizIDs) != 0 {
		deleteIDs = slicex.Chunk(req.DatabaseBizIDs, GetChunkNumber())
	}
	updateCnt := uint64(0)
	for _, ids := range deleteIDs {
		db := r.DB.Model(&model.KnowledgeRoleDatabase{}).
			Where(model.ColumnCorpBizID+sqlEqual, req.CorpBizID).
			Where(model.ColumnAppBizID+sqlEqual, req.AppBizID).
			Where(model.ColumnDeleted+sqlEqual, 0)
		if req.RoleBizID != 0 {
			db = db.Where(model.ColumnRoleBizID+sqlEqual, req.RoleBizID)
		}
		if req.KnowledgeBizID != 0 {
			db = db.Where(model.ColumnKnowledgeBizID+sqlEqual, req.KnowledgeBizID)
		}
		if len(req.KnowledgeBizIDs) != 0 {
			db = db.Where(model.ColumnKnowledgeBizID+sqlIn, req.KnowledgeBizIDs)
		}

		if req.DatabaseBizID != 0 {
			db = db.Where(model.ColumnDatabaseBizID+sqlEqual, req.DatabaseBizID)
		}
		if len(req.DatabaseBizIDs) != 0 {
			db = db.Where(model.ColumnDatabaseBizID+sqlIn, ids)
		}
		if req.Limit != 0 {
			db = db.Limit(req.Limit)
		}
		res := db.Update(model.ColumnDeleted, 1)
		if res.Error != nil {
			log.ErrorContextf(ctx, "feature_permission DeleteKnowledgeRoleDatabase err:%v,req:%v",
				res.Error, req)
			return 0, res.Error
		}
		updateCnt += uint64(res.RowsAffected)
	}
	return updateCnt, nil
}

// DeleteKnowledgeRoleDbTables 删除角色数据表关联(逻辑删除)
func (r *RoleDao) DeleteKnowledgeRoleDbTables(ctx context.Context, knowBizID uint64, dbTableBizIDs []uint64) error {
	log.DebugContextf(ctx, "DeleteKnowledgeRoleDbTables knowBizID:%v,dbTableBizIDs:%v", knowBizID, dbTableBizIDs)
	pageSize, length := 200, len(dbTableBizIDs) //200个一批
	for start := 0; start < length; start += pageSize {
		deleteMaxRow, end := int64(10000), min(start+pageSize, length)
		tmp := dbTableBizIDs[start:end]
		for deleteMaxRow == 10000 { //每次删除一万行
			res := r.DB.Model(&model.KnowledgeRoleDatabase{}).
				Where(model.ColumnKnowledgeBizID+sqlEqual, knowBizID). //兼容共享知识库需求
				Where(model.ColumnDatabaseBizID+sqlIn, tmp).Limit(10000).
				Update(model.ColumnDeleted, 1)
			if res.Error != nil {
				log.ErrorContextf(ctx, "DeleteKnowledgeRoleDbTables err:%v,knowBizID:%v,dbTableBizIDs:%v",
					res.Error, knowBizID, dbTableBizIDs)
				return res.Error
			}
			deleteMaxRow = res.RowsAffected
		}
	}
	return nil
}

// ListKnowledgeRoleDatabase 查询角色数据库关联列表
func (r *RoleDao) ListKnowledgeRoleDatabase(ctx context.Context, req *KnowledgeRoleDatabaseReq) ([]*model.KnowledgeRoleDatabase, error) {
	var records []*model.KnowledgeRoleDatabase
	db := r.DB.Model(&model.KnowledgeRoleDatabase{}).
		Where(model.ColumnCorpBizID+sqlEqual, req.CorpBizID).
		Where(model.ColumnAppBizID+sqlEqual, req.AppBizID).
		Where(model.ColumnDeleted+sqlEqual, 0)
	if req.RoleBizID != 0 {
		db = db.Where(model.ColumnRoleBizID+sqlEqual, req.RoleBizID)
	}
	if req.KnowledgeBizID != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlEqual, req.KnowledgeBizID)
	}
	if len(req.KnowledgeBizIDs) != 0 {
		db = db.Where(model.ColumnKnowledgeBizID+sqlIn, req.KnowledgeBizIDs)
	}
	if req.DatabaseBizID != 0 {
		db = db.Where(model.ColumnDatabaseBizID+sqlEqual, req.DatabaseBizID)
	}
	if len(req.DatabaseBizIDs) != 0 {
		db = db.Where(model.ColumnDatabaseBizID+sqlIn, req.DatabaseBizIDs)
	}
	return records, db.Find(&records).Error
}

// GetRoleByDbBiz 获取数据库关联的所有角色业务ids
func (r *RoleDao) GetRoleByDbBiz(ctx context.Context, knowBizID, dbBizId uint64) ([]uint64, error) {
	log.DebugContextf(ctx, "GetRoleByDbBiz knowBizID:%v,dbBizId:%v", knowBizID, dbBizId)
	roleBizIds, maxId, selectRow := make([]uint64, 0), 0, 10000 //一次取1万行，因为一个文档可能被无限个角色引用，这边会有耗时问题
	for selectRow == 10000 {
		var roleList []*model.KnowledgeRoleDatabase
		err := r.DB.WithContext(ctx).Model(&model.KnowledgeRoleDatabase{}).Where(model.ColumnDeleted+sqlEqual, 0).
			Where(model.ColumnKnowledgeBizID+sqlEqual, knowBizID). //兼容共享知识库处理
			Where(model.ColumnDatabaseBizID+sqlEqual, dbBizId).
			Where(model.ColumnID+sqlMore, maxId). //避免深分页问题
			Select([]string{model.ColumnID, model.ColumnRoleBizID}).Limit(10000).
			Order(model.ColumnID + " " + SqlOrderByAsc).Find(&roleList).Error
		if err != nil {
			log.ErrorContextf(ctx, "GetRoleByDbBiz err:%v,knowBizID:%v,dbBizId:%v", err, knowBizID, dbBizId)
			return nil, err
		}
		for _, role := range roleList {
			roleBizIds = append(roleBizIds, role.RoleBizID)
		}
		selectRow = len(roleList)
		if selectRow != 0 {
			maxId = int(roleList[selectRow-1].ID)
		}
	}
	return roleBizIds, nil
}

// RemoveKnowledgeAssociation 移除知识库关联关系
func (r *RoleDao) RemoveKnowledgeAssociation(ctx context.Context, corpBizID, appBizID uint64, knowledgeBizIds []uint64) error {
	if len(knowledgeBizIds) == 0 {
		return errors.New("knowledgeBizIds is empty")
	}

	client := r

	// 查询角色对应的知识库关联
	knows, err := client.ListKnowledgeRoleKnow(ctx, &KnowledgeRoleKnowReq{
		KnowledgeBase: KnowledgeBase{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
		},
		KnowledgeBizIDs: knowledgeBizIds,
	})
	if err != nil {
		log.ErrorContextf(ctx, "ListKnowledgeRoleKnow failed, err: %v", err)
		return err
	}

	role2knows := make(map[uint64][]uint64, 3)
	for _, know := range knows {
		if _, ok := role2knows[know.RoleBizID]; !ok {
			role2knows[know.RoleBizID] = make([]uint64, 0, 3)
		}
		role2knows[know.RoleBizID] = append(role2knows[know.RoleBizID], know.KnowledgeBizID)
	}

	log.DebugContextf(ctx, "role2knows: %v remove:%+v", role2knows, knowledgeBizIds)

	// 删除知识库关联
	if err := client.DeleteKnowledgeRoleKnow(ctx, &KnowledgeRoleKnowReq{
		KnowledgeBase: KnowledgeBase{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
		},
		KnowledgeBizIDs: knowledgeBizIds,
	}); err != nil {
		log.ErrorContextf(ctx, "DeleteKnowledgeRoleKnow failed, err: %v", err)
		return err
	}

	go func(ctx1 context.Context) {
		defer func() {
			if err := recover(); err != nil {
				log.ErrorContextf(ctx1, "RemoveKnowledgeAssociation failed, err: %v", err)
				return
			}
		}()

		// 删除文档关联
		if err := client.DeleteKnowledgeRoleDoc(ctx1, &KnowledgeRoleDocReq{
			KnowledgeBase: KnowledgeBase{
				CorpBizID: corpBizID,
				AppBizID:  appBizID,
			},
			KnowledgeBizIDs: knowledgeBizIds,
		}); err != nil {
			log.ErrorContextf(ctx, "DeleteKnowledgeRoleDoc failed, err: %v", err)
			return
		}

		// 删除问答关联
		if err := client.DeleteKnowledgeRoleQA(ctx1, &KnowledgeRoleQAReq{
			KnowledgeBase: KnowledgeBase{
				CorpBizID: corpBizID,
				AppBizID:  appBizID,
			},
			KnowledgeBizIDs: knowledgeBizIds,
		}); err != nil {
			log.ErrorContextf(ctx, "DeleteKnowledgeRoleQA failed, err: %v", err)
			return
		}

		// 删除分类关联
		if _, err := client.DeleteKnowledgeRoleCate(ctx1, &KnowledgeRoleCateReq{
			KnowledgeBase: KnowledgeBase{
				CorpBizID: corpBizID,
				AppBizID:  appBizID,
			},
			KnowledgeBizIDs: knowledgeBizIds,
		}); err != nil {
			log.ErrorContextf(ctx, "DeleteKnowledgeRoleCate failed, err: %v", err)
			return
		}

		// 删除标签关联
		if _, err := client.DeleteKnowledgeRoleAttributeLabel(ctx1, &KnowledgeRoleAttributeLabelReq{
			KnowledgeBase: KnowledgeBase{
				CorpBizID: corpBizID,
				AppBizID:  appBizID,
			},
			KnowledgeBizIDs: knowledgeBizIds,
		}); err != nil {
			log.ErrorContextf(ctx, "DeleteKnowledgeRoleAttributeLabel failed, err: %v", err)
			return
		}

		// 删除数据库关联
		if _, err := client.DeleteKnowledgeRoleDatabase(ctx1, &KnowledgeRoleDatabaseReq{
			KnowledgeBase: KnowledgeBase{
				CorpBizID: corpBizID,
				AppBizID:  appBizID,
			},
			KnowledgeBizIDs: knowledgeBizIds,
		}); err != nil {
			log.ErrorContextf(ctx, "DeleteKnowledgeRoleAttributeLabel failed, err: %v", err)
			return
		}

		// 删除缓存
		for k, knows := range role2knows {
			if err := client.CleanKnowledgeCache(ctx1, corpBizID, appBizID, k, knows); err != nil {
				log.ErrorContextf(ctx, "CleanKnowledgeCache failed, err: %v", err)
				return
			}
		}
	}(trpc.CloneContext(ctx))
	return nil
}

// 删除知识库关联缓存
func (l *RoleDao) CleanKnowledgeCache(ctx context.Context, corpBizID, appBizID uint64, roleBizID uint64, knowledgeBizIds []uint64) error {
	log.DebugContextf(ctx, "CleanKnowledgeCachCleanKnowledgeCachee corpBizID: %d, appBizID: %d, roleBizID: %d, knowledgeBizIds: %+v", corpBizID, appBizID, roleBizID, knowledgeBizIds)
	redisCli, err := redis.GetGoRedisClient(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "GetGoRedisClient failed, err: %v", err)
		return err
	}
	for _, know := range knowledgeBizIds {
		key := fmt.Sprintf(model.RoleKnowledgeRedisKey, corpBizID, appBizID, roleBizID)
		if err := redisCli.HDel(ctx, key, cast.ToString(know)).Err(); err != nil {
			log.ErrorContextf(ctx, "HDel failed, key: %s, field: %d err:%v", key, know, err)
			return err
		}
	}
	return nil
}

// UpdateRoleKnowledgeByAttrChange 更新缓存
func (l *RoleDao) UpdateRoleKnowledgeByAttrChange(ctx context.Context, knowBizID uint64, attrBizIds []uint64, labelBizIds []uint64) func() error {
	updates, err := l.ListKnowledgeRoleAttributeLabel(ctx, &KnowledgeRoleAttributeLabelReq{
		KnowledgeBizID: knowBizID,
		AttrBizIDs:     attrBizIds,
		LabelBizIDs:    labelBizIds,
	})
	log.DebugContextf(ctx, "UpdateRoleKnowledgeByAttrChange %+v req:%d %+v %+v", updates, knowBizID, attrBizIds, labelBizIds)
	return func() error {
		if err != nil {
			return err
		}
		for _, update := range updates {
			if err := l.UpdateRoleKnowledgeCache(ctx, update.CorpBizID, update.AppBizID, update.RoleBizID, update.KnowledgeBizID); err != nil {
				log.ErrorContextf(ctx, "UpdateRoleKnowledgeCache failed, err: %v", err)
				return err
			}
		}
		return nil
	}
}

// UpdateRoleKnowledgeCache 更新缓存
func (l *RoleDao) UpdateRoleKnowledgeCache(ctx context.Context, corpBizID, appBizID uint64, roleBizID uint64, knowBizID uint64) error {
	key := fmt.Sprintf(model.RoleKnowledgeRedisKey, corpBizID, appBizID, roleBizID)
	log.DebugContextf(ctx, "UpdateRoleKnowledgeCache key: %s, field: %d", key, knowBizID)
	redisCli, err := redis.GetGoRedisClient(ctx)
	if err != nil {
		return err
	}

	if redisCli.Exists(ctx, key).Val() == 1 {
		err = redisCli.HSet(ctx, key, cast.ToString(knowBizID), "").Err()
		if err != nil {
			log.ErrorContextf(ctx, "HSet failed, key: %s, field: %d", key, knowBizID)
			return err
		}
	}
	return nil
}

// UpdateRoleKnowledgeByCate 更新缓存
func (l *RoleDao) UpdateRoleKnowledgeByCate(ctx context.Context, corpBizID, knowBizID uint64, cateBizIDs []uint64) error {
	defer func() {
		if err := recover(); err != nil {
			log.ErrorContextf(ctx, "UpdateRoleKnowledgeByCate panic, err: %v", err)
			return
		}
	}()
	log.DebugContextf(ctx, "UpdateRoleKnowledgeByCate corpBizID: %d,  knowBizID: %d, cateBizIDs: %+v", corpBizID, knowBizID, cateBizIDs)
	updates, err := l.ListKnowledgeRoleCate(ctx, &KnowledgeRoleCateReq{
		KnowledgeBase: KnowledgeBase{
			CorpBizID: corpBizID,
		},
		KnowledgeBizID: knowBizID,
		CateBizIDs:     cateBizIDs,
	})
	if err != nil {
		return err
	}
	for _, update := range updates {
		if err := l.UpdateRoleKnowledgeCache(ctx, update.CorpBizID, update.AppBizID, update.RoleBizID, update.KnowledgeBizID); err != nil {
			log.ErrorContextf(ctx, "UpdateRoleKnowledgeCache failed, err: %v", err)
			return err
		}
	}
	return nil
}
