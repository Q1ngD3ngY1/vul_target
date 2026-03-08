package dao

import (
	"context"
	"errors"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"gorm.io/gorm"
	"time"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

var globalCorpCOSDocDao *CorpCOSDocDao

const (
	corpCOSDocTableName = "t_corp_cos_doc"

	CorpCOSDocTblColID              = "id"
	CorpCOSDocTblColBusinessID      = "business_id"
	CorpCOSDocTblColCorpID          = "corp_id"
	CorpCOSDocTblColRobotID         = "robot_id"
	CorpCOSDocTblColStaffID         = "staff_id"
	CorpCOSDocTblColCosBucket       = "cos_bucket"
	CorpCOSDocTblColCosPath         = "cos_path"
	CorpCOSDocTblColCosHash         = "cos_hash"
	CorpCOSDocTblColCosTag          = "cos_tag"
	CorpCOSDocTblColIsDeleted       = "is_deleted"
	CorpCOSDocTblColStatus          = "status"
	CorpCOSDocTblColFailReason      = "fail_reason"
	CorpCOSDocTblColSyncTime        = "sync_time"
	CorpCOSDocTblColBusinessCosURL  = "business_cos_url"
	CorpCOSDocTblColBusinessCosHash = "business_cos_hash"
	CorpCOSDocTblColBusinessCosTag  = "business_cos_tag"
	CorpCOSDocTblColCreateTime      = "create_time"
	CorpCOSDocTblColUpdateTime      = "update_time"
)

// CorpCOSDocFilter 企业cos文档过滤器
type CorpCOSDocFilter struct {
	BusinessIDs     []uint64
	CorpID          uint64
	RobotID         uint64
	CosTag          string
	BusinessCosHash string
}

type CorpCOSDocDao struct {
	BaseDao
}

// GetCorpCOSDocDao 获取全局的数据操作对象
func GetCorpCOSDocDao() *CorpCOSDocDao {
	if globalCorpCOSDocDao == nil {
		globalCorpCOSDocDao = &CorpCOSDocDao{*globalBaseDao}
	}
	return globalCorpCOSDocDao
}

// 生成查询条件，必须按照索引的顺序排列
func (d *CorpCOSDocDao) generateCondition(ctx context.Context, session *gorm.DB, filter *CorpCOSDocFilter) {
	if len(filter.BusinessIDs) != 0 {
		session = session.Where(CorpCOSDocTblColBusinessID+sqlIn, filter.BusinessIDs)
	}
	if filter.CorpID != 0 {
		session = session.Where(CorpCOSDocTblColCorpID+sqlEqual, filter.CorpID)
	}
	if filter.RobotID != 0 {
		session = session.Where(CorpCOSDocTblColRobotID+sqlEqual, filter.RobotID)
	}
	if filter.CosTag != "" {
		session = session.Where(CorpCOSDocTblColCosTag+sqlEqual, filter.CosTag)
	}
	if filter.BusinessCosHash != "" {
		session = session.Where(CorpCOSDocTblColBusinessCosHash+sqlEqual, filter.BusinessCosHash)
	}
}

// GetCorpCosDoc 获取企业cos文档信息
func (d *CorpCOSDocDao) GetCorpCosDoc(ctx context.Context, selectColumns []string, filter CorpCOSDocFilter,
) (*model.CorpCOSDoc, error) {
	doc := new(model.CorpCOSDoc)
	session := d.gormDB.WithContext(ctx).Table(corpCOSDocTableName).Select(selectColumns)
	d.generateCondition(ctx, session, &filter)
	err := session.First(doc).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		log.ErrorContextf(ctx, "GetCorpCosDoc|filter:%+v|err:%+v", filter, err)
		return doc, err
	}
	return doc, nil
}

// CreateCorpCosDoc 创建企业cos文档信息
func (d *CorpCOSDocDao) CreateCorpCosDoc(ctx context.Context, doc *model.CorpCOSDoc) error {
	now := time.Now()
	doc.UpdateTime = now
	doc.CreateTime = now
	err := d.gormDB.WithContext(ctx).Table(corpCOSDocTableName).Create(doc).Error
	if err != nil {
		log.ErrorContextf(ctx, "SaveCorpCosDoc err:%+v", err)
		return err
	}
	return nil
}

// UpdateCorpCosDoc 更新企业cos文档信息
func (d *CorpCOSDocDao) UpdateCorpCosDoc(ctx context.Context, doc *model.CorpCOSDoc) error {
	err := d.gormDB.WithContext(ctx).Table(corpCOSDocTableName).
		Where(CorpCOSDocTblColBusinessID+sqlEqual, doc.BusinessID).
		Updates(doc).Error
	if err != nil {
		log.ErrorContextf(ctx, "UpdateCorpCosDoc err:%+v", err)
		return err
	}
	return nil
}

// GetCorpCosDocs 获取企业cos文档信息
func (d *CorpCOSDocDao) GetCorpCosDocs(ctx context.Context, selectColumns []string, filter CorpCOSDocFilter,
) ([]*model.CorpCOSDoc, error) {
	var docs []*model.CorpCOSDoc
	session := d.gormDB.WithContext(ctx).Table(corpCOSDocTableName).Select(selectColumns)
	d.generateCondition(ctx, session, &filter)
	err := session.Find(&docs).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		log.ErrorContextf(ctx, "GetCorpCosDoc|filter:%+v|err:%+v", filter, err)
		return docs, err
	}
	return docs, nil
}
