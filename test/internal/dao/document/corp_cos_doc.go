package document

import (
	"context"
	"errors"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"gorm.io/gorm"
)

// 生成查询条件，必须按照索引的顺序排列
func (d *daoImpl) generateCondition(ctx context.Context, session *gorm.DB, filter *docEntity.CorpCOSDocFilter) *gorm.DB {
	if len(filter.BusinessIDs) != 0 {
		session = session.Where(docEntity.CorpCOSDocTblColBusinessID+util.SqlIn, filter.BusinessIDs)
	}
	if filter.CorpID != 0 {
		session = session.Where(docEntity.CorpCOSDocTblColCorpID+util.SqlEqual, filter.CorpID)
	}
	if filter.RobotID != 0 {
		session = session.Where(docEntity.CorpCOSDocTblColRobotID+util.SqlEqual, filter.RobotID)
	}
	if filter.CosTag != "" {
		session = session.Where(docEntity.CorpCOSDocTblColCosTag+util.SqlEqual, filter.CosTag)
	}
	if filter.BusinessCosHash != "" {
		session = session.Where(docEntity.CorpCOSDocTblColBusinessCosHash+util.SqlEqual, filter.BusinessCosHash)
	}
	return session
}

// GetCorpCosDoc 获取企业cos文档信息
func (d *daoImpl) DescribeCorpCosDoc(ctx context.Context, selectColumns []string, filter *docEntity.CorpCOSDocFilter,
) (*docEntity.CorpCOSDoc, error) {
	tbl := d.mysql.TCorpCosDoc.WithContext(ctx)
	session := tbl.UnderlyingDB().Table(tbl.TableName())

	if len(selectColumns) > 0 {
		session = session.Select(selectColumns)
	}

	session = d.generateCondition(ctx, session, filter)
	doc := new(model.TCorpCosDoc)
	err := session.First(doc).Error
	if err != nil {
		logx.W(ctx, "GetCorpCosDoc|filter:%+v|err:%+v", filter, err)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return new(docEntity.CorpCOSDoc), nil
		}
		return nil, err
	}
	return ConvertCorpCosDocPO2DO(doc), nil
}

// CreateCorpCosDoc 创建企业cos文档信息
func (d *daoImpl) CreateCorpCosDoc(ctx context.Context, doc *docEntity.CorpCOSDoc) error {
	now := time.Now()
	doc.UpdateTime = now
	doc.CreateTime = now
	tCreate := ConvertCorpCosDocDO2PO(doc)

	tbl := d.mysql.TCorpCosDoc.WithContext(ctx)
	session := tbl.UnderlyingDB()

	err := session.Table(tbl.TableName()).Create(tCreate).Error
	if err != nil {
		logx.E(ctx, "SaveCorpCosDoc err:%+v", err)
		return err
	}
	doc.ID = tCreate.ID
	return nil
}

// ModifyCorpCosDoc 更新企业cos文档信息
func (d *daoImpl) ModifyCorpCosDoc(ctx context.Context, updateColumns []string, filter *docEntity.CorpCOSDocFilter,
	doc *docEntity.CorpCOSDoc) error {
	tUpdate := ConvertCorpCosDocDO2PO(doc)

	tbl := d.mysql.TCorpCosDoc.WithContext(ctx)
	session := tbl.UnderlyingDB().Table(tbl.TableName())

	if len(updateColumns) > 0 {
		session = session.Select(updateColumns)
	}

	session = d.generateCondition(ctx, session, filter)

	if len(filter.BusinessIDs) == 0 {
		session = session.Where(docEntity.CorpCOSDocTblColBusinessID+util.SqlEqual, doc.BusinessID)
	}

	err := session.Updates(tUpdate).Error
	if err != nil {
		logx.E(ctx, "UpdateCorpCosDoc err:%+v", err)
		return err
	}
	return nil
}

// DescribeCorpCosDocList 获取企业cos文档信息
func (d *daoImpl) DescribeCorpCosDocList(ctx context.Context, selectColumns []string, filter *docEntity.CorpCOSDocFilter,
) ([]*docEntity.CorpCOSDoc, error) {
	var docs []*model.TCorpCosDoc
	tbl := d.mysql.TCorpCosDoc.WithContext(ctx)
	session := tbl.UnderlyingDB().Table(tbl.TableName())

	if len(selectColumns) > 0 {
		session = session.Select(selectColumns)
	}

	session = d.generateCondition(ctx, session, filter)
	err := session.Find(&docs).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		logx.E(ctx, "GetCorpCosDoc|filter:%+v|err:%+v", filter, err)
		return []*docEntity.CorpCOSDoc{}, err
	}
	return BatchConvertCorpCosDocPO2DO(docs), nil
}
