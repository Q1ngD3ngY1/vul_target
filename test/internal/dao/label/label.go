package label

import (
	"context"
	"fmt"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/boolx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"gorm.io/gen"
	"gorm.io/gen/field"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	sqlEqual     = " = ?"
	sqlNotEqual  = " != ?"
	sqlLess      = " < ?"
	sqlLessEqual = " <= ?"
	sqlMore      = " > ?"
	sqlMoreEqual = " >= ?"
	sqlLike      = " LIKE ?"
	sqlIn        = " IN ?"
	sqlSubIn     = " IN (?)"
	sqlSubNotIn  = " NOT IN (?)"
	sqlOrderAND  = " AND "
	sqlOr        = " OR "

	SqlOrderByAsc  = "ASC"
	SqlOrderByDesc = "DESC"

	IsNotDeleted = 0
	IsDeleted    = 1
)

var (
	attributeTableName          = "t_attribute"
	attributeProdTableName      = "t_attribute_prod"
	attributeLabelProdTableName = "t_attribute_label_prod"
	attributeLabelTableName     = "t_attribute_label"
	docAttributeLabelTableName  = "t_doc_attribute_label"
	attributeLabelTaskTableName = "t_attribute_label_task"
	docTableName                = "t_doc"
	docQaTableName              = "t_doc_qa"
	qaAttributeLabelTableName   = "t_qa_attribute_label"
)

func getAttributeListsPO2DO(pos []*model.TAttribute) []*entity.Attribute {
	return slicex.Map(pos, func(po *model.TAttribute) *entity.Attribute {
		return getAttributeListPO2DO(po)
	})
}

func getAttributeListPO2DO(po *model.TAttribute) *entity.Attribute {
	if po == nil {
		return nil
	}
	return &entity.Attribute{
		ID:            po.ID,
		BusinessID:    po.BusinessID,
		RobotID:       po.RobotID,
		AttrKey:       po.AttrKey,
		Name:          po.Name,
		IsUpdating:    po.IsUpdating,
		ReleaseStatus: po.ReleaseStatus,
		NextAction:    po.NextAction,
		IsDeleted:     po.IsDeleted,
		DeletedTime:   po.DeletedTime,
		CreateTime:    po.CreateTime,
		UpdateTime:    po.UpdateTime,
	}
}

func getQAAttributeLabelsPO2DO(pos []*model.TQaAttributeLabel) []*entity.QAAttributeLabel {
	return slicex.Map(pos, func(po *model.TQaAttributeLabel) *entity.QAAttributeLabel {
		return getQAAttributeLabelPO2DO(po)
	})
}

func getQAAttributeLabelPO2DO(po *model.TQaAttributeLabel) *entity.QAAttributeLabel {
	if po == nil {
		return nil
	}
	return &entity.QAAttributeLabel{
		ID:         po.ID,
		RobotID:    po.RobotID,
		QAID:       po.QaID,
		Source:     po.Source,
		AttrID:     po.AttrID,
		LabelID:    po.LabelID,
		IsDeleted:  po.IsDeleted,
		CreateTime: po.CreateTime,
		UpdateTime: po.CreateTime,
	}
}

func getQAAttributeLabelsDO2PO(pos []*entity.QAAttributeLabel) []*model.TQaAttributeLabel {
	return slicex.Map(pos, func(do *entity.QAAttributeLabel) *model.TQaAttributeLabel {
		return getQAAttributeLabelDO2PO(do)
	})
}

func getQAAttributeLabelDO2PO(do *entity.QAAttributeLabel) *model.TQaAttributeLabel {
	if do == nil {
		return nil
	}
	return &model.TQaAttributeLabel{
		ID:         do.ID,
		RobotID:    do.RobotID,
		QaID:       do.QAID,
		Source:     do.Source,
		AttrID:     do.AttrID,
		LabelID:    do.LabelID,
		IsDeleted:  do.IsDeleted,
		CreateTime: do.CreateTime,
		UpdateTime: do.CreateTime,
	}
}
func getDocAttributeLabelsPO2DO(pos []*model.TDocAttributeLabel) []*entity.DocAttributeLabel {
	return slicex.Map(pos, func(po *model.TDocAttributeLabel) *entity.DocAttributeLabel {
		return getDocAttributeLabelPO2DO(po)
	})
}

func getDocAttributeLabelPO2DO(po *model.TDocAttributeLabel) *entity.DocAttributeLabel {
	if po == nil {
		return nil
	}
	return &entity.DocAttributeLabel{
		ID:         po.ID,
		RobotID:    po.RobotID,
		DocID:      po.DocID,
		Source:     po.Source,
		AttrID:     po.AttrID,
		LabelID:    po.LabelID,
		IsDeleted:  po.IsDeleted,
		CreateTime: po.CreateTime,
		UpdateTime: po.UpdateTime,
	}
}

func getAttributeLabelsPO2DO(pos []*model.TAttributeLabel) []*entity.AttributeLabel {
	return slicex.Map(pos, func(po *model.TAttributeLabel) *entity.AttributeLabel {
		return getAttributeLabelPO2DO(po)
	})
}

func getAttributeLabelPO2DO(po *model.TAttributeLabel) *entity.AttributeLabel {
	if po == nil {
		return nil
	}
	return &entity.AttributeLabel{
		ID:            po.ID,
		RobotID:       uint64(po.RobotID),
		BusinessID:    po.BusinessID,
		AttrID:        uint64(po.AttrID),
		Name:          po.Name,
		SimilarLabel:  po.SimilarLabel,
		ReleaseStatus: po.ReleaseStatus,
		NextAction:    po.NextAction,
		IsDeleted:     po.IsDeleted,
		CreateTime:    po.CreateTime,
		UpdateTime:    po.UpdateTime,
	}
}

func getAttributeLabeProdlPO2DO(po *model.TAttributeLabelProd) *entity.AttributeLabel {
	if po == nil {
		return nil
	}
	return &entity.AttributeLabel{
		ID:           po.ID,
		RobotID:      po.RobotID,
		BusinessID:   po.BusinessID,
		AttrID:       po.AttrID,
		LabelID:      po.LabelID,
		Name:         po.Name,
		SimilarLabel: po.SimilarLabel,
		IsDeleted:    po.IsDeleted,
		CreateTime:   po.CreateTime,
		UpdateTime:   po.UpdateTime,
	}
}

func getDocAttributeLabelsDO2PO(pos []*entity.DocAttributeLabel) []*model.TDocAttributeLabel {
	return slicex.Map(pos, func(po *entity.DocAttributeLabel) *model.TDocAttributeLabel {
		return getDocAttributeLabelDO2PO(po)
	})
}

func getDocAttributeLabelDO2PO(po *entity.DocAttributeLabel) *model.TDocAttributeLabel {
	if po == nil {
		return nil
	}
	return &model.TDocAttributeLabel{
		ID:         po.ID,
		RobotID:    po.RobotID,
		DocID:      po.DocID,
		Source:     po.Source,
		AttrID:     po.AttrID,
		LabelID:    po.LabelID,
		IsDeleted:  po.IsDeleted,
		CreateTime: time.Time{},
		UpdateTime: time.Time{},
	}
}

func buildGetAttributeListCondition(q *mysqlquery.Query, filter *entity.AttributeFilter) []gen.Condition {
	conds := make([]gen.Condition, 0)
	if len(filter.Ids) > 0 {
		conds = append(conds, q.TAttribute.ID.In(filter.Ids...))
	}
	if len(filter.BusinessIds) > 0 {
		conds = append(conds, q.TAttribute.BusinessID.In(filter.BusinessIds...))
	}
	if filter.RobotId != 0 {
		conds = append(conds, q.TAttribute.RobotID.Eq(filter.RobotId))
	}
	if filter.NameSubStr != "" {
		conds = append(conds, q.TAttribute.Name.Like("%"+filter.NameSubStr+"%"))
	}
	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		conds = append(conds, q.TAttribute.IsDeleted.Is(*filter.IsDeleted))
	}
	return conds
}

// GetAttributeList 查询属性列表（Gen风格）
func (d *daoImpl) GetAttributeList(ctx context.Context, robotID uint64, queryStr string, page, pageSize uint32,
	ids []uint64) ([]*entity.Attribute, error) {
	// 1. 初始化Gen生成的Query
	gormClient, err := knowClient.GormClient(ctx, attributeTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeList  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	tbl := mysqlquery.Use(gormClient).TAttribute
	q := tbl.WithContext(ctx)
	// q := d.tdsql.TAttribute.WithContext(ctx)
	// 2. 构建动态查询条件
	q = q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Is(false),
	)
	if queryStr != "" {
		q = q.Where(
			q.Where(tbl.Name.Like(fmt.Sprintf("%%%s%%", queryStr))).
				Or(tbl.ID.In(d.getAttributeIDsByLabel(ctx, queryStr)...)),
		)
	}
	if len(ids) > 0 {
		q = q.Where(tbl.ID.In(ids...))
	}
	// 3. 分页处理
	offset := (page - 1) * pageSize
	var attrs []*entity.Attribute
	res, err := q.
		Order(tbl.UpdateTime.Desc(), tbl.ID.Desc()).
		Offset(int(offset)).
		Limit(int(pageSize)).
		Find()
	// 4. 错误处理
	if err != nil {
		logx.E(ctx, "查询失败: robotID=%d, query=%s, err=%v",
			robotID, queryStr, err)
		return nil, fmt.Errorf("查询属性列表失败: %w", err)
	}
	logx.D(ctx, "查询成功: robotID=%d, 结果数量=%d", robotID, len(attrs))
	return getAttributeListsPO2DO(res), nil
}

// getAttributeIDsByLabel 通过标签名查询关联的属性ID列表
func (d *daoImpl) getAttributeIDsByLabel(ctx context.Context, label string) []uint64 {
	var ids []uint64
	d.mysql.TAttributeLabel.
		WithContext(ctx).
		Where(
			d.mysql.TAttributeLabel.Name.Like(fmt.Sprintf("%%%s%%", label)),
			d.mysql.TAttributeLabel.IsDeleted.Is(false),
		).Pluck(d.mysql.TAttributeLabel.AttrID, &ids)
	return ids
}

func (d *daoImpl) GetAttributeListInfo(ctx context.Context, selectColumns []string, filter *entity.AttributeFilter) ([]*entity.Attribute, error) {
	attributeList := make([]*entity.Attribute, 0)
	if filter.Limit == 0 {
		return attributeList, nil
	}
	if filter.Limit > entity.AttributeTableMaxPageSize {
		err := fmt.Errorf("invalid limit: %d", filter.Limit)
		return attributeList, err
	}
	gormClient, err := knowClient.GormClient(ctx, attributeTableName, filter.RobotId, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeListInfo  get GormClient err:%v,robotID:%v", err, filter.RobotId)
		return nil, err
	}
	tbl := mysqlquery.Use(gormClient).TAttribute
	db := tbl.WithContext(ctx)
	// 构造条件 - 只有不为空的字段才加入查询
	conds := buildGetAttributeListCondition(mysqlquery.Use(gormClient), filter)
	if len(conds) > 0 {
		db = db.Where(conds...)
	}
	// 排序
	if len(filter.OrderColumn) > 0 {
		for i, orderColumn := range filter.OrderColumn {
			if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
				continue
			}
			// 修正：应该使用 TAttribute 而不是 TRejectedQuestion
			orderField, ret := tbl.GetFieldByName(orderColumn)
			if !ret {
				return nil, fmt.Errorf("getAttributeList GetFieldByName failed for column: %s", orderColumn)
			}
			if filter.OrderDirection[i] == SqlOrderByAsc {
				db = db.Order(orderField)
			} else {
				db = db.Order(orderField.Desc())
			}
		}
	}
	// 分页
	db = db.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	// 查询
	qs, err := db.Find()
	if err != nil {
		return nil, err
	}
	return getAttributeListsPO2DO(qs), nil
}

func getAttributeLists(pos []*model.TAttribute) []*entity.Attribute {
	return slicex.Map(pos, func(po *model.TAttribute) *entity.Attribute {
		return getAttributeListPO2DO(po)
	})
}

// GetAttributeTotal 这个函数是真恶心
func (d *daoImpl) GetAttributeTotal(ctx context.Context, robotID uint64, queryStr string, ids []uint64) (uint64, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeTotal  get GormClient err:%v,robotID:%v", err, robotID)
		return 0, err
	}
	tbl := mysqlquery.Use(gormClient).TAttribute
	db := tbl.WithContext(ctx)
	db = db.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Is(false),
	)
	// 如果有关键字search
	if queryStr != "" {
		likeStr := fmt.Sprintf("%%%s%%", queryStr)
		var attrIDs []uint64 // 存储去重后的 attr_id 集合
		// 先查出来
		// SELECT  DISTINCT attr_id FROM t_attribute_label WHERE  robot_id = ? AND is_deleted = ? AND name LIKE ?
		err := mysqlquery.Use(gormClient).TAttributeLabel.WithContext(ctx).Select(mysqlquery.Use(gormClient).TAttributeLabel.AttrID).Distinct().Where(
			mysqlquery.Use(gormClient).TAttributeLabel.RobotID.Eq(robotID),
			mysqlquery.Use(gormClient).TAttributeLabel.IsDeleted.Is(false),
			mysqlquery.Use(gormClient).TAttributeLabel.Name.Like(likeStr),
		).Pluck(mysqlquery.Use(gormClient).TAttributeLabel.AttrID, &attrIDs) // 将结果提取到切片
		if err != nil {
			return 0, fmt.Errorf("GetAttributeTotal Pluck failed ,err = %v", err)
		}
		db = db.Where(
			field.Or(tbl.Name.Like(likeStr), tbl.ID.In(attrIDs...)),
		)
	}
	// 如果有ids列表
	if len(ids) > 0 {
		db = db.Where(tbl.ID.In(ids...))
	}
	count, err := db.Count()
	if err != nil {
		return 0, err
	}
	return uint64(count), nil
}

func (d *daoImpl) GetAttributeListByIDs(ctx context.Context, robotID uint64, ids []uint64) ([]*entity.Attribute, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeListByIDs  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	tbl := mysqlquery.Use(gormClient).TAttribute
	q := tbl.WithContext(ctx)
	if len(ids) == 0 {
		return []*entity.Attribute{}, nil
	}
	var allAttrs []*entity.Attribute
	const chunkSize = 200
	for _, idChunk := range slicex.Chunk(ids, chunkSize) {
		qs, err := q.Where(
			tbl.RobotID.Eq(robotID),
			tbl.IsDeleted.Is(false), // AttributeIsNotDeleted
			tbl.ID.In(idChunk...),   // id分块处理
		).
			Find()
		if err != nil {
			return nil, fmt.Errorf("GetAttributeListByIDs failed, robotID:%d ids:%+v err:%v", robotID, ids, err)
		}
		attrs := getAttributeLists(qs)
		allAttrs = append(allAttrs, attrs...) // 合并结果
	}
	return allAttrs, nil
}

func (d *daoImpl) GetAttributeByIDs(ctx context.Context, robotID uint64, ids []uint64) (map[uint64]*entity.Attribute, error) {
	result := make(map[uint64]*entity.Attribute, len(ids))
	attrs, err := d.GetAttributeListByIDs(ctx, robotID, ids)
	if err != nil {
		return nil, fmt.Errorf("GetAttributeByIDs failed, robotID:%d ids:%+v err:%v", robotID, ids, err)
	}
	for _, attr := range attrs {
		if attr != nil {
			result[attr.ID] = attr
		}
	}
	return result, nil
}

func (d *daoImpl) GetAttributeByBizIDs(ctx context.Context, robotID uint64, bizIDs []uint64) (map[uint64]*entity.Attribute, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeByBizIDs  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	// 空输入直接返回空 map
	result := make(map[uint64]*entity.Attribute, len(bizIDs))
	if len(bizIDs) == 0 {
		return result, nil
	}
	tbl := mysqlquery.Use(gormClient).TAttribute
	q := tbl.WithContext(ctx)
	attrs, err := q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Is(false),
		tbl.BusinessID.In(bizIDs...),
	).Find()
	if err != nil {
		return nil, fmt.Errorf("GetAttributeByBizIDs failed, robotID:%d bizIDs:%+v err:%v", robotID, bizIDs, err)
	}
	for _, attr := range attrs {
		rsp := getAttributeListPO2DO(attr)
		result[attr.BusinessID] = rsp
	}
	logx.D(ctx, "GetAttributeByBizIDs len(result):%d", len(result))
	return result, nil
}

func (d *daoImpl) GetAttributeByKeys(ctx context.Context, robotID uint64, keys []string) (map[string]*entity.Attribute, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeByKeys  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	result := make(map[string]*entity.Attribute, len(keys))
	if len(keys) == 0 {
		return result, nil
	}
	tbl := mysqlquery.Use(gormClient).TAttribute
	q := tbl.WithContext(ctx)
	attrs, err := q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Is(false),
		tbl.AttrKey.In(keys...),
	).
		Find()
	if err != nil {
		return nil, fmt.Errorf("GetAttributeByKeys failed, robotID:%d keys:%+v err:%v", robotID, keys, err)
	}
	for _, attr := range attrs {
		if attr != nil {
			rsp := getAttributeListPO2DO(attr)
			result[attr.AttrKey] = rsp
		}
	}
	return result, nil
}

func (d *daoImpl) GetAttributeByNames(ctx context.Context, robotID uint64, names []string) (map[string]*entity.Attribute, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeByNames  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	result := make(map[string]*entity.Attribute, len(names))
	if len(names) == 0 {
		return result, nil
	}
	tbl := mysqlquery.Use(gormClient).TAttribute
	q := tbl.WithContext(ctx)
	attrs, err := q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Is(false),
		tbl.Name.In(names...),
	).
		Find()
	if err != nil {
		return nil, fmt.Errorf("GetAttributeByNames failed, robotID:%d names:%+v err:%v", robotID, names, err)
	}

	for _, attr := range attrs {
		if attr != nil {
			result[attr.Name] = getAttributeListPO2DO(attr)
		}
	}
	return result, nil
}

// GetAttributeByRobotID 查询机器人下的属性信息
func (d *daoImpl) GetAttributeByRobotID(ctx context.Context, robotID uint64) (map[string]struct{}, map[string]struct{}, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeByRobotID  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, nil, err
	}
	tbl := mysqlquery.Use(gormClient).TAttribute
	q := tbl.WithContext(ctx)
	attrs, err := q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Is(false),
	).
		Find()
	if err != nil {
		return nil, nil, fmt.Errorf("GetAttributeByRobotID failed, robotID:%d err:%v", robotID, err)
	}
	// key 和 name 去重集合
	mapAttrKey := make(map[string]struct{})
	mapAttrName := make(map[string]struct{})
	for _, attr := range attrs {
		if attr != nil {
			mapAttrKey[attr.AttrKey] = struct{}{}
			mapAttrName[attr.Name] = struct{}{}
		}
	}
	return mapAttrKey, mapAttrName, nil
}

func (d *daoImpl) GetAttributeKeyAndIDsByRobotID(ctx context.Context, robotID uint64) ([]*entity.AttributeKeyAndID, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeKeyAndIDsByRobotID  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	tbl := mysqlquery.Use(gormClient).TAttributeProd
	q := tbl.WithContext(ctx)
	attrs, err := q.Select(tbl.ID, tbl.AttrKey).
		Where(
			tbl.RobotID.Eq(int64(robotID)),
			tbl.IsDeleted.Is(false),
		).
		Find()
	if err != nil {
		return nil, fmt.Errorf("GetAttributeKeyAndIDsByRobotID failed, robotID:%d err:%v", robotID, err)
	}
	rsp := getAttributeKeyAndIDsByRobotIDPO2DO(attrs)
	return rsp, nil
}

func getAttributeKeyAndIDsByRobotIDPO2DO(pos []*model.TAttributeProd) []*entity.AttributeKeyAndID {
	return slicex.Map(pos, func(po *model.TAttributeProd) *entity.AttributeKeyAndID {
		return getAttributeKeyAndIDByRobotIDPO2DO(po)
	})
}

func getAttributeKeyAndIDByRobotIDPO2DO(po *model.TAttributeProd) *entity.AttributeKeyAndID {
	if po == nil {
		return nil
	}
	return &entity.AttributeKeyAndID{
		ID:      po.ID,
		AttrKey: po.AttrKey,
	}
}

func (d *daoImpl) GetAttributeKeyAndIDsByRobotIDProd(ctx context.Context, robotID uint64) ([]*entity.AttributeKeyAndID, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeProdTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeKeyAndIDsByRobotIDProd  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	tbl := mysqlquery.Use(gormClient).TAttributeProd
	q := tbl.WithContext(ctx)
	attrs, err := q.
		Select(tbl.ID, tbl.AttrID, tbl.AttrKey).
		Where(
			tbl.RobotID.Eq(int64(robotID)),
			tbl.IsDeleted.Is(false),
		).
		Find()
	if err != nil {
		return nil, fmt.Errorf("GetAttributeKeyAndIDsByRobotIDProd failed, robotID:%d err:%v", robotID, err)
	}
	rsp := getAttributeKeyAndIDsByRobotIDProdPO2DO(attrs)
	return rsp, nil
}

func getAttributeKeyAndIDsByRobotIDProdPO2DO(pos []*model.TAttributeProd) []*entity.AttributeKeyAndID {
	return slicex.Map(pos, func(po *model.TAttributeProd) *entity.AttributeKeyAndID {
		return getAttributeKeyAndIDByRobotIDProdPO2DO(po)
	})
}

func getAttributeKeyAndIDByRobotIDProdPO2DO(po *model.TAttributeProd) *entity.AttributeKeyAndID {
	if po == nil {
		return nil
	}
	return &entity.AttributeKeyAndID{
		ID:      po.ID,
		AttrID:  po.AttrID,
		AttrKey: po.AttrKey,
	}
}

func (d *daoImpl) GetAttributeLabelCountByFilter(ctx context.Context, selectColumns []string, filter *entity.AttributeLabelFilter) (int64, error) {
	db, err := knowClient.GormClient(ctx, attributeLabelTableName, filter.RobotId, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return 0, err
	}
	session := db.WithContext(ctx).Table(attributeLabelTableName).Select(selectColumns)
	session = generateConditionForGetAttributeLabelCountByFilter(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// 生成查询条件，必须按照索引的顺序排列
func generateConditionForGetAttributeLabelCountByFilter(ctx context.Context, session *gorm.DB, filter *entity.AttributeLabelFilter) *gorm.DB {
	if filter.RobotId != 0 {
		session = session.Where(entity.AttributeLabelTblColRobotId+sqlEqual, filter.RobotId)
	}
	if len(filter.BusinessIds) != 0 {
		session = session.Where(entity.AttributeLabelTblColBusinessId+sqlIn, filter.BusinessIds)
	}
	if len(filter.AttrIds) != 0 {
		session = session.Where(entity.AttributeLabelTblColAttrId+sqlIn, filter.AttrIds)
	}
	if filter.NotEmptySimilarLabel != nil {
		if *filter.NotEmptySimilarLabel {
			session = session.Where(entity.AttributeLabelTblColSimilarLabel+sqlNotEqual, "")
		} else {
			session = session.Where(entity.AttributeLabelTblColSimilarLabel+sqlEqual, "")
		}
	}
	if filter.NameOrSimilarLabelSubStr != "" {
		session = session.Where(session.Where(entity.AttributeLabelTblColName+sqlLike, "%"+filter.NameOrSimilarLabelSubStr+"%").
			Or(entity.AttributeLabelTblColSimilarLabel+sqlLike, "%"+filter.NameOrSimilarLabelSubStr+"%"))
	}
	if filter.IsDeleted == nil {
		// 默认查询未删除的数据
		session = session.Where(entity.AttributeLabelTblColIsDeleted+sqlEqual, IsNotDeleted)
	} else {
		session = session.Where(entity.AttributeLabelTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}
	return session
}

func (d *daoImpl) GetAttributeLabelCount(ctx context.Context, attrID uint64, queryStr string, queryScope string, robotID uint64) (uint64, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeLabelTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeLabelCount  get GormClient err:%v,robotID:%v", err, robotID)
		return 0, err
	}
	tbl := mysqlquery.Use(gormClient).TAttributeLabel
	db := tbl.WithContext(ctx)
	db = db.Where(
		tbl.RobotID.Eq(robotID),
		tbl.AttrID.Eq(attrID),
		tbl.IsDeleted.Is(false),
	)
	if queryStr != "" {
		likeStr := fmt.Sprintf("%%%s%%", queryStr)
		switch queryScope {
		case entity.AttributeLabelQueryScopeStandard:
			db = db.Where(tbl.Name.Like(likeStr))
		case entity.AttributeLabelQueryScopeSimilar:
			db = db.Where(tbl.SimilarLabel.Like(likeStr))
		case entity.AttributeLabelQueryScopeAll:
			fallthrough
		default:
			db = db.Where(
				field.Or(tbl.Name.Like(likeStr), tbl.SimilarLabel.Like(likeStr)),
			)
		}
	}
	count, err := db.Count()
	if err != nil {
		return 0, fmt.Errorf("GetAttributeLabelCount failed, attrID:%d, robotID:%d, query:%v, queryScope:%v, err:%v", attrID, robotID, queryStr, queryScope, err)
	}
	return uint64(count), nil
}

func (d *daoImpl) GetAttributeLabelByIDs(ctx context.Context, ids []uint64, robotID uint64) (map[uint64]*entity.AttributeLabel, error) {
	if len(ids) == 0 {
		return make(map[uint64]*entity.AttributeLabel), nil
	}

	gormClient, err := knowClient.GormClient(ctx, attributeLabelTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeLabelByIDs get GormClient err:%v, robotID:%v", err, robotID)
		return nil, err
	}

	const chunkSize = 200 // ids 分批大小

	result := make(map[uint64]*entity.AttributeLabel, len(ids))
	tbl := mysqlquery.Use(gormClient).TAttributeLabel

	// 按 ids 分批处理（ID 唯一，无需游标）
	for _, idChunk := range slicex.Chunk(ids, chunkSize) {
		labels, err := tbl.WithContext(ctx).Where(
			tbl.RobotID.Eq(robotID),
			tbl.IsDeleted.Is(false),
			tbl.ID.In(idChunk...),
		).Find()
		if err != nil {
			return nil, fmt.Errorf("GetAttributeLabelByIDs query fail robotID=%d err=%v", robotID, err)
		}

		for _, label := range labels {
			result[label.ID] = getAttributeLabelPO2DO(label)
		}
	}

	return result, nil
}

// GetAttributeLabelByBizIDs 获取指定标签ID的信息
func (d *daoImpl) GetAttributeLabelByBizIDs(ctx context.Context, ids []uint64, robotID uint64) (
	map[uint64]*entity.AttributeLabel, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeLabelTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeLabelByBizIDs  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	var MaxSqlInCount = 200
	result := make(map[uint64]*entity.AttributeLabel)
	tbl := mysqlquery.Use(gormClient).TAttributeLabel
	for _, chunk := range slicex.Chunk(ids, MaxSqlInCount) {
		labels, err := mysqlquery.Use(gormClient).TAttributeLabel.WithContext(ctx).
			Where(
				tbl.RobotID.Eq(robotID),
				tbl.BusinessID.In(chunk...),
				tbl.IsDeleted.Is(false),
			).Find()
		if err != nil {
			return nil, fmt.Errorf("GetAttributeLabelByBizIDs Find failed, chunk=%+v robotID=%d err=%+v", chunk, robotID, err)
		}
		for _, label := range labels {
			result[label.BusinessID] = getAttributeLabelPO2DO(label)
		}
	}
	return result, nil
}

func (d *daoImpl) GetAttributeLabelByIDOrder(ctx context.Context, robotID uint64, ids []uint64) ([]*entity.AttributeLabel, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeLabelTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeLabelByIDOrder  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	tbl := mysqlquery.Use(gormClient).TAttributeLabel
	q := tbl.WithContext(ctx)
	conds := []gen.Condition{
		tbl.RobotID.Eq(robotID),
		tbl.ID.In(ids...),
	}
	// q.Where(conds...).UnderlyingDB().Raw().
	//  获取原生 GORM DB 对象并注入排序子句
	var results []*model.TAttributeLabel
	orders := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		orders = append(orders, id)
	}

	db := q.Where(conds...).UnderlyingDB().Clauses(clause.OrderBy{
		Expression: clause.Expr{
			SQL:  fmt.Sprintf("FIELD(id, %s)", util.Placeholder(len(orders))), // MySQL 的 FIELD 函数
			Vars: orders,
		},
	}).Find(&results)
	if db.Error != nil {
		return nil, fmt.Errorf("query attribute label failed: %w", db.Error)
	}
	return getAttributeLabelsPO2DO(results), nil
}

// GetAttributeLabelByAttrIDs 获取指定属性下的标签信息
func (d *daoImpl) GetAttributeLabelByAttrIDs(ctx context.Context, attrIDs []uint64, robotID uint64) (map[uint64][]*entity.AttributeLabel, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeLabelTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeLabelByAttrIDs  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	if len(attrIDs) == 0 {
		return nil, nil
	}
	tbl := mysqlquery.Use(gormClient).TAttributeLabel
	q := tbl.WithContext(ctx)
	labels, err := q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Is(false),
		tbl.AttrID.In(attrIDs...),
	).Find()
	if err != nil {
		return nil, fmt.Errorf("GetAttributeLabelByAttrIDs Failed to query the standard term of the tag err =%v", err)
	}
	// 构建 attrID -> labels 映射
	result := make(map[uint64][]*entity.AttributeLabel)
	for _, label := range labels {
		result[label.AttrID] = append(result[label.AttrID], getAttributeLabelPO2DO(label))
	}
	return result, nil
}

// GetAttributeLabelByAttrIDsWithNotEmptySimilarLabelProd 获取发布环境指定属性下相似标签不为空的标签信息
func (d *daoImpl) GetAttributeLabelByAttrIDsWithNotEmptySimilarLabelProd(ctx context.Context, attrIDs []uint64, robotID uint64) (map[uint64][]*entity.AttributeLabel, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeLabelProdTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeLabelByAttrIDsWithNotEmptySimilarLabelProd  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	if len(attrIDs) == 0 {
		return nil, fmt.Errorf("GetAttributeLabelByAttrIDsWithNotEmptySimilarLabelProd attrIDs is empty")
	}
	// 使用 GORM Gen 查询
	tbl := mysqlquery.Use(gormClient).TAttributeLabelProd
	q := tbl.WithContext(ctx)
	labels, err := q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Is(false),
		tbl.AttrID.In(attrIDs...),
		tbl.SimilarLabel.Neq(""), // 相似标签不为空
	).Find()

	if err != nil {
		return nil, fmt.Errorf("GetAttributeLabelByAttrIDsWithNotEmptySimilarLabelProd failed, robotID:%d ,err:%v", robotID, err)
	}
	// 构建 attrID -> labels 映射
	result := make(map[uint64][]*entity.AttributeLabel)
	for _, label := range labels {
		result[label.AttrID] = append(result[label.AttrID], getAttributeLabeProdlPO2DO(label))
	}
	return result, nil
}

// GetAttributeLabelByName 根据名称或相似名称查询标签（支持模糊匹配）
//
//			 SELECT
//	           %s
//	       FROM
//	           t_attribute_label
//	       WHERE
//	           robot_id = ? AND is_deleted = ? AND attr_id = ? AND (name LIKE ? OR similar_label LIKE ?)
func (d *daoImpl) GetAttributeLabelByName(ctx context.Context, attrID uint64, name string, robotID uint64) ([]*entity.AttributeLabel, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeLabelTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeLabelByName  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	if name == "" {
		return nil, fmt.Errorf("GetAttributeLabelByName name is empty") // 空名称直接返回，避免无效查询
	}
	tbl := mysqlquery.Use(gormClient).TAttributeLabel
	db := tbl.WithContext(ctx)
	labelNameStr := fmt.Sprintf("%%%s%%", name)
	labels, err := db.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Is(false),
		tbl.AttrID.Eq(attrID),
		db.Where(
			db.Where(tbl.Name.Like(labelNameStr)).
				Or(tbl.SimilarLabel.Like(labelNameStr)),
		),
	).Find()
	if err != nil {
		return nil, fmt.Errorf("GetAttributeLabelByName failed ,err = %v", err)
	}
	return getAttributeLabelsPO2DO(labels), nil
}

// GetDocAttributeLabel 根据文档ID列表查询关联的标签（支持批量查询）
// 对 docIDs 按 200 分批，每批内按 tbl.ID 游标分页查询，每次最多 500 条
func (d *daoImpl) GetDocAttributeLabel(ctx context.Context, robotID uint64, docIDs []uint64) ([]*entity.DocAttributeLabel, error) {
	if len(docIDs) == 0 {
		return nil, fmt.Errorf("GetDocAttributeLabel docIDs is empty")
	}

	gormClient, err := knowClient.GormClient(ctx, docAttributeLabelTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetDocAttributeLabel get GormClient err:%v, robotID:%v", err, robotID)
		return nil, err
	}

	const (
		chunkSize       = 200 // docIDs 分批大小
		cursorBatchSize = 500 // 游标分页每次查询数量
	)

	allLabels := make([]*entity.DocAttributeLabel, 0)
	tbl := mysqlquery.Use(gormClient).TDocAttributeLabel

	// 按 docIDs 分批处理
	for _, docIDChunk := range slicex.Chunk(docIDs, chunkSize) {
		// 对每批 docIDs 使用游标分页查询
		var lastID uint64 = 0
		for {
			q := tbl.WithContext(ctx)
			labels, err := q.Where(
				tbl.RobotID.Eq(robotID),
				tbl.IsDeleted.Is(false),
				tbl.DocID.In(docIDChunk...),
				tbl.ID.Gt(lastID), // 游标条件
			).Order(tbl.ID.Asc()).Limit(cursorBatchSize).Find()
			if err != nil {
				return nil, fmt.Errorf("GetDocAttributeLabel failed, robotID=%d, err=%v", robotID, err)
			}
			if len(labels) == 0 {
				break // 当前批次查询完毕
			}

			allLabels = append(allLabels, getDocAttributeLabelsPO2DO(labels)...)
			lastID = labels[len(labels)-1].ID

			// 如果返回数量小于 limit，说明已经查完
			if len(labels) < cursorBatchSize {
				break
			}
		}
	}

	return allLabels, nil
}

// GetDocAttributeLabelCountByAttrLabelIDs 通过属性和标签ID获取文档属性标签数量
func (d *daoImpl) GetDocAttributeLabelCountByAttrLabelIDs(ctx context.Context, robotID uint64, source uint32, attrIDs, labelIDs []uint64) (uint64, error) {
	gormClient, err := knowClient.GormClient(ctx, docAttributeLabelTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetDocAttributeLabelCountByAttrLabelIDs  get GormClient err:%v,robotID:%v", err, robotID)
		return 0, err
	}
	if len(attrIDs) == 0 && len(labelIDs) == 0 {
		return 0, fmt.Errorf("GetDocAttributeLabelCountByAttrLabelIDs attrIDs or  labelIDs is empty") // 空名称直接返回，避免无效查询
	}
	tbl := mysqlquery.Use(gormClient).TDocAttributeLabel
	q := tbl.WithContext(ctx)
	q = q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Is(false),
		tbl.Source.Eq(source),
	)
	// 动态添加 attrIDs 和 labelIDs 条件
	if len(attrIDs) > 0 {
		q = q.Where(tbl.AttrID.In(attrIDs...))
	}
	if len(labelIDs) > 0 {
		q = q.Where(tbl.LabelID.In(labelIDs...))
	}
	count, err := q.Count()
	if err != nil {
		return 0, fmt.Errorf("GetDocAttributeLabelCountByAttrLabelIDs Count failed ,err = %v", err)
	}
	return uint64(count), nil
}

// GetDocAttributeLabelByAttrLabelIDs 通过属性和标签ID分页查询文档标签（支持动态条件组合）
func (d *daoImpl) GetDocAttributeLabelByAttrLabelIDs(ctx context.Context, robotID uint64, source uint32,
	attrIDs []uint64, labelIDs []uint64, page uint32, pageSize uint32) ([]*entity.DocAttributeLabel, error) {
	gormClient, err := knowClient.GormClient(ctx, docAttributeLabelTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetDocAttributeLabelByAttrLabelIDs  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	if len(attrIDs) == 0 && len(labelIDs) == 0 {
		return nil, fmt.Errorf("GetDocAttributeLabelCountByAttrLabelIDs attrIDs or labelIDs is empty") // 空名称直接返回，避免无效查询
	}
	tbl := mysqlquery.Use(gormClient).TDocAttributeLabel
	q := tbl.WithContext(ctx)
	q = q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Is(false),
		tbl.Source.Eq(source),
	)
	// 动态添加条件
	if len(attrIDs) > 0 {
		q = q.Where(tbl.AttrID.In(attrIDs...))
	}
	if len(labelIDs) > 0 {
		q = q.Where(tbl.LabelID.In(labelIDs...))
	}
	// 执行分页查询
	offset := (page - 1) * pageSize
	labels, err := q.Order(tbl.ID.Desc()).Limit(int(pageSize)).Offset(int(offset)).Find()
	if err != nil {
		return nil, fmt.Errorf("GetDocAttributeLabelByAttrLabelIDs Find failed, err = %v", err)
	}
	return getDocAttributeLabelsPO2DO(labels), nil
}

// GetDocCountByAttributeLabel 统计关联标签的文档数量（支持动态排除状态和标签条件）
func (d *daoImpl) GetDocCountByAttributeLabel(ctx context.Context, robotID uint64, noStatusList []uint32, attrID uint64, labelIDs []uint64) (uint64, error) {
	gormClient, err := knowClient.GormClient(ctx, docTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetDocCountByAttributeLabel  get GormClient err:%v,robotID:%v", err, robotID)
		return 0, err
	}
	// 1. 获取符合条件的文档ID列表
	docIDs, err := d.getDocIDsByAttributeLabel(ctx, gormClient, robotID, entity.AttributeLabelSourceKg, attrID, labelIDs)
	if err != nil {
		if errx.Is(err, errx.ErrNotFound) {
			logx.W(ctx, "GetDocCountByAttributeLabel docIDs is empty, robotID:%v, attrID:%v, labelIDs:%v",
				robotID, attrID, labelIDs)
			return 0, nil
		}

		return 0, err
	}
	tbl := mysqlquery.Use(gormClient).TDoc
	q := tbl.WithContext(ctx)
	q = q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Is(false),
		tbl.ID.In(docIDs...), // 限定在子查询结果范围内
	)
	// 动态添加状态排除条件
	if len(noStatusList) > 0 {
		q = q.Where(
			field.Or(tbl.Status.NotIn(noStatusList...), tbl.IsCreatingQa.Is(true)),
		)
	}
	count, err := q.Count()
	if err != nil {
		if errx.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("GetDocCountByAttributeLabel Count failed, err = %v", err)
	}
	if count > 0 {
		logx.I(ctx, "获取关联标签标准词的文档总数成功 total:%d", count)
	}
	return uint64(count), nil
}

// getDocIDsByAttributeLabel 查询符合条件的文档ID列表（返回[]uint64）
func (d *daoImpl) getDocIDsByAttributeLabel(ctx context.Context, db *gorm.DB, robotID uint64, source uint32, attrID uint64, labelIDs []uint64) ([]uint64, error) {
	tbl := mysqlquery.Use(db).TDocAttributeLabel
	q := tbl.WithContext(ctx)
	q = q.Select(tbl.DocID.Distinct()).Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Is(false),
		tbl.Source.Eq(source),
	)
	// 动态添加条件
	if attrID > 0 {
		q = q.Where(tbl.AttrID.Eq(attrID))
	}
	if len(labelIDs) > 0 {
		q = q.Where(tbl.LabelID.In(labelIDs...))
	}
	// 执行查询并提取结果
	var docIDs []uint64
	err := q.Pluck(tbl.DocID, &docIDs)
	if err != nil {
		if errx.Is(err, gorm.ErrRecordNotFound) {
			return nil, errx.ErrNotFound
		}
		return nil, fmt.Errorf("getDocIDsByAttributeLabel failed err  = %v", err)
	}
	if len(docIDs) == 0 {
		return nil, errx.ErrNotFound // 无匹配文档直接返回
	}
	return docIDs, nil
}

// GetQAAttributeLabel 查询QA关联的标签标准词列表（返回[]*attrEntity.QAAttributeLabel）
func (d *daoImpl) GetQAAttributeLabel(ctx context.Context, robotID uint64, qaIDs []uint64) ([]*entity.QAAttributeLabel, error) {
	gormClient, err := knowClient.GormClient(ctx, qaAttributeLabelTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetQAAttributeLabel  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	if len(qaIDs) == 0 {
		return nil, fmt.Errorf("GetQAAttributeLabel attrIDs qaIDs is empty") // 空名称直接返回，避免无效查询
	}
	tbl := mysqlquery.Use(gormClient).TQaAttributeLabel
	q := tbl.WithContext(ctx)
	q = q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Eq(entity.QAAttributeLabelIsNotDeleted),
		tbl.QaID.In(qaIDs...),
	).Order(tbl.ID)
	// 执行查询
	lables, err := q.Find()
	if err != nil {
		return nil, fmt.Errorf("GetQAAttributeLabel failed  robotID=%d, qaIDs=%v, err=%v", robotID, qaIDs, err)
	}
	return getQAAttributeLabelsPO2DO(lables), nil
}

// GetQAAttributeLabelForExport 根据robotID使用游标分页查询QA属性标签（专用于导出功能）
// 注意：此方法专为导出场景设计，使用游标分页遍历所有数据，不适用于常规业务查询
// lastID: 上次查询的最后一条记录的ID，首次查询传0
// limit: 每批查询的数量
func (d *daoImpl) GetQAAttributeLabelForExport(ctx context.Context, robotID uint64, lastID uint64, limit int) ([]*entity.QAAttributeLabel, error) {
	gormClient, err := knowClient.GormClient(ctx, qaAttributeLabelTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetQAAttributeLabelByRobotID get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	tbl := mysqlquery.Use(gormClient).TQaAttributeLabel
	q := tbl.WithContext(ctx)
	q = q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Eq(entity.QAAttributeLabelIsNotDeleted),
		tbl.ID.Gt(lastID), // 游标分页：ID > lastID
	).Order(tbl.ID).Limit(limit)
	// 执行查询
	labels, err := q.Find()
	if err != nil {
		return nil, fmt.Errorf("GetQAAttributeLabelForExport failed robotID=%d, lastID=%d, limit=%d, err=%v", robotID, lastID, limit, err)
	}
	return getQAAttributeLabelsPO2DO(labels), nil
}

// GetQAAttributeLabelCountByAttrLabelIDs 通过属性和标签ID获取QA属性标签数量（返回uint64）
func (d *daoImpl) GetQAAttributeLabelCountByAttrLabelIDs(ctx context.Context, robotID uint64, source uint32, attrIDs []uint64, labelIDs []uint64) (uint64, error) {
	gormClient, err := knowClient.GormClient(ctx, qaAttributeLabelTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetQAAttributeLabelCountByAttrLabelIDs  get GormClient err:%v,robotID:%v", err, robotID)
		return 0, err
	}
	// 空参数直接返回
	if len(attrIDs) == 0 && len(labelIDs) == 0 {
		return 0, fmt.Errorf("GetQAAttributeLabelCountByAttrLabelIDs attrIDs or labelIDs is empty") // 空名称直接返回，避免无效查询
	}
	tbl := mysqlquery.Use(gormClient).TQaAttributeLabel
	q := tbl.WithContext(ctx)
	q = q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Eq(entity.QAAttributeLabelIsNotDeleted),
		tbl.Source.Eq(source),
	)
	// 动态添加条件
	if len(attrIDs) > 0 {
		q = q.Where(tbl.AttrID.In(attrIDs...))
	}
	if len(labelIDs) > 0 {
		q = q.Where(tbl.LabelID.In(labelIDs...))
	}
	// 执行计数查询
	total, err := q.Count()
	if err != nil {
		return 0, fmt.Errorf("GetQAAttributeLabelCountByAttrLabelIDs: robotID=%d, attrIDs=%v, labelIDs=%v, err=%v",
			robotID, attrIDs, labelIDs, err)
	}
	return uint64(total), nil
}

// GetQAAttributeLabelByAttrLabelIDs 通过属性和标签ID分页查询QA属性标签列表
func (d *daoImpl) GetQAAttributeLabelByAttrLabelIDs(ctx context.Context, robotID uint64, source uint32,
	attrIDs []uint64, labelIDs []uint64, page, pageSize uint32) ([]*entity.QAAttributeLabel, error) {
	gormClient, err := knowClient.GormClient(ctx, qaAttributeLabelTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetQAAttributeLabelByAttrLabelIDs  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	// 空参数直接返回
	if len(attrIDs) == 0 && len(labelIDs) == 0 {
		return nil, nil
	}
	tbl := mysqlquery.Use(gormClient).TQaAttributeLabel
	q := tbl.WithContext(ctx)
	q = q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Eq(entity.QAAttributeLabelIsNotDeleted),
		tbl.Source.Eq(source),
	)
	// 动态添加条件
	if len(attrIDs) > 0 {
		q = q.Where(tbl.AttrID.In(attrIDs...))
	}
	if len(labelIDs) > 0 {
		q = q.Where(tbl.LabelID.In(labelIDs...))
	}
	// 执行分页查询
	offset := (page - 1) * pageSize
	labels, err := q.Offset(int(offset)).Limit(int(pageSize)).Find()
	if err != nil {
		return nil, fmt.Errorf("GetQAAttributeLabelByAttrLabelIDs failed, robotID=%d, attrIDs=%v, labelIDs=%v, err=%v",
			robotID, attrIDs, labelIDs, err)
	}
	return getQAAttributeLabelsPO2DO(labels), nil
}

// GetQACountByAttributeLabel 统计关联属性标签的QA数量（支持动态排除状态和标签条件）
func (d *daoImpl) GetQACountByAttributeLabel(ctx context.Context, robotID uint64,
	noReleaseStatusList []uint32, attrID uint64, labelIDs []uint64) (uint64, error) {
	gormClient, err := knowClient.GormClient(ctx, docQaTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetQACountByAttributeLabel  get GormClient err:%v,robotID:%v", err, robotID)
		return 0, err
	}
	// 1. 获取符合条件的QA ID列表
	qaIDs, err := d.getQAIDsByAttributeLabel(ctx, gormClient, robotID, entity.AttributeLabelSourceKg, attrID, labelIDs)
	if err != nil {
		if errx.Is(err, errx.ErrNotFound) {
			logx.W(ctx, "GetQACountByAttributeLabel qaIDs is empty, robotID:%v, attrID:%v, labelIDs:%v",
				robotID, attrID, labelIDs)
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get QA IDs: %w", err)
	}
	tbl := mysqlquery.Use(gormClient).TDocQa
	q := tbl.WithContext(ctx)
	// 2. 构建主查询
	q = q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Eq(qaEntity.QAIsNotDeleted),
		tbl.ID.In(qaIDs...), // 限定在子查询结果范围内
	)
	// 动态添加发布状态排除条件
	if len(noReleaseStatusList) > 0 {
		q = q.Where(tbl.ReleaseStatus.NotIn(noReleaseStatusList...))
	}
	// 执行计数查询
	count, err := q.Count()
	if err != nil {
		if errx.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("统计QA数量失败: %w", err)
	}
	return uint64(count), nil
}

// getQAIDsByAttributeLabel 查询符合条件的QA ID列表（返回[]uint64）
func (d *daoImpl) getQAIDsByAttributeLabel(ctx context.Context, db *gorm.DB, robotID uint64, source uint32, attrID uint64, labelIDs []uint64) ([]uint64, error) {
	tbl := mysqlquery.Use(db).TQaAttributeLabel
	q := tbl.WithContext(ctx)
	q = q.Select(d.mysql.TQaAttributeLabel.QaID.Distinct()).Where(
		tbl.RobotID.Eq(robotID),
		tbl.IsDeleted.Eq(entity.QAAttributeLabelIsNotDeleted),
		tbl.Source.Eq(source),
	)
	// 动态添加条件
	if attrID > 0 {
		q = q.Where(tbl.AttrID.Eq(attrID))
	}
	if len(labelIDs) > 0 {
		q = q.Where(tbl.LabelID.In(labelIDs...))
	}
	// 执行查询并提取结果
	var qaIDs []uint64
	if err := q.Pluck(tbl.QaID, &qaIDs); err != nil {
		if errx.Is(err, gorm.ErrRecordNotFound) {
			return nil, errx.ErrNotFound
		}
		return nil, err
	}
	if len(qaIDs) == 0 {
		return nil, errx.ErrNotFound
	}
	return qaIDs, nil
}

// GetAttributeKeysDelStatusAndIDs 获取属性标签的删除状态和ID（返回map[attrKey]*Attribute）
func (d *daoImpl) GetAttributeKeysDelStatusAndIDs(ctx context.Context, robotID uint64, attrKeys []string) (map[string]*entity.Attribute, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeProdTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeKeysDelStatusAndIDs  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	// 1. 参数校验
	if len(attrKeys) == 0 || len(attrKeys) == 0 {
		return nil, fmt.Errorf("GetDocAttributeLabelCountByAttrLabelIDs attrIDs or attrKeys is empty") // 空直接返回，避免无效查询
	}
	tbl := mysqlquery.Use(gormClient).TAttributeProd
	q := tbl.WithContext(ctx)
	q = q.Where(
		tbl.RobotID.Eq(int64(robotID)),
		tbl.AttrKey.In(attrKeys...),
	).Select(tbl.ID, tbl.AttrID, tbl.IsDeleted, tbl.AttrKey)

	// 3. 执行查询
	var attrs []*entity.Attribute
	if err := q.Scan(&attrs); err != nil {
		return nil, fmt.Errorf("GetAttributeKeysDelStatusAndIDs Scan failed: %v", err)
	}
	// 4. 构建结果映射
	result := make(map[string]*entity.Attribute)
	for _, attr := range attrs {
		result[attr.AttrKey] = attr
	}
	return result, nil
}

func (d *daoImpl) GetAttributeLabelListInfo(ctx context.Context, selectColumns []string, filter *entity.AttributeLabelFilter) ([]*entity.AttributeLabel, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeLabelTableName, filter.RobotId, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeLabelListInfo  get GormClient err:%v,robotID:%v", err, filter.RobotId)
		return nil, err
	}
	attributeList := make([]*entity.AttributeLabel, 0)
	if filter.Limit == 0 {
		return attributeList, nil
	}
	if filter.Limit > entity.AttributeTableMaxPageSize {
		err := fmt.Errorf("GetAttributeLabelList invalid limit: %d", filter.Limit)
		return attributeList, err
	}
	tbl := mysqlquery.Use(gormClient).TAttributeLabel
	db := tbl.WithContext(ctx)
	// 构造条件 - 只有不为空的字段才加入查询
	db = buildGetAttributeLabelCondition(mysqlquery.Use(gormClient), filter, db)
	// 排序
	if len(filter.OrderColumn) > 0 {
		for i, orderColumn := range filter.OrderColumn {
			if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
				continue
			}
			orderField, ret := tbl.GetFieldByName(orderColumn)
			if !ret {
				return nil, fmt.Errorf("GetAttributeLabelList GetFieldByName failed for column: %s", orderColumn)
			}
			if filter.OrderDirection[i] == SqlOrderByAsc {
				db = db.Order(orderField)
			} else {
				db = db.Order(orderField.Desc())
			}
		}
	}
	// 分页
	db = db.Offset(filter.Offset).Limit(filter.Limit)
	// 查询
	qs, err := db.Find()
	if err != nil {
		return nil, err
	}
	return getAttributeLabelsPO2DO(qs), nil
}

// BuildGetAttributeLabelCondition 生成查询条件，必须按照索引的顺序排列
func buildGetAttributeLabelCondition(q *mysqlquery.Query, filter *entity.AttributeLabelFilter, db mysqlquery.ITAttributeLabelDo) mysqlquery.ITAttributeLabelDo {
	if filter.RobotId != 0 {
		db = db.Where(q.TAttributeLabel.RobotID.Eq(filter.RobotId))
	}
	if len(filter.BusinessIds) > 0 {
		db = db.Where(q.TAttributeLabel.BusinessID.In(filter.BusinessIds...))
	}
	if filter.AttrId != 0 {
		db = db.Where(q.TAttributeLabel.AttrID.Eq(filter.AttrId))
	}
	if len(filter.AttrIds) > 0 {
		db = db.Where(q.TAttributeLabel.AttrID.In(filter.AttrIds...))
	}
	if filter.NotEmptySimilarLabel != nil {
		if *filter.NotEmptySimilarLabel {
			db = db.Where(q.TAttributeLabel.SimilarLabel.Neq(""))
		} else {
			db = db.Where(q.TAttributeLabel.SimilarLabel.Eq(""))
		}
	}
	if filter.NameOrSimilarLabelSubStr != "" {
		likePattern := "%" + filter.NameOrSimilarLabelSubStr + "%"
		db = db.Where(field.Or(q.TAttributeLabel.Name.Like(likePattern), q.TAttributeLabel.SimilarLabel.Like(likePattern)))
	}
	if len(filter.Names) != 0 {
		db = db.Where(q.TAttributeLabel.Name.In(filter.Names...))
	}
	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		// 默认查询未删除的数据
		db = db.Where(q.TAttributeLabel.IsDeleted.Is(*filter.IsDeleted))
	} else {
		db = db.Where(q.TAttributeLabel.IsDeleted.Is(false))
	}
	return db
}

// GetAttributeChunkByRobotID 分页获取属性列表（按ID升序，用于导出场景）
func (d *daoImpl) GetAttributeChunkByRobotID(ctx context.Context, robotID, startID uint64, limit int) ([]*entity.Attribute, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeTableName, robotID, 0, client.WithCalleeMethod("GetAttributeChunkByRobotID"))
	if err != nil {
		logx.E(ctx, "GetAttributeChunkByRobotID get GormClient err:%v, robotID:%v", err, robotID)
		return nil, err
	}
	tbl := mysqlquery.Use(gormClient).TAttribute
	q := tbl.WithContext(ctx)
	q = q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.ID.Gt(startID),
		tbl.IsDeleted.Is(false),
	).Order(tbl.ID.Asc()).Limit(limit)
	attrs, err := q.Find()
	if err != nil {
		return nil, fmt.Errorf("GetAttributeChunkByRobotID failed robotID=%d, startID=%d, limit=%d, err=%v",
			robotID, startID, limit, err)
	}
	return getAttributeListsPO2DO(attrs), nil
}

// GetAttributeLabelChunkByAttrID 分页获取属性标签列表（按ID升序）
func (d *daoImpl) GetAttributeLabelChunkByAttrID(ctx context.Context, selectColumns []string, robotID, attrID, startID uint64, limit int) ([]*entity.AttributeLabel, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeLabelTableName, robotID, 0, client.WithCalleeMethod("GetAttributeLabelChunkByAttrID"))
	if err != nil {
		logx.E(ctx, "GetAttributeLabelChunkByAttrID  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	tbl := mysqlquery.Use(gormClient).TAttributeLabel
	q := tbl.WithContext(ctx)
	// 动态选择列（若指定列则优化查询性能）
	if len(selectColumns) > 0 {
		fields := make([]field.Expr, 0, len(selectColumns))
		for _, col := range selectColumns {
			if f, ok := tbl.GetFieldByName(col); ok {
				fields = append(fields, f)
			}
		}
		q = q.Select(fields...)
	}
	q = q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.AttrID.Eq(attrID),
		tbl.ID.Gt(startID),
		tbl.IsDeleted.Is(false), // 默认查未删除
	).Order(tbl.ID.Asc()).Limit(limit)
	// 2. 执行查询
	labels, err := q.Find()
	if err != nil {
		return nil, fmt.Errorf("GetAttributeLabelChunkByAttrID faield  robotID=%d, attrID=%d, startID=%d, limit=%d, err=%v",
			robotID, attrID, startID, limit, err)
	}
	return getAttributeLabelsPO2DO(labels), nil
}

// GetDocAttributeLabelCount 获取属性标签总数（Gen风格）
func (d *daoImpl) GetDocAttributeLabelCount(ctx context.Context, selectColumns []string, filter *entity.DocAttributeLabelFilter) (int64, error) {
	gormClient, err := knowClient.GormClient(ctx, docAttributeLabelTableName, filter.RobotId, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeByBizIDs  get GormClient err:%v,robotID:%v", err, filter.RobotId)
		return 0, err
	}
	// 1. 初始化Gen生成的Query
	tbl := mysqlquery.Use(gormClient).TDocAttributeLabel
	q := tbl.WithContext(ctx)
	// 2. 动态选择列（若指定列则优化查询性能）
	if len(selectColumns) > 0 {
		fields := make([]field.Expr, 0, len(selectColumns))
		for _, col := range selectColumns {
			if f, ok := tbl.GetFieldByName(col); ok {
				fields = append(fields, f)
			}
		}
		q = q.Select(fields...)
	}
	// 3. 构建查询条件（类型安全字段引用）
	q = q.Where(
		tbl.RobotID.Eq(filter.RobotId),
		tbl.Source.Eq(uint32(filter.Source)),
	)
	if len(filter.AttrIDs) > 0 {
		q = q.Where(tbl.AttrID.In(filter.AttrIDs...))
	}
	if len(filter.LabelIDs) > 0 {
		q = q.Where(tbl.LabelID.In(filter.LabelIDs...))
	}
	if filter.IsDeleted != nil {
		q = q.Where(tbl.IsDeleted.Is(*filter.IsDeleted))
	}

	// 4. 执行计数查询
	count, err := q.Count()
	if err != nil {
		logx.E(ctx, "文档标签统计失败: robotID=%d, attrIDs=%v, labelIDs=%v, err=%v",
			filter.RobotId, filter.AttrIDs, filter.LabelIDs, err)
		return 0, fmt.Errorf("数据库查询失败: %w", err)
	}

	logx.D(ctx, "统计成功: robotID=%d, 总数=%d", filter.RobotId, count)
	return count, nil
}
func generateDocAttributeLabelListInfoCondition(q *mysqlquery.Query, db mysqlquery.ITDocAttributeLabelDo, filter *entity.DocAttributeLabelFilter) mysqlquery.ITDocAttributeLabelDo {
	// 1. 必选条件（索引顺序优先）
	if filter.RobotId != 0 {
		db = db.Where(q.TDocAttributeLabel.RobotID.Eq(filter.RobotId))
	}
	if filter.Source != 0 {
		db = db.Where(q.TDocAttributeLabel.Source.Eq(uint32(filter.Source)))
	}
	// 2. 动态条件
	if len(filter.AttrIDs) > 0 {
		db = db.Where(q.TDocAttributeLabel.AttrID.In(filter.AttrIDs...))
	}
	if len(filter.LabelIDs) > 0 {
		db = db.Where(q.TDocAttributeLabel.LabelID.In(filter.LabelIDs...))
	}
	// 3. 默认行为（未删除数据优先）
	if filter.IsDeleted != nil {
		db = db.Where(q.TDocAttributeLabel.IsDeleted.Is(*filter.IsDeleted))
	}
	return db
}

func (d *daoImpl) GetDocAttributeLabelListInfo(ctx context.Context, selectColumns []string, filter *entity.DocAttributeLabelFilter) ([]*entity.DocAttributeLabel, error) {
	gormClient, err := knowClient.GormClient(ctx, docAttributeLabelTableName, filter.RobotId, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetDocAttributeLabelListInfo  get GormClient err:%v,robotID:%v", err, filter.RobotId)
		return nil, err
	}
	attributeList := make([]*entity.DocAttributeLabel, 0)
	if filter.Limit == 0 {
		return attributeList, nil
	}
	if filter.Limit > entity.AttributeTableMaxPageSize {
		err := fmt.Errorf("GetAttributeLabelList invalid limit: %d", filter.Limit)
		return attributeList, err
	}
	tbl := mysqlquery.Use(gormClient).TDocAttributeLabel
	db := tbl.WithContext(ctx)
	// 构造条件 - 只有不为空的字段才加入查询
	db = generateDocAttributeLabelListInfoCondition(mysqlquery.Use(gormClient), db, filter)
	// 排序
	if len(filter.OrderColumn) > 0 {
		for i, orderColumn := range filter.OrderColumn {
			if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
				continue
			}
			orderField, ret := tbl.GetFieldByName(orderColumn)
			if !ret {
				return nil, fmt.Errorf("GetAttributeLabelList GetFieldByName failed for column: %s", orderColumn)
			}
			if filter.OrderDirection[i] == SqlOrderByAsc {
				db = db.Order(orderField)
			} else {
				db = db.Order(orderField.Desc())
			}
		}
	}
	// 分页
	db = db.Offset(filter.Offset).Limit(filter.Limit)
	// 查询
	qs, err := db.Find()
	if err != nil {
		return nil, err
	}
	return getDocAttributeLabelsPO2DO(qs), nil
}

func (d *daoImpl) DeleteAttribute(ctx context.Context, robotID uint64, attrIDs []uint64, tx *gorm.DB) error {
	if len(attrIDs) == 0 {
		return nil
	}
	// 使用 gen 的链式调用构建更新条件
	q := mysqlquery.Use(tx).TAttribute.WithContext(ctx)
	// 构建更新字段的映射
	updateMap := map[string]any{
		mysqlquery.Use(tx).TAttribute.IsDeleted.ColumnName().String():     entity.AttributeLabelDeleted,
		mysqlquery.Use(tx).TAttribute.UpdateTime.ColumnName().String():    time.Now(),
		mysqlquery.Use(tx).TAttribute.DeletedTime.ColumnName().String():   time.Now().UnixNano(),
		mysqlquery.Use(tx).TAttribute.ReleaseStatus.ColumnName().String(): entity.AttributeStatusWaitRelease,
		mysqlquery.Use(tx).TAttribute.NextAction.ColumnName().String(): gorm.Expr(
			"IF(next_action = ?, ?, ?)",
			entity.AttributeNextActionAdd,
			entity.AttributeNextActionDelete,
			entity.AttributeNextActionAdd,
		),
	}
	res, err := q.Where(
		mysqlquery.Use(tx).TAttribute.IsDeleted.Is(false), // 只更新未删除的记录
		mysqlquery.Use(tx).TAttribute.RobotID.Eq(robotID), // 指定机器人ID
		mysqlquery.Use(tx).TAttribute.ID.In(attrIDs...),   // 指定属性ID列表
	).Updates(updateMap)
	if err != nil {
		logx.E(ctx, "deleteAttribute failed: robotID=%d, attrIDs=%v, err=%v", robotID, attrIDs, err)
		return fmt.Errorf("deleteAttribute failed: %w", err)
	}
	logx.I(ctx, "DeleteAttribute ok robotID=%d, affected=%d", robotID, res.RowsAffected)
	return nil
}

// DeleteAttributeLabel 删除属性标签（Gen风格）
func (d *daoImpl) DeleteAttributeLabel(ctx context.Context, robotID uint64, attrID uint64, deleteLabelIDs []uint64, tx *gorm.DB) error {
	if len(deleteLabelIDs) == 0 {
		return nil
	}
	// 使用 gen 的链式调用构建更新条件
	q := mysqlquery.Use(tx).TAttributeLabel.WithContext(ctx)
	// 构建更新字段的映射
	updateMap := map[string]any{
		mysqlquery.Use(tx).TAttributeLabel.IsDeleted.ColumnName().String():     entity.AttributeLabelDeleted,
		mysqlquery.Use(tx).TAttributeLabel.UpdateTime.ColumnName().String():    time.Now(),
		mysqlquery.Use(tx).TAttributeLabel.ReleaseStatus.ColumnName().String(): entity.AttributeStatusWaitRelease,
		mysqlquery.Use(tx).TAttributeLabel.NextAction.ColumnName().String(): gorm.Expr(
			"IF(next_action = ?, ?, ?)",
			entity.AttributeNextActionAdd,
			entity.AttributeNextActionAdd,
			entity.AttributeNextActionDelete,
		),
	}
	// 执行更新操作
	res, err := q.Where(
		mysqlquery.Use(tx).TAttributeLabel.IsDeleted.Is(false),      // 只更新未删除的记录
		mysqlquery.Use(tx).TAttributeLabel.RobotID.Eq(robotID),      // 指定机器人ID
		mysqlquery.Use(tx).TAttributeLabel.AttrID.Eq(attrID),        // 指定属性ID
		mysqlquery.Use(tx).TAttributeLabel.ID.In(deleteLabelIDs...), // 指定要删除的标签ID列表
	).Updates(updateMap)
	if err != nil {
		logx.E(ctx, "DeleteAttributeLabel failed: robotID=%d, attrID=%d, labelIDs=%v, err=%v",
			robotID, attrID, deleteLabelIDs, err)
		return fmt.Errorf("delete attribute label failed: %w", err)
	}
	logx.I(ctx, "DeleteAttributeLabel ok robotID=%d, attrID=%d, affected=%d",
		robotID, attrID, res.RowsAffected)
	return nil
}
func (d *daoImpl) DeleteAttributeLabelByAttrIDs(ctx context.Context, robotID uint64, attrIDs []uint64, tx *gorm.DB) error {
	if len(attrIDs) == 0 {
		return nil
	}
	// 使用 gen 的链式调用构建更新条件
	q := mysqlquery.Use(tx).TAttributeLabel.WithContext(ctx)
	// 构建更新字段的映射
	updateMap := map[string]any{
		mysqlquery.Use(tx).TAttributeLabel.IsDeleted.ColumnName().String():     entity.AttributeLabelDeleted,
		mysqlquery.Use(tx).TAttributeLabel.ReleaseStatus.ColumnName().String(): entity.AttributeStatusWaitRelease,
		mysqlquery.Use(tx).TAttributeLabel.UpdateTime.ColumnName().String():    time.Now(),
		// 使用 GORM 的 Expr 来表达复杂的 IF 条件
		mysqlquery.Use(tx).TAttributeLabel.NextAction.ColumnName().String(): gorm.Expr(
			"IF(next_action = ?, ?, ?)",
			entity.AttributeNextActionAdd,
			entity.AttributeNextActionDelete,
			entity.AttributeNextActionAdd,
		),
	}
	// 执行更新
	res, err := q.Where(
		mysqlquery.Use(tx).TAttributeLabel.RobotID.Eq(robotID),   // 指定机器人ID
		mysqlquery.Use(tx).TAttributeLabel.IsDeleted.Is(false),   // 只更新未删除的记录
		mysqlquery.Use(tx).TAttributeLabel.AttrID.In(attrIDs...), // 指定属性ID列表（注意这里是 AttrID 不是 ID）
	).Updates(updateMap)

	if err != nil {
		logx.E(ctx, "deleteAttributeLabelByAttrIDs failed: robotID=%d, attrIDs=%v, err=%v",
			robotID, attrIDs, err)
		return fmt.Errorf("deleteAttributeLabelByAttrIDs failed: %w", err)
	}
	// 检查实际更新行数（可选，根据业务需求）
	if res.RowsAffected == 0 {
		logx.W(ctx, "deleteAttributeLabelByAttrIDs RowsAffected = 0, attrIDs=%v, robotID=%d",
			attrIDs, robotID)
	}
	logx.I(ctx, "deleteAttributeLabelByAttrIDs success !!! : robotID=%d, affected=%d",
		robotID, res.RowsAffected)
	return nil
}

// UpdateAttributeSuccess 更新属性状态为成功（Gen风格）
func (d *daoImpl) UpdateAttributeSuccess(ctx context.Context, attr *entity.Attribute, tx *gorm.DB) error {
	// 1. 使用Gen生成的Query（自动绑定事务）
	q := mysqlquery.Use(tx).TAttribute.WithContext(ctx)
	// 2. 构建类型安全更新条件
	// 2.1 更新属性状态
	_, err := q.Where(
		mysqlquery.Use(tx).TAttribute.RobotID.Eq(attr.RobotID),
		mysqlquery.Use(tx).TAttribute.IsDeleted.Is(attr.IsDeleted),
		mysqlquery.Use(tx).TAttribute.ID.Eq(attr.ID),
	).Updates(map[string]any{
		mysqlquery.Use(tx).TAttribute.IsUpdating.ColumnName().String():    boolx.FalseNumber,
		mysqlquery.Use(tx).TAttribute.ReleaseStatus.ColumnName().String(): entity.AttributeStatusWaitRelease,
		mysqlquery.Use(tx).TAttribute.UpdateTime.ColumnName().String():    time.Now(),
	})
	if err != nil {
		return fmt.Errorf("UpdateAttributeSuccess failed : robotID=%d, attrID=%d, err=%w",
			attr.RobotID, attr.ID, err)
	}
	return nil
}

// UpdateAttributeFail 更新属性状态为成功（Gen风格）
func (d *daoImpl) UpdateAttributeFail(ctx context.Context, attr *entity.Attribute, tx *gorm.DB) error {
	// 1. 使用Gen生成的Query（自动绑定事务）
	q := mysqlquery.Use(tx).TAttribute.WithContext(ctx)
	// 2. 更新属性状态
	res, err := q.Where(
		mysqlquery.Use(tx).TAttribute.RobotID.Eq(attr.RobotID),
		mysqlquery.Use(tx).TAttribute.IsDeleted.Is(true),
		mysqlquery.Use(tx).TAttribute.ID.Eq(attr.ID),
	).Updates(map[string]any{
		mysqlquery.Use(tx).TAttribute.IsUpdating.ColumnName().String():    boolx.FalseNumber,
		mysqlquery.Use(tx).TAttribute.ReleaseStatus.ColumnName().String(): entity.AttributeStatusReleaseFail,
		mysqlquery.Use(tx).TAttribute.UpdateTime.ColumnName().String():    time.Now(),
	})
	if err != nil {
		return fmt.Errorf("UpdateAttributeFail failed : robotID=%d, attrID=%d, err=%w",
			attr.RobotID, attr.ID, err)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("UpdateAttributeFail RowsAffected= 0  robotID=%d, attrID=%d",
			attr.RobotID, attr.ID)
	}
	return nil
}

// GetAttributeLabelList 分页查询属性标签列表
func (d *daoImpl) GetAttributeLabelList(ctx context.Context, attrID uint64, queryStr string, queryScope string,
	lastLabelID uint64, limit uint32, robotID uint64) ([]*entity.AttributeLabel, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeLabelTableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeLabelList  get GormClient err:%v,robotID:%v", err, robotID)
		return nil, err
	}
	// 1. 初始化Gen生成的Query
	tbl := mysqlquery.Use(gormClient).TAttributeLabel
	q := tbl.WithContext(ctx)
	// 2. 构建基础条件（类型安全）
	q = q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.AttrID.Eq(attrID),
		tbl.IsDeleted.Is(false),
	)
	// 3. 动态添加查询条件
	if queryStr != "" {
		filter := fmt.Sprintf("%%%s%%", strings.ReplaceAll(queryStr, "%", "\\%"))
		switch queryScope {
		case entity.AttributeLabelQueryScopeStandard:
			q = q.Where(tbl.Name.Like(filter))
		case entity.AttributeLabelQueryScopeSimilar:
			q = q.Where(tbl.SimilarLabel.Like(filter))
		default: // 包括attributeLabelQueryScopeAll和其他情况
			q = q.Where(
				field.Or(tbl.Name.Like(filter), tbl.SimilarLabel.Like(filter)),
			)
		}
	}
	if lastLabelID > 0 {
		q = q.Where(tbl.ID.Lt(lastLabelID))
	}
	// 4. 执行ID列表查询
	var ids []uint64
	if err := q.Order(tbl.ID.Desc()).Limit(int(limit)).Pluck(tbl.ID, &ids); err != nil {
		return nil, fmt.Errorf("GetAttributeLabelList Pluck failed err : %v", err)
	}
	// 5. 调用原有方法按ID顺序获取完整标签数据
	return d.GetAttributeLabelByIDOrder(ctx, robotID, ids)
}

// UpdateAttribute 更新属性标签（Gen风格）
func (d *daoImpl) UpdateAttribute(ctx context.Context, attr *entity.Attribute, tx *gorm.DB) error {
	// 1. 参数校验
	if attr.ID == 0 || attr.RobotID == 0 {
		logx.E(ctx, "更新参数错误: id=%d, robotID=%d", attr.ID, attr.RobotID)
		return errs.ErrParams
	}
	// 2. 使用Gen生成的Query
	q := mysqlquery.Use(tx).TAttribute.WithContext(ctx)
	// 3. 构建更新字段
	updates := map[string]any{
		"name":           attr.Name,
		"is_updating":    attr.IsUpdating,
		"next_action":    attr.NextAction,
		"release_status": attr.ReleaseStatus,
		"update_time":    time.Now(),
	}
	// 4. 执行更新操作（包含ID条件）
	_, err := q.Where(
		mysqlquery.Use(tx).TAttribute.ID.Eq(attr.ID),
		mysqlquery.Use(tx).TAttribute.RobotID.Eq(attr.RobotID),
		mysqlquery.Use(tx).TAttribute.IsDeleted.Is(attr.IsDeleted),
	).Updates(updates)
	if err != nil {
		return fmt.Errorf("update attribute failed: %v", err)
	}
	return nil
}

// UpdateAttributeLabels 批量更新属性标签（Gen风格）
func (d *daoImpl) UpdateAttributeLabels(ctx context.Context, labels []*entity.AttributeLabel, tx *gorm.DB) error {
	if len(labels) == 0 {
		return nil
	}
	// 批量更新每条记录
	for _, label := range labels {
		if err := d.updateAttributeLabel(ctx, label); err != nil {
			logx.E(ctx, "批量更新标签失败: id=%d, err: %v", label.ID, err)
			return fmt.Errorf("batch update attribute labels failed: %v", err)
		}
	}
	return nil
}

// UpdateAttributeLabel 更新属性标签（Gen风格）
func (d *daoImpl) updateAttributeLabel(ctx context.Context, label *entity.AttributeLabel) error {
	// 1. 参数校验
	if label.ID == 0 || label.RobotID == 0 || label.AttrID == 0 {
		logx.E(ctx, "更新参数错误: id=%d, robotID=%d, attrID=%d",
			label.ID, label.RobotID, label.AttrID)
		return errs.ErrParams
	}
	gormClient, err := knowClient.GormClient(ctx, attributeLabelTableName, label.RobotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "updateAttributeLabel  get GormClient err:%v,robotID:%v", err, label.RobotID)
		return err
	}
	// 2. 使用Gen生成的Query
	q := mysqlquery.Use(gormClient).TAttributeLabel.WithContext(ctx)
	// 3. 构建更新字段
	updates := map[string]any{
		"name":           label.Name,
		"similar_label":  label.SimilarLabel,
		"next_action":    label.NextAction,
		"release_status": label.ReleaseStatus,
		"is_deleted":     label.IsDeleted,
		"update_time":    time.Now(),
	}
	// 4. 执行更新操作（包含ID条件）
	res, err := q.Where(
		mysqlquery.Use(gormClient).TAttributeLabel.ID.Eq(label.ID),
		mysqlquery.Use(gormClient).TAttributeLabel.RobotID.Eq(label.RobotID),
		mysqlquery.Use(gormClient).TAttributeLabel.AttrID.Eq(label.AttrID),
	).Updates(updates)

	if err != nil {
		logx.E(ctx, "更新标签标准词失败: %+v, err: %v", label, err)
		return fmt.Errorf("update attribute label failed: %v", err)
	}
	// 5. 检查实际更新行数
	if res.RowsAffected == 0 {
		return fmt.Errorf("UpdateAttributeLabel RowsAffected = 0, id=%d, robotID=%d",
			label.ID, label.RobotID)
	}
	return nil
}

// CreateAttributeLabel 创建属性标签（Gen风格）
func (d *daoImpl) CreateAttributeLabel(ctx context.Context, labels []*entity.AttributeLabel, tx *gorm.DB) error {
	if len(labels) == 0 {
		return nil
	}
	// 转换为PO模型列表
	poList := make([]*model.TAttributeLabel, 0, len(labels))
	now := time.Now()

	for _, label := range labels {
		if label != nil {
			poList = append(poList, &model.TAttributeLabel{
				// ID:            label.ID,
				RobotID:       label.RobotID,
				BusinessID:    label.BusinessID,
				AttrID:        label.AttrID,
				Name:          label.Name,
				SimilarLabel:  label.SimilarLabel,
				ReleaseStatus: label.ReleaseStatus,
				NextAction:    label.NextAction,
				IsDeleted:     label.IsDeleted,
				CreateTime:    now,
				UpdateTime:    now,
			})
		}
	}

	if len(poList) == 0 {
		return nil
	}
	// 使用Gen的批量插入功能
	err := mysqlquery.Use(tx).TAttributeLabel.
		WithContext(ctx).
		CreateInBatches(poList, 100) // 每批100条

	if err != nil {
		logx.E(ctx, "CreateAttributeLabel faield : count=%d, err=%v", len(poList), err)
		return fmt.Errorf("create attribute label failed: %v", err)
	}

	logx.I(ctx, "Create Attribute Label success: count=%d", len(poList))
	for i, tLabel := range poList {
		labels[i].ID = tLabel.ID
	}

	return nil
}

// CreateAttribute 创建属性标签（Gen风格）, 必须要传入自增ID
func (d *daoImpl) CreateAttribute(ctx context.Context, attr *entity.Attribute, tx *gorm.DB) (uint64, error) {
	// 1. 参数校验
	if attr.RobotID == 0 || attr.Name == "" {
		return 0, fmt.Errorf("创建参数错误: robotID=%d, name=%s", attr.RobotID, attr.Name)
	}
	// 2. 设置创建时间
	now := time.Now()
	attr.CreateTime = now
	attr.UpdateTime = now
	// 3. 转换为PO模型
	po := convertAttributeDO2PO(attr)
	// 4. 执行插入操作
	if err := tx.Table(attributeTableName).Create(po).Error; err != nil {
		logx.E(ctx, "创建标签失败: %+v, err: %v", attr, err)
		return 0, fmt.Errorf("create attribute failed: %v", err)
	}
	// 5. 返回自增ID（Gen会自动填充到po.ID）
	return po.ID, nil
}

// convertAttributeDO2PO 领域对象转持久化对象
func convertAttributeDO2PO(attr *entity.Attribute) *model.TAttribute {
	if attr == nil {
		return nil
	}
	return &model.TAttribute{
		ID:            attr.ID,
		BusinessID:    attr.BusinessID,
		RobotID:       attr.RobotID,
		AttrKey:       attr.AttrKey,
		Name:          attr.Name,
		IsUpdating:    attr.IsUpdating,
		ReleaseStatus: attr.ReleaseStatus,
		NextAction:    attr.NextAction,
		IsDeleted:     attr.IsDeleted,
		DeletedTime:   attr.DeletedTime,
		CreateTime:    attr.CreateTime,
		UpdateTime:    attr.UpdateTime,
	}
}

func (d *daoImpl) UpdateDocAttributeLabelByTx(ctx context.Context, robotID, docID uint64,
	attributeLabelReq *entity.UpdateDocAttributeLabelReq, tx *gorm.DB) error {
	if !attributeLabelReq.IsNeedChange {
		return nil
	}
	for _, v := range attributeLabelReq.AttributeLabels {
		v.RobotID, v.DocID = robotID, docID
	}
	if err := d.DeleteDocAttributeLabel(ctx, robotID, docID, tx); err != nil {
		return err
	}
	if err := d.CreateDocAttributeLabel(ctx, attributeLabelReq.AttributeLabels, tx); err != nil {
		return err
	}
	return nil
}

func (d *daoImpl) CreateDocAttributeLabel(ctx context.Context, labels []*entity.DocAttributeLabel, tx *gorm.DB) error {
	if len(labels) == 0 {
		return nil
	}
	/*
			`
		        INSERT INTO
		            t_doc_attribute_label (%s)
		        VALUES
		            (:id,:robot_id,:doc_id,:source,:attr_id,:label_id,:is_deleted,NOW(),NOW())
		    `
	*/
	// 1. 使用Gen生成的Query（自动绑定事务）
	tbl := d.mysql.TDocAttributeLabel
	if tx != nil {
		tbl = mysqlquery.Use(tx).TDocAttributeLabel
	}
	q := tbl.WithContext(ctx)
	// 2. 构建类型安全创建条件
	tLables := getDocAttributeLabelsDO2PO(labels)
	if err := q.UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		for _, tLabel := range tLables {
			if err := tx.Create(&tLabel).Error; err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (d *daoImpl) DeleteDocAttributeLabel(ctx context.Context, robotID uint64, docID uint64, tx *gorm.DB) error {
	/*
		`
		        UPDATE
		            t_doc_attribute_label
		        SET
		            is_deleted = ?
		        WHERE
		            robot_id = ? AND doc_id = ? AND is_deleted = ?
		    `
	*/

	tbl := d.mysql.TDocAttributeLabel
	if tx != nil {
		tbl = mysqlquery.Use(tx).TDocAttributeLabel
	}
	q := tbl.WithContext(ctx)
	updateMap := map[string]any{
		tbl.IsDeleted.ColumnName().String(): entity.DocAttributeLabelDeleted,
	}
	// 执行更新（带条件校验）
	_, err := q.Where(
		tbl.RobotID.Eq(robotID),
		tbl.DocID.Eq(docID),
	).Updates(updateMap)
	if err != nil {
		return fmt.Errorf("DeleteDocAttributeLabel failed : robotID=%d, docID:%d, err=%v",
			robotID, docID, err)
	}
	logx.I(ctx, "DeleteDocAttributeLabel ok robotID=%d, docID=%d", robotID, docID)
	return nil
}

func (d *daoImpl) CreateQAAttributeLabel(ctx context.Context, labels []*entity.QAAttributeLabel) error {
	if len(labels) == 0 {
		logx.I(ctx, "CreateQAAttributeLabel|labels is empty. skip it")
		return nil
	}

	/*
			`
		        INSERT INTO
		            t_qa_attribute_label (%s)
		        VALUES
		            (:id,:robot_id,:qa_id,:source,:attr_id,:label_id,:is_deleted,NOW(),NOW())
		    `
	*/

	// 1. 使用Gen生成的Query（自动绑定事务）
	q := d.mysql.TQaAttributeLabel.WithContext(ctx)
	// 2. 构建类型安全创建条件
	tLables := getQAAttributeLabelsDO2PO(labels)
	if err := q.UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		for _, tLabel := range tLables {
			if err := tx.Create(&tLabel).Error; err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (d *daoImpl) DeleteQAAttributeLabel(ctx context.Context, robotID uint64, qaID uint64) error {
	/*
			`
		        UPDATE
		            t_qa_attribute_label
		        SET
		            is_deleted = ?
		        WHERE
		            robot_id = ? AND qa_id = ? AND is_deleted = ?
		    `
	*/

	q := d.mysql.TQaAttributeLabel.WithContext(ctx)
	updateMap := map[string]any{
		d.mysql.TQaAttributeLabel.IsDeleted.ColumnName().String(): entity.QAAttributeLabelDeleted,
	}
	// 执行更新（带条件校验）
	_, err := q.Where(
		d.mysql.TQaAttributeLabel.RobotID.Eq(robotID),
		d.mysql.TQaAttributeLabel.QaID.Eq(qaID),
		d.mysql.TQaAttributeLabel.IsDeleted.Eq(entity.QAAttributeLabelIsNotDeleted),
	).Updates(updateMap)
	if err != nil {
		return fmt.Errorf("DeleteQAAttributeLabel failed : robotID=%d, qaID:%d, err=%v",
			robotID, qaID, err)
	}
	logx.I(ctx, "DeleteQAAttributeLabel ok robotID=%d, qaID=%d", robotID, qaID)
	return nil
}

// GetAttributeLabelCountV2 获取属性标签总数 原attributeLabelDao对象用的
func (d *daoImpl) GetAttributeLabelCountV2(ctx context.Context, selectColumns []string,
	filter *entity.AttributeLabelFilter) (int64, error) {
	gormClient, err := knowClient.GormClient(ctx, attributeLabelTableName, filter.RobotId, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "GetAttributeLabelCountV2  get GormClient err:%v,robotID:%v", err, filter.RobotId)
		return 0, err
	}
	q := mysqlquery.Use(gormClient).TAttributeLabel.WithContext(ctx)
	gormDB := q.UnderlyingDB()
	gormDB = d.getAttributeLabelCountV2Condition(ctx, gormDB, filter)
	count := int64(0)
	res := gormDB.Select(selectColumns).Count(&count)
	if res.Error != nil {
		logx.E(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// 生GetAttributeLabelCountCondition 成查询条件，必须按照索引的顺序排列
func (d *daoImpl) getAttributeLabelCountV2Condition(ctx context.Context, session *gorm.DB, filter *entity.AttributeLabelFilter) *gorm.DB {
	if filter.RobotId != 0 {
		session = session.Where(entity.AttributeLabelTblColRobotId+sqlEqual, filter.RobotId)
	}
	if len(filter.BusinessIds) != 0 {
		session = session.Where(entity.AttributeLabelTblColBusinessId+sqlIn, filter.BusinessIds)
	}
	if len(filter.AttrIds) != 0 {
		session = session.Where(entity.AttributeLabelTblColAttrId+sqlIn, filter.AttrIds)
	}
	if filter.NotEmptySimilarLabel != nil {
		if *filter.NotEmptySimilarLabel {
			session = session.Where(entity.AttributeLabelTblColSimilarLabel+sqlNotEqual, "")
		} else {
			session = session.Where(entity.AttributeLabelTblColSimilarLabel+sqlEqual, "")
		}
	}
	if filter.NameOrSimilarLabelSubStr != "" {
		session = session.Where(session.Where(entity.AttributeLabelTblColName+sqlLike, "%"+filter.NameOrSimilarLabelSubStr+"%").
			Or(entity.AttributeLabelTblColSimilarLabel+sqlLike, "%"+filter.NameOrSimilarLabelSubStr+"%"))
	}
	if filter.IsDeleted == nil {
		// 默认查询未删除的数据
		session = session.Where(entity.AttributeLabelTblColIsDeleted+sqlEqual, IsNotDeleted)
	} else {
		session = session.Where(entity.AttributeLabelTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}
	return session
}

func fillAttributeWaitReleaseParams(robotID uint64, name string, actions []uint32, startTime,
	endTime time.Time) ([]any, string) {
	args := make([]any, 0)
	args = append(args, robotID, releaseEntity.LabelReleaseStatusInit, releaseEntity.LabelNextActionAdd, true)
	condition := ""
	if len(name) > 0 {
		condition += " AND name LIKE ?"
		args = append(args, fmt.Sprintf("%%%s%%", util.Special.Replace(name)))
	}
	if len(actions) > 0 {
		condition += fmt.Sprintf(" AND next_action IN (%s)", util.Placeholder(len(actions)))
		for _, action := range actions {
			args = append(args, action)
		}
	}
	if !startTime.IsZero() {
		condition += " AND update_time >= ?"
		args = append(args, startTime)
	}
	if !endTime.IsZero() {
		condition += " AND update_time <= ?"
		args = append(args, endTime)
	}
	return args, condition
}

func (d *daoImpl) GetWaitReleaseAttributeCount(ctx context.Context, robotID uint64, name string,
	actions []uint32, startTime, endTime time.Time) (uint64, error) {
	/*
				`
		        SELECT
		            COUNT(1)
		        FROM
		            t_attribute
		        WHERE
		            robot_id = ? AND release_status = ? AND !(next_action = ? AND is_deleted = ?) %s
		    `
	*/
	conditionStr := "robot_id = ? AND release_status = ? AND !(next_action = ? AND is_deleted = ?)"

	args, condition := fillAttributeWaitReleaseParams(robotID, name, actions, startTime, endTime)
	conditionStr = fmt.Sprintf("%s %s", conditionStr, condition)

	var total int64
	db, err := knowClient.GormClient(ctx, d.mysql.TAttribute.TableName(), robotID, 0)
	if err != nil {
		logx.E(ctx, "GetWaitReleaseAttributeCount get GormClient failed, err: %+v", err)
		return 0, err
	}

	if err := mysqlquery.Use(db).TAttribute.WithContext(ctx).Debug().UnderlyingDB().
		Model(&model.TAttribute{}).
		Select("COUNT(1)").
		Where(conditionStr, args...).Take(&total).Error; err != nil {
		logx.E(ctx, "GetWaitReleaseAttributeCount query failed, err: %+v", err)
		return 0, err
	}
	return uint64(total), nil
}

func (d *daoImpl) GetWaitReleaseAttributeList(ctx context.Context, robotID uint64, name string,
	actions []uint32, page, pageSize uint32, startTime, endTime time.Time) ([]*entity.Attribute, error) {
	/*
				`
		        SELECT
		            %s
		        FROM
		            t_attribute
		        WHERE
		            robot_id = ? AND release_status = ? AND !(next_action = ? AND is_deleted = ?) %s
		        ORDER BY
		            id ASC
		        LIMIT
		            ?,?
		    `
	*/
	labels := make([]*model.TAttribute, 0)
	conditionStr := "robot_id = ? AND release_status = ? AND !(next_action = ? AND is_deleted = ?)"
	args, condition := fillAttributeWaitReleaseParams(robotID, name, actions, startTime, endTime)
	conditionStr = fmt.Sprintf("%s %s", conditionStr, condition)
	offset, limit := utilx.Page(page, pageSize)

	selectColumns := []string{
		d.mysql.TAttribute.ID.ColumnName().String(),
		d.mysql.TAttribute.RobotID.ColumnName().String(),
		d.mysql.TAttribute.BusinessID.ColumnName().String(),
		d.mysql.TAttribute.AttrKey.ColumnName().String(),
		d.mysql.TAttribute.Name.ColumnName().String(),
		d.mysql.TAttribute.ReleaseStatus.ColumnName().String(),
		d.mysql.TAttribute.NextAction.ColumnName().String(),
		d.mysql.TAttribute.IsDeleted.ColumnName().String(),
		d.mysql.TAttribute.IsUpdating.ColumnName().String(),
		d.mysql.TAttribute.DeletedTime.ColumnName().String(),
		d.mysql.TAttribute.CreateTime.ColumnName().String(),
		d.mysql.TAttribute.UpdateTime.ColumnName().String(),
	}

	db, err := knowClient.GormClient(ctx, d.mysql.TAttribute.TableName(), robotID, 0)
	if err != nil {
		logx.E(ctx, "GetWaitReleaseAttributeList get GormClient failed, err: %+v", err)
		return nil, err
	}

	if err := mysqlquery.Use(db).TAttribute.WithContext(ctx).Debug().UnderlyingDB().Select(selectColumns).
		Where(conditionStr, args...).
		Limit(limit).Offset(offset).
		Order(d.mysql.TAttribute.ID.Asc()).
		Find(&labels).Error; err != nil {
		return nil, err
	}

	return getAttributeListsPO2DO(labels), nil

}

func (d *daoImpl) GetWaitReleaseAttributeLables(ctx context.Context, robotID uint64, attrIDs []uint64) (
	[]*entity.AttributeLabel, error) {
	/*
		`
		        SELECT
		            %s
		        FROM
		            t_attribute_label
		        WHERE
		            robot_id = ? AND release_status = ? AND attr_id IN (%s)
		    `
	*/
	labelValues := make([]*model.TAttributeLabel, 0)
	selectColumns := []string{
		d.mysql.TAttributeLabel.ID.ColumnName().String(),
		d.mysql.TAttributeLabel.BusinessID.ColumnName().String(),
		d.mysql.TAttributeLabel.RobotID.ColumnName().String(),
		d.mysql.TAttributeLabel.AttrID.ColumnName().String(),
		d.mysql.TAttributeLabel.Name.ColumnName().String(),
		d.mysql.TAttributeLabel.SimilarLabel.ColumnName().String(),
		d.mysql.TAttributeLabel.ReleaseStatus.ColumnName().String(),
		d.mysql.TAttributeLabel.NextAction.ColumnName().String(),
		d.mysql.TAttributeLabel.IsDeleted.ColumnName().String(),
		d.mysql.TAttributeLabel.CreateTime.ColumnName().String(),
		d.mysql.TAttributeLabel.UpdateTime.ColumnName().String(),
	}

	db, err := knowClient.GormClient(ctx, d.mysql.TAttributeLabel.TableName(), robotID, 0)
	if err != nil {
		logx.E(ctx, "GetWaitReleaseAttributeLables get GormClient failed, err: %+v", err)
		return nil, err
	}

	if err := db.Select(selectColumns).
		Where("robot_id = ? AND release_status = ? AND attr_id IN (?)",
			robotID, releaseEntity.LabelReleaseStatusInit, attrIDs).
		Find(&labelValues).Error; err != nil {
		return nil, err
	}

	return getAttributeLabelsPO2DO(labelValues), nil

}

func (d *daoImpl) BatchUpdateAttributes(ctx context.Context, attrReq *entity.AttributeFilter, updateColumns map[string]any, tx *gorm.DB) error {
	session := tx
	if session == nil {
		session = d.mysql.TAttribute.WithContext(ctx).UnderlyingDB()
	}
	session = session.Table(d.mysql.TAttribute.TableName())

	if attrReq != nil {
		if attrReq.RobotId != 0 {
			session = session.Where(entity.AttributeTblColRobotId+sqlEqual, attrReq.RobotId)
		}
		if len(attrReq.BusinessIds) != 0 {
			session = session.Where(entity.AttributeTblColBusinessId+sqlIn, attrReq.BusinessIds)
		}
		if len(attrReq.Ids) != 0 {
			session = session.Where(entity.AttributeTblColId+sqlIn, attrReq.Ids)
		}

	}

	if err := session.Updates(updateColumns).Error; err != nil {
		logx.E(ctx, "BatchUpdateAttributes failed, req:%+v err: %+v", attrReq, err)
		return err
	}

	return nil

}
func (d *daoImpl) BatchUpdateAttributeLabels(ctx context.Context, attrLabelReq *entity.AttributeLabelFilter, updateColumns map[string]any, tx *gorm.DB) error {
	session := tx
	if session == nil {
		session = d.mysql.TAttributeLabel.WithContext(ctx).UnderlyingDB()
	}

	session = session.Table(d.mysql.TAttributeLabel.TableName())

	if attrLabelReq != nil {
		if attrLabelReq.RobotId != 0 {
			session = session.Where(entity.AttributeLabelTblColRobotId+sqlEqual, attrLabelReq.RobotId)
		}
		if len(attrLabelReq.AttrIds) != 0 {
			session = session.Where(entity.AttributeLabelTblColAttrId+sqlIn, attrLabelReq.AttrIds)
		}
		if attrLabelReq.AttrId != 0 {
			session = session.Where(entity.AttributeLabelTblColAttrId+sqlEqual, attrLabelReq.AttrId)
		}
		if len(attrLabelReq.Ids) != 0 {
			session = session.Where(entity.AttributeLabelTblColId+sqlIn, attrLabelReq.Ids)
		}

	}

	if err := session.Updates(updateColumns).Error; err != nil {
		logx.E(ctx, "BatchUpdateAttributeLabels failed, req:%+v err: %+v", attrLabelReq, err)
		return err
	}

	return nil
}

// UpdateAttributeStatus -
func (d *daoImpl) UpdateAttributeStatus(ctx context.Context, attribute *entity.Attribute, tx *gorm.DB) error {
	tbl := mysqlquery.Use(tx).TAttributeLabel
	releaseAttributeUpdateValues := map[string]interface{}{
		tbl.NextAction.ColumnName().String():    attribute.NextAction,
		tbl.ReleaseStatus.ColumnName().String(): attribute.ReleaseStatus,
		tbl.UpdateTime.ColumnName().String():    time.Now(),
	}
	err := tx.Table(model.TableNameTAttribute).
		Where("robot_id = ? AND id = ? ", attribute.RobotID, attribute.ID).
		Updates(releaseAttributeUpdateValues).Error
	if err != nil {
		return fmt.Errorf("UpdateAttributeStatus faield , robotID:%d, status:%v, err:%+v",
			attribute.RobotID, attribute.ReleaseStatus, err)
	}
	return nil
}

// UpdateAttributeLabelStatus -
func (d *daoImpl) UpdateAttributeLabelStatus(ctx context.Context, attributeLabel *entity.AttributeLabel, tx *gorm.DB) error {
	tbl := mysqlquery.Use(tx).TAttributeLabel
	releaseAttributeUpdateValues := map[string]interface{}{
		tbl.NextAction.ColumnName().String():    attributeLabel.NextAction,
		tbl.ReleaseStatus.ColumnName().String(): attributeLabel.ReleaseStatus,
		tbl.UpdateTime.ColumnName().String():    time.Now(),
	}
	err := tx.Table(attributeLabelTableName).
		Where("id = ? AND robot_id = ? AND attr_id = ?", attributeLabel.ID, attributeLabel.RobotID, attributeLabel.AttrID).
		Updates(releaseAttributeUpdateValues).Error
	if err != nil {
		return fmt.Errorf("UpdateAttributeLabelStatus failed , robotID:%d, status:%s, err:%+v",
			attributeLabel.RobotID, attributeLabel.ReleaseStatus, err)
	}
	return nil
}

func getAttributeProdDO2PO(do *entity.AttributeProd) *model.TAttributeProd {
	if do == nil {
		return nil
	}
	return &model.TAttributeProd{
		AttrID:      do.AttrID,
		BusinessID:  do.BusinessID,
		RobotID:     int64(do.RobotID),
		AttrKey:     do.AttrKey,
		Name:        do.Name,
		IsDeleted:   do.IsDeleted,
		DeletedTime: do.DeletedTime,
		CreateTime:  do.CreateTime,
		UpdateTime:  do.UpdateTime,
	}
}

func (d *daoImpl) ReleaseAttributeProd(ctx context.Context, releaseAttribute *releaseEntity.ReleaseAttribute, tx *gorm.DB) error {
	tbl := mysqlquery.Use(tx).TAttributeProd
	now := time.Now()
	attributeProd := &entity.AttributeProd{
		AttrID:      releaseAttribute.AttrID,
		BusinessID:  releaseAttribute.BusinessID,
		RobotID:     releaseAttribute.RobotID,
		AttrKey:     releaseAttribute.AttrKey,
		Name:        releaseAttribute.Name,
		IsDeleted:   releaseAttribute.IsDeleted,
		DeletedTime: releaseAttribute.DeletedTime,
		CreateTime:  now,
		UpdateTime:  now,
	}
	var err2 error
	if releaseAttribute.Action == releaseEntity.LabelNextActionAdd {
		// 新增
		err2 = tx.Table(model.TableNameTAttributeProd).Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: tbl.BusinessID.ColumnName().String()}},
			DoUpdates: clause.AssignmentColumns([]string{
				tbl.AttrKey.ColumnName().String(),
				tbl.Name.ColumnName().String(),
				tbl.IsDeleted.ColumnName().String(),
				tbl.DeletedTime.ColumnName().String(),
				tbl.UpdateTime.ColumnName().String(),
			}),
		}).
			Create(getAttributeProdDO2PO(attributeProd)).Error
	} else {
		// 更新
		updateValues := map[string]interface{}{
			tbl.UpdateTime.ColumnName().String():  now,
			tbl.AttrKey.ColumnName().String():     releaseAttribute.AttrKey,
			tbl.Name.ColumnName().String():        releaseAttribute.Name,
			tbl.IsDeleted.ColumnName().String():   releaseAttribute.IsDeleted,
			tbl.DeletedTime.ColumnName().String(): releaseAttribute.DeletedTime,
		}
		err2 = tx.Table(model.TableNameTAttributeProd).
			Where("robot_id = ? AND attr_id = ?", releaseAttribute.RobotID, releaseAttribute.AttrID).
			Updates(updateValues).Error
	}

	if err2 != nil {
		return fmt.Errorf("Failed to ReleaseAttributeProd. attrID:%d, action:%v, args:%+v, err:%+v",
			releaseAttribute.AttrID, releaseAttribute.Action, attributeProd, err2)
	}
	return nil
}

func getAttributeLabelProdDO2PO(do *entity.AttributeLabelProd) *model.TAttributeLabelProd {
	if do == nil {
		return nil
	}
	return &model.TAttributeLabelProd{
		RobotID:      do.RobotID,
		BusinessID:   do.BusinessID,
		AttrID:       do.AttrID,
		LabelID:      do.LabelID,
		Name:         do.Name,
		SimilarLabel: do.SimilarLabel,
		IsDeleted:    do.IsDeleted,
		CreateTime:   do.CreateTime,
		UpdateTime:   do.UpdateTime,
	}
}

func (d *daoImpl) ReleaseAttributeLabelProd(ctx context.Context, releaseAttributeLabel *releaseEntity.ReleaseAttributeLabel, tx *gorm.DB) error {
	tbl := mysqlquery.Use(tx).TAttributeLabelProd
	now := time.Now()
	attributeLabelProd := &entity.AttributeLabelProd{
		RobotID:      releaseAttributeLabel.RobotID,
		BusinessID:   releaseAttributeLabel.BusinessID,
		AttrID:       releaseAttributeLabel.AttrID,
		LabelID:      releaseAttributeLabel.LabelID,
		Name:         releaseAttributeLabel.Name,
		SimilarLabel: releaseAttributeLabel.SimilarLabel,
		IsDeleted:    releaseAttributeLabel.IsDeleted,
		CreateTime:   now,
		UpdateTime:   now,
	}
	var err2 error
	if releaseAttributeLabel.Action == releaseEntity.LabelNextActionAdd {
		// 新增
		err2 = tx.Table(model.TableNameTAttributeLabelProd).Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: tbl.BusinessID.ColumnName().String()}},
			DoUpdates: clause.AssignmentColumns([]string{
				tbl.Name.ColumnName().String(),
				tbl.SimilarLabel.ColumnName().String(),
				tbl.IsDeleted.ColumnName().String(),
				tbl.UpdateTime.ColumnName().String(),
			}),
		}).
			Create(getAttributeLabelProdDO2PO(attributeLabelProd)).Error
	} else {
		// 更新 - 使用 robot_id + label_id + attr_id 作为条件
		updateValues := map[string]interface{}{
			"update_time":   now,
			"name":          releaseAttributeLabel.Name,
			"similar_label": releaseAttributeLabel.SimilarLabel,
			"is_deleted":    releaseAttributeLabel.IsDeleted,
		}
		err2 = tx.Table(model.TableNameTAttributeLabelProd).
			Where("robot_id = ? AND label_id = ? AND attr_id = ?",
				releaseAttributeLabel.RobotID,
				releaseAttributeLabel.LabelID,
				releaseAttributeLabel.AttrID).
			Updates(updateValues).Error
	}
	if err2 != nil {
		return fmt.Errorf("ReleaseAttributeLabelProd failed, robotID:%d, attrID:%d, labelID:%d, action:%v, err:%+v",
			releaseAttributeLabel.RobotID, releaseAttributeLabel.AttrID, releaseAttributeLabel.LabelID, releaseAttributeLabel.Action, err2)
	}
	return nil
}
