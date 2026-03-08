package dao

import (
	"context"
	"errors"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/client"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"gorm.io/gorm"
)

var globalDocAttributeLabelDao *docAttributeLabelDao

const (
	docAttributeLabelTableName = "t_doc_attribute_label"

	DocAttributeLabelTblColId         = "id"
	DocAttributeLabelTblColRobotId    = "robot_id"
	DocAttributeLabelTblColDocId      = "doc_id"
	DocAttributeLabelTblColSource     = "source"
	DocAttributeLabelTblColAttrId     = "attr_id"
	DocAttributeLabelTblColLabelId    = "label_id"
	DocAttributeLabelTblColIsDeleted  = "is_deleted"
	DocAttributeLabelTblColCreateTime = "create_time"
	DocAttributeLabelTblColUpdateTime = "update_time"

	DocAttributeLabelTableMaxPageSize = 1000
)

type docAttributeLabelDao struct {
	BaseDao
	tableName string
}

// GetDocAttributeLabelDao 获取全局的数据操作对象
func GetDocAttributeLabelDao() *docAttributeLabelDao {
	if globalDocAttributeLabelDao == nil {
		globalDocAttributeLabelDao = &docAttributeLabelDao{*globalBaseDao, docAttributeLabelTableName}
	}
	return globalDocAttributeLabelDao
}

type AttrLabel struct {
	AttrId  uint64
	LabelId uint64
}

type DocAttributeLabelFilter struct {
	RobotId        uint64
	Source         uint64
	AttrIDs        []uint64
	LabelIDs       []uint64
	IsDeleted      *int
	Offset         uint32
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string
}

// 生成查询条件，必须按照索引的顺序排列
func (d *docAttributeLabelDao) generateCondition(ctx context.Context, session *gorm.DB, filter *DocAttributeLabelFilter) {
	if filter.RobotId != 0 {
		session = session.Where(DocAttributeLabelTblColRobotId+sqlEqual, filter.RobotId)
	}
	if filter.Source != 0 {
		session = session.Where(DocAttributeLabelTblColSource+sqlEqual, filter.Source)
	}
	if len(filter.AttrIDs) != 0 {
		session = session.Where(DocAttributeLabelTblColAttrId+sqlIn, filter.AttrIDs)
	}
	if len(filter.LabelIDs) != 0 {
		session = session.Where(DocAttributeLabelTblColLabelId+sqlIn, filter.LabelIDs)
	}
	if filter.IsDeleted != nil {
		session = session.Where(DocAttributeLabelTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}
}

// getDocAttributeLabelList 获取属性标签列表
func (d *docAttributeLabelDao) getDocAttributeLabelList(ctx context.Context, selectColumns []string,
	filter *DocAttributeLabelFilter) ([]*model.DocAttributeLabel, error) {
	docAttributeLabelList := make([]*model.DocAttributeLabel, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return docAttributeLabelList, nil
	}
	if filter.Limit > DocAttributeLabelTableMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		log.ErrorContextf(ctx, "getDocAttributeLabelList err: %+v", err)
		return docAttributeLabelList, err
	}
	db, err := knowClient.GormClient(ctx, d.tableName, filter.RobotId, 0, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "get GormClient failed, err: %+v", err)
		return docAttributeLabelList, err
	}
	session := db.WithContext(ctx).Table(docAttributeLabelTableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			log.ErrorContextf(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&docAttributeLabelList)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return docAttributeLabelList, res.Error
	}
	return docAttributeLabelList, nil
}

// GetDocAttributeLabelCount 获取属性标签总数
func (d *docAttributeLabelDao) GetDocAttributeLabelCount(ctx context.Context, selectColumns []string,
	filter *DocAttributeLabelFilter) (int64, error) {
	db, err := knowClient.GormClient(ctx, d.tableName, filter.RobotId, 0, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "get GormClient failed, err: %+v", err)
		return 0, err
	}
	session := db.WithContext(ctx).Table(docAttributeLabelTableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// GetDocAttributeLabelCountAndList 获取属性标签总数和分页列表
func (d *docAttributeLabelDao) GetDocAttributeLabelCountAndList(ctx context.Context, selectColumns []string,
	filter *DocAttributeLabelFilter) ([]*model.DocAttributeLabel, int64, error) {
	count, err := d.GetDocAttributeLabelCount(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	list, err := d.GetDocAttributeLabelList(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	return list, count, nil
}

// GetDocAttributeLabelList 获取属性标签结果列表
func (d *docAttributeLabelDao) GetDocAttributeLabelList(ctx context.Context, selectColumns []string,
	filter *DocAttributeLabelFilter) ([]*model.DocAttributeLabel, error) {
	log.DebugContextf(ctx, "GetDocAttributeLabelList filter:%+v", filter)
	allDocAttributeLabelList := make([]*model.DocAttributeLabel, 0)
	beginTime := time.Now()
	offset := filter.Offset
	alreadyGetCount := uint32(0)
	wantedCount := filter.Limit
	for {
		filter.Offset = offset
		filter.Limit = CalculateLimit(wantedCount, alreadyGetCount)
		if filter.Limit == 0 {
			break
		}
		docAttributeLabelList, err := d.getDocAttributeLabelList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetDocAttributeLabelList failed, err: %+v", err)
			return nil, err
		}
		allDocAttributeLabelList = append(allDocAttributeLabelList, docAttributeLabelList...)
		if uint32(len(docAttributeLabelList)) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	log.DebugContextf(ctx, "GetDocAttributeLabelList count:%d cost:%dms",
		len(allDocAttributeLabelList), time.Since(beginTime).Milliseconds())
	return allDocAttributeLabelList, nil
}
