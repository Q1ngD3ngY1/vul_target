package dao

import (
	"context"
	"errors"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"gorm.io/gorm"
)

var globalAttributeDao *attributeDao

const (
	attributeTableName = "t_attribute"

	AttributeTblColId            = "id"
	AttributeTblColBusinessId    = "business_id"
	AttributeTblColRobotId       = "robot_id"
	AttributeTblColAttrKey       = "attr_key"
	AttributeTblColName          = "name"
	AttributeTblColIsUpdating    = "is_updating"
	AttributeTblColReleaseStatus = "release_status"
	AttributeTblColNextAction    = "next_action"
	AttributeTblColIsDeleted     = "is_deleted"
	AttributeTblColDeletedTime   = "deleted_time"
	AttributeTblColCreateTime    = "create_time"
	AttributeTblColUpdateTime    = "update_time"

	AttributeTableMaxPageSize = 1000
)

type attributeDao struct {
	BaseDao
	tableName string
}

// GetAttributeDao 获取全局的数据操作对象
func GetAttributeDao() *attributeDao {
	if globalAttributeDao == nil {
		globalAttributeDao = &attributeDao{*globalBaseDao, attributeTableName}
	}
	return globalAttributeDao
}

type AttributeFilter struct {
	Ids            []uint64
	BusinessIds    []uint64
	RobotId        uint64
	NameSubStr     string
	IsDeleted      *int
	Offset         uint32
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string
}

// 生成查询条件，必须按照索引的顺序排列
func (d *attributeDao) generateCondition(ctx context.Context, session *gorm.DB, filter *AttributeFilter) {
	if len(filter.Ids) != 0 {
		session = session.Where(AttributeTblColId+sqlIn, filter.Ids)
	}
	if len(filter.BusinessIds) != 0 {
		session = session.Where(AttributeTblColBusinessId+sqlIn, filter.BusinessIds)
	}
	if filter.RobotId != 0 {
		session = session.Where(AttributeTblColRobotId+sqlEqual, filter.RobotId)
	}
	if filter.NameSubStr != "" {
		session = session.Where(AttributeTblColName+sqlLike, "%"+filter.NameSubStr+"%")
	}
	// is_deleted必须加入查询条件
	if filter.IsDeleted != nil {
		session = session.Where(AttributeTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}
}

// getAttributeList 获取属性列表
func (d *attributeDao) getAttributeList(ctx context.Context, selectColumns []string,
	filter *AttributeFilter) ([]*model.Attribute, error) {
	attributeList := make([]*model.Attribute, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return attributeList, nil
	}
	if filter.Limit > AttributeTableMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		log.ErrorContextf(ctx, "getAttributeList err: %+v", err)
		return attributeList, err
	}
	db, err := knowClient.GormClient(ctx, d.tableName, filter.RobotId, 0, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "get GormClient failed, err: %+v", err)
		return attributeList, err
	}
	session := db.WithContext(ctx).Table(d.tableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			log.ErrorContextf(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&attributeList)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return attributeList, res.Error
	}
	return attributeList, nil
}

// GetAttributeCount 获取属性总数
func (d *attributeDao) GetAttributeCount(ctx context.Context, selectColumns []string,
	filter *AttributeFilter) (int64, error) {
	db, err := knowClient.GormClient(ctx, d.tableName, filter.RobotId, 0, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "get GormClient failed, err: %+v", err)
		return 0, err
	}
	session := db.WithContext(ctx).Table(d.tableName).Select(selectColumns)
	d.generateCondition(ctx, session, filter)
	count := int64(0)
	res := session.Count(&count)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return 0, res.Error
	}
	return count, nil
}

// GetAttributeCountAndList 获取属性总数和分页列表
func (d *attributeDao) GetAttributeCountAndList(ctx context.Context, selectColumns []string,
	filter *AttributeFilter) ([]*model.Attribute, int64, error) {
	count, err := d.GetAttributeCount(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	list, err := d.GetAttributeList(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	return list, count, nil
}

// GetAttributeList 获取属性列表
func (d *attributeDao) GetAttributeList(ctx context.Context, selectColumns []string,
	filter *AttributeFilter) ([]*model.Attribute, error) {
	allAttributeList := make([]*model.Attribute, 0)
	if filter.Limit == 0 {
		log.WarnContextf(ctx, "GetAttributeList limit is 0")
		filter.Limit = AttributeTableMaxPageSize
	}
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
		attributeList, err := d.getAttributeList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetAttributeList failed, err: %+v", err)
			return nil, err
		}
		allAttributeList = append(allAttributeList, attributeList...)
		if uint32(len(attributeList)) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	log.DebugContextf(ctx, "GetAttributeList count:%d cost:%dms",
		len(allAttributeList), time.Since(beginTime).Milliseconds())
	return allAttributeList, nil
}
