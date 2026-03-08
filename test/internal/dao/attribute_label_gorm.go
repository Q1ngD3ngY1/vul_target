package dao

import (
	"context"
	"errors"
	"fmt"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"gorm.io/gorm"
)

var globalAttributeLabelDao *attributeLabelDao

const (
	attributeLabelTableName = "t_attribute_label"

	AttributeLabelTblColId            = "id"
	AttributeLabelTblColRobotId       = "robot_id"
	AttributeLabelTblColBusinessId    = "business_id"
	AttributeLabelTblColAttrId        = "attr_id"
	AttributeLabelTblColName          = "name"
	AttributeLabelTblColSimilarLabel  = "similar_label"
	AttributeLabelTblColReleaseStatus = "release_status"
	AttributeLabelTblColNextAction    = "next_action"
	AttributeLabelTblColIsDeleted     = "is_deleted"
	AttributeLabelTblColCreateTime    = "create_time"
	AttributeLabelTblColUpdateTime    = "update_time"

	AttributeLabelTableMaxPageSize = 1000
)

type attributeLabelDao struct {
	BaseDao
	tableName string
}

// GetAttributeLabelDao 获取全局的数据操作对象
func GetAttributeLabelDao() *attributeLabelDao {
	if globalAttributeLabelDao == nil {
		globalAttributeLabelDao = &attributeLabelDao{*globalBaseDao, attributeLabelTableName}
	}
	return globalAttributeLabelDao
}

type AttributeLabelFilter struct {
	RobotId                  uint64
	AttrIds                  []uint64
	BusinessIds              []uint64
	NotEmptySimilarLabel     *bool
	NameOrSimilarLabelSubStr string
	IsDeleted                *int
	Offset                   uint32
	Limit                    uint32
	OrderColumn              []string
	OrderDirection           []string
}

// 生成查询条件，必须按照索引的顺序排列
func (d *attributeLabelDao) generateCondition(ctx context.Context, session *gorm.DB, filter *AttributeLabelFilter) {
	if filter.RobotId != 0 {
		session = session.Where(AttributeLabelTblColRobotId+sqlEqual, filter.RobotId)
	}
	if len(filter.BusinessIds) != 0 {
		session = session.Where(AttributeLabelTblColBusinessId+sqlIn, filter.BusinessIds)
	}
	if len(filter.AttrIds) != 0 {
		session = session.Where(AttributeLabelTblColAttrId+sqlIn, filter.AttrIds)
	}
	if filter.NotEmptySimilarLabel != nil {
		if *filter.NotEmptySimilarLabel {
			session = session.Where(AttributeLabelTblColSimilarLabel+sqlNotEqual, "")
		} else {
			session = session.Where(AttributeLabelTblColSimilarLabel+sqlEqual, "")
		}
	}
	if filter.NameOrSimilarLabelSubStr != "" {
		session = session.Where(d.gormDB.Where(AttributeLabelTblColName+sqlLike, "%"+filter.NameOrSimilarLabelSubStr+"%").
			Or(AttributeLabelTblColSimilarLabel+sqlLike, "%"+filter.NameOrSimilarLabelSubStr+"%"))
	}
	if filter.IsDeleted == nil {
		// 默认查询未删除的数据
		session = session.Where(AttributeLabelTblColIsDeleted+sqlEqual, IsNotDeleted)
	} else {
		session = session.Where(AttributeLabelTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}
}

// getAttributeLabelList 获取属性标签列表
func (d *attributeLabelDao) getAttributeLabelList(ctx context.Context, selectColumns []string,
	filter *AttributeLabelFilter) ([]*model.AttributeLabel, error) {
	attributeLabelList := make([]*model.AttributeLabel, 0)
	if filter.Limit == 0 {
		// 为0正常返回空结果即可
		return attributeLabelList, nil
	}
	if filter.Limit > AttributeLabelTableMaxPageSize {
		// 限制单次查询最大条数
		err := errors.New(fmt.Sprintf("invalid limit: %d", filter.Limit))
		log.ErrorContextf(ctx, "getAttributeLabelList err: %+v", err)
		return attributeLabelList, err
	}
	db, err := knowClient.GormClient(ctx, d.tableName, filter.RobotId, 0, []client.Option{}...)
	if err != nil {
		log.ErrorContextf(ctx, "get GormClient failed, err: %+v", err)
		return attributeLabelList, err
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
	res := session.Find(&attributeLabelList)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return attributeLabelList, res.Error
	}
	return attributeLabelList, nil
}

// GetAttributeLabelCount 获取属性标签总数
func (d *attributeLabelDao) GetAttributeLabelCount(ctx context.Context, selectColumns []string,
	filter *AttributeLabelFilter) (int64, error) {
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

// GetAttributeLabelCountAndList 获取属性标签总数和分页列表
func (d *attributeLabelDao) GetAttributeLabelCountAndList(ctx context.Context, selectColumns []string,
	filter *AttributeLabelFilter) ([]*model.AttributeLabel, int64, error) {
	count, err := d.GetAttributeLabelCount(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	list, err := d.GetAttributeLabelList(ctx, selectColumns, filter)
	if err != nil {
		return nil, 0, err
	}
	return list, count, nil
}

// GetAttributeLabelList 获取属性标签结果列表
func (d *attributeLabelDao) GetAttributeLabelList(ctx context.Context, selectColumns []string,
	filter *AttributeLabelFilter) ([]*model.AttributeLabel, error) {
	allAttributeLabelList := make([]*model.AttributeLabel, 0)
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
		attributeLabelList, err := d.getAttributeLabelList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetAttributeLabelList failed, err: %+v", err)
			return nil, err
		}
		allAttributeLabelList = append(allAttributeLabelList, attributeLabelList...)
		if uint32(len(attributeLabelList)) < filter.Limit {
			// 已分页遍历完所有数据
			break
		}
		offset += filter.Limit
		alreadyGetCount += filter.Limit
	}
	log.DebugContextf(ctx, "GetAttributeLabelList count:%d cost:%dms",
		len(allAttributeLabelList), time.Since(beginTime).Milliseconds())
	return allAttributeLabelList, nil
}

// GetAttributeLabelChunkByAttrID 分批获取attribute_label
func (d *attributeLabelDao) GetAttributeLabelChunkByAttrID(ctx context.Context, selectColumns []string, robotID, attrID, startID uint64,
	limit int) ([]*model.AttributeLabel, error) {
	db, err := knowClient.GormClient(ctx, d.tableName, robotID, 0, client.WithCalleeMethod("GetAttributeLabelChunkByAttrID"))
	if err != nil {
		return nil, err
	}
	var attributeLabelList []*model.AttributeLabel
	err = db.WithContext(ctx).Table(d.tableName).Select(selectColumns).
		Where("robot_id = ? and attr_id = ? and id > ? and  is_deleted = 0", robotID, attrID, startID).
		Order("id asc").Limit(limit).Find(&attributeLabelList).Error
	if err != nil {
		log.InfoContextf(ctx, "GetAttributeLabelChunkByAttrID error, robotID: %v, attr id: %v, start id: %v, "+
			"limit %v, err: %v", robotID, attrID, startID, limit, err)
		return attributeLabelList, err
	}
	return attributeLabelList, nil
}
