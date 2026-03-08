package dao

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	jsoniter "github.com/json-iterator/go"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"gorm.io/gorm"
)

var globalRobotDao *RobotDao

const (
	robotTableName = "t_robot"

	RobotTblColId                  = "id"
	RobotTblColBusinessId          = "business_id"
	RobotTblColCorpId              = "corp_id"
	RobotTblColQaVersion           = "qa_version"
	RobotTblColIsDeleted           = "is_deleted"
	RobotTblColUsedCharSize        = "used_char_size"
	RobotTblColIsCreateVectorIndex = "is_create_vector_index"
	RobotTblColEmbedding           = "embedding"
	RobotTblColName                = "name"
	RobotTblColIsShared            = "is_shared"
	RobotTblColSpaceId             = "space_id"

	robotTableMaxPageSize = 1000
)

const (
	getAllAppIDs           = `SELECT id FROM t_robot WHERE is_deleted = 0 AND is_shared = 0 AND id > ? ORDER BY id ASC limit ?`
	getAllAppEmbeddingInfo = `SELECT id, business_id, embedding  FROM t_robot WHERE is_deleted = 0 AND id > ? ORDER BY id ASC limit ?`
)

type RobotDao struct {
	BaseDao
}

// GetRobotDao 获取全局的数据操作对象
func GetRobotDao() *RobotDao {
	if globalRobotDao == nil {
		globalRobotDao = &RobotDao{*globalBaseDao}
	}
	return globalRobotDao
}

type RobotFilter struct {
	CorpId         uint64   // 企业 ID
	IDs            []uint64 // 应用自增ID
	BusinessIds    []uint64 // 应用业务ID
	IsDeleted      *int
	Offset         uint32
	Limit          uint32
	OrderColumn    []string
	OrderDirection []string
	SpaceID        string
}

// 生成查询条件，必须按照索引的顺序排列
func generateRobotCondition(ctx context.Context, session *gorm.DB, filter *RobotFilter) {
	if len(filter.IDs) > 0 {
		session = session.Where(RobotTblColId+sqlIn, filter.IDs)
	}
	if filter.CorpId != 0 {
		session = session.Where(RobotTblColCorpId+sqlEqual, filter.CorpId)
	}
	if len(filter.IDs) != 0 {
		session = session.Where(RobotTblColId+sqlIn, filter.IDs)
	}
	if len(filter.BusinessIds) != 0 {
		// 业务Id为该表唯一索引
		session = session.Where(RobotTblColBusinessId+sqlIn, filter.BusinessIds)
	}
	if filter.SpaceID != "" {
		session = session.Where(RobotTblColSpaceId+sqlEqual, filter.SpaceID)
	}
	// 默认查询未删除的数据
	if filter.IsDeleted != nil {
		session = session.Where(RobotTblColIsDeleted+sqlEqual, *filter.IsDeleted)
	}
}

// GetAppList 获取应用信息
func (d *RobotDao) GetAppList(ctx context.Context, selectColumns []string, filter *RobotFilter) ([]*model.AppDB, error) {
	apps := make([]*model.AppDB, 0)
	session := d.gormDB.Table(robotTableName).Select(selectColumns)
	generateRobotCondition(ctx, session, filter)
	if filter.Limit == 0 || filter.Limit > robotTableMaxPageSize {
		filter.Limit = robotTableMaxPageSize
	}
	session = session.Offset(int(filter.Offset)).Limit(int(filter.Limit))
	for i, orderColumn := range filter.OrderColumn {
		if filter.OrderDirection[i] != SqlOrderByAsc && filter.OrderDirection[i] != SqlOrderByDesc {
			log.ErrorContextf(ctx, "invalid order direction: %s", filter.OrderDirection[i])
			continue
		}
		session.Order(orderColumn + " " + filter.OrderDirection[i])
	}
	res := session.Find(&apps)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return apps, res.Error
	}
	return apps, nil
}

// GetAllValidApps 获取所有应用信息
func (d *RobotDao) GetAllValidApps(ctx context.Context, selectColumns []string, filter *RobotFilter) ([]*model.AppDB, error) {
	beginTime := time.Now()
	offset := 0
	limit := robotTableMaxPageSize
	allApps := make([]*model.AppDB, 0)
	for {
		if filter == nil {
			filter = &RobotFilter{}
		}
		filter.IsDeleted = pkg.GetIntPtr(IsNotDeleted)
		filter.Offset = uint32(offset)
		filter.Limit = uint32(limit)
		apps, err := d.GetAppList(ctx, selectColumns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "GetAllValidApps failed, err: %+v", err)
			return nil, err
		}
		allApps = append(allApps, apps...)
		if len(apps) < limit {
			// 已分页遍历完所有数据
			break
		}
		offset += limit
	}
	log.DebugContextf(ctx, "GetAllValidApps count:%d cost:%dms",
		len(allApps), time.Since(beginTime).Milliseconds())
	return allApps, nil
}

// GetAllValidAppIDs 获取所有的未删除的应用ID
func (d *RobotDao) GetAllValidAppIDs(ctx context.Context, startID uint64) ([]uint64, error) {
	limit := robotTableMaxPageSize
	var allIDs []uint64
	t0 := time.Now()
	for {
		var ids []uint64
		err := d.gormDB.WithContext(ctx).Raw(getAllAppIDs, startID, limit).Scan(&ids).Error
		if err != nil {
			log.ErrorContextf(ctx, "GetAllValidAppIDs error, %v", err)
			return allIDs, err
		}
		allIDs = append(allIDs, ids...)
		if len(ids) < limit {
			break
		}
		startID = ids[len(ids)-1]
	}
	log.InfoContextf(ctx, "GetAllValidAppIDs get: %v, round: %v, cost: %vms",
		len(allIDs), len(allIDs)/limit+1, time.Now().Sub(t0).Milliseconds())
	return allIDs, nil
}

type AppEmbeddingInfo struct {
	AppID            uint64 `db:"id" gorm:"column:id"`
	Embedding        string `gorm:"column:embedding"`
	AppBizID         uint64 `gorm:"column:business_id"`
	EmbeddingVersion uint64
}

// GetAllValidAppEmbeddingInfos 获取所有的未删除的应用的embedding 信息
func (d *RobotDao) GetAllValidAppEmbeddingInfos(ctx context.Context, startID uint64) ([]AppEmbeddingInfo, error) {
	limit := robotTableMaxPageSize
	var allEmbeddingInfos []AppEmbeddingInfo
	t0 := time.Now()
	for {
		var info []AppEmbeddingInfo
		err := d.gormDB.WithContext(ctx).Raw(getAllAppEmbeddingInfo, startID, limit).Scan(&info).Error
		if err != nil {
			log.ErrorContextf(ctx, "GetAllValidAppEmbeddingInfos error, %v", err)
			return allEmbeddingInfos, err
		}
		for idx, _ := range info {
			if info[idx].Embedding == "" {
				info[idx].EmbeddingVersion = config.App().RobotDefault.Embedding.Version
			} else {
				embedding := config.RobotEmbedding{}
				err := jsoniter.UnmarshalFromString(info[idx].Embedding, &embedding)
				if err != nil {
					info[idx].EmbeddingVersion = config.App().RobotDefault.Embedding.Version
					continue
				} else {
					info[idx].EmbeddingVersion = embedding.Version
				}
			}
			allEmbeddingInfos = append(allEmbeddingInfos, info[idx])
		}
		if len(info) < limit {
			break
		}
		startID = info[len(info)-1].AppID
	}
	log.DebugContextf(ctx, "GetAllValidAppEmbeddingInfos get: %v, round: %v, cost: %vms",
		len(allEmbeddingInfos), len(allEmbeddingInfos)/limit+1, time.Now().Sub(t0).Milliseconds())
	return allEmbeddingInfos, nil
}
