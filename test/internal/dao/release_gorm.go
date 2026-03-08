package dao

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

var globalReleaseDao *ReleaseDao

const (
	releaseTableName = "t_release"

	ReleaseTblColId      = "id"       // 自增ID
	ReleaseTblColCorpId  = "corp_id"  // 企业ID
	ReleaseTblColRobotId = "robot_id" // 机器人ID
	ReleaseTblColStatus  = "status"   // 状态
)

type ReleaseDao struct {
	BaseDao
}

// GetReleaseDao 获取全局的数据操作对象
func GetReleaseDao() *ReleaseDao {
	if globalReleaseDao == nil {
		globalReleaseDao = &ReleaseDao{*globalBaseDao}
	}
	return globalReleaseDao
}

// GetLatestRelease 获取最近一次正式发布id和状态
func (d *ReleaseDao) GetLatestRelease(ctx context.Context, corpID, robotID uint64) (*model.Release, error) {
	session := d.gormDB.WithContext(ctx).Table(releaseTableName).Select(ReleaseTblColId, ReleaseTblColStatus)
	session.Where(ReleaseTblColCorpId+sqlEqual, corpID).
		Where(ReleaseTblColRobotId+sqlEqual, robotID).
		Order(ReleaseTblColId + " " + SqlOrderByDesc).
		Limit(1)
	record := &model.Release{}
	res := session.Find(record)
	if res.Error != nil {
		log.ErrorContextf(ctx, "execute sql failed, err: %+v", res.Error)
		return nil, res.Error
	}
	return record, nil
}
