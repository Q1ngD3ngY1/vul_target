package kb

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
)

func (l *Logic) DescribeRetrievalConfigCache(ctx context.Context, appPrimaryId uint64) (*entity.RetrievalConfig, error) {
	retrievalConfig, err := l.kbDao.DescribeRetrievalConfigCache(ctx, appPrimaryId)
	if err != nil {
		logx.E(ctx, "DescribeRetrievalConfigCache failed, appPrimaryId:%d, err:%v", appPrimaryId, err)
		return nil, err
	}
	return retrievalConfig, nil
}

func (l *Logic) ModifyRetrievalConfig(ctx context.Context, retrievalConfig *entity.RetrievalConfig) error {
	needUpdate, err := l.checkRetrievalConfigNeedUpdate(ctx, retrievalConfig.RobotID, retrievalConfig)
	if err != nil {
		logx.E(ctx, "ModifyRetrievalConfig checkRetrievalConfigNeedUpdate failed err:%v", err)
		return err
	}
	if !needUpdate {
		logx.I(ctx, "ModifyRetrievalConfig nothing need update,robotID:%d retrievalConfig:%+v", retrievalConfig.RobotID, retrievalConfig)
		return nil
	}

	if err = l.kbDao.ModifyRetrievalConfig(ctx, retrievalConfig); err != nil {
		logx.E(ctx, "ModifyRetrievalConfig failed err:%v", err)
		return err
	}
	logx.I(ctx, "ModifyRetrievalConfig success retrievalConfig:%+v", retrievalConfig)
	return nil
}

// checkRetrievalConfigNeedUpdate 检查是否需要更新
func (l *Logic) checkRetrievalConfigNeedUpdate(ctx context.Context, robotID uint64, retrievalConfig *entity.RetrievalConfig) (bool, error) {
	logx.I(ctx, "checkRetrievalConfigNeedUpdate robotID:%d,retrievalConfig:%+v", robotID, retrievalConfig)
	redisRetrievalConfig, err := l.kbDao.DescribeRetrievalConfigCache(ctx, robotID)
	if err != nil {
		logx.E(ctx, "检查是否需要更新  GetRetrievalConfig robotID:%d,err:%+v", robotID, err)
		return false, err
	}
	logx.I(ctx, "checkRetrievalConfigNeedUpdate redisRetrievalConfig%+v", redisRetrievalConfig)
	// 如果输入值与缓存中或者默认配置中相同，则不更新
	if redisRetrievalConfig.CheckRetrievalConfigDiff(retrievalConfig) {
		return false, nil
	}
	dbRetrievalConfig, err := l.kbDao.DescribeRetrievalConfig(ctx, robotID)
	if err != nil {
		logx.E(ctx, "检查是否需要更新  GetRetrievalConfigByRobotID robotID:%d,err:%+v", robotID, err)
		return false, err
	}
	// db中没有数据直接更新
	if dbRetrievalConfig == nil {
		return true, nil
	}
	logx.I(ctx, "checkRetrievalConfigNeedUpdate dbRetrievalConfig:%+v", dbRetrievalConfig)
	// 输入值跟db中相同,不更新
	if dbRetrievalConfig.CheckRetrievalConfigDiff(retrievalConfig) {
		logx.I(ctx, "checkRetrievalConfigNeedUpdate redis is db diff, dbRetrievalConfig:%+v, redisRetrievalConfig:%+v", dbRetrievalConfig, redisRetrievalConfig)
		return false, nil
	}
	return true, nil
}

func (l *Logic) SyncRetrievalConfigFromDBToCache(ctx context.Context, appPrimaryIds []uint64) error {
	dbRetrievalConfigs, err := l.kbDao.DescribeRetrievalConfigList(ctx, appPrimaryIds)
	if err != nil {
		return err
	}
	for _, dbRetrievalConfig := range dbRetrievalConfigs {
		appPrimaryId := dbRetrievalConfig.RobotID
		redisRetrievalConfig, err := l.kbDao.DescribeRetrievalConfigCache(ctx, appPrimaryId)
		if err != nil {
			logx.E(ctx, "GetRetrievalConfig err:%+v, appPrimaryId:%d", err, appPrimaryId)
			return err
		}
		if dbRetrievalConfig.CheckRetrievalConfigDiff(redisRetrievalConfig) {
			logx.I(ctx, "CheckRetrievalConfigDiff equal and NOT update, appPrimaryId:%d, retrievalConfig:%+v, redis:%+v", appPrimaryId, dbRetrievalConfig, redisRetrievalConfig)
			continue
		}
		logx.I(ctx, "CheckRetrievalConfigDiff NOT equal and update, appPrimaryId:%d, retrievalConfig:%+v, redis:%+v", appPrimaryId, dbRetrievalConfig, redisRetrievalConfig)
		if err = l.kbDao.ModifyRetrievalConfigCache(ctx, dbRetrievalConfig); err != nil {
			logx.E(ctx, "Redis SET failed err:%v", err)
			return err
		}
	}
	return nil
}
