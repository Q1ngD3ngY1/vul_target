package kb

import (
	"context"
	"errors"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"gorm.io/gorm"
)

func (l *Logic) IsExistedConfigHistory(ctx context.Context, appBizID, kbBizId, versionID uint64) (bool, error) {
	filter := &kbe.KnowledgeConfigHistoryFilter{
		AppBizID:       appBizID,
		KnowledgeBizID: kbBizId,
		VersionID:      versionID,
	}
	history, err := l.kbDao.DescribeKnowledgeConfigHistory(ctx, filter)
	if err != nil {
		logx.W(ctx, "describe knowledge config history error:%v", err)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	if history != nil {
		return true, nil
	}
	return false, nil
}

func (l *Logic) CreateKnowledgeConfigHistories(ctx context.Context, versionID uint64, configs []*kbe.KnowledgeConfig) error {
	logx.I(ctx, "create knowledge config history... (from %d configs)", len(configs))
	if err := l.kbDao.TDSQLQuery().Transaction(func(tx *tdsqlquery.Query) error {
		for _, config := range configs {
			if isExisted, err := l.IsExistedConfigHistory(ctx, config.AppBizID, config.KnowledgeBizID, versionID); err != nil {
				return err
			} else if isExisted {
				continue
			} else {
				configHistory := &kbe.KnowledgeConfigHistory{
					CorpBizID:      config.CorpBizID,
					KnowledgeBizID: config.KnowledgeBizID,
					AppBizID:       config.AppBizID,
					Type:           config.Type,
					VersionID:      versionID,
					ReleaseJSON:    config.PreviewConfig,
					IsRelease:      false,
					IsDeleted:      config.IsDeleted,
					CreateTime:     config.CreateTime,
					UpdateTime:     config.UpdateTime,
				}
				if err := l.kbDao.CreateKnowledgeConfigHistory(ctx, configHistory, tx); err != nil {
					logx.E(ctx, "create knowledge config history error:%v", err)
					return err
				}
			}

		}

		return nil

	}); err != nil {
		logx.E(ctx, "create knowledge config history error:%v", err)
		return err
	}
	return nil
}
