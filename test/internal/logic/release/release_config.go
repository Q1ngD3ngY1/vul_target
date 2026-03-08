package release

import (
	"context"
	"errors"
	"time"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	kbconfig "git.woa.com/adp/pb-go/kb/kb_config"
)

// ReleaseKnowledgeConfig 发布知识库配置
func (l *Logic) ReleaseKnowledgeConfig(ctx context.Context, corpBizID, appBizID, versionID uint64, isRelease bool) error {
	logx.I(ctx, "[ReleaseKnowledgeConfig] Prepare to ReleaseKnowledgeConfig... (appBizID: %d, versionID: %d, isRelease: %t)", appBizID, versionID, isRelease)

	filter := &kbe.KnowledgeConfigHistoryFilter{
		AppBizID:  appBizID,
		VersionID: versionID,
	}

	historyConfigList, err := l.kbLogic.GetKbDao().DescribeKnowledgeConfigHistoryList(ctx, filter)
	if err != nil {
		logx.E(ctx, "describe knowledge config history error:%v", err)
		return err
	}

	if len(historyConfigList) == 0 {
		logx.W(ctx, "[ReleaseKnowledgeConfig] Warning: knowledge config history not found")
		return nil
	}

	configsNeedToClearCache := make([]*kbe.KnowledgeConfig, 0)
	if err := l.kbLogic.GetKbDao().TDSQLQuery().Transaction(func(tx *tdsqlquery.Query) error {
		for _, history := range historyConfigList {
			configs, err := l.kbLogic.GetKbDao().DescribeAppKnowledgeConfig(ctx, corpBizID, appBizID, history.KnowledgeBizID)
			configsNeedToClearCache = append(configsNeedToClearCache, configs...)
			if err != nil {
				logx.E(ctx, "[ReleaseKnowledgeConfig] describe app knowledge config error:%v", err)
				return err
			}
			history.IsRelease = isRelease
			updateFilter := &kbe.KnowledgeConfigHistoryFilter{
				CorpBizID: history.CorpBizID,
				AppBizID:  history.AppBizID,
				VersionID: versionID,
				ID:        history.ID,
			}
			err = l.kbLogic.GetKbDao().ModifyKnowledgeConfigHistory(ctx, updateFilter, history, tx)
			if err != nil {
				logx.E(ctx, "[ReleaseKnowledgeConfig] modify knowledge config history error:%v", err)
				return err
			}
			for _, config := range configs {
				if history.Type == config.Type {
					logx.I(ctx, "[ReleaseKnowledgeConfig] ReleaseKnowledgeConfig... "+
						"(appBizID: %d, knowledgeBizID: %d, versionID: %d, isRelease: %t (config:%+v))",
						appBizID, config.KnowledgeBizID, versionID, isRelease, config)
					config.Config = history.ReleaseJSON
					err = l.kbLogic.GetKbDao().SetKnowledgeConfig(ctx, config, tx, true)
					if err != nil {
						logx.E(ctx, "[ReleaseKnowledgeConfig] set knowledge config error:%v", err)
						return err
					}
				}

			}
		}

		return nil
	}); err != nil {
		logx.E(ctx, "[ReleaseKnowledgeConfig] Failed to ReleaseKnowledgeConfig. error:%v", err)
		return err
	}

	// 这里清除缓存不能在事务中，因为缓存即使被清除了，事务还没提交，此时的并发查询还是会查到旧数据，回填缓存的就是旧数据，导致缓存不一致
	l.kbLogic.DeleteKBConfigCache(ctx, corpBizID, configsNeedToClearCache)

	return nil
}

/*
RollbackKbConfig 发布回滚
从历史表读取指定版本的配置，覆盖当前表的 preview_config
同时处理共享知识库引用关系的变化
*/
func (l *Logic) RollbackKbConfig(ctx context.Context, corpPrimaryId, corpBizId, appBizId, versionId uint64) error {
	logx.I(ctx, "[RollbackKbConfig] req corpBizID:%d, appBizID:%d, versionID:%d", corpBizId, appBizId, versionId)

	// 1. 从历史表查询指定版本的配置
	filter := &kbe.KnowledgeConfigHistoryFilter{
		AppBizID:  appBizId,
		VersionID: versionId,
	}

	historyList, err := l.kbLogic.GetKbDao().DescribeKnowledgeConfigHistoryList(ctx, filter)
	if err != nil {
		logx.E(ctx, "[RollbackKbConfig] DescribeKnowledgeConfigHistoryList filter:%+v err:%v", filter, err)
		return err
	}

	if len(historyList) == 0 {
		logx.W(ctx, "[RollbackKbConfig] No history config found appBizID:%d, versionID:%d", appBizId, versionId)
		return errx.ErrNotFound
	}

	// 2. 获取当前应用引用的共享知识库列表
	currentSharedKbList, err := l.kbLogic.GetKbDao().GetAppShareKGList(ctx, appBizId)
	if err != nil {
		logx.E(ctx, "[RollbackKbConfig] GetAppShareKGList appBizID:%d err:%v", appBizId, err)
		return err
	}
	currentSharedKbBizIds := slicex.Pluck(currentSharedKbList, func(v *kbe.AppShareKnowledge) uint64 { return v.KnowledgeBizID })

	// 3. 从历史配置中提取共享知识库ID（AppBizID != KnowledgeBizID 的配置为共享知识库）
	var historySharedKbBizIds []uint64
	for _, history := range historyList {
		// 如果共享知识库在某个版本解除引用，它在这个版本也会有条记录，且 ReleaseJSON 字段为空。这种记录不需要处理
		if history.AppBizID != history.KnowledgeBizID && history.ReleaseJSON != "" {
			historySharedKbBizIds = append(historySharedKbBizIds, history.KnowledgeBizID)
		}
	}
	historySharedKbBizIds = slicex.Unique(historySharedKbBizIds)
	logx.I(ctx, "[RollbackKbConfig] currentSharedKbBizIds:%v, historySharedKbBizIds:%v", currentSharedKbBizIds, historySharedKbBizIds)

	// 4. 计算需要增加和删除的共享知识库引用
	// 需要添加的：历史版本有引用但当前没有引用的
	toAddKbBizIds := slicex.Diff(historySharedKbBizIds, currentSharedKbBizIds)
	// 需要删除的：当前有引用但历史版本没有引用的
	toDelKbBizIds := slicex.Diff(currentSharedKbBizIds, historySharedKbBizIds)
	logx.I(ctx, "[RollbackKbConfig] toAddKbBizIds:%v, toDelKbBizIds:%v", toAddKbBizIds, toDelKbBizIds)

	configsNeedToClearCache := make([]*kbe.KnowledgeConfig, 0)
	err = l.kbLogic.GetKbDao().TDSQLQuery().Transaction(func(tx *tdsqlquery.Query) error {
		// 5. 处理配置回滚
		for _, history := range historyList {
			configs, err := l.kbLogic.GetKbDao().DescribeAppKnowledgeConfig(ctx, corpBizId, appBizId, history.KnowledgeBizID)
			if err != nil {
				logx.E(ctx, "[RollbackKbConfig] describe app knowledge config error:%v", err)
				return err
			}
			configsNeedToClearCache = append(configsNeedToClearCache, configs...)
			for _, config := range configs {
				if history.Type != config.Type {
					continue
				}
				if config.Type == uint32(kbconfig.KnowledgeBaseConfigType_EMBEDDING_MODEL) {
					// 知识库没有知识的时候，才能修改embedding模型;
					checkErr := l.checkEmbeddingModelModifyCondition(ctx, appBizId)
					if checkErr != nil {
						logx.I(ctx, "[RollbackKbConfig] checkEmbeddingModelModifyCondition failed, err:%v", checkErr)
						continue
					}
				}
				logx.I(ctx, "[RollbackKbConfig] appBizID:%d, knowledgeBizID:%d, versionID:%d, config:%+v", appBizId, config.KnowledgeBizID, versionId, config)

				config.PreviewConfig = history.ReleaseJSON // 重点，使用历史版本的发布域配置覆盖开发域的 preview_config
				err = l.kbLogic.GetKbDao().SetKnowledgeConfig(ctx, config, tx, true)
				if err != nil {
					logx.E(ctx, "[RollbackKbConfig] set knowledge config error:%v", err)
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 6. 处理共享知识库引用关系变更（在事务外处理，避免事务过大）
	// 6.1 添加新的共享知识库引用（使用历史配置）
	if err := l.addSharedKbReferences(ctx, corpBizId, appBizId, toAddKbBizIds, historyList); err != nil {
		logx.E(ctx, "[RollbackKbConfig] addShareKGReferences err:%v", err)
		return err
	}

	// 6.2 删除不再引用的共享知识库
	if err := l.kbLogic.MultiUnbindShareKb(ctx, corpPrimaryId, corpBizId, appBizId, toDelKbBizIds); err != nil {
		logx.E(ctx, "[RollbackKbConfig] removeShareKGReferences err:%v", err)
		return err
	}

	// 确保清除当前 App 的缓存，即使 configsNeedToClearCache 为空（例如所有知识库都是新添加的）
	configsNeedToClearCache = append(configsNeedToClearCache, &kbe.KnowledgeConfig{
		AppBizID:  appBizId,
		CorpBizID: corpBizId,
	})

	// 这里清除缓存不能在事务中，因为缓存即使被清除了，事务还没提交，此时的并发查询还是会查到旧数据，回填缓存的就是旧数据，导致缓存不一致
	l.kbLogic.DeleteKBConfigCache(ctx, corpBizId, configsNeedToClearCache)

	logx.I(ctx, "[RollbackKbConfig] Successfully rolled back %d configs to versionID:%d, added %d sharedKb refs, removed %d sharedKb refs",
		len(configsNeedToClearCache), versionId, len(toAddKbBizIds), len(toDelKbBizIds))
	return nil
}

// addSharedKbReferences 添加共享知识库引用关系
// 使用历史配置列表中的配置来恢复共享知识库的配置
func (l *Logic) addSharedKbReferences(ctx context.Context, corpBizID, appBizID uint64, knowledgeBizIDs []uint64, historyList []*kbe.KnowledgeConfigHistory) error {
	logx.I(ctx, "[addShareKGReferences] corpBizID:%d, appBizID:%d, knowledgeBizIDs:%v", corpBizID, appBizID, knowledgeBizIDs)
	if len(knowledgeBizIDs) == 0 {
		logx.D(ctx, "[addShareKGReferences] no knowledgeBizIDs found, skip creating refs")
		return nil
	}
	appListReq := appconfig.ListAppBaseInfoReq{
		AppBizIds: knowledgeBizIDs,
		IsShared:  ptrx.Bool(true),
	}
	shareKbs, _, err := l.rpc.AppAdmin.ListAppBaseInfo(ctx, &appListReq)
	if err != nil {
		// 查询失败，视作所有的共享知识库都不存在了。
		logx.E(ctx, "[addShareKGReferences] ListAppBaseInfo err:%v", err)
		return nil
	}
	validKnowledgeBizIds := slicex.Map(shareKbs, func(v *entity.AppBaseInfo) uint64 { return v.BizId })
	logx.I(ctx, "[addShareKGReferences] validKnowledgeBizIds:%v", validKnowledgeBizIds)
	if len(validKnowledgeBizIds) == 0 {
		logx.W(ctx, "[addShareKGReferences] no valid knowledgeBizIDs found, skip creating refs")
		return nil
	}

	// 构建知识库ID到历史配置的映射
	kbID2HistoryConfigs := make(map[uint64][]*kbe.KnowledgeConfigHistory)
	for _, history := range historyList {
		if history.AppBizID != history.KnowledgeBizID { // 只处理共享知识库配置
			kbID2HistoryConfigs[history.KnowledgeBizID] = append(kbID2HistoryConfigs[history.KnowledgeBizID], history)
		}
	}

	// 1. 创建引用关系记录
	var addSharedKbList []*kbe.AppShareKnowledge
	var addKnowledgeConfigList []*kbe.KnowledgeConfig

	now := time.Now()
	for _, knowledgeBizID := range validKnowledgeBizIds {
		addSharedKbList = append(addSharedKbList, &kbe.AppShareKnowledge{
			AppBizID:       appBizID,
			KnowledgeBizID: knowledgeBizID,
			CorpBizID:      corpBizID,
			UpdateTime:     now,
			CreateTime:     now,
		})

		// 从历史配置中获取该知识库的配置，使用历史版本的配置来恢复
		if historyConfigs, ok := kbID2HistoryConfigs[knowledgeBizID]; ok {
			for _, historyConfig := range historyConfigs {
				addKnowledgeConfigList = append(addKnowledgeConfigList, &kbe.KnowledgeConfig{
					CorpBizID:      corpBizID,
					KnowledgeBizID: knowledgeBizID,
					Type:           historyConfig.Type,
					IsDeleted:      false,
					CreateTime:     now,
					UpdateTime:     now,
					AppBizID:       appBizID,
					PreviewConfig:  historyConfig.ReleaseJSON, // 使用历史版本的配置
				})
			}
		} else {
			logx.W(ctx, "[addShareKGReferences] no history config found for knowledgeBizID:%d, skip creating config", knowledgeBizID)
		}
	}

	return l.kbLogic.MultiBindShareKb(ctx, addSharedKbList, addKnowledgeConfigList)
}

func (l *Logic) checkEmbeddingModelModifyCondition(ctx context.Context, appBizId uint64) error {
	appBaseInfo, err := l.rpc.AppAdmin.GetAppBaseInfo(ctx, appBizId)
	if err != nil || appBaseInfo == nil {
		logx.E(ctx, "checkEmbeddingModelModifyCondition, GetAppBaseInfo err: %v", err)
		return err
	}
	corpID := appBaseInfo.CorpPrimaryId
	wg, wgCtx := errgroupx.WithContext(ctx)
	wg.SetLimit(3)
	wg.Go(func() error {
		logx.I(wgCtx, "checkEmbeddingModelModifyCondition, checkDocCount.")
		docCount, err := l.docLogic.GetDocCount(wgCtx, []string{}, &docEntity.DocFilter{
			CorpId:    corpID,
			RobotId:   appBaseInfo.PrimaryId,
			IsDeleted: ptrx.Bool(false),
		})
		if err != nil {
			return err
		}
		if docCount > 0 {
			logx.W(wgCtx, "checkEmbeddingModelModifyCondition, checkDocCount, docCount: %d", docCount)
			return errors.New("checkEmbeddingModelModifyCondition failed with documents are not empty")
		}
		return nil
	})
	wg.Go(func() error {
		logx.I(wgCtx, "checkEmbeddingModelModifyCondition, checkQaCount.")
		qaCount, err := l.qaLogic.GetDocQaCount(wgCtx, []string{}, &qaEntity.DocQaFilter{
			CorpId:    corpID,
			RobotId:   appBaseInfo.PrimaryId,
			IsDeleted: ptrx.Uint32(qaEntity.QAIsNotDeleted),
		})
		if err != nil {
			return err
		}
		if qaCount > 0 {
			logx.W(wgCtx, "checkEmbeddingModelModifyCondition, checkQaCount, qaCount: %d", qaCount)
			return errors.New("checkEmbeddingModelModifyCondition failed with qas are not empty")
		}
		return nil
	})
	wg.Go(func() error {
		logx.I(wgCtx, "checkEmbeddingModelModifyCondition, checkDbSourceCount.")
		_, count, err := l.dbLogic.ListDbSourcesWithTables(wgCtx, appBizId, 1, 1)
		if err != nil {
			return err
		}
		if count > 0 {
			logx.W(wgCtx, "checkEmbeddingModelModifyCondition, checkDbSourceCount, count: %d", count)
			return errors.New("checkEmbeddingModelModifyCondition failed with db sources are not empty")
		}
		return nil
	})
	err = wg.Wait()
	if err != nil {
		return err
	}
	return nil
}
