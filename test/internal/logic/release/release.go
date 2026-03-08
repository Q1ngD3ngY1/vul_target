package release

import (
	"context"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/logx"
	async "git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_common"
)

func (l *Logic) GetLatestRelease(ctx context.Context, corpID uint64, robotID uint64) (*releaseEntity.Release, error) {
	return l.releaseDao.GetLatestRelease(ctx, corpID, robotID)
}

func (l *Logic) GetReleaseByID(ctx context.Context, id uint64) (*releaseEntity.Release, error) {
	return l.releaseDao.GetReleaseByID(ctx, id)
}

func (l *Logic) GetReleaseByBizID(ctx context.Context, bizID uint64) (
	*releaseEntity.Release, error) {
	return l.releaseDao.GetReleaseByBizID(ctx, bizID)
}

/*
ReleasePrepare 发布准备
将配置从配置表拷贝到历史快照表。
*/
func (l *Logic) ReleasePrepare(ctx context.Context, releaseType uint32, corpBizID, appBizID, versionID uint64) error {
	logx.I(ctx, "[ReleasePrepare](releaseType:%d, corpBizID:%d, appBizID:%d, versionID:%d)", releaseType, corpBizID, appBizID, versionID)
	configList, err := l.kbLogic.DescribeAppKnowledgeBaseConfigList(ctx, corpBizID, []uint64{appBizID}, false, 0)

	if err != nil {
		logx.E(ctx, "[ReleasePrepare] Failed to DescribeAppKnowledgeConfigList(appBizID:%d). err:%+v,", appBizID, err)
		return err
	}

	if err = l.kbLogic.CreateKnowledgeConfigHistories(ctx, versionID, configList); err != nil {
		logx.E(ctx, "[ReleasePrepare] Failed to CreateKnowledgeConfigHistories(appBizID:%d). err:%+v,", appBizID, err)
		return err
	}
	return nil
}

/*
ReleaseCollect 发布采集
1. 将各个业务对象的从 t_x -> t_release_x
2. t_release_x中release_status=init
3. t_x中的release_status=init
*/
func (l *Logic) ReleaseCollect(ctx context.Context, releaseType uint32, corpID, appID, appBizID, versionID uint64) error {
	logx.I(ctx, "[ReleaseCollect]Start to collect(releaseType:%d, vecsionID:%d)", releaseType, versionID)
	defer timeTrack(ctx, time.Now(), fmt.Sprintf("ReleaseCollect:%s", releaseType))
	switch releaseType {
	case releaseEntity.ReleaseDBType:
		logx.I(ctx, "[ReleaseCollect]collectDB(releaseType:%d, vecsionID:%d) is deprecated", releaseType, versionID)
		//if err := l.collectDB(ctx, appBizID, versionID); err != nil {
		//	return err
		//}
	case releaseEntity.ReleaseTypeDocument:
		logx.I(ctx, "[ReleaseCollect]collectReleaseDoc(releaseType:%d, vecsionID:%d)", releaseType, versionID)
		//if err := l.simpleCollectReleaseDoc(ctx, corpID, appID, appBizID, versionID); err != nil {
		//	return err
		//}

	case releaseEntity.ReleaseTypeQA:
		logx.I(ctx, "[ReleaseCollect]collectQA(releaseType:%d, vecsionID:%d) is deprecated", releaseType, versionID)
		//if err := l.collectReleaseQA(ctx, corpID, appID, appBizID, versionID); err != nil {
		//	return err
		//}
	case releaseEntity.ReleaseTypeRejectedQuestion:
		if err := l.collectReleaseRejectedQuestion(ctx, corpID, appID, versionID); err != nil {
			return err
		}
	case releaseEntity.ReleaseTypeLabel:
		if err := l.collectReleaseLabel(ctx, appID, versionID); err != nil {
			return err
		}
	default:
		logx.E(ctx, "[CollectSyncTask]unknown releaseType:%d", releaseType)
		return errs.ErrParameterInvalid
	}

	return nil
}

func (l *Logic) ReleaseRelease(ctx context.Context, releaseType uint32, corpID, corpBizID, appID, appBizID, versionID uint64) error {
	logx.I(ctx, "[ReleaseRelease] Start to Do Release_Release releaseType:%d, corpID:%d, appID:%d, appBizID:%d, versionID:%d",
		releaseType, corpID, appID, appBizID, versionID)
	appDB, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, appID)
	if err != nil {
		return err
	}

	var embeddingModel string
	var embeddingVersion uint64

	if releaseType == releaseEntity.ReleaseTypeQA || releaseType == releaseEntity.ReleaseTypeDocument {
		// 这里要拿评测端
		embeddingModel, err = l.kbLogic.GetDefaultKnowledgeBaseConfig(ctx, appDB.CorpBizId, appDB.BizId, appDB.BizId,
			uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL), bot_common.AdpDomain_ADP_DOMAIN_DEV)

		if err != nil {
			return err
		}

		logx.D(ctx, "[ReleaseRelease] embeddingModel:%s", embeddingModel)

		embeddingVersion = entity.GetEmbeddingVersion(embeddingModel)
		if embeddingVersion == 0 {
			embeddingVersion = appDB.Embedding.Version
		}
	} else {
		embeddingModel = ""
		embeddingVersion = appDB.Embedding.Version
	}

	commonRetrievalTaskParams := entity.TaskReleaseVectorParams{
		ReleaseParams: entity.ReleaseParams{
			ReleaseType: releaseType,
		},
		CorpID:             corpID,
		StaffID:            appDB.StaffID,
		AppID:              appID,
		AppBizID:           appBizID,
		VersionID:          entity.AppVersionID(versionID),
		VersionName:        fmt.Sprintf("%d.%d", appID, versionID),
		EmbeddingVersionID: entity.EmbeddingVersionID(embeddingVersion),
		EmbeddingModelName: embeddingModel,
		LastQAVersion:      appDB.QaVersion,
		IsCreateGroup:      appDB.QaVersion == 0, // retrieval代码这里是根据这个字段来判断的，这里需要和sinute确认一下 @wemysschen
	}

	logx.I(ctx, "[ReleaseRelease] commonRetrievalTaskParams: %+v", commonRetrievalTaskParams)

	switch releaseType {
	case releaseEntity.ReleaseDBType:
		logx.I(ctx, "[ReleaseRelease] ReleaseDBType is deprecated")
		//_, err = async.NewReleaseDBTask(ctx, &entity.ReleaseDBParams{
		//	ReleaseParams: entity.ReleaseParams{
		//		Name:        entity.TaskTypeNameMap[entity.ReleaseDBTask],
		//		ReleaseType: releaseType,
		//	},
		//	CorpBizID:    corpBizID,
		//	RobotID:      appID,
		//	AppBizID:     appBizID,
		//	ReleaseBizID: versionID,
		//})

	case releaseEntity.ReleaseTypeDocument:
		logx.I(ctx, "[ReleaseRelease] ReleaseTypeDocument is deprecated")
		//_, err = async.NewReleaseDocTask(ctx, &entity.ReleaseDocParams{
		//	TaskReleaseVectorParams: commonRetrievalTaskParams,
		//	CorpBizID:               corpBizID,
		//	RobotID:                 appID,
		//	AppBizID:                appBizID,
		//	ReleaseID:               versionID,
		//})

	case releaseEntity.ReleaseTypeQA:
		logx.I(ctx, "[ReleaseRelease] ReleaseTypeQA is deprecated")
		//_, err = async.NewReleaseDocQATask(ctx, &entity.ReleaseQAParams{
		//	TaskReleaseVectorParams: commonRetrievalTaskParams,
		//	CorpBizID:               corpBizID,
		//	RobotID:                 appID,
		//	AppBizID:                appBizID,
		//	ReleaseID:               versionID,
		//})
	case releaseEntity.ReleaseTypeRejectedQuestion:
		_, err = async.NewReleaseRejectedQuestionTask(ctx, &entity.ReleaseRejectedQuestionParams{
			TaskReleaseVectorParams: commonRetrievalTaskParams,
			CorpBizID:               corpBizID,
			RobotID:                 appID,
			AppBizID:                appBizID,
			ReleaseID:               versionID,
		})
	case releaseEntity.ReleaseTypeLabel:
		_, err = async.NewReleaseLabelTask(ctx, &entity.ReleaseLabelParams{
			ReleaseParams: entity.ReleaseParams{
				Name:        entity.TaskTypeNameMap[entity.ReleaseLabelTask],
				ReleaseType: releaseType,
			},
			CorpBizID: corpBizID,
			RobotID:   appID,
			AppBizID:  appBizID,
			ReleaseID: versionID,
		})
	case releaseEntity.ReleaseTypeConfig:
		_, err = async.NewReleaseKnowledgeConfigTask(ctx, &entity.ReleaseKnowledgeConfigParams{
			ReleaseParams: entity.ReleaseParams{
				Name:        entity.TaskTypeNameMap[entity.ReleaseKnowledgeConfigTask],
				ReleaseType: releaseType,
			},
			CorpBizID: corpBizID,
			RobotID:   appID,
			AppBizID:  appBizID,
			ReleaseID: versionID,
		})

	default:
		logx.E(ctx, "release type error:%v", releaseType)
		return errs.ErrParameterInvalid
	}
	if err != nil {
		logx.E(ctx, "create release task error:%v", err)
		return err
	}
	return nil
}

func (l *Logic) GetReleaseModifyRejectedQuestion(ctx context.Context, release *releaseEntity.Release,
	rejectedQuestion []*qaEntity.RejectedQuestion) (map[uint64]*releaseEntity.ReleaseRejectedQuestion, error) {
	return l.releaseDao.GetReleaseModifyRejectedQuestion(ctx, release, rejectedQuestion)
}

// 这里的逻辑来自于retrieval服务中 publish_incremental.go # notifySyncStatus 函数
func (l *Logic) DoReleaseAfterVectorized(ctx context.Context, robotID, appBizID uint64, relatedIDs []uint64,
	docType entity.DocType, qaType int, release *releaseEntity.Release) error {
	var err error
	logx.I(ctx, "DoReleaseAfterVectorized robotID:%d versionID:%d relatedIDs:%d docType:%d qaType:%d",
		robotID, release.ID, relatedIDs, docType, qaType)

	isSuccess := true

	if docType == entity.DocTypeQA {
		for _, v := range relatedIDs {
			if qaType == entity.QATypeReleaseSimilar { // 相似问答
				err = l.releaseSimilarQANotify(ctx, isSuccess, "", appBizID, v, release)
			} else { // 主问答
				err = l.releaseQANotify(ctx, isSuccess, "", appBizID, v, release)
			}
		}

	} else if docType == entity.DocTypeSegment {
		err = l.releaseSegmentNotify(ctx, isSuccess, "", robotID, relatedIDs, release)
	} else if docType == entity.DocTypeRejectedQuestion {
		for _, v := range relatedIDs {
			err = l.releaseRejectedQuestionNotify(ctx, isSuccess, "", v, release)
		}
	}
	if err != nil {
		logx.E(ctx, "Failed to Process DoReleaseAfterVectorized. %v", err)
		return err
	}

	return nil
}

func (l *Logic) DoSuccessNotifyRelease(ctx context.Context, appID, appBizID, versionID uint64,
	releaseNotifyCallback uint32) error {
	req := &appconfig.ReleaseNotifyReq{
		RobotId:        appID,
		VersionId:      versionID,
		IsSuccess:      true,
		CallbackSource: releaseNotifyCallback,
		RobotBizId:     appBizID,
	}
	_, err := l.rpc.AppAdmin.ReleaseNotify(ctx, req)
	if err != nil {
		logx.E(ctx, "ReleaseNotify req: %+v, error %v", req, err)
		return err
	}

	return nil

}
