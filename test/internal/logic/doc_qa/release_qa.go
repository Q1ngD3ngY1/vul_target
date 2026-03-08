package doc_qa

import (
	"context"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
)

// GetReleasingQaId 获取发布中的问答id
func GetReleasingQaId(ctx context.Context, robotID uint64, qaIds []uint64) (map[uint64]struct{}, error) {
	corpID := pkg.CorpID(ctx)
	releaseQas := make(map[uint64]struct{}, 0)
	latestRelease, err := dao.GetReleaseDao().GetLatestRelease(ctx, corpID, robotID)
	if err != nil {
		return nil, err
	}
	if latestRelease == nil {
		return releaseQas, nil
	}
	if latestRelease.IsPublishDone() {
		return releaseQas, nil
	}
	releaseQas, err = dao.GetReleaseQaDao().GetReleaseQaIdMap(ctx, corpID, robotID, latestRelease.ID, qaIds)
	if err != nil {
		return nil, err
	}
	return releaseQas, nil
}

// GetDocQaReleaseCount 获取问答未发布状态总数
func GetDocQaReleaseCount(ctx context.Context, corpID, robotID uint64) (int64, error) {
	//isDeleted := model.QAIsNotDeleted
	filter := &dao.DocQaFilter{
		CorpId:  corpID,
		RobotId: robotID,
		ReleaseStatus: []uint32{model.QAReleaseStatusInit, model.QAReleaseStatusLearning, model.QAReleaseStatusAuditing,
			model.QAReleaseStatusAppealIng},
		AcceptStatus: model.AcceptYes,
		ReleaseCount: true,
		//IsDeleted:    &isDeleted,
	}
	count, err := dao.GetDocQaDao().GetDocQaCount(ctx, []string{dao.DocQaTblColId}, filter)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// CheckSaveDocQa 检查问答是否可以保存
func CheckSaveDocQa(ctx context.Context, corpID, robotID uint64) (bool, error) {
	releaseCount, err := GetDocQaReleaseCount(ctx, corpID, robotID)
	if err != nil {
		return false, err
	}
	if releaseCount > int64(config.App().RobotDefault.DocReleaseMaxLimit) {
		return false, nil
	}

	return true, nil
}
