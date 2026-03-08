package qa

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

// CleanDeletedQas 清理已删除的问答
func (l *Logic) CleanDeletedQas(ctx context.Context, maxUpdateTime time.Time, batchSize int) {
	// 1. 获取已删除的文档
	docQaFilter := &qaEntity.DocQaFilter{
		ReleaseStatusList: []uint32{qaEntity.QAReleaseStatusSuccess},
		IsDeleted:         ptrx.Uint32(qaEntity.QAIsDeleted),
		MaxUpdateTime:     maxUpdateTime,
		Offset:            0,
		Limit:             batchSize,
	}
	selectColumns := []string{qaEntity.DocQaTblColId, qaEntity.DocQaTblColBusinessId,
		qaEntity.DocQaTblColReleaseStatus,
		qaEntity.DocQaTblColIsDeleted, qaEntity.DocQaTblColUpdateTime}
	docQas, err := l.qaDao.GetDocQaList(ctx, selectColumns, docQaFilter)
	if err != nil {
		logx.E(ctx, "GetDocQaList failed, err: %+v", err)
		return
	}
	for _, qa := range docQas {
		if qa.UpdateTime.After(maxUpdateTime) {
			logx.E(ctx, "qa.UpdateTime:%+v > maxUpdateTime:%+v", qa.UpdateTime, maxUpdateTime)
			continue
		}
		err = l.CleanDeletedQa(ctx, qa)
		if err != nil {
			logx.E(ctx, "CleanDeletedDoc qa.ID:%d failed, err: %+v", qa.ID, err)
			continue
		}
	}
}

// CleanDeletedQa 清理单个已删除的问答
func (l *Logic) CleanDeletedQa(ctx context.Context, qa *qaEntity.DocQA) error {
	if qa.IsDeleted != qaEntity.QAIsDeleted || qa.ReleaseStatus != qaEntity.QAReleaseStatusSuccess {
		// 高危操作，再次检查问答信息
		logx.E(ctx, "qa.IsDeleted:%+v || qa.ReleaseStatus:%+v not match", qa.IsDeleted, qa.ReleaseStatus)
		return errs.ErrSystem
	}

	// TODO:先删除问答相关的其他表信息，最后删除问答信息，避免中途失败导致的数据不一致
	// 删除问答相关的其他表信息
	return nil
}
