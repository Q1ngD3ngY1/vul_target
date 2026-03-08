package clean_database

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"time"
)

const (
	qaTableName = "t_doc_qa"

	tableColumnQaId = "qa_id"
)

// qaRelatedTables 问答相关需要清理的表
var qaRelatedTables = map[string]string{
	"t_doc_qa_similar": tableColumnQaId,
}

// CleanDeletedQas 清理已删除的问答
func CleanDeletedQas(ctx context.Context, maxUpdateTime time.Time, batchSize uint32) {
	// 1. 获取已删除的文档
	docQaFilter := &dao.DocQaFilter{
		ReleaseStatus: []uint32{model.QAReleaseStatusSuccess},
		IsDeleted:     pkg.GetIntPtr(model.QAIsDeleted),
		MaxUpdateTime: maxUpdateTime,
		Offset:        0,
		Limit:         batchSize,
	}
	selectColumns := []string{dao.DocQaTblColId, dao.DocQaTblColBusinessId, dao.DocQaTblColReleaseStatus,
		dao.DocQaTblColIsDeleted, dao.DocQaTblColUpdateTime}
	docQas, err := dao.GetDocQaDao().GetDocQaList(ctx, selectColumns, docQaFilter)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocQaList failed, err: %+v", err)
		return
	}
	for _, qa := range docQas {
		if qa.UpdateTime.After(maxUpdateTime) {
			log.ErrorContextf(ctx, "qa.UpdateTime:%+v > maxUpdateTime:%+v", qa.UpdateTime, maxUpdateTime)
			continue
		}
		err = CleanDeletedQa(ctx, qa)
		if err != nil {
			log.ErrorContextf(ctx, "CleanDeletedDoc qa.ID:%d failed, err: %+v", qa.ID, err)
			continue
		}
	}
}

// CleanDeletedQa 清理单个已删除的问答
func CleanDeletedQa(ctx context.Context, qa *model.DocQA) error {
	if qa.IsDeleted != model.QAIsDeleted || qa.ReleaseStatus != model.QAReleaseStatusSuccess {
		// 高危操作，再次检查问答信息
		log.ErrorContextf(ctx, "qa.IsDeleted:%+v != model.QAIsDeleted:%+v || qa.ReleaseStatus:%+v != model.QAReleaseStatusSuccess:%+v",
			qa.IsDeleted, model.IsDeleted, qa.ReleaseStatus, model.DocStatusReleaseSuccess)
		return errs.ErrSystem
	}

	// TODO:先删除问答相关的其他表信息，最后删除问答信息，避免中途失败导致的数据不一致
	// 删除问答相关的其他表信息
	return nil
}
