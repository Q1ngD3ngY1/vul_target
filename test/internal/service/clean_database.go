package service

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/clean_database"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
)

// CleanDatabaseCommonData 清理已删除的数据
func (s *Service) CleanDatabaseCommonData(ctx context.Context) error {
	if !config.GetMainConfig().DefaultDatabaseCleanConfig.Enable {
		// 未开启清理功能
		log.InfoContextf(ctx, "CleanDatabaseCommonData is not enable")
		return nil
	}
	// 配置校验
	docBatchSize := config.GetMainConfig().DefaultDatabaseCleanConfig.DocBatchSize
	qaBatchSize := config.GetMainConfig().DefaultDatabaseCleanConfig.QaBatchSize
	deleteBatchSize := config.GetMainConfig().DefaultDatabaseCleanConfig.DeleteBatchSize
	deleteDelayTimeMinutes := config.GetMainConfig().DefaultDatabaseCleanConfig.DeleteDelayTimeMinutes
	if docBatchSize == 0 ||
		qaBatchSize == 0 ||
		deleteBatchSize == 0 ||
		deleteDelayTimeMinutes == 0 {
		log.ErrorContext(ctx, "CleanDatabaseCommonData config is invalid")
		return errs.ErrSystem
	}
	maxUpdateTime := time.Now().Add(-time.Duration(deleteDelayTimeMinutes) * time.Minute)
	log.InfoContextf(ctx, "CleanDatabaseCommonData begin docBatchSize:%d qaBatchSize:%d deleteBatchSize:%d maxUpdateTime:%s",
		docBatchSize, qaBatchSize, deleteBatchSize, maxUpdateTime.Format("2006-01-02 15:04:05"))
	// 清理已删除且发布的文档
	clean_database.CleanDeletedDocs(ctx, maxUpdateTime, docBatchSize)
	// 清理已删除且发布的问答
	// clean_database.CleanDeletedQas(ctx, maxUpdateTime, qaBatchSize)
	// TODO: 清理实时文档

	return nil
}
