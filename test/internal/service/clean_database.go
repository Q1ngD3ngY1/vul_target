package service

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	deleteHandler "git.woa.com/adp/kb/kb-config/internal/logic/kb/handler"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

// CleanDatabaseCommonData 清理已删除的数据
func (s *Service) CleanDatabaseCommonData(ctx context.Context) error {
	if !config.GetMainConfig().DefaultDatabaseCleanConfig.Enable {
		// 未开启清理功能
		logx.D(ctx, "CleanDatabaseCommonData is not enable")
		return nil
	}
	// 配置校验
	docBatchSize := int(config.GetMainConfig().DefaultDatabaseCleanConfig.DocBatchSize)
	qaBatchSize := config.GetMainConfig().DefaultDatabaseCleanConfig.QaBatchSize
	deleteBatchSize := config.GetMainConfig().DefaultDatabaseCleanConfig.DeleteBatchSize
	deleteDelayTimeMinutes := config.GetMainConfig().DefaultDatabaseCleanConfig.DeleteDelayTimeMinutes
	if docBatchSize == 0 ||
		qaBatchSize == 0 ||
		deleteBatchSize == 0 ||
		deleteDelayTimeMinutes == 0 {
		logx.E(ctx, "CleanDatabaseCommonData config is invalid")
		return errs.ErrSystem
	}
	maxUpdateTime := time.Now().Add(-time.Duration(deleteDelayTimeMinutes) * time.Minute)
	logx.D(ctx, "CleanDatabaseCommonData begin docBatchSize:%d qaBatchSize:%d deleteBatchSize:%d maxUpdateTime:%s",
		docBatchSize, qaBatchSize, deleteBatchSize, maxUpdateTime.Format("2006-01-02 15:04:05"))
	// 清理已删除且发布的文档
	deleteHandler.CleanDeletedDocs(ctx, maxUpdateTime, docBatchSize, s.docLogic.GetDao(), s.rpc, s.cacheLogic)
	// 清理已删除且发布的问答
	// clean_database.CleanDeletedQas(ctx, maxUpdateTime, qaBatchSize)
	// TODO: 清理实时文档

	return nil
}
