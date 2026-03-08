package third_doc_dao

import (
	"context"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
)

// ThirdDocDao 第三方文档数据访问接口
type ThirdDocDao interface {
	// Migrate 迁移文档
	Migrate(ctx context.Context, platformID int32, appBizID uint64, operationIDAndFileIDMap map[string]uint64) error

	// GetMigrateProgress 获取迁移进度
	GetMigrateProgress(ctx context.Context, operationIDs []uint64) ([]*model.TThirdDocMigrateProgress, error)

	// UpdateMigrateProgress 更新迁移进度
	UpdateMigrateProgress(ctx context.Context, success, fail map[uint64]*model.TThirdDocMigrateProgress) error
}
