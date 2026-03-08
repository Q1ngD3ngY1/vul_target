package export

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/dao/export"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

type Logic struct {
	dao export.Dao
	rpc *rpc.RPC
}

func NewLogic(rpc *rpc.RPC, dao export.Dao) *Logic {
	return &Logic{
		rpc: rpc,
		dao: dao,
	}
}

func (l *Logic) CreateExportTask(ctx context.Context, corpID, staffID, robotID uint64, export *entity.Export, params *entity.ExportParams) (uint64, error) {
	taskID, err := l.dao.CreateExportTask(ctx, export)
	params.TaskID = taskID
	if err = scheduler.NewExportTask(ctx, robotID, *params); err != nil {
		return 0, err
	}
	// 不满意度导出目前没有横条，这里目前仅针对QA导出。
	operations := make([]releaseEntity.Operation, 0)
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithPageID(params.NoticePageID),
		releaseEntity.WithLevel(releaseEntity.LevelInfo),
		releaseEntity.WithContent(i18n.Translate(ctx, params.NoticeContentIng)),
		releaseEntity.WithForbidCloseFlag(),
	}
	notice := releaseEntity.NewNotice(params.NoticeTypeExport, uint64(taskID), corpID, robotID, staffID,
		noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return 0, err
	}
	if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		return 0, err
	}
	return taskID, nil
}

func (l *Logic) ModifyExportTask(ctx context.Context, export *entity.Export) error {
	return l.dao.ModifyExportTask(ctx, export)
}

func (l *Logic) DescribeExportTask(ctx context.Context, taskID, corpID, robotID uint64) (*entity.Export, error) {
	return l.dao.DescribeExportTask(ctx, taskID, corpID, robotID)
}
