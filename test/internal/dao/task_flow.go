package dao

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/KEP"
	"github.com/jmoiron/sqlx"
)

// SendDataSyncTask 任务型通知事件
func (d *dao) SendDataSyncTask(ctx context.Context, robotID, versionID, corpID, staffID uint64, event string) (
	*pb.SendDataSyncTaskEventRsp, error) {
	appDB, err := d.GetAppByID(ctx, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "通知任务型 获取机器人失败 req:%+v err:%v", robotID, err)
		return nil, err
	}
	if appDB == nil {
		err = errs.ErrRobotNotFound
		log.ErrorContextf(ctx, "通知任务型 获取机器人失败 req:%+v err:%v", robotID, err)
		return nil, err
	}
	req := &pb.SendDataSyncTaskEventReq{
		BotBizId:     appDB.BusinessID,
		BusinessName: model.TaskConfigBusinessNameTextRobot,
		Event:        event,
		TaskID:       versionID,
		CorpID:       corpID,
		StaffID:      staffID,
	}
	rsp, err := d.taskFlowCli.SendDataSyncTaskEvent(ctx, req)
	log.DebugContextf(ctx, "通知任务型%+v事件, req:%+v, rsp:%+v", event, req, rsp)
	if err != nil {
		log.ErrorContextf(ctx, "通知任务型%+v事件失败 req:%+v err:%+v", event, req, err)
		return nil, err
	}
	return rsp, nil
}

// GetDataSyncTaskDetail 任务型获取事件详情
func (d *dao) GetDataSyncTaskDetail(ctx context.Context, robotID, versionID, corpID, staffID uint64) (
	*pb.GetDataSyncTaskRsp, error) {
	appDB, err := d.GetAppByID(ctx, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "任务型获取事件详情 获取机器人失败 req:%+v err:%v", robotID, err)
		return nil, err
	}
	if appDB == nil {
		err = errs.ErrRobotNotFound
		log.ErrorContextf(ctx, "任务型获取事件详情 获取机器人失败 req:%+v err:%v", robotID, err)
		return nil, err
	}
	req := &pb.GetDataSyncTaskReq{
		BotBizId: appDB.BusinessID,
		TaskID:   versionID,
		CorpID:   corpID,
		StaffID:  staffID,
	}
	rsp, err := d.taskFlowCli.GetDataSyncTask(ctx, req)
	log.DebugContextf(ctx, "任务型获取事件详情, req:%+v, rsp:%+v", req, rsp)
	if err != nil {
		log.ErrorContextf(ctx, "任务型获取事件详情失败 req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// RetryTaskConfigRelease 重试任务型暂停的发布
func (d *dao) RetryTaskConfigRelease(ctx context.Context, release *model.Release) error {
	if _, err := d.SendDataSyncTask(ctx, release.RobotID, release.BusinessID, release.CorpID,
		release.StaffID, model.TaskConfigEventRetry); err != nil {
		return err
	}
	// 发布状态设置为发布中，并且页面通知发布中
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		querySQL := updateReleaseStatus
		release.Status = model.ReleaseStatusPending
		release.Message = "发布暂停重试中"
		release.UpdateTime = time.Now()
		if _, err := tx.NamedExecContext(ctx, querySQL, release); err != nil {
			log.ErrorContextf(ctx, "重试暂停的发布失败 sql:%s args:%+v err:%+v", querySQL, release, err)
			return err
		}
		if err := d.sendNotifyReleasing(ctx, tx, release); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "重试暂停的发布失败 err:%+v", err)
		return err
	}
	return nil
}

// GetUnreleasedTaskQACount 获取任务型待发布数量
func (d *dao) GetUnreleasedTaskQACount(ctx context.Context, robotBizID, corpID, staffID uint64) (uint32, error) {
	appDB, err := d.GetAppByAppBizID(ctx, robotBizID)
	if err != nil {
		log.ErrorContextf(ctx, "任务型获取待发布数量 获取机器人失败 req:%+v err:%v", robotBizID, err)
		return 0, err
	}
	if appDB == nil {
		err = errs.ErrRobotNotFound
		log.ErrorContextf(ctx, "任务型获取待发布数量 获取机器人失败 req:%+v err:%v", robotBizID, err)
		return 0, err
	}
	req := &pb.GetUnreleasedCountReq{
		BotBizId: appDB.BusinessID,
		CorpID:   corpID,
		StaffID:  staffID,
	}
	rsp, err := d.taskFlowCli.GetUnreleasedCount(ctx, req)
	log.DebugContextf(ctx, "任务型获取待发布数量 req:%+v rsp:%+v", req, rsp)
	if err != nil {
		log.ErrorContextf(ctx, "任务型获取待发布数量失败 req:%+v err:%+v", req, err)
		return 0, err
	}
	return rsp.GetCount(), nil
}

// GetVarList 获取自定义变量数据
func (d *dao) GetVarList(ctx context.Context, appBizId string, varIDs []string) (*admin.FetchVarListRsp, error) {
	req := &admin.FetchVarListReq{
		AppBizId: appBizId,
		VarIds:   varIDs,
	}
	rsp, err := d.adminApiCli.FetchVarList(ctx, req)
	log.DebugContextf(ctx, "获取自定义变量数据 req:%+v rsp:%+v", req, rsp)
	if err != nil {
		log.ErrorContextf(ctx, "获取自定义变量数据失败 req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}
