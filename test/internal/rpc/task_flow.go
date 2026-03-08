package rpc

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/KEP"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/KEP_WF"
)

type TaskFlowRPC interface {
	SendDataSyncTask(ctx context.Context, appBizID, versionID, corpID, staffID uint64, event string) error
	GetDataSyncTaskDetail(ctx context.Context, appBizID, versionID, corpID, staffID uint64) (
		*KEP.GetDataSyncTaskRsp, error)
	GetUnreleasedTaskQACount(ctx context.Context, appBizID, corpID, staffID uint64) (
		uint32, error)
	GetWorkflowListByDoc(ctx context.Context, appBizID string, docBizIDs []string) (*KEP_WF.GetWorkflowListByDocRsp, error)
	GetWorkflowListByAttribute(ctx context.Context, appBizId string, attributeBizIds []string) (*KEP_WF.GetWorkflowListByAttributeRsp, error)
	GetWorkflowListByAttributeLabel(ctx context.Context, appBizId string, attributeLabelBizIds []string) (*KEP_WF.GetWorkflowListByAttributeLabelRsp, error)
	SendDataSyncTaskEvent(context.Context, *KEP.SendDataSyncTaskEventReq) (*KEP.SendDataSyncTaskEventRsp, error)
	GetDataSyncTask(context.Context, *KEP.GetDataSyncTaskReq) (*KEP.GetDataSyncTaskRsp, error)
	GetUnreleasedCount(context.Context, *KEP.GetUnreleasedCountReq) (*KEP.GetUnreleasedCountRsp, error)
}

// GetWorkflowListByDoc 批量获取文档被引用的工作流列表
func (r *RPC) GetWorkflowListByDoc(ctx context.Context, appBizID string, docBizIDs []string) (
	*KEP_WF.GetWorkflowListByDocRsp, error) {
	req := &KEP_WF.GetWorkflowListByDocReq{
		AppBizId:  appBizID,
		DocBizIds: docBizIDs,
	}
	rsp, err := r.taskFlow.GetWorkflowListByDoc(ctx, req)
	logx.D(ctx, "批量获取文档被引用的工作流列表 req:%+v rsp:%+v", req, rsp)
	if err != nil {
		logx.E(ctx, "批量获取文档被引用的工作流列表 req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// GetWorkflowListByAttribute 批量获取标签被引用的工作流列表
func (r *RPC) GetWorkflowListByAttribute(ctx context.Context, appBizId string, attributeBizIds []string) (
	*KEP_WF.GetWorkflowListByAttributeRsp, error) {
	req := &KEP_WF.GetWorkflowListByAttributeReq{
		AppBizId:        appBizId,
		AttributeBizIds: attributeBizIds,
	}
	rsp, err := r.taskFlow.GetWorkflowListByAttribute(ctx, req)
	logx.D(ctx, "批量获取标签被引用的工作流列表 req:%+v rsp:%+v", req, rsp)
	if err != nil {
		logx.E(ctx, "批量获取标签被引用的工作流列表 req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// GetWorkflowListByAttributeLabel 批量获取标签值被引用的工作流列表
func (r *RPC) GetWorkflowListByAttributeLabel(ctx context.Context, appBizId string, attributeLabelBizIds []string) (
	*KEP_WF.GetWorkflowListByAttributeLabelRsp, error) {
	req := &KEP_WF.GetWorkflowListByAttributeLabelReq{
		AppBizId:             appBizId,
		AttributeLabelBizIds: attributeLabelBizIds,
	}
	rsp, err := r.taskFlow.GetWorkflowListByAttributeLabel(ctx, req)
	logx.D(ctx, "批量获取标签值被引用的工作流列表 req:%+v rsp:%+v", req, rsp)
	if err != nil {
		logx.E(ctx, "批量获取标签值被引用的工作流列表 req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// 任务型通知事件
func (r *RPC) SendDataSyncTask(ctx context.Context, appBizID, versionID, corpID, staffID uint64, event string) error {
	req := &KEP.SendDataSyncTaskEventReq{
		BotBizId:     appBizID,
		BusinessName: entity.TaskConfigBusinessNameTextRobot,
		Event:        event,
		TaskID:       versionID,
		CorpID:       corpID,
		StaffID:      staffID,
	}
	rsp, err := r.taskFlow.SendDataSyncTaskEvent(ctx, req)
	logx.D(ctx, "notice task event:%s, req:%+v, rsp:%+v", event, req, rsp)
	if err != nil {
		logx.E(ctx, "failed to notice task event:%s req:%+v err:%+v", event, req, err)
		return err
	}
	return nil
}

// 任务型获取事件详情
func (r *RPC) GetDataSyncTaskDetail(ctx context.Context, appBizID, versionID, corpID, staffID uint64) (
	*KEP.GetDataSyncTaskRsp, error) {
	req := &KEP.GetDataSyncTaskReq{
		BotBizId: appBizID,
		TaskID:   versionID,
		CorpID:   corpID,
		StaffID:  staffID,
	}
	rsp, err := r.taskFlow.GetDataSyncTask(ctx, req)
	logx.D(ctx, "get dataSyncTask details, req:%+v, rsp:%+v", req, rsp)
	if err != nil {
		logx.E(ctx, "failed to get dataSyncTask details req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// 获取任务型待发布数量
func (r *RPC) GetUnreleasedTaskQACount(ctx context.Context, appBizID, corpID, staffID uint64) (uint32, error) {
	req := &KEP.GetUnreleasedCountReq{
		BotBizId: appBizID,
		CorpID:   corpID,
		StaffID:  staffID,
	}
	rsp, err := r.taskFlow.GetUnreleasedCount(ctx, req)
	logx.D(ctx, "UnreleasedCount of task qa count, req:%+v rsp:%+v", req, rsp)
	if err != nil {
		logx.E(ctx, "Failed to get unreleasedCount of released qa: req:%+v err:%+v", req, err)
		return 0, err
	}
	return rsp.GetCount(), nil
}

func (r *RPC) SendDataSyncTaskEvent(ctx context.Context, req *KEP.SendDataSyncTaskEventReq) (*KEP.SendDataSyncTaskEventRsp, error) {
	return r.taskFlow.SendDataSyncTaskEvent(ctx, req)
}

func (r *RPC) GetDataSyncTask(ctx context.Context, req *KEP.GetDataSyncTaskReq) (*KEP.GetDataSyncTaskRsp, error) {
	return r.taskFlow.GetDataSyncTask(ctx, req)
}

func (r *RPC) GetUnreleasedCount(ctx context.Context, req *KEP.GetUnreleasedCountReq) (*KEP.GetUnreleasedCountRsp, error) {
	return r.taskFlow.GetUnreleasedCount(ctx, req)
}
