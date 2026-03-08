// bot-knowledge-config-server
//
// @(#)task_flow.go  星期三, 五月 21, 2025
// Copyright(c) 2025, zrwang@Tencent. All rights reserved.

package client

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/KEP_WF"
)

// GetWorkflowListByDoc 批量获取文档被引用的工作流列表
func GetWorkflowListByDoc(ctx context.Context, appBizID string, docBizIDs []string) (
	*KEP_WF.GetWorkflowListByDocRsp, error) {
	req := &KEP_WF.GetWorkflowListByDocReq{
		AppBizId:  appBizID,
		DocBizIds: docBizIDs,
	}
	rsp, err := taskFlowCli.GetWorkflowListByDoc(ctx, req)
	log.DebugContextf(ctx, "批量获取文档被引用的工作流列表 req:%+v rsp:%+v", req, rsp)
	if err != nil {
		log.ErrorContextf(ctx, "批量获取文档被引用的工作流列表 req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// GetWorkflowListByAttribute 批量获取标签被引用的工作流列表
func GetWorkflowListByAttribute(ctx context.Context, appBizId string, attributeBizIds []string) (
	*KEP_WF.GetWorkflowListByAttributeRsp, error) {
	req := &KEP_WF.GetWorkflowListByAttributeReq{
		AppBizId:        appBizId,
		AttributeBizIds: attributeBizIds,
	}
	rsp, err := taskFlowCli.GetWorkflowListByAttribute(ctx, req)
	log.DebugContextf(ctx, "批量获取标签被引用的工作流列表 req:%+v rsp:%+v", req, rsp)
	if err != nil {
		log.ErrorContextf(ctx, "批量获取标签被引用的工作流列表 req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// GetWorkflowListByAttributeLabel 批量获取标签值被引用的工作流列表
func GetWorkflowListByAttributeLabel(ctx context.Context, appBizId string, attributeLabelBizIds []string) (
	*KEP_WF.GetWorkflowListByAttributeLabelRsp, error) {
	req := &KEP_WF.GetWorkflowListByAttributeLabelReq{
		AppBizId:             appBizId,
		AttributeLabelBizIds: attributeLabelBizIds,
	}
	rsp, err := taskFlowCli.GetWorkflowListByAttributeLabel(ctx, req)
	log.DebugContextf(ctx, "批量获取标签值被引用的工作流列表 req:%+v rsp:%+v", req, rsp)
	if err != nil {
		log.ErrorContextf(ctx, "批量获取标签值被引用的工作流列表 req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}
