package api

import (
	"context"
	"database/sql"
	"errors"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/share_knowledge"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/common/v3/utils"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	"time"
)

// ListReferShareKnowledge 查看引用共享文档列表
func (s *Service) ListReferShareKnowledge(ctx context.Context,
	req *pb.ListReferSharedKnowledgeReq) (*pb.ListReferSharedKnowledgeRsp, error) {
	start := time.Now()
	rsp := new(pb.ListReferSharedKnowledgeRsp)
	log.InfoContextf(ctx, "ListReferShareKnowledge Req: %+v", utils.Any2String(req))
	app, err := client.GetAppInfo(ctx, req.GetAppBizId(), model.RunEnvSandbox)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	// 1. 获取应用引用共享库列表
	shareKGList, err := dao.GetAppShareKGDao().GetAppShareKGList(ctx, req.GetAppBizId())
	if err != nil {
		log.ErrorContextf(ctx, "GetAppShareKGList failed, err: %+v", err)
		return nil, errs.ErrGetAppShareKGListFailed
	}
	if len(shareKGList) == 0 {
		log.WarnContextf(ctx, "ListReferShareKnowledge GetAppShareKGList is empty")
		return rsp, nil
	}
	// 2. 批量获取共享库详情
	var shareKGBizIDs []uint64
	for _, val := range shareKGList {
		shareKGBizIDs = append(shareKGBizIDs, val.KnowledgeBizID)
	}
	shareKGInfoList, err := s.dao.RetrieveBaseSharedKnowledge(ctx, app.GetCorpBizId(), shareKGBizIDs)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return rsp, nil
		}
		log.ErrorContextf(ctx, "ListReferShareKnowledge.RetrieveBaseSharedKnowledge failed, error: %+v", err)
		return nil, errs.ErrRetrieveBaseSharedKGFailed
	}

	// NOTICE: 检索模型配置
	shareKGInfoList, err = share_knowledge.RetrieveModelConfig(ctx, app.GetCorpBizId(), shareKGInfoList)
	if err != nil {
		return rsp, errs.ErrQueryKnowledgeModelConfigFailed
	}
	log.InfoContextf(ctx, "ListReferShareKnowledge.RetrieveModelConfig, shareKGInfoList(%d): %+v",
		len(shareKGInfoList), shareKGInfoList)

	shareKGInfoMap := make(map[uint64]*model.SharedKnowledgeInfo)
	for _, info := range shareKGInfoList {
		shareKGInfoMap[info.BusinessID] = info
	}
	for _, val := range shareKGList {
		shareKGInfo, ok := shareKGInfoMap[val.KnowledgeBizID]
		if !ok || shareKGInfo == nil {
			log.WarnContextf(ctx, "shareKGInfoMap[%d] not found", val.KnowledgeBizID)
			continue
		}

		knowledge, _ := share_knowledge.ConvertSharedKnowledgeBaseInfo(ctx, shareKGInfo)
		rsp.List = append(rsp.List, knowledge)
	}

	log.InfoContextf(ctx, "ListReferShareKnowledge Rsp: %+v, cost: %d", utils.Any2String(rsp),
		time.Since(start).Milliseconds())
	return rsp, nil
}
