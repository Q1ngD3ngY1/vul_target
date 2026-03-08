package api

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/share_knowledge"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	knowledge_pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
)

// BatchGetSharedKnowledge 批量获取共享知识库
func (s *Service) BatchGetSharedKnowledge(ctx context.Context, req *pb.BatchGetSharedKnowledgeReq) (
	*pb.BatchGetSharedKnowledgeRsp, error) {
	start := time.Now()

	var err error
	rsp := new(pb.BatchGetSharedKnowledgeRsp)

	log.InfoContextf(ctx, "BatchGetSharedKnowledge, request: %+v", req)
	defer func() {
		log.InfoContextf(ctx, "BatchGetSharedKnowledge, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()

	if !share_knowledge.VerifyData([]*share_knowledge.DataValidation{
		{
			Data:      req.GetCorpBizId(),
			Validator: share_knowledge.NewRangeValidator(share_knowledge.WithMin(1))},
		{
			Data:      len(req.GetKnowledgeBizIdList()),
			Validator: share_knowledge.NewRangeValidator(share_knowledge.WithMin(1))},
	}) {
		err = errs.ErrParameterInvalid
		return rsp, err
	}

	// NOTICE: 内部服务接口不做账号鉴权
	knowledgeList, err := s.dao.RetrieveBaseSharedKnowledge(ctx, req.GetCorpBizId(),
		req.GetKnowledgeBizIdList())
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			//NOTICE: 未找到任何记录 不报错errs.ErrSharedKnowledgeRecordNotFound
			return rsp, nil
		} else {
			return rsp, errs.ErrQuerySharedKnowledgeRecordFailed
		}
	}
	log.InfoContextf(ctx, "BatchGetSharedKnowledge, GetKnowledgeBizIdList.size: %d, knowledgeList(%d): %+v",
		len(req.GetKnowledgeBizIdList()), len(knowledgeList), knowledgeList)

	// NOTICE: 检索模型配置
	knowledgeList, err = share_knowledge.RetrieveModelConfig(ctx, req.GetCorpBizId(), knowledgeList)
	if err != nil {
		return rsp, errs.ErrQueryKnowledgeModelConfigFailed
	}

	infoList := make([]*pb.KnowledgeBaseInfo, 0)
	for _, item := range knowledgeList {
		knowledge, _ := share_knowledge.ConvertSharedKnowledgeBaseInfo(ctx, item)
		infoList = append(infoList, knowledge)
	}

	rsp.InfoList = infoList
	return rsp, nil
}

// ClearSpaceKnowledge 清理空间知识库，删除空间时调用
func (s *Service) ClearSpaceKnowledge(ctx context.Context, req *knowledge_pb.ClearSpaceKnowledgeReq) (
	*knowledge_pb.ClearSpaceKnowledgeRsp, error) {
	rsp := &knowledge_pb.ClearSpaceKnowledgeRsp{}
	log.InfoContextf(ctx, "ClearSpaceKnowledge, request: %+v", req)

	rowsAffected, err := s.dao.ClearSpaceSharedKnowledge(ctx, req.GetCorpBizId(), req.GetSpaceId())
	if err != nil {
		log.ErrorContextf(ctx, "ClearSpaceKnowledge dao.ClearSpaceSharedKnowledge fail, err=%+v", err)
		return rsp, err
	}
	log.DebugContextf(ctx, "ClearSpaceKnowledge, rowsAffected: %+v", rowsAffected)

	return rsp, nil
}

func (s *Service) GetSpaceShareKnowledgeList(
	ctx context.Context,
	req *knowledge_pb.GetSpaceShareKnowledgeListReq) (*knowledge_pb.GetSpaceShareKnowledgeListRsp, error) {
	rsp := &knowledge_pb.GetSpaceShareKnowledgeListRsp{}
	log.InfoContextf(ctx, "GetSpaceShareKnowledgeList, request: %+v", req)
	total, list, err := share_knowledge.GetSpaceShareKnowledgeListExSelf(ctx, s.dao,
		req.GetCorpBizId(), req.GetExcludeStaffId(), req.GetSpaceId(),
		req.GetKeyword(), req.GetPageNumber(), req.GetPageSize())
	if err != nil {
		log.ErrorContextf(ctx, "GetSpaceShareKnowledgeListExSelf fail, err=%+v", err)
		return rsp, err
	}
	rsp.Total = uint32(total)
	rsp.ShareKnowledgeList = list

	return rsp, nil
}
