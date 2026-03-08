package api

import (
	"context"
	"net/http"
	"strings"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
)

// GlobalKnowledge 全局知识库
// 没有跟着机器人走, 也就意味着无法找到关联的 embedding 版本, 只能使用当前版本, 因此当 embedding 模型更新时, 需要刷新所有的全局知识
func (s *Service) GlobalKnowledge(
	ctx context.Context, req *pb.GlobalKnowledgeReq,
) (*pb.GlobalKnowledgeRsp, error) {
	rsp := new(pb.GlobalKnowledgeRsp)
	key := utils.When(req.GetFilterKey() == "", model.SearchGlobalFilterKey, req.GetFilterKey())
	filter, ok := config.App().RobotDefault.Filters[key]
	if !ok {
		log.ErrorContextf(ctx, "filter(%s) not found", key)
		return nil, errs.ErrFilterNotFound
	}

	filters := make([]*retrieval.DirectSearchVectorReq_Filter, 0, len(filter.Filter))
	for _, f := range filter.Filter {
		filters = append(filters, &retrieval.DirectSearchVectorReq_Filter{
			IndexId:         uint64(f.IndexID),
			Confidence:      f.Confidence,
			TopN:            f.TopN,
			DocType:         f.DocType,
			LabelExprString: fillLabelExprString(req.GetLabels()), // 2.6之前旧的表达式，保持逻辑不变
		})
	}
	r, err := s.dao.DirectSearchVector(ctx, &retrieval.DirectSearchVectorReq{
		Name:             model.GlobalKnowledgeGroupName,
		Question:         req.GetQuestion(),
		Filters:          filters,
		TopN:             filter.TopN,
		EmbeddingVersion: config.App().RobotDefault.Embedding.Version,
		Rerank:           &retrieval.DirectSearchVectorReq_Rerank{Enable: false},
	})
	if err != nil {
		log.ErrorContextf(ctx, "DirectSearchVector err: %v", err)
		return nil, errs.ErrGetGlobalKnowledge
	}

	var ids []model.GlobalKnowledgeID
	for _, v := range r.GetDocs() {
		ids = append(ids, model.GlobalKnowledgeID(v.GetId()))
	}
	knowledges, err := s.dao.GetGlobalKnowledges(ctx, ids)
	if err != nil {
		log.ErrorContextf(ctx, "GetGlobalKnowledges err: %v", err)
		return nil, errs.ErrGetGlobalKnowledge
	}
	knowledgeMap := make(map[model.GlobalKnowledgeID]model.GlobalKnowledge)
	for _, v := range knowledges {
		knowledgeMap[v.ID] = v
	}
	var docs []*pb.GlobalKnowledgeRsp_Doc
	for _, v := range r.GetDocs() {
		k, ok := knowledgeMap[model.GlobalKnowledgeID(v.GetId())]
		if !ok {
			continue
		}
		docs = append(docs, &pb.GlobalKnowledgeRsp_Doc{
			DocId:      0,
			DocType:    v.GetDocType(),
			RelatedId:  v.GetId(),
			Question:   k.Question,
			Answer:     k.Answer,
			Confidence: v.GetConfidence(),
			OrgData:    "",
		})
	}
	rsp.Docs = docs
	return rsp, nil
}

// ListGlobalKnowledge 添加全局知识
func (s *Service) ListGlobalKnowledge(ctx context.Context, req *pb.ListGlobalKnowledgeReq) (*pb.ListGlobalKnowledgeRsp,
	error) {
	rsp := new(pb.ListGlobalKnowledgeRsp)
	total, globalKnowledges, err := s.dao.ListGlobalKnowledge(ctx, req.Query, req.GetPageNumber(), req.GetPageSize())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	rsp.Total = total
	rsp.List = make([]*pb.ListGlobalKnowledgeRsp_GlobalKnowledge, 0, len(globalKnowledges))
	for _, gk := range globalKnowledges {
		rsp.List = append(rsp.List, &pb.ListGlobalKnowledgeRsp_GlobalKnowledge{
			Id:         uint64(gk.ID),
			Question:   gk.Question,
			Answer:     gk.Answer,
			IsSync:     gk.IsSync,
			IsDeleted:  gk.IsDeleted,
			CreateTime: gk.CreateTime.Unix(),
			UpdateTime: gk.UpdateTime.Unix(),
		})
	}
	return rsp, nil
}

// AddGlobalKnowledge 添加全局知识
func (s *Service) AddGlobalKnowledge(
	ctx context.Context, req *pb.AddGlobalKnowledgeReq,
) (*pb.AddGlobalKnowledgeRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.AddGlobalKnowledgeRsp)
	if s.isEmbeddingUpgrading() {
		return rsp, errs.ErrEmbeddingUpgrading
	}
	knowledge := model.GlobalKnowledge{
		Question:  req.GetQuestion(),
		Answer:    req.GetAnswer(),
		IsSync:    false,
		IsDeleted: false,
	}
	var err error
	if knowledge.ID, err = s.dao.InsertGlobalKnowledge(ctx, knowledge); err != nil {
		log.ErrorContextf(ctx, "InsertGlobalKnowledge fail, knowledge: %+v, err: %v", knowledge, err)
		return rsp, errs.ErrAddGlobalKnowledge
	}
	if _, err = s.dao.DirectAddVector(ctx, &retrieval.DirectAddVectorReq{
		Name:             model.GlobalKnowledgeGroupName,
		IndexId:          model.SearchGlobalVersionID,
		Id:               uint64(knowledge.ID),
		PageContent:      req.GetQuestion(),
		DocType:          model.DocTypeQA,
		EmbeddingVersion: config.App().RobotDefault.Embedding.Version,
		Labels:           nil,
	}); err != nil {
		log.ErrorContextf(ctx, "DirectAddVector fail, knowledge: %+v, err: %v", knowledge, err)
		return rsp, errs.ErrAddGlobalKnowledge
	}
	knowledge.IsSync = true
	if err = s.dao.UpdateGlobalKnowledge(ctx, knowledge); err != nil {
		log.ErrorContextf(ctx, "UpdateGlobalKnowledge fail, knowledge: %+v, err: %v", knowledge, err)
		return rsp, errs.ErrAddGlobalKnowledge
	}
	rsp.Id = uint64(knowledge.ID)
	return rsp, nil
}

// DelGlobalKnowledge 删除全局知识
func (s *Service) DelGlobalKnowledge(
	ctx context.Context, req *pb.DelGlobalKnowledgeReq,
) (*pb.DelGlobalKnowledgeRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.DelGlobalKnowledgeRsp)
	if s.isEmbeddingUpgrading() {
		return rsp, errs.ErrEmbeddingUpgrading
	}
	knowledge, err := s.dao.GetGlobalKnowledge(ctx, model.GlobalKnowledgeID(req.GetId()))
	if err != nil {
		log.ErrorContextf(ctx, "GetGlobalKnowledge fail, id: %d, err: %v", req.GetId(), err)
		return rsp, errs.ErrDelGlobalKnowledge
	}
	if _, err = s.dao.DirectDeleteVector(ctx, &retrieval.DirectDeleteVectorReq{
		Name:             model.GlobalKnowledgeGroupName,
		IndexId:          model.SearchGlobalVersionID,
		Id:               uint64(knowledge.ID),
		EmbeddingVersion: config.App().RobotDefault.Embedding.Version,
	}); err != nil {
		log.ErrorContextf(ctx, "DirectDeleteVector fail, id: %d, err: %v", req.GetId(), err)
		return rsp, errs.ErrDelGlobalKnowledge
	}
	if err = s.dao.DeleteGlobalKnowledge(ctx, knowledge.ID); err != nil {
		log.ErrorContextf(ctx, "DeleteGlobalKnowledge fail, id: %d, err: %v", req.GetId(), err)
		return rsp, errs.ErrDelGlobalKnowledge
	}
	return rsp, nil
}

// UpdGlobalKnowledge 更新全局知识
func (s *Service) UpdGlobalKnowledge(
	ctx context.Context, req *pb.UpdGlobalKnowledgeReq,
) (*pb.UpdGlobalKnowledgeRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.UpdGlobalKnowledgeRsp)
	if s.isEmbeddingUpgrading() {
		return rsp, errs.ErrEmbeddingUpgrading
	}
	knowledge, err := s.dao.GetGlobalKnowledge(ctx, model.GlobalKnowledgeID(req.GetId()))
	if err != nil {
		log.ErrorContextf(ctx, "GetGlobalKnowledge fail, id: %d, err: %v", req.GetId(), err)
		return rsp, errs.ErrUpdGlobalKnowledge
	}
	knowledge.IsSync = false
	knowledge.Question = req.GetQuestion()
	knowledge.Answer = req.GetAnswer()
	if err = s.dao.UpdateGlobalKnowledge(ctx, knowledge); err != nil {
		log.ErrorContextf(ctx, "UpdateGlobalKnowledge fail, id: %d, err: %v", req.GetId(), err)
		return rsp, errs.ErrUpdGlobalKnowledge
	}
	if _, err = s.dao.DirectUpdateVector(ctx, &retrieval.DirectUpdateVectorReq{
		Name:             model.GlobalKnowledgeGroupName,
		IndexId:          model.SearchGlobalVersionID,
		Id:               uint64(knowledge.ID),
		PageContent:      knowledge.Question,
		DocType:          model.DocTypeQA,
		EmbeddingVersion: config.App().RobotDefault.Embedding.Version,
		Labels:           nil,
	}); err != nil {
		log.ErrorContextf(ctx, "DirectUpdateVector fail, id: %d, err: %v", req.GetId(), err)
		return rsp, errs.ErrUpdGlobalKnowledge
	}
	knowledge.IsSync = true
	if err = s.dao.UpdateGlobalKnowledge(ctx, knowledge); err != nil {
		log.ErrorContextf(ctx, "UpdateGlobalKnowledge fail, id: %d, err: %v", req.GetId(), err)
		return rsp, errs.ErrUpdGlobalKnowledge
	}
	return rsp, nil
}

// ForceSyncGlobalKnowledge 强制同步全局知识库
func (s *Service) ForceSyncGlobalKnowledge(
	ctx context.Context, req *pb.ForceSyncGlobalKnowledgeReq,
) (*pb.ForceSyncGlobalKnowledgeRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.ForceSyncGlobalKnowledgeRsp)
	if s.isEmbeddingUpgrading() {
		return rsp, errs.ErrEmbeddingUpgrading
	}
	knowledge, err := s.dao.GetGlobalKnowledge(ctx, model.GlobalKnowledgeID(req.GetId()))
	if err != nil {
		log.ErrorContextf(ctx, "GetGlobalKnowledge fail, id: %d, err: %v", req.GetId(), err)
		return rsp, errs.ErrForceSyncGlobalKnowledge
	}
	if _, err = s.dao.DirectAddVector(ctx, &retrieval.DirectAddVectorReq{
		Name:             model.GlobalKnowledgeGroupName,
		IndexId:          model.SearchGlobalVersionID,
		Id:               uint64(knowledge.ID),
		PageContent:      knowledge.Question,
		DocType:          model.DocTypeQA,
		EmbeddingVersion: config.App().RobotDefault.Embedding.Version,
		Labels:           nil,
	}); err != nil {
		log.ErrorContextf(ctx, "DirectAddVector fail, knowledge: %+v, err: %v", knowledge, err)
		return rsp, errs.ErrForceSyncGlobalKnowledge
	}
	if knowledge.IsSync == true {
		return rsp, nil
	}
	knowledge.IsSync = true
	if err = s.dao.UpdateGlobalKnowledge(ctx, knowledge); err != nil {
		log.ErrorContextf(ctx, "UpdateGlobalKnowledge fail, knowledge: %+v, err: %v", knowledge, err)
		return rsp, errs.ErrForceSyncGlobalKnowledge
	}
	return rsp, nil
}

// InitGlobalKnowledge 初始化全局知识库
func (s *Service) InitGlobalKnowledge(w http.ResponseWriter, r *http.Request) {
	log.ErrorContextf(r.Context(), "准备删除的接口收到了请求 deprecated interface req:%+v", r)

	if strings.ToUpper(r.Method) != "POST" {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("only POST is allowed"))
		return
	}
	if _, err := s.dao.DirectCreateIndex(r.Context(), &retrieval.DirectCreateIndexReq{
		Name:             model.GlobalKnowledgeGroupName,
		IndexId:          model.SearchGlobalVersionID,
		EmbeddingVersion: config.App().RobotDefault.Embedding.Version,
		DocType:          model.DocTypeQA,
	}); err != nil {
		log.ErrorContextf(r.Context(), "DirectCreateIndex fail, err: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if _, err := w.Write([]byte("success")); err != nil {
		log.ErrorContextf(r.Context(), "w.Write fail, err: %v", err)
		return
	}
	log.InfoContextf(r.Context(), "InitGlobalKnowledge success")
}

func (s *Service) isEmbeddingUpgrading() bool {
	return config.App().RobotDefault.Embedding.Version != config.App().RobotDefault.Embedding.UpgradeVersion
}
