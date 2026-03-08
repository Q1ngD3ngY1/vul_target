package api

import (
	"context"
	"fmt"

	"git.code.oa.com/trpc-go/trpc-go/errs"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
)

// 权限底座调用接口

// GetCustomResource 底座获取应用资源
func (s *Service) GetCustomResource(
	ctx context.Context,
	req *pb.GetCustomResourceReq,
) (*pb.GetCustomResourceRsp, error) {
	log.DebugContextf(ctx, "GetCustomResource req:%+v", req.String())
	rsp := &pb.GetCustomResourceRsp{
		RequestId: req.GetRequestId(),
		Code:      0,
		Message:   "success",
		List:      make([]*pb.PermissionResource, 0),
	}
	si, err := s.dao.GetSystemIntegrator(ctx, model.CloudSIUin, model.CloudSISubAccountUin)
	if err != nil {
		log.ErrorContext(ctx, "GetSystemIntegrator error:", err)
		rsp.Code = int32(errs.Code(err))
		rsp.Message = err.Error()
		return rsp, err
	}
	corp, err := s.dao.GetValidCorpBySidAndUin(ctx, si.ID, req.GetOwnerUin())
	if err != nil {
		log.ErrorContext(ctx, "GetValidCorpBySidAndUin error:", err)
		rsp.Code = int32(errs.Code(err))
		rsp.Message = err.Error()
		return rsp, err
	}
	if corp == nil {
		log.ErrorContext(ctx, "request corp info not exist.")
		return rsp, nil
	}
	permissionResource := &pb.PermissionResource{
		PermissionId:    model.ListShareKnowledgePermissionID(),
		Resources:       make([]*pb.Resource, 0),
		PropertyMapping: nil,
		PropertyGroup:   nil,
	}
	pageNumber := uint32(1)
	pageSize := uint32(100)
	for {
		knowledgeList, err := s.dao.ListBaseSharedKnowledge(ctx, corp.ID, nil, pageNumber, pageSize, "", "")
		if err != nil {
			log.ErrorContext(ctx, "ListBaseSharedKnowledge error:", err)
			rsp.Code = int32(errs.Code(err))
			rsp.Message = err.Error()
			return rsp, err
		}
		for _, knowledge := range knowledgeList {
			permissionResource.Resources = append(permissionResource.Resources, &pb.Resource{
				ResourceId:   fmt.Sprintf("%d", knowledge.BusinessID),
				ResourceName: knowledge.Name,
			})
		}
		if uint32(len(knowledgeList)) != pageSize {
			break
		}
		pageNumber++
	}
	rsp.List = []*pb.PermissionResource{
		permissionResource,
	}
	return rsp, nil
}
