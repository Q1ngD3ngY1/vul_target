package rpc

import (
	"context"
	"fmt"
	"time"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	pb "git.woa.com/adp/pb-go/resource_gallery/resource_gallery"
)

type ResourceRPC interface {
	GetDefaultModelConfig(ctx context.Context, modelCategory string) (*pb.GetDefaultModelConfigRsp, error)
	GetModelInfo(ctx context.Context, corpBizId uint64, modelName string) (*pb.GetModelInfoRsp, error)
	GetModelInfoByModelName(ctx context.Context, modelNames []string) (*pb.ListProviderModelByNameRsp, error)
	GetModelAliasName(ctx context.Context, corpBizID uint64, spaceID string) (map[string]string, error)
	GetModelMapping(ctx context.Context, modelNames []string) (map[string]string, error)
	GetAllModelMapping(ctx context.Context, uin string) (map[string]string, error)
	ListCorpModel(ctx context.Context, corpBizID uint64, appType, spaceID string) (map[string]string, error)
	GetModelFreeStatus(ctx context.Context, modelNames []string, uin string, sid uint64) (*pb.ListProviderModelByNameRsp, error)
}

// GetDefaultModelConfig 获取默认模型配置
func (r *RPC) GetDefaultModelConfig(ctx context.Context, modelCategory string) (*pb.GetDefaultModelConfigRsp, error) {
	logx.I(ctx, "GetDefaultModelConfig modelCategory:%s", modelCategory)
	req := &pb.GetDefaultModelConfigReq{
		Pattern:       entity.AppStandardPattern, // 固定标准模式
		ModelCategory: modelCategory,
	}
	rsp, err := r.resource.GetDefaultModelConfig(ctx, req)
	if err != nil {
		logx.E(ctx, "GetDefaultModelConfig Failed modelCategory:%s, err:%+v", modelCategory, err)
		return nil, err
	}
	return rsp, nil
}

// GetModelInfo 获取模型信息
func (r *RPC) GetModelInfo(ctx context.Context, corpBizId uint64, modelName string) (*pb.GetModelInfoRsp, error) {
	logx.I(ctx, "GetModelInfo corpBizId:%d, modelName:%s", corpBizId, modelName)
	req := &pb.GetModelInfoReq{
		CorpBizId: corpBizId,
		ModelName: modelName,
	}
	rsp, err := r.resource.GetModelInfo(ctx, req)
	if err != nil {
		logx.E(ctx, "GetModelInfo Failed req:%+v, err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

func (r *RPC) GetModelInfoByModelName(ctx context.Context, modelNames []string) (*pb.ListProviderModelByNameRsp, error) {
	logx.I(ctx, "GetModelInfoByModelName modelNames:%+v", modelNames)
	rsp, err := r.resource.ListProviderModelByName(ctx, &pb.ListProviderModelByNameReq{
		ModelNames: modelNames,
	})
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

// GetModelFreeStatus 获取模型免费状态
func (r *RPC) GetModelFreeStatus(ctx context.Context, modelNames []string, uin string, sid uint64) (*pb.ListProviderModelByNameRsp, error) {
	logx.I(ctx, "GetModelFreeStatus modelNames:%+v", modelNames)
	rsp, err := r.resource.ListProviderModelByName(ctx, &pb.ListProviderModelByNameReq{
		ModelNames: modelNames,
		Uin:        uin,
		Sid:        sid,
	})
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func (r *RPC) GetModelAliasName(ctx context.Context, corpBizID uint64, spaceID string) (map[string]string, error) {
	rsp, err := r.resource.GetModelAliasName(ctx, &pb.GetModelAliasNameReq{
		CorpBizId: corpBizID, SpaceId: spaceID,
	})
	if err != nil {
		logx.E(ctx, "GetNonPresetModelAliasName err:%v,corpBizID:%v", err, corpBizID)
		return nil, err
	}
	customModelAliasNameMap := make(map[string]string)
	for _, v := range rsp.GetList() {
		customModelAliasNameMap[v.ModelName] = v.AliasName
	}
	return customModelAliasNameMap, nil
}

// GetModelMapping 获取模型名映射
func (r *RPC) GetModelMapping(ctx context.Context, modelNames []string) (map[string]string, error) {
	start := time.Now()
	req := &pb.GetModelMappingReq{
		ModelNames: modelNames,
		Uin:        contextx.Metadata(ctx).Uin(),
	}
	rsp, err := r.resource.GetModelMapping(ctx, req)
	if err != nil {
		logx.W(ctx, "GetModelMapping Failed req:%+v, err:%+v", req, err)
		return nil, fmt.Errorf("GetModelMapping err:%w", err)
	}

	// 将响应转换为 map，key 为原始模型名
	modelMappingMap := make(map[string]string)
	for _, v := range rsp.GetMappings() {
		modelMappingMap[v.GetOriginalModelName()] = v.GetMappedModelName()
	}
	logx.I(ctx, "GetModelMapping modelNames:%+v result:%+v,cost:%d", modelNames, modelMappingMap, time.Since(start).Milliseconds())
	return modelMappingMap, nil
}

// GetAllModelMapping 获取模型名映射
func (r *RPC) GetAllModelMapping(ctx context.Context, uin string) (map[string]string, error) {
	rsp, err := r.resource.GetModelMapping(ctx, &pb.GetModelMappingReq{Uin: uin})
	if err != nil {
		logx.E(ctx, "GetModelMapping err:%v", err)
		return nil, err
	}
	modelMappingMap := make(map[string]string)
	for _, v := range rsp.GetMappings() {
		modelMappingMap[v.GetOriginalModelName()] = v.GetMappedModelName()
	}
	return modelMappingMap, nil
}

// ListCorpModel 获取企业模型
func (r *RPC) ListCorpModel(ctx context.Context, corpBizID uint64, appType, spaceID string) (map[string]string, error) {
	rsp, err := r.resource.ListCorpModel(ctx, &pb.ListCorpModelReq{
		CorpBizId: corpBizID, AppType: appType, SpaceId: spaceID,
	})
	if err != nil {
		logx.E(ctx, "ListCorpModel err:%v,corpBizID:%v,appType:%v,spaceID:%v", err, corpBizID, appType, spaceID)
		return nil, err
	}
	res := make(map[string]string)
	for _, v := range rsp.GetList() {
		res[v.GetModelName()] = v.GetAliasName()
	}
	return res, nil
}
