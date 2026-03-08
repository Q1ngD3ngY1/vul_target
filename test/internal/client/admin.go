package client

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
)

const (
	StandardPattern = "standard" // 标准模式

	ModelCategoryGenerate  = "generate"  // 生成模型
	ModelCategoryThought   = "thought"   // 思考模型
	ModelCategoryEmbedding = "embedding" // embedding模型
	ModelCategoryRerank    = "rerank"    // rerank模型
)

// GetAppInfo 获取应用详情信息
func GetAppInfo(ctx context.Context, appBizID uint64, scenes uint32) (*admin.GetAppInfoRsp, error) {
	log.InfoContextf(ctx, "GetAppInfo scenes:%d, appBizID:%d", scenes, appBizID)
	req := &admin.GetAppInfoReq{
		AppBizId:      appBizID,
		Scenes:        scenes,
		DisablePrompt: true, // prompt通过统一sdk获取，避免GetAppInfo接口数据量太大
	}
	rsp, err := adminApiCli.GetAppInfo(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "GetAppInfo Failed,appBizID:%d,scenes:%d, err:%+v", appBizID, scenes, err)
		return nil, err
	}
	log.DebugContextf(ctx, "GetAppInfo rsp:%+v", rsp)
	return rsp, nil
}

// GetModelFinanceInfo 获取模型计费状态
func GetModelFinanceInfo(ctx context.Context, modelNames []string) (*admin.GetModelFinanceInfoRsp, error) {
	log.InfoContextf(ctx, "GetModelFinanceInfo model:%+v", modelNames)
	req := &admin.GetModelFinanceInfoReq{
		ModelNames: modelNames,
	}
	rsp, err := adminApiCli.GetModelFinanceInfo(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "GetModelFinanceInfo Failed, modelNames:%+v, err:%+v", modelNames, err)
		return nil, err
	}
	return rsp, nil
}

// StartEmbeddingUpgradeApp 开始升级应用embedding版本
func StartEmbeddingUpgradeApp(ctx context.Context, appBizID, fromVersion, toVersion uint64) error {
	log.InfoContextf(ctx, "StartEmbeddingUpgradeApp appBizID:%d, fromVersion:%d, toVersion:%d",
		appBizID, fromVersion, toVersion)
	req := &admin.StartEmbeddingUpgradeAppReq{
		AppBizId:             appBizID,
		FromEmbeddingVersion: fromVersion,
		ToEmbeddingVersion:   toVersion,
	}
	_, err := adminApiCli.StartEmbeddingUpgradeApp(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "StartEmbeddingUpgradeApp Failed, appBizID:%d, fromVersion:%d, toVersion:%d, err:%+v",
			appBizID, fromVersion, toVersion, err)
		return err
	}
	return nil
}

// FinishEmbeddingUpgradeApp 完成升级应用embedding版本
func FinishEmbeddingUpgradeApp(ctx context.Context, appBizID, fromVersion, toVersion uint64) error {
	log.InfoContextf(ctx, "FinishEmbeddingUpgradeApp appBizID:%d, fromVersion:%d, toVersion:%d",
		appBizID, fromVersion, toVersion)
	req := &admin.FinishEmbeddingUpgradeAppReq{
		AppBizId:             appBizID,
		FromEmbeddingVersion: fromVersion,
		ToEmbeddingVersion:   toVersion,
	}
	_, err := adminApiCli.FinishEmbeddingUpgradeApp(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "FinishEmbeddingUpgradeApp Failed, appBizID:%d, fromVersion:%d, toVersion:%d, err:%+v",
			appBizID, fromVersion, toVersion, err)
		return err
	}
	return nil
}

// GetAppsByBizIDs 根据appBizIDs获取应用信息
func GetAppsByBizIDs(ctx context.Context, appBizIDs []uint64, scenes uint32) (*admin.GetAppsByBizIDsRsp, error) {
	log.InfoContextf(ctx, "GetAppsByBizIDsReq appBizIDs:%+v", appBizIDs)
	req := &admin.GetAppsByBizIDsReq{
		AppBizIds: appBizIDs,
		Scenes:    scenes,
	}
	rsp, err := adminApiCli.GetAppsByBizIDs(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "GetAppsByBizIDsReq Failed, appBizIDs:%+v, err:%+v", appBizIDs, err)
		return nil, err
	}
	return rsp, nil
}

// CreateShareKnowledgeBaseApp 创建共享知识库应用
func CreateShareKnowledgeBaseApp(ctx context.Context, uin, name, avatar, spaceID string) (
	*admin.CreateShareKnowledgeBaseAppRsp, error) {
	log.InfoContextf(ctx, "CreateShareKnowledgeBaseApp, uin: %s, name: %s, avatar: %s",
		uin, name, avatar)

	req := &admin.CreateShareKnowledgeBaseAppReq{
		Uin:     uin,
		Name:    name,
		Avatar:  avatar,
		SpaceId: spaceID,
	}
	rsp, err := adminApiCli.CreateShareKnowledgeBaseApp(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "CreateShareKnowledgeBaseApp failed, request: %+v, error: %+v",
			req, err)
		return nil, err
	}

	return rsp, nil
}

// DeleteShareKnowledgeBaseApp 删除共享知识库应用
func DeleteShareKnowledgeBaseApp(ctx context.Context, uin string, appBizID uint64) (
	*admin.DeleteShareKnowledgeBaseAppRsp, error) {
	log.InfoContextf(ctx, "DeleteShareKnowledgeBaseApp, uin: %s, appBizID: %d", uin, appBizID)

	req := &admin.DeleteShareKnowledgeBaseAppReq{
		Uin:      uin,
		AppBizId: appBizID,
	}
	rsp, err := adminApiCli.DeleteShareKnowledgeBaseApp(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteShareKnowledgeBaseApp failed, request: %+v, error: %+v",
			req, err)
		return nil, err
	}

	return rsp, nil
}

// GetCorpStaffName 查询用户昵称
func GetCorpStaffName(ctx context.Context, uin, subAccountUin string) (
	*admin.GetCorpStaffNameRsp, error) {
	log.InfoContextf(ctx, "GetCorpStaffName, uin: %s, subAccountUin: %s", uin, subAccountUin)

	req := &admin.GetCorpStaffNameReq{
		Uin:           uin,
		SubAccountUin: subAccountUin,
	}
	rsp, err := adminApiCli.GetCorpStaffName(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "GetCorpStaffName failed, request: %+v, error: %+v",
			req, err)
		return nil, err
	}

	return rsp, nil
}

// ListCorpStaffByIds 查询员工名称
func ListCorpStaffByIds(ctx context.Context, corpBizID uint64, staffIDs []uint64) (
	map[uint64]string, error) {
	staffIDs = slicex.Unique(staffIDs)
	staffByID := make(map[uint64]string)
	if len(staffIDs) == 0 || (len(staffIDs) == 1 && staffIDs[0] == 0) {
		return staffByID, nil
	}
	log.DebugContextf(ctx, "ListCorpStaffByIds,corpBizID:%v,staffIDs:%v", corpBizID, staffIDs)
	req := &admin.ListCorpStaffByIdsReq{
		CorpBizId: corpBizID,
		Page:      1,
		PageSize:  uint32(len(staffIDs)),
		StaffIds:  staffIDs,
	}
	rsp, err := adminApiCli.ListCorpStaffByIds(ctx, req)
	if err != nil || rsp == nil {
		log.ErrorContextf(ctx, "ListCorpStaffByIds failed,req:%+v,error:%+v",
			req, err)
		return nil, err
	}
	for _, v := range rsp.List {
		staffByID[v.Id] = v.NickName
	}
	return staffByID, nil
}

// GetDefaultModelConfig 获取默认模型配置
func GetDefaultModelConfig(ctx context.Context, modelCategory string) (*admin.GetDefaultModelConfigRsp, error) {
	log.InfoContextf(ctx, "GetDefaultModelConfig modelCategory:%s", modelCategory)
	req := &admin.GetDefaultModelConfigReq{
		Pattern:       StandardPattern, // 固定标准模式
		ModelCategory: modelCategory,
	}
	rsp, err := adminApiCli.GetDefaultModelConfig(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "GetDefaultModelConfig Failed modelCategory:%s, err:%+v", modelCategory, err)
		return nil, err
	}
	return rsp, nil
}

// GetModelInfo 获取模型信息
func GetModelInfo(ctx context.Context, corpId uint64, modelName string) (*admin.GetModelInfoRsp, error) {
	log.InfoContextf(ctx, "GetModelInfo corpId:%d, modelName:%s", corpId, modelName)
	req := &admin.GetModelInfoReq{
		CorpId:    corpId,
		ModelName: modelName,
	}
	rsp, err := adminApiCli.GetModelInfo(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "GetModelInfo Failed req:%+v, err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}
