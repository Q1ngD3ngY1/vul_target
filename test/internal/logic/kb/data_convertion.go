package kb

import (
	"context"
	"strconv"

	"git.woa.com/adp/common/x/logx"
	entity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

const (
	// SharedKnowledgeAppAvatar 应用Avatar
	// TODO: 调整到业务配置
	SharedKnowledgeAppAvatar = "https://cdn.xiaowei.qq.com/static/lke/app-icon-knowledge_qa.png"

	// SharedKnowledgeMaxPageSize 知识库清单分页大小
	SharedKnowledgeMaxPageSize = 200
)

// ConvertSharedKnowledgeBaseInfo 转换共享知识库基础信息
func ConvertSharedKnowledgeBaseInfo(ctx context.Context,
	dbInfo *entity.SharedKnowledgeInfo) (*pb.KnowledgeBaseInfo, *pb.UserBaseInfo) {

	return &pb.KnowledgeBaseInfo{
			KnowledgeBizId:       dbInfo.BusinessID,
			KnowledgeName:        dbInfo.Name,
			KnowledgeDescription: dbInfo.Description,

			EmbeddingModel:       dbInfo.EmbeddingModel,
			QaExtractModel:       dbInfo.QaExtractModel,
			KnowledgeSchemaModel: dbInfo.KnowledgeSchemaModel,
			OwnerStaffId:         strconv.FormatUint(dbInfo.OwnerStaffID, 10),
			OwnerStaffName:       dbInfo.OwnerStaffName,

			UpdateTime: dbInfo.UpdateTime.Unix(),
		},
		&pb.UserBaseInfo{
			UserBizId: dbInfo.UserBizID,
			UserName:  dbInfo.UserName,
		}
}

// ConvertSharedKnowledgeAppInfo 转换共享知识库应用信息
func ConvertSharedKnowledgeAppInfo(ctx context.Context, rpc *rpc.RPC, sharedKGAppList []*entity.AppShareKnowledge) ([]*pb.AppBaseInfo, error) {
	appNameMap, err := BatchGetAppName(ctx, rpc, sharedKGAppList)
	if err != nil {
		return nil, err
	}

	var appList []*pb.AppBaseInfo
	for _, sharedKGApp := range sharedKGAppList {
		name, ok := appNameMap[sharedKGApp.AppBizID]
		if !ok {
			logx.W(ctx, "ConvertSharedKnowledgeAppInfo, appInfo not found, sharedKGApp: %+v", sharedKGApp)
			continue
		}
		appList = append(appList, &pb.AppBaseInfo{
			AppBizId: sharedKGApp.AppBizID,
			AppName:  name,
		})
	}

	return appList, nil
}

// GenerateSharedKnowledgeDetailList 生成共享知识库详情清单
func GenerateSharedKnowledgeDetailList(ctx context.Context,
	knowledgeList []*entity.SharedKnowledgeInfo,
	knowledgeAppMap map[uint64][]*pb.AppBaseInfo,
	otherAllPermissionIDs []string, mapShareKnowledgeBizIDs map[uint64][]string) []*pb.KnowledgeDetailInfo {

	detailList := make([]*pb.KnowledgeDetailInfo, 0)
	for _, item := range knowledgeList {
		knowledge, user := ConvertSharedKnowledgeBaseInfo(ctx, item)
		appList, ok := knowledgeAppMap[item.BusinessID]
		if !ok {
			appList = nil
		}
		permissionIDs := otherAllPermissionIDs
		if promptTemplatePermissionIDs, ok := mapShareKnowledgeBizIDs[item.BusinessID]; ok {
			permissionIDs = promptTemplatePermissionIDs
		}
		detailList = append(detailList, &pb.KnowledgeDetailInfo{
			Knowledge:     knowledge,
			AppList:       appList,
			User:          user,
			PermissionIds: permissionIDs,
		})
	}
	logx.I(ctx, "GenerateSharedKnowledgeDetailList, knowledgeList.size: %d, "+
		"knowledgeAppMap.size: %d, detailList.size: %d",
		len(knowledgeList), len(knowledgeAppMap), len(detailList))
	return detailList
}

// ConvertAppBySharedKnowledge 转换共享知识库应用清单
func ConvertAppBySharedKnowledge(ctx context.Context, rpc *rpc.RPC,
	sharedKGAppList []*entity.AppShareKnowledge) (map[uint64][]*pb.AppBaseInfo, error) {
	logx.I(ctx, "ConvertAppBySharedKnowledge, sharedKGAppList(%d): %+v",
		len(sharedKGAppList), sharedKGAppList)
	knowledgeAppMap := make(map[uint64][]*pb.AppBaseInfo)

	appNameMap, err := BatchGetAppName(ctx, rpc, sharedKGAppList)
	if err != nil {
		return nil, err
	}
	if appNameMap == nil || len(appNameMap) == 0 {
		return knowledgeAppMap, nil
	}

	for _, sharedKGApp := range sharedKGAppList {
		name, ok := appNameMap[sharedKGApp.AppBizID]
		if !ok {
			logx.W(ctx, "ConvertAppBySharedKnowledge, appInfo not found, sharedKGApp: %+v", sharedKGApp)
			continue
		}

		appList, ok := knowledgeAppMap[sharedKGApp.KnowledgeBizID]
		if !ok {
			appList = make([]*pb.AppBaseInfo, 0)
		}

		knowledgeAppMap[sharedKGApp.KnowledgeBizID] = append(appList, &pb.AppBaseInfo{
			AppBizId: sharedKGApp.AppBizID,
			AppName:  name,
		})
	}

	return knowledgeAppMap, nil
}

// GenerateSharedKnowledgeAppName 生成应用名称
func GenerateSharedKnowledgeAppName(req *pb.CreateSharedKnowledgeReq) string {
	// return fmt.Sprintf("%s-%s", SharedKnowledgeAppPrefix, req.GetKnowledgeName())
	return req.GetKnowledgeName()
}
