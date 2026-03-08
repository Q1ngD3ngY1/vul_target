package rpc

import (
	"context"
	"fmt"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/timex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/app/app_config"
)

type AppAdminRPC interface {
	DescribeAppList(ctx context.Context, req *pb.GetAppListReq) ([]*entity.App, uint64, error)
	DescribeAppById(ctx context.Context, appBizID uint64) (*entity.App, error)
	DescribeAppByPrimaryId(ctx context.Context, appPrimaryId uint64) (*entity.App, error)
	DescribeAppInfoUsingScenesById(context.Context, uint64, uint32) (*entity.App, error)
	DescribeAppByPrimaryIdWithoutNotFoundError(ctx context.Context, primaryId uint64) (*entity.App, error)
	DescribeApp(ctx context.Context, req *pb.DescribeAppReq) (*pb.DescribeAppRsp, error)
	CountApp(ctx context.Context, req *pb.CountAppReq) (uint64, error)
	ModifyApp(ctx context.Context, req *pb.ModifyAppReq) (*pb.ModifyAppRsp, error)
	GetAppBaseInfo(ctx context.Context, appBizId uint64) (*entity.AppBaseInfo, error)
	GetAppBaseInfoByPrimaryId(ctx context.Context, appPrimaryId uint64) (*entity.AppBaseInfo, error)
	// ListAppBaseInfo 是 DescribeAppList 的简化版，只返回应用的基础信息
	ListAppBaseInfo(ctx context.Context, req *pb.ListAppBaseInfoReq) ([]*entity.AppBaseInfo, uint64, error)
	ListAllAppBaseInfo(ctx context.Context, req *pb.ListAppBaseInfoReq) ([]*entity.AppBaseInfo, uint64, error)

	DescribeSynonymsList(ctx context.Context, req *pb.DescribeSynonymsListReq) (*pb.DescribeSynonymsListRsp, error)
	CreateSynonyms(ctx context.Context, req *pb.CreateSynonymsReq) (*pb.CreateSynonymsRsp, error)
	DeleteSynonyms(ctx context.Context, req *pb.DeleteSynonymsReq) (*pb.DeleteSynonymsRsp, error)
	ModifySynonyms(ctx context.Context, req *pb.ModifySynonymsReq) (*pb.ModifySynonymsRsp, error)
	UploadSynonymsList(ctx context.Context, req *pb.UploadSynonymsListReq) (*pb.UploadSynonymsListRsp, error)
	ExportSynonymsList(ctx context.Context, req *pb.ExportSynonymsListReq) (*pb.ExportSynonymsListRsp, error)
	DescribeSynonymsCateList(ctx context.Context, req *pb.DescribeCateListReq) (*pb.DescribeCateListRsp, error)
	CreateSynonymsCate(ctx context.Context, req *pb.CreateCateReq) (*pb.CreateCateRsp, error)
	ModifySynonymsCate(ctx context.Context, req *pb.ModifyCateReq) (*pb.ModifyCateRsp, error)
	DeleteSynonymsCate(ctx context.Context, req *pb.DeleteCateReq) (*pb.DeleteCateRsp, error)
	GroupSynonyms(ctx context.Context, req *pb.GroupObjectReq) (*pb.GroupObjectRsp, error)
	SynonymsNER(ctx context.Context, req *pb.SynonymsNERReq) (*pb.SynonymsNERRsp, error)

	DescribeUnsatisfiedReplyList(ctx context.Context, req *pb.DescribeUnsatisfiedReplyListReq) (rsp *pb.DescribeUnsatisfiedReplyListRsp, err error)
	ModifyUnsatisfiedReply(ctx context.Context, req *pb.ModifyUnsatisfiedReplyReq) (rsp *pb.ModifyUnsatisfiedReplyRsp, err error)
	IgnoreUnsatisfiedReply(ctx context.Context, req *pb.IgnoreUnsatisfiedReplyReq) (rsp *pb.IgnoreUnsatisfiedReplyRsp, err error)
	ExportUnsatisfiedReply(ctx context.Context, req *pb.ExportUnsatisfiedReplyReq) (rsp *pb.ExportUnsatisfiedReplyRsp, err error)
	DescribeUnsatisfiedReplyContext(ctx context.Context, req *pb.DescribeUnsatisfiedReplyContextReq) (rsp *pb.DescribeUnsatisfiedReplyContextRsp, err error)
	AddUnsatisfiedReply(ctx context.Context, req *pb.AddUnsatisfiedReplyReq) (rsp *pb.AddUnsatisfiedReplyRsp, err error)
	DescribeLikeDataCount(ctx context.Context, req *pb.DescribeLikeDataCountReq) (rsp *pb.DescribeLikeDataCountRsp, err error)
	DescribeAnswerTypeDataCount(ctx context.Context, req *pb.DescribeAnswerTypeDataCountReq) (rsp *pb.DescribeAnswerTypeDataCountRsp, err error)
	DescribeRobotConfigHistory(ctx context.Context, req *pb.GetRobotConfigByVersionIDReq) (rsp *pb.GetRobotConfigByVersionIDRsp, err error)
	ModifyRobotConfigHistory(ctx context.Context, req *pb.ModifyRobotConfigByVersionReq) (rsp *pb.ModifyRobotConfigByVersionRsp, err error)
	ReleaseNotify(ctx context.Context, req *pb.ReleaseNotifyReq) (rsp *pb.ReleaseNotifyRsp, err error)
	SetAppUsage(ctx context.Context, usage entity.CapacityUsage, appID uint64) error
	UpdateAppUsage(ctx context.Context, usage entity.CapacityUsage, appID uint64) error
	DescribeLatestRelease(ctx context.Context, appID uint64) (*pb.DescribeLatestReleaseRsp, error)
	DescribeAppByPrimaryIdOrBizIdList(ctx context.Context, corpPrimaryId uint64, ids []uint64) (map[uint64]uint64, error)

	ClearAppResourceCallback(ctx context.Context, req *pb.ClearAppResourceCallbackReq) (*pb.ClearAppResourceCallbackRsp, error)
	CreateShareKnowledgeBaseApp(ctx context.Context, uin, name, avatar, spaceID string) (*pb.CreateShareKnowledgeBaseAppRsp, error)
	DeleteShareKnowledgeBaseApp(ctx context.Context, uin string, appBizID uint64) (*pb.DeleteShareKnowledgeBaseAppRsp, error)
	CountCorpAppCharSize(ctx context.Context, corpPrimaryID uint64) (uint64, error)
	StartEmbeddingUpgradeApp(ctx context.Context, req *pb.StartEmbeddingUpgradeAppReq) (*pb.StartEmbeddingUpgradeAppRsp, error)
	FinishEmbeddingUpgradeApp(ctx context.Context, req *pb.FinishEmbeddingUpgradeAppReq) (*pb.FinishEmbeddingUpgradeAppRsp, error)
	ImportExportComponentCallback(ctx context.Context, req *pb.ImportExportComponentCallbackReq) (*pb.ImportExportComponentCallbackRsp, error)
}

// AppApiRPC 应用 API RPC
type AppApiRPC interface {
	// DescribeCorpKnowledgeCapacity 查询企业知识库容量使用情况
	DescribeCorpKnowledgeCapacity(ctx context.Context, corpBizID uint64, knowledgeBaseIDs []uint64) (entity.CapacityUsage, error)
}

// DescribeCorpKnowledgeCapacity 查询企业下知识库容量使用情况
func (r *RPC) DescribeCorpKnowledgeCapacity(ctx context.Context, corpBizID uint64, knowledgeBaseIDs []uint64) (entity.CapacityUsage, error) {
	req := &pb.DescribeCorpKnowledgeCapacityReq{
		CorpBizId: corpBizID,
	}
	if len(knowledgeBaseIDs) > 0 {
		req.KnowledgeBaseIds = knowledgeBaseIDs
	}
	rsp, err := r.appApi.DescribeCorpKnowledgeCapacity(ctx, req)
	if err != nil {
		logx.E(ctx, "DescribeCorpKnowledgeCapacity failed, corpBizID:%d err:%+v", corpBizID, err)
		return entity.CapacityUsage{}, err
	}
	return entity.CapacityUsage{
		CharSize:          int64(rsp.GetUsedCharSize()),
		StorageCapacity:   int64(rsp.GetUsedStorageCapacity()),
		ComputeCapacity:   int64(rsp.GetUsedComputeCapacity()),
		KnowledgeCapacity: int64(rsp.GetUsedKnowledgeCapacity()),
	}, nil
}

// DescribeKnowledgeCapacityByKBID 查询制定知识库的容量使用情况
func (r *RPC) DescribeKnowledgeCapacityByKBID(ctx context.Context, corpBizID uint64, knowledgeBaseID uint64) (entity.CapacityUsage, error) {
	req := &pb.DescribeCorpKnowledgeCapacityReq{
		CorpBizId:        corpBizID,
		KnowledgeBaseIds: []uint64{knowledgeBaseID},
	}
	rsp, err := r.appApi.DescribeCorpKnowledgeCapacity(ctx, req)
	if err != nil {
		logx.E(ctx, "DescribeCorpKnowledgeCapacity failed, corpBizID:%d err:%+v", corpBizID, err)
		return entity.CapacityUsage{}, err
	}
	return entity.CapacityUsage{
		CharSize:          int64(rsp.GetUsedCharSize()),
		StorageCapacity:   int64(rsp.GetUsedStorageCapacity()),
		ComputeCapacity:   int64(rsp.GetUsedComputeCapacity()),
		KnowledgeCapacity: int64(rsp.GetUsedKnowledgeCapacity()),
	}, nil
}

func appInfoPB2DO(v *pb.GetAppInfoRsp) *entity.App {
	if v == nil {
		return nil
	}
	knowledgeQaConfig := &entity.KnowledgeQaConfig{
		Greeting:            v.GetKnowledgeQa().GetGreeting(),
		RoleDescription:     v.GetKnowledgeQa().GetRoleDescription(),
		Method:              v.GetKnowledgeQa().GetOutput().GetMethod(),
		UseGeneralKnowledge: v.GetKnowledgeQa().GetOutput().GetUseGeneralKnowledge(),
		BareAnswer:          v.GetKnowledgeQa().GetOutput().GetBareAnswer(),
		ReplyFlexibility:    v.GetKnowledgeQa().GetReplyFlexibility(),
		ShowSearchEngine:    v.GetKnowledgeQa().GetShowSearchEngine(),
		EnableRerank:        v.GetKnowledgeQa().GetEnableRerank(),
		AdvancedConfig: &entity.AdvancedConfig{
			RerankModel:     v.GetKnowledgeQa().GetKnowledgeAdvancedConfig().GetRerankModel(),
			RerankRecallNum: v.GetKnowledgeQa().GetKnowledgeAdvancedConfig().GetRerankRecallNum(),
		},
	}
	knowledgeQaConfig.Model = make(map[string]config.AppModelDetail)
	for k, model := range v.GetKnowledgeQa().GetModel() {
		knowledgeQaConfig.Model[k] = config.AppModelDetail{
			Prompt:            model.GetPrompt(),
			Path:              model.GetPath(),
			PromptWordsLimit:  model.GetPromptWordsLimit(),
			Target:            model.GetTarget(),
			Type:              model.GetType(),
			HistoryLimit:      model.GetHistoryLimit(),
			HistoryWordsLimit: model.GetHistoryWordsLimit(),
			ModelName:         model.GetModelName(),
			ServiceName:       model.GetServiceName(),
			IsEnabled:         model.GetIsEnabled(),
			PromptVersion:     model.GetPromptVersion(),
		}
	}
	knowledgeQaConfig.Filters = make(map[string]config.RobotFilter)
	for k, filters := range v.GetKnowledgeQa().GetFilters() {
		var filterList []config.RobotFilterDetail
		for _, filter := range filters.GetFilter() {
			filterList = append(filterList, config.RobotFilterDetail{
				DocType:    filter.GetDocType(),
				IndexID:    filter.GetIndexId(),
				Confidence: filter.GetConfidence(),
				TopN:       filter.GetTopN(),
				IsEnabled:  filter.GetIsEnable(),
			})
		}
		knowledgeQaConfig.Filters[k] = config.RobotFilter{
			TopN:   filters.GetTopN(),
			Filter: filterList,
		}
	}
	knowledgeQaConfig.SearchRange = &entity.SearchRange{
		Condition: v.GetKnowledgeQa().GetSearchRange().GetCondition(),
		APIVarMap: v.GetKnowledgeQa().GetSearchRange().GetApiVarMap(),
	}
	knowledgeQaConfig.SearchStrategy = &entity.SearchStrategy{
		StrategyType:      uint32(v.GetKnowledgeQa().GetSearchStrategy().GetStrategyType()),
		TableEnhancement:  v.GetKnowledgeQa().GetSearchStrategy().GetTableEnhancement(),
		EmbeddingModel:    v.GetKnowledgeQa().GetSearchStrategy().GetEmbeddingModel(),
		RerankModelSwitch: v.GetKnowledgeQa().GetSearchStrategy().GetRerankModelSwitch(),
		RerankModel:       v.GetKnowledgeQa().GetSearchStrategy().GetRerankModel(),
	}
	for _, attrInfo := range v.GetKnowledgeQa().GetSearchRange().GetApiVarAttrInfos() {
		knowledgeQaConfig.SearchRange.ApiVarAttrInfos = append(knowledgeQaConfig.SearchRange.ApiVarAttrInfos,
			entity.ApiVarAttrInfo{
				ApiVarID:  attrInfo.GetApiVarId(),
				AttrBizID: attrInfo.GetAttrBizId(),
			})
	}
	shareKnowledgeBases := make(map[uint64]*entity.App)
	for _, shareKb := range v.GetKnowledgeQa().GetShareKnowledgeBases() {
		var shareKbApiVarAttrInfos []entity.ApiVarAttrInfo
		for _, attrInfo := range shareKb.GetSearchRange().GetApiVarAttrInfos() {
			shareKbApiVarAttrInfos = append(shareKbApiVarAttrInfos,
				entity.ApiVarAttrInfo{
					ApiVarID:  attrInfo.GetApiVarId(),
					AttrBizID: attrInfo.GetAttrBizId(),
				})
		}
		shareKbFilters := make(map[string]config.RobotFilter)
		for k, filters := range shareKb.GetFilters() {
			var filterList []config.RobotFilterDetail
			for _, filter := range filters.GetFilter() {
				filterList = append(filterList, config.RobotFilterDetail{
					DocType:    filter.GetDocType(),
					IndexID:    filter.GetIndexId(),
					Confidence: filter.GetConfidence(),
					TopN:       filter.GetTopN(),
					IsEnabled:  filter.GetIsEnable(),
				})
			}
			shareKbFilters[k] = config.RobotFilter{
				TopN:   filters.GetTopN(),
				Filter: filterList,
			}
		}
		shareKnowledgeBases[shareKb.GetKnowledgeBizId()] = &entity.App{
			BizId: shareKb.GetKnowledgeBizId(),
			Name:  shareKb.GetKnowledgeName(),
			QaConfig: &entity.KnowledgeQaConfig{
				SearchRange: &entity.SearchRange{
					Condition:       shareKb.GetSearchRange().GetCondition(),
					APIVarMap:       shareKb.GetSearchRange().GetApiVarMap(),
					ApiVarAttrInfos: shareKbApiVarAttrInfos,
				},
				SearchStrategy: &entity.SearchStrategy{
					StrategyType:      uint32(shareKb.GetSearchStrategy().GetStrategyType()),
					TableEnhancement:  shareKb.GetSearchStrategy().GetTableEnhancement(),
					EmbeddingModel:    shareKb.GetSearchStrategy().GetEmbeddingModel(),
					RerankModelSwitch: shareKb.GetSearchStrategy().GetRerankModelSwitch(),
					RerankModel:       shareKb.GetSearchStrategy().GetRerankModel(),
				},
				Filters: shareKbFilters,
			},
		}
	}
	return &entity.App{
		PrimaryId:              v.GetId(),
		AppKey:                 v.GetAppKey(),
		BizId:                  v.GetAppBizId(),
		CorpPrimaryId:          v.GetCorpId(),
		CorpBizId:              v.GetCorpBizId(),
		AppType:                v.GetAppType(),
		AppStatus:              uint32(v.GetStatus()),
		Name:                   v.GetBaseConfig().GetName(),
		NameInAudit:            v.GetNameInAudit(),
		Avatar:                 v.GetBaseConfig().GetAvatar(),
		AvatarInAudit:          v.GetAvatarInAudit(),
		Description:            v.GetBaseConfig().GetDesc(),
		RoleDescription:        v.GetKnowledgeQa().GetRoleDescription(),
		RoleDescriptionInAudit: v.GetKnowledgeQa().GetRoleDescriptionInAudit(),
		Greeting:               v.GetKnowledgeQa().GetGreeting(),
		GreetingInAudit:        v.GetKnowledgeQa().GetGreetingInAudit(),
		Embedding: &config.RobotEmbedding{
			Version:        v.GetKnowledgeQa().GetEmbedding().GetVersion(),
			UpgradeVersion: v.GetKnowledgeQa().GetEmbedding().GetUpgradeVersion(),
		},
		QaVersion:           v.GetKnowledgeQa().GetQaVersion(),
		UsedCharSize:        gox.IfElse(v.GetUsedCharSize() > 0, uint64(v.GetUsedCharSize()), uint64(0)),
		IsDeleted:           v.GetIsDelete(),
		BareAnswer:          v.GetKnowledgeQa().GetOutput().GetBareAnswer(),
		BareAnswerInAudit:   v.GetBareAnswerInAudit(),
		CreateTime:          timex.Unix(v.GetCreateTime()),
		UpdateTime:          timex.Unix(v.GetUpdateTime()),
		StaffID:             v.GetStaffId(),
		InfosecBizType:      v.GetInfosecBizType(),
		IsShared:            v.GetIsShareKnowledgeBase(),
		Uin:                 v.GetUin(),
		QaConfig:            knowledgeQaConfig,
		SpaceId:             v.GetSpaceId(),
		ShareKnowledgeBases: shareKnowledgeBases,
	}
}

func appInfoListPB2DO(apps []*pb.GetAppListRsp_AppInfo) []*entity.App {
	if apps == nil {
		return nil
	}
	list := make([]*entity.App, 0, len(apps))
	for _, v := range apps {
		list = append(list, &entity.App{
			PrimaryId:     v.GetId(),
			BizId:         v.GetAppBizId(),
			CorpPrimaryId: v.GetCorpId(),
			IsShared:      v.GetIsShareKnowledgeBase(),
			Name:          v.GetName(),
			Embedding: &config.RobotEmbedding{
				Version:        v.GetKnowledgeQa().GetEmbedding().GetVersion(),
				UpgradeVersion: v.GetKnowledgeQa().GetEmbedding().GetUpgradeVersion(),
			},
		})
	}
	return list
}

// CountApp 统计应用数量
func (r *RPC) CountApp(ctx context.Context, req *pb.CountAppReq) (uint64, error) {
	rsp, err := r.app.CountApp(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("CountApp err: %w", err)
	}
	return rsp.GetCount(), nil
}

// DescribeAppByPrimaryIdOrBizIdList ids 可以是主键ID，也可以是业务ID
func (r *RPC) DescribeAppByPrimaryIdOrBizIdList(ctx context.Context, corpPrimaryId uint64, ids []uint64) (map[uint64]uint64, error) {
	req := &pb.GetAppListReq{
		Page:          1,
		PageSize:      uint32(len(ids)),
		CorpPrimaryId: corpPrimaryId,
		BotBizIds:     ids,
		DeleteFlag:    0,
		DisablePrompt: true,
	}
	rsp, err := r.app.GetAppList(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("GetAppList err: %w", err)
	}
	appIdMap := make(map[uint64]uint64, len(rsp.GetList()))
	for _, app := range rsp.GetList() {
		appIdMap[app.GetId()] = app.GetAppBizId()
	}
	return appIdMap, nil
}

// DescribeAppList 获取应用详细信息
// 通常不用此接口，常规字段信息建议使用 ListAppBaseInfo
func (r *RPC) DescribeAppList(ctx context.Context, req *pb.GetAppListReq) ([]*entity.App, uint64, error) {
	if req == nil {
		req = &pb.GetAppListReq{}
	}
	req.DeleteFlag = 1 // 未删除

	rsp, err := r.app.GetAppList(ctx, req)
	if err != nil {
		return nil, 0, fmt.Errorf("DescribeAppList err: %w", err)
	}
	return appInfoListPB2DO(rsp.GetList()), uint64(rsp.GetTotal()), nil
}

// ListAllAppBaseInfo 获取企业下所有应用的基础信息
func (r *RPC) ListAllAppBaseInfo(ctx context.Context, req *pb.ListAppBaseInfoReq) ([]*entity.AppBaseInfo, uint64, error) {
	if req == nil {
		req = &pb.ListAppBaseInfoReq{}
	}
	var (
		page     = 1
		pageSize = 500
		allApps  []*entity.AppBaseInfo
		total    uint64
	)

	for {
		req.PageNumber = uint32(page)
		req.PageSize = uint32(pageSize)
		rsp, err := r.app.ListAppBaseInfo(ctx, req)
		if err != nil {
			return nil, 0, fmt.Errorf("ListAllAppBaseInfo app.GetAppList err: %w", err)
		}
		total = uint64(rsp.GetTotal())
		apps := appBaseInfoListPB2DO(rsp.GetAppBaseInfos())
		allApps = append(allApps, apps...)
		if len(rsp.GetAppBaseInfos()) < pageSize {
			break
		}
		page++
	}
	logx.I(ctx, "ListAllAppBaseInfo req:%s, rsp total:%d", req, total)
	return allApps, total, nil
}

func (r *RPC) DescribeAppById(ctx context.Context, appBizID uint64) (*entity.App, error) {
	return r.DescribeAppInfoUsingScenesById(ctx, appBizID, entity.AppTestScenes)
}

func (r *RPC) DescribeAppByPrimaryId(ctx context.Context, appPrimaryID uint64) (*entity.App, error) {
	req := &pb.GetAppInfoReq{
		AppPrimaryId:  appPrimaryID,
		Scenes:        entity.AppTestScenes,
		DisablePrompt: true,
	}
	rsp, err := r.app.GetAppInfo(ctx, req)
	if err != nil {
		logx.E(ctx, "DescribeAppInfoUsingScenesById err: %+v", err)
		return nil, err
	}
	return appInfoPB2DO(rsp), nil
}

func (r *RPC) DescribeAppInfoUsingScenesById(ctx context.Context, appBizID uint64, scenes uint32) (*entity.App, error) {
	req := &pb.GetAppInfoReq{
		AppBizId:      appBizID,
		Scenes:        scenes,
		DisablePrompt: true,
	}
	rsp, err := r.app.GetAppInfo(ctx, req)
	if err != nil {
		logx.E(ctx, "DescribeAppInfoUsingScenesById err: %+v", err)
		return nil, err
	}
	return appInfoPB2DO(rsp), nil
}

// ModifyApp 修改应用信息
func (r *RPC) ModifyApp(ctx context.Context, req *pb.ModifyAppReq) (*pb.ModifyAppRsp, error) {
	return r.app.ModifyApp(ctx, req)
}

func (r *RPC) DescribeSynonymsList(ctx context.Context, req *pb.DescribeSynonymsListReq) (*pb.DescribeSynonymsListRsp, error) {
	return r.app.DescribeSynonymsList(ctx, req)
}
func (r *RPC) CreateSynonyms(ctx context.Context, req *pb.CreateSynonymsReq) (*pb.CreateSynonymsRsp, error) {
	return r.app.CreateSynonyms(ctx, req)
}

func (r *RPC) DeleteSynonyms(ctx context.Context, req *pb.DeleteSynonymsReq) (*pb.DeleteSynonymsRsp, error) {
	return r.app.DeleteSynonyms(ctx, req)
}

func (r *RPC) ModifySynonyms(ctx context.Context, req *pb.ModifySynonymsReq) (*pb.ModifySynonymsRsp, error) {
	return r.app.ModifySynonyms(ctx, req)
}

func (r *RPC) UploadSynonymsList(ctx context.Context, req *pb.UploadSynonymsListReq) (*pb.UploadSynonymsListRsp, error) {
	return r.app.UploadSynonymsList(ctx, req)
}

func (r *RPC) ExportSynonymsList(ctx context.Context, req *pb.ExportSynonymsListReq) (*pb.ExportSynonymsListRsp, error) {
	return r.app.ExportSynonymsList(ctx, req)
}

func (r *RPC) DescribeSynonymsCateList(ctx context.Context, req *pb.DescribeCateListReq) (*pb.DescribeCateListRsp, error) {
	return r.app.DescribeSynonymsCateList(ctx, req)
}

func (r *RPC) CreateSynonymsCate(ctx context.Context, req *pb.CreateCateReq) (*pb.CreateCateRsp, error) {
	return r.app.CreateSynonymsCate(ctx, req)
}

func (r *RPC) ModifySynonymsCate(ctx context.Context, req *pb.ModifyCateReq) (*pb.ModifyCateRsp, error) {
	return r.app.ModifySynonymsCate(ctx, req)
}

func (r *RPC) DeleteSynonymsCate(ctx context.Context, req *pb.DeleteCateReq) (*pb.DeleteCateRsp, error) {
	return r.app.DeleteSynonymsCate(ctx, req)
}

func (r *RPC) GroupSynonyms(ctx context.Context, req *pb.GroupObjectReq) (*pb.GroupObjectRsp, error) {
	return r.app.GroupSynonyms(ctx, req)
}

func (r *RPC) SynonymsNER(ctx context.Context, req *pb.SynonymsNERReq) (*pb.SynonymsNERRsp, error) {
	return r.app.SynonymsNER(ctx, req)
}

func (r *RPC) DescribeUnsatisfiedReplyList(ctx context.Context, req *pb.DescribeUnsatisfiedReplyListReq) (rsp *pb.DescribeUnsatisfiedReplyListRsp, err error) {
	return r.app.DescribeUnsatisfiedReplyList(ctx, req)
}

func (r *RPC) ModifyUnsatisfiedReply(ctx context.Context, req *pb.ModifyUnsatisfiedReplyReq) (rsp *pb.ModifyUnsatisfiedReplyRsp, err error) {
	return r.app.ModifyUnsatisfiedReply(ctx, req)
}

func (r *RPC) IgnoreUnsatisfiedReply(ctx context.Context, req *pb.IgnoreUnsatisfiedReplyReq) (rsp *pb.IgnoreUnsatisfiedReplyRsp, err error) {
	return r.app.IgnoreUnsatisfiedReply(ctx, req)
}

func (r *RPC) ExportUnsatisfiedReply(ctx context.Context, req *pb.ExportUnsatisfiedReplyReq) (rsp *pb.ExportUnsatisfiedReplyRsp, err error) {
	return r.app.ExportUnsatisfiedReply(ctx, req)
}

func (r *RPC) DescribeUnsatisfiedReplyContext(ctx context.Context, req *pb.DescribeUnsatisfiedReplyContextReq) (rsp *pb.DescribeUnsatisfiedReplyContextRsp, err error) {
	return r.app.DescribeUnsatisfiedReplyContext(ctx, req)
}

func (r *RPC) AddUnsatisfiedReply(ctx context.Context, req *pb.AddUnsatisfiedReplyReq) (rsp *pb.AddUnsatisfiedReplyRsp, err error) {
	return r.app.AddUnsatisfiedReply(ctx, req)
}

func (r *RPC) DescribeLikeDataCount(ctx context.Context, req *pb.DescribeLikeDataCountReq) (rsp *pb.DescribeLikeDataCountRsp, err error) {
	return r.app.DescribeLikeDataCount(ctx, req)
}

func (r *RPC) DescribeAnswerTypeDataCount(ctx context.Context, req *pb.DescribeAnswerTypeDataCountReq) (rsp *pb.DescribeAnswerTypeDataCountRsp, err error) {
	return r.app.DescribeAnswerTypeDataCount(ctx, req)
}

func (r *RPC) DescribeRobotConfigHistory(ctx context.Context, req *pb.GetRobotConfigByVersionIDReq) (rsp *pb.GetRobotConfigByVersionIDRsp, err error) {
	return r.app.GetRobotConfigByVersionID(ctx, req)

}
func (r *RPC) ModifyRobotConfigHistory(ctx context.Context, req *pb.ModifyRobotConfigByVersionReq) (rsp *pb.ModifyRobotConfigByVersionRsp, err error) {
	return r.app.ModifyRobotConfigByVersionID(ctx, req)
}

func (r *RPC) ReleaseNotify(ctx context.Context, req *pb.ReleaseNotifyReq) (rsp *pb.ReleaseNotifyRsp, err error) {
	return r.app.ReleaseNotify(ctx, req)
}

// UpdateAppUsage 更新应用使用情况
// 问答不涉及cos, 不涉及存储容量
// 第三方cos文档，不涉及存储容量
func (r *RPC) UpdateAppUsage(ctx context.Context, usage entity.CapacityUsage, appID uint64) error {
	req := pb.ModifyAppReq{
		AppId: appID,
		Inner: &pb.ModifyAppInner{
			AppPrimaryId:              appID,
			IncrUsedCharSize:          ptrx.Int64(usage.CharSize),
			IncrUsedComputeCapacity:   ptrx.Int64(usage.ComputeCapacity),
			IncrUsedStorageCapacity:   ptrx.Int64(usage.StorageCapacity),
			IncrUsedKnowledgeCapacity: ptrx.Int64(usage.KnowledgeCapacity),
		},
	}
	if _, err := r.app.ModifyApp(ctx, &req); err != nil {
		logx.E(ctx, "UpdateAppCapacityUsage|ModifyApp error:%+v", err)
		return fmt.Errorf("UpdateAppCapacityUsage|ModifyApp error:%w", err)
	}
	return nil
}

// SetAppUsage 设置应用容量使用情况（直接设置，非增量更新）
// 通过ModifyApp接口填充Inner结构体中的UsedKnowledgeCapacity、UsedStorageCapacity和UsedComputeCapacity字段
func (r *RPC) SetAppUsage(ctx context.Context, usage entity.CapacityUsage, appID uint64) error {
	req := pb.ModifyAppReq{
		AppId: appID,
		Inner: &pb.ModifyAppInner{
			AppPrimaryId:          appID,
			UsedKnowledgeCapacity: ptrx.Uint64(uint64(usage.KnowledgeCapacity)),
			UsedStorageCapacity:   ptrx.Uint64(uint64(usage.StorageCapacity)),
			UsedComputeCapacity:   ptrx.Uint64(uint64(usage.ComputeCapacity)),
		},
	}
	if _, err := r.app.ModifyApp(ctx, &req); err != nil {
		logx.E(ctx, "SetAppUsage|ModifyApp error:%+v", err)
		return fmt.Errorf("SetAppUsage|ModifyApp error:%w", err)
	}

	logx.I(ctx, "SetAppUsage success, appID:%d, knowledge_capacity:%d, storage_capacity:%d, compute_capacity:%d",
		appID, usage.KnowledgeCapacity, usage.StorageCapacity, usage.ComputeCapacity)
	return nil
}

// DescribeLatestRelease 获取应用最新发布信息
func (r *RPC) DescribeLatestRelease(ctx context.Context, appID uint64) (*pb.DescribeLatestReleaseRsp, error) {
	req := &pb.DescribeLatestReleaseReq{AppBizId: appID}
	return r.app.DescribeLatestRelease(ctx, req)
}

func (r *RPC) ClearAppResourceCallback(ctx context.Context, req *pb.ClearAppResourceCallbackReq) (*pb.ClearAppResourceCallbackRsp, error) {
	logx.I(ctx, "ClearAppResourceCallback req:%+v", req)
	rsp, err := r.app.ClearAppResourceCallback(ctx, req)
	if err != nil {
		logx.E(ctx, "ClearAppResourceCallback req:%+v, error:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// CreateShareKnowledgeBaseApp 创建共享知识库应用
func (r *RPC) CreateShareKnowledgeBaseApp(ctx context.Context, uin, name, avatar, spaceID string) (*pb.CreateShareKnowledgeBaseAppRsp, error) {
	logx.I(ctx, "CreateShareKnowledgeBaseApp, uin: %s, name: %s, avatar: %s", uin, name, avatar)

	req := &pb.CreateShareKnowledgeBaseAppReq{
		Uin:     uin,
		Name:    name,
		Avatar:  avatar,
		SpaceId: spaceID,
	}
	rsp, err := r.app.CreateShareKnowledgeBaseApp(ctx, req)
	if err != nil {
		logx.E(ctx, "CreateShareKnowledgeBaseApp failed, request: %+v, error: %+v", req, err)
		return nil, err
	}

	return rsp, nil
}

// DeleteShareKnowledgeBaseApp 删除共享知识库应用
func (r *RPC) DeleteShareKnowledgeBaseApp(ctx context.Context, uin string, appBizID uint64) (*pb.DeleteShareKnowledgeBaseAppRsp, error) {
	logx.I(ctx, "DeleteShareKnowledgeBaseApp, uin: %s, appBizID: %d", uin, appBizID)

	req := &pb.DeleteShareKnowledgeBaseAppReq{
		Uin:      uin,
		AppBizId: appBizID,
	}
	rsp, err := r.app.DeleteShareKnowledgeBaseApp(ctx, req)
	if err != nil {

		if errs.Is(err, errs.ErrAppNotFound) {
			logx.W(ctx, "DeleteShareKnowledgeBaseApp failed, request: %+v, error: %+v", req, err)
			return rsp, nil
		}
		logx.E(ctx, "DeleteShareKnowledgeBaseApp failed, request: %+v, error: %+v", req, err)
		return nil, err
	}

	return rsp, nil
}

// DescribeAppByPrimaryIdWithoutNotFoundError 获取应用信息。查不到时不返回错误，为了兼容旧逻辑
func (r *RPC) DescribeAppByPrimaryIdWithoutNotFoundError(ctx context.Context, primaryId uint64) (*entity.App, error) {
	app, err := r.DescribeAppByPrimaryId(ctx, primaryId)
	if err != nil {
		if errx.Is(err, errx.ErrNotFound) || errx.Is(err, errs.ErrRobotNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("DescribeAppByPrimaryIdWithoutNotFoundError error:%w (primaryId:%d)", err, primaryId)
	}
	return app, nil
}

func (r *RPC) DescribeApp(ctx context.Context, req *pb.DescribeAppReq) (*pb.DescribeAppRsp, error) {
	return r.app.DescribeApp(ctx, req)
}

// GetAppBaseInfo 获取应用的基础信息
func (r *RPC) GetAppBaseInfo(ctx context.Context, appBizId uint64) (*entity.AppBaseInfo, error) {
	req := &pb.ListAppBaseInfoReq{
		AppBizIds:  []uint64{appBizId},
		PageNumber: 1,
		PageSize:   1,
	}
	rsp, err := r.app.ListAppBaseInfo(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("GetAppBaseInfo err: %w", err)
	}
	apps := rsp.GetAppBaseInfos()
	if len(apps) == 0 {
		return nil, errs.ErrAppNotFound
	}
	return appBaseInfoPB2DO(apps[0]), nil
}

// GetAppBaseInfoByPrimaryId 按app主键ID获取应用的基础信息
func (r *RPC) GetAppBaseInfoByPrimaryId(ctx context.Context, appPrimaryId uint64) (*entity.AppBaseInfo, error) {
	req := &pb.ListAppBaseInfoReq{
		AppPrimaryIds: []uint64{appPrimaryId},
		PageNumber:    1,
		PageSize:      1,
	}
	rsp, err := r.app.ListAppBaseInfo(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("GetAppBaseInfo err: %w", err)
	}
	apps := rsp.GetAppBaseInfos()
	if len(apps) == 0 {
		return nil, errs.ErrAppNotFound
	}
	return appBaseInfoPB2DO(apps[0]), nil
}

// ListAppBaseInfo 是 DescribeAppList 的简化版，只返回应用的基础信息
func (r *RPC) ListAppBaseInfo(ctx context.Context, req *pb.ListAppBaseInfoReq) ([]*entity.AppBaseInfo, uint64, error) {
	rsp, err := r.app.ListAppBaseInfo(ctx, req)
	if err != nil {
		return nil, 0, fmt.Errorf("ListAppBaseInfo err: %w", err)
	}
	return appBaseInfoListPB2DO(rsp.GetAppBaseInfos()), uint64(rsp.GetTotal()), nil
}

func appBaseInfoPB2DO(v *pb.AppBaseInfo) *entity.AppBaseInfo {
	if v == nil {
		return nil
	}
	return &entity.AppBaseInfo{
		CorpPrimaryId: v.GetCorpPrimaryId(),
		PrimaryId:     v.GetAppPrimaryId(),
		BizId:         v.GetAppBizId(),
		Name:          v.GetName(),
		SpaceId:       v.GetSpaceId(),
		IsExpCenter:   v.GetIsExpCenter(),
		IsShared:      v.GetIsShared(),
		Uin:           v.GetUin(),
		UsedCharSize:  v.GetUsedCharSize(),
		QaVersion:     v.GetQaVersion(),
	}
}

func appBaseInfoListPB2DO(apps []*pb.AppBaseInfo) []*entity.AppBaseInfo {
	if apps == nil {
		return nil
	}
	list := make([]*entity.AppBaseInfo, 0, len(apps))
	for _, v := range apps {
		list = append(list, appBaseInfoPB2DO(v))
	}
	return list
}

// CountCorpAppCharSize 判断企业应用使用的总字符数
func (r *RPC) CountCorpAppCharSize(ctx context.Context, corpPrimaryID uint64) (uint64, error) {
	countCharSizeReq := &pb.CountCorpAppCharSizeReq{
		CorpPrimaryId: corpPrimaryID,
	}
	countCharSizeRsp, err := r.app.CountCorpAppCharSize(ctx, countCharSizeReq)
	if err != nil {
		return 0, err
	}

	return countCharSizeRsp.Total, nil
}

// StartEmbeddingUpgradeApp 应用升级embedding开始
func (r *RPC) StartEmbeddingUpgradeApp(ctx context.Context, req *pb.StartEmbeddingUpgradeAppReq) (*pb.StartEmbeddingUpgradeAppRsp, error) {
	logx.I(ctx, "StartEmbeddingUpgradeApp req:%+v", req)
	rsp, err := r.app.StartEmbeddingUpgradeApp(ctx, req)
	if err != nil {
		logx.E(ctx, "StartEmbeddingUpgradeApp req:%+v, error:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// FinishEmbeddingUpgradeApp 应用升级embedding结束
func (r *RPC) FinishEmbeddingUpgradeApp(ctx context.Context, req *pb.FinishEmbeddingUpgradeAppReq) (*pb.FinishEmbeddingUpgradeAppRsp, error) {
	logx.I(ctx, "FinishEmbeddingUpgradeApp req:%+v", req)
	rsp, err := r.app.FinishEmbeddingUpgradeApp(ctx, req)
	if err != nil {
		logx.E(ctx, "FinishEmbeddingUpgradeApp req:%+v, error:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// ImportExportComponentCallback 应用包导入导出结束回调
func (r *RPC) ImportExportComponentCallback(ctx context.Context, req *pb.ImportExportComponentCallbackReq) (*pb.ImportExportComponentCallbackRsp, error) {
	logx.I(ctx, "ImportExportComponentCallback req:%+v", req)
	rsp, err := r.app.ImportExportComponentCallback(ctx, req)
	if err != nil {
		logx.E(ctx, "ImportExportComponentCallback req:%+v, error:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}
