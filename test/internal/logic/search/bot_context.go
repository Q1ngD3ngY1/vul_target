package search

import (
	"fmt"
	"strconv"
	"strings"

	logicKnowConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_config"
	"github.com/spf13/cast"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	Permis "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/permissions"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/search"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/common/v3/utils"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	pbKnowledge "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	"golang.org/x/net/context"
)

// BotContext 上下文信息，包括全局变量等
type BotContext struct {
	dao         dao.Dao
	permisLogic Permis.PermisLogic

	FuncType   search.FuncType
	App        *admin.GetAppInfoRsp                         `json:"-"` // 应用信息
	ReplaceApp *admin.GetAppInfoRsp                         `json:"-"` // isearch项目临时方案：映射其他应用知识库
	ShareApps  map[uint64]*admin.GetAppsByBizIDsRsp_AppInfo // 共享知识库对应应用信息

	SearchBatchReq       *pb.SearchKnowledgeBatchReq
	SearchKGReq          *pb.SearchKnowledgeReq
	KnowledgeType        pb.KnowledgeType           // 检索知识类型
	SceneType            pb.SceneType               // 1:评测，2：线上
	AppID                uint64                     // 应用自增id
	AppBizID             uint64                     // 应用业务id
	Question             string                     // 用户问题
	SubQuestions         []string                   // 拆解的子问题
	ImageURLs            []string                   // 图片信息
	UsePlaceholder       bool                       // 是否使用占位符
	DefaultKG            search.RetrievalKGConfig   // 默认知识库的检索配置
	ShareKGs             []search.RetrievalKGConfig // 共享知识库检索配置
	ModelName            string                     // 模型名
	SearchScope          search.EnumSearchScope     // 检索范围
	Filters              []*retrieval.SearchFilter
	FilterKey            string                                // 过滤key
	TopN                 uint32                                // 检索返回的TopN
	APISearchRange       map[uint64]*admin.AppSearchRange      // 知识库的api参数配置
	CustomVariables      map[string]string                     // api请求时，设置的自定义参数custom_variables
	VisitorLabels        []*pb.VectorLabel                     // api请求时，设置的自定义参数visitor_labels[已废弃，兼容存量数据]
	RoleLabels           map[uint64]*retrieval.LabelExpression // 角色标签<key:knowledgeBizId，value：labelExpre>
	RoleNotAllowedSearch bool                                  // 角色是否允许检索
	VisitorBizID         string                                // lke_userid,api传入的访客id,用于控制用户权限
	SearchDocBizIDs      []uint64                              // 指定文档id检索
}

// Init 接口初始化
func (imp *BotContext) Init(ctx context.Context, searchKGBatchReq *pb.SearchKnowledgeBatchReq, searchKGReq *pb.SearchKnowledgeReq, dao dao.Dao) error {
	imp.dao = dao
	imp.permisLogic = Permis.NewPermisLogic(dao)

	imp.APISearchRange = make(map[uint64]*admin.AppSearchRange)
	imp.CustomVariables = make(map[string]string)
	imp.ShareApps = make(map[uint64]*admin.GetAppsByBizIDsRsp_AppInfo)
	imp.RoleLabels = make(map[uint64]*retrieval.LabelExpression)

	if searchKGReq != nil {
		imp.FuncType = search.EnumFuncTypeSearchKG
		imp.SearchKGReq = searchKGReq
		imp.SearchBatchReq = convertSearchKnowledgeReq2SearchKnowledgeBatchReq(searchKGReq)
	} else {
		imp.FuncType = search.EnumFuncTypeSearchBatchKG
		imp.SearchBatchReq = searchKGBatchReq
	}
	app, replaceApp, err := imp.getRealAppInfo(ctx, imp.SearchBatchReq.GetAppBizId(), uint32(imp.SearchBatchReq.GetSceneType()))
	if err != nil {
		log.ErrorContextf(ctx, "getRealAppInfo failed, req: %+v, err: %+v", imp.SearchBatchReq, err)
		return err
	}
	imp.App = app
	imp.ReplaceApp = replaceApp
	imp.KnowledgeType = imp.SearchBatchReq.GetKnowledgeType()
	imp.SceneType = imp.SearchBatchReq.GetSceneType()
	imp.AppID = app.GetId()
	imp.AppBizID = app.GetAppBizId()
	imp.Question = imp.SearchBatchReq.GetQuestion()
	imp.SubQuestions = imp.SearchBatchReq.GetSubQuestions()
	imp.ImageURLs = imp.SearchBatchReq.GetImageUrls()
	imp.UsePlaceholder = imp.SearchBatchReq.GetUsePlaceholder()
	imp.ModelName = imp.SearchBatchReq.GetModelName()
	imp.SearchScope = search.EnumSearchScope(imp.SearchBatchReq.GetSearchScope())
	imp.VisitorBizID = imp.SearchBatchReq.GetVisitorBizId()
	imp.SearchDocBizIDs = imp.getSearchDocBizIDs(ctx)
	imp.CustomVariables = imp.getGetCustomVariables()
	imp.VisitorLabels = imp.genVisitorLabels()
	imp.APISearchRange = imp.getAPISearchRange()
	imp.FilterKey = imp.genFilterKey()
	imp.ShareApps = imp.getShareAPPInfos(ctx)
	imp.DefaultKG = imp.genDefaultKG(ctx, imp.ReplaceApp)
	imp.ShareKGs, err = imp.genShareKGs(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "genShareKGs err: %+v", err)
		return err
	}
	imp.RoleLabels = imp.getRoleLabels(ctx)
	return nil
}

// getSearchDocBizIDs 获取指定文档检索的docBizId
func (imp *BotContext) getSearchDocBizIDs(ctx context.Context) []uint64 { // todo 后续工作流指定文档id可以统一收敛到这个地方
	docBizIDS := make([]uint64, 0)
	for k, v := range imp.SearchBatchReq.GetCustomVariables() {
		if k == CustomVariableKeyLkeDocBizID { // 过滤空值 和 isearch场景的特殊字段
			docBizIDArr := strings.Split(v, "|")
			for _, idStr := range docBizIDArr {
				docBizID, err := strconv.ParseUint(idStr, 10, 64)
				if err != nil {
					log.WarnContextf(ctx, "getSearchDocBizIDs docBizId invalid: %+v", err)
					continue
				}
				docBizIDS = append(docBizIDS, docBizID)
			}
			imp.SearchBatchReq.GetCustomVariables()[CustomVariableKeyLkeDocBizID] = "" // 干掉特殊字段
		}
	}
	return docBizIDS
}

// getGetCustomVariables 获取自定义参数
func (imp *BotContext) getGetCustomVariables() map[string]string {
	customVariables := make(map[string]string)
	for k, v := range imp.SearchBatchReq.GetCustomVariables() {
		if v != "" { // 过滤空值
			customVariables[k] = v
		}
	}
	return customVariables
}

// getRealAppInfo 如果后续isearch不能直接替换，就同时返回两个app
func (imp *BotContext) getRealAppInfo(ctx context.Context, srcAppBizID uint64, sceneType uint32) (*admin.GetAppInfoRsp,
	*admin.GetAppInfoRsp, error) {
	app, err := client.GetAppInfo(ctx, srcAppBizID, sceneType)
	if err != nil {
		return nil, nil, errs.ErrAppNotFound
	}
	if app.GetKnowledgeQa() == nil {
		return nil, nil, errs.ErrAppTypeSupportFilters
	}
	replaceApp := app
	if newAppBizID, ok := utilConfig.GetMainConfig().SearchKnowledgeAppIdReplaceMap[srcAppBizID]; ok {
		replaceApp, err = client.GetAppInfo(ctx, newAppBizID, sceneType)
		if err != nil {
			log.ErrorContextf(ctx, "Get replace AppInfo failed, %v", err)
			return nil, nil, errs.ErrAppNotFound
		}
		if replaceApp.GetKnowledgeQa() == nil {
			return nil, nil, errs.ErrAppTypeSupportFilters
		}
		// iSearch项目的bot_biz_id替换，临时实现共享知识库功能
		log.InfoContextf(ctx, "iSearch项目的bot_biz_id替换，临时实现共享知识库功能 srcApp:%+v replaceApp:%+v",
			srcAppBizID, replaceApp.GetAppBizId())
		return app, replaceApp, nil
	}
	return app, replaceApp, nil
}

// getAPISearchRange 获取api参数的配置信息
func (imp *BotContext) getAPISearchRange() map[uint64]*admin.AppSearchRange {
	searchRange := make(map[uint64]*admin.AppSearchRange)
	searchRange[imp.AppBizID] = imp.App.GetKnowledgeQa().GetSearchRange()
	searchRange[imp.ReplaceApp.GetAppBizId()] = imp.ReplaceApp.GetKnowledgeQa().GetSearchRange()
	for _, info := range imp.App.GetKnowledgeQa().GetShareKnowledgeBases() {
		searchRange[info.GetKnowledgeBizId()] = info.GetSearchRange()
	}
	return searchRange
}

// getShareAPPInfos 获取共享应用的信息
func (imp *BotContext) getShareAPPInfos(ctx context.Context) map[uint64]*admin.GetAppsByBizIDsRsp_AppInfo {
	mAppInfo := make(map[uint64]*admin.GetAppsByBizIDsRsp_AppInfo)
	var appBizIDs []uint64
	var err error
	needSearchShareKG := false
	if imp.FuncType == search.EnumFuncTypeSearchBatchKG {
		if len(imp.SearchBatchReq.GetSearchConfig()) == 0 { // 如果是批量接口，但是知识库没有配置，说明是要检索全部知识
			needSearchShareKG = true
		}
	}
	if imp.FuncType == search.EnumFuncTypeSearchKG || needSearchShareKG { //非批量接口，需要自己去获取关联的共享知识库
		var shareKGList []*model.AppShareKnowledge
		if imp.SceneType == pb.SceneType_PROD { // 发布库
			shareKGList, err = dao.GetAppShareKGDao().GetAppShareKGListProd(ctx, imp.AppBizID)
			if err != nil {
				log.WarnContextf(ctx, "SearchKnowledge GetAppShareKGListProd failed, %v", err)
			}
		} else {
			shareKGList, err = dao.GetAppShareKGDao().GetAppShareKGList(ctx, imp.AppBizID)
			if err != nil {
				log.WarnContextf(ctx, "SearchKnowledge GetAppShareKGList failed, %v", err)
			}
		}
		shareKGSearchRange := make(map[uint64]*admin.AppSearchRange)
		for _, info := range imp.App.GetKnowledgeQa().GetShareKnowledgeBases() {
			shareKGSearchRange[info.GetKnowledgeBizId()] = info.GetSearchRange()
		}
		for _, share := range shareKGList {
			appBizIDs = append(appBizIDs, share.KnowledgeBizID)
		}
	} else if imp.FuncType == search.EnumFuncTypeSearchBatchKG {
		for _, info := range imp.SearchBatchReq.GetSearchConfig() {
			if info.GetKnowledgeBizId() != imp.SearchBatchReq.GetAppBizId() { // 说明是共享知识库
				appBizIDs = append(appBizIDs, info.GetKnowledgeBizId())
			}
		}
	}
	if len(appBizIDs) > 0 {
		appInfos, err := client.GetAppsByBizIDs(ctx, appBizIDs, uint32(imp.SceneType))
		if err != nil {
			log.WarnContextf(ctx, "SearchKnowledge GetAppsByBizIDs failed, %v", err)
		}
		if appInfos != nil {
			for _, info := range appInfos.GetApps() {
				mAppInfo[info.AppBizId] = info
			}
		}
	}
	return mAppInfo
}

// genShareKGs 构建应用默认知识库配置
func (imp *BotContext) genDefaultKG(ctx context.Context, app *admin.GetAppInfoRsp) search.RetrievalKGConfig {
	defaultKG := search.RetrievalKGConfig{} // 这里只能用空结构体，因为后续构造检索请求时会通过KnowledgeID和KnowledgeBizID是否为0来判断是否需要检索默认知识库
	needSearchDefaultKG := false
	if imp.FuncType == search.EnumFuncTypeSearchBatchKG {
		if len(imp.SearchBatchReq.GetSearchConfig()) == 0 { // 说明检索全部知识
			needSearchDefaultKG = true
		} else {
			for _, info := range imp.SearchBatchReq.GetSearchConfig() {
				if info.GetKnowledgeBizId() == imp.SearchBatchReq.GetAppBizId() { // 说明是应用默认知识库
					needSearchDefaultKG = true
					break
				}
			}
		}
	} else if imp.FuncType == search.EnumFuncTypeSearchKG {
		needSearchDefaultKG = true
	}
	if needSearchDefaultKG {
		defaultKG.KnowledgeID = app.GetId()
		defaultKG.KnowledgeBizID = app.GetAppBizId()
		defaultKG.KnowledgeName = app.GetBaseConfig().GetName()
		defaultKG.IsShareKG = false
		if imp.SceneType == pb.SceneType_PROD {
			defaultKG.QAVersion = app.GetKnowledgeQa().GetQaVersion()
		} else {
			defaultKG.EmbeddingVersion = app.GetKnowledgeQa().GetEmbedding().GetVersion()
		}
		for _, val := range imp.SearchBatchReq.GetSearchConfig() {
			if val.GetKnowledgeBizId() == imp.SearchBatchReq.GetAppBizId() {
				defaultKG.WorkflowKGCfg = val.GetWorkflowKnowledgeParam()
			}
		}
		defaultKG.FilterKey = imp.FilterKey

		embeddingModelName, err := logicKnowConfig.GetKnowledgeBaseConfig(ctx, app.CorpBizId, app.GetAppBizId(),
			uint32(pbKnowledge.KnowledgeBaseConfigType_EMBEDDING_MODEL))
		if err != nil {
			log.ErrorContextf(ctx, "getKnowledgeBaseConfig err: %v", err)
			return defaultKG
		}
		defaultKG.EmbeddingModelName = embeddingModelName
		searchStrategy := app.GetKnowledgeQa().GetSearchStrategy()
		if searchStrategy != nil {
			defaultKG.Rerank = &retrieval.Rerank{
				Model:  searchStrategy.GetRerankModel(),
				TopN:   imp.getRerankTopN(ctx),
				Enable: searchStrategy.GetRerankModelSwitch() == "on",
			}
		}
		defaultKG.SearchStrategy = imp.HandleSearchStrategy(ctx)
		defaultKG.Filters, err = imp.HandleRetrievalFilters(ctx, []*admin.AppFiltersInfo{})
		if err != nil {
			log.ErrorContextf(ctx, "HandleRetrievalFilters err: %v", err)
			return defaultKG
		}
	}
	return defaultKG
}

// genShareKGs 构建共享知识库配置
func (imp *BotContext) genShareKGs(ctx context.Context) ([]search.RetrievalKGConfig, error) {
	var shareKGs []search.RetrievalKGConfig
	needSearchShareKG := false
	if imp.FuncType == search.EnumFuncTypeSearchBatchKG {
		if len(imp.SearchBatchReq.GetSearchConfig()) == 0 { // 如果是批量接口，但是知识库没有配置，说明是要检索全部知识
			needSearchShareKG = true
		}
	}
	if imp.FuncType == search.EnumFuncTypeSearchKG || needSearchShareKG {
		for _, info := range imp.ShareApps {
			shareApp, err := client.GetAppInfo(ctx, info.GetAppBizId(), uint32(pb.SceneType_TEST))
			if err != nil {
				log.ErrorContextf(ctx, "getAppInfo err: %v", err)
				// 降级
				continue
			}
			if shareApp.GetCorpId() != imp.App.GetCorpId() {
				log.ErrorContextf(ctx, "shareApp:%d corpId:%d not match app:%d corpId:%d",
					shareApp.GetAppBizId(), shareApp.GetCorpId(), imp.App.GetAppBizId(), imp.App.GetCorpId())
				return nil, errs.ErrSharedKnowledgeRecordNotFound
			}

			shareKG := search.RetrievalKGConfig{}
			shareKG.IsShareKG = true
			shareKG.QAVersion = 0                                                            // 共享知识库不需要发布，qaversion 为0
			shareKG.EmbeddingVersion = shareApp.GetKnowledgeQa().GetEmbedding().GetVersion() // 共享知识库使用自己的embedding版本
			shareKG.KnowledgeID = info.GetAppId()
			shareKG.KnowledgeBizID = info.GetAppBizId()
			shareKG.KnowledgeName = info.GetName()
			shareKG.WorkflowKGCfg = imp.DefaultKG.WorkflowKGCfg
			shareKG.FilterKey = model.AppSearchPreviewFilterKey // 共享知识库，默认检索评测库
			if imp.FilterKey == model.AppPreviewQuestionFilterKey || imp.FilterKey == model.AppReleaseQuestionFilterKey {
				shareKG.FilterKey = model.AppPreviewQuestionFilterKey // qa 0.97特殊逻辑
			}
			shareKGs = append(shareKGs, shareKG)
		}
	} else if imp.FuncType == search.EnumFuncTypeSearchBatchKG {
		for _, info := range imp.SearchBatchReq.GetSearchConfig() {
			if info.GetKnowledgeBizId() == imp.SearchBatchReq.GetAppBizId() { //过滤默认知识库
				continue
			}
			shareApp, err := client.GetAppInfo(ctx, info.GetKnowledgeBizId(), uint32(pb.SceneType_TEST))
			if err != nil {
				log.ErrorContextf(ctx, "getAppInfo err: %v", err)
				// 降级
				continue
			}
			if shareApp.GetCorpId() != imp.App.GetCorpId() {
				log.ErrorContextf(ctx, "shareApp:%d corpId:%d not match app:%d corpId:%d",
					shareApp.GetAppBizId(), shareApp.GetCorpId(), imp.App.GetAppBizId(), imp.App.GetCorpId())
				return nil, errs.ErrSharedKnowledgeRecordNotFound
			}
			shareKG := search.RetrievalKGConfig{}
			shareKG.IsShareKG = true
			shareKG.QAVersion = 0 // 共享知识库不需要发布，qaversion 为0
			shareKG.EmbeddingVersion = shareApp.GetKnowledgeQa().GetEmbedding().GetVersion()
			shareKG.KnowledgeID = imp.ShareApps[info.GetKnowledgeBizId()].GetAppId()
			shareKG.KnowledgeBizID = info.GetKnowledgeBizId()
			shareKG.KnowledgeName = shareApp.GetBaseConfig().GetName()
			shareKG.WorkflowKGCfg = info.GetWorkflowKnowledgeParam()
			shareKG.FilterKey = model.AppSearchPreviewFilterKey // 共享知识库，默认检索评测库
			if imp.FilterKey == model.AppPreviewQuestionFilterKey || imp.FilterKey == model.AppReleaseQuestionFilterKey {
				shareKG.FilterKey = model.AppPreviewQuestionFilterKey // qa 0.97特殊逻辑
			}
			shareKGs = append(shareKGs, shareKG)
		}
	}

	// todo cooper 待确认应用配置里面配置的embedding模型怎么联动的？
	// 获取embedding model name
	appBizIds := make([]uint64, 0)
	for _, shareKG := range shareKGs {
		appBizIds = append(appBizIds, shareKG.KnowledgeBizID)
	}
	configs, err := logicKnowConfig.GetKnowledgeBaseConfigs(ctx, imp.ReplaceApp.GetCorpBizId(), appBizIds,
		[]uint32{uint32(pbKnowledge.KnowledgeBaseConfigType_EMBEDDING_MODEL)})
	if err != nil {
		// 降级
		log.ErrorContextf(ctx, "getKnowledgeBaseConfigs err: %v", err)
	}
	if configs != nil && len(configs) > 0 {
		configMap := make(map[uint64]string)
		for _, config := range configs {
			log.DebugContextf(ctx, "getKnowledgeBaseConfigs config: %+v", config)
			configMap[config.KnowledgeBizId] = config.Config
		}
		for i, shareKG := range shareKGs {
			if config, ok := configMap[shareKG.KnowledgeBizID]; ok {
				shareKGs[i].EmbeddingModelName = config
			}
		}
	}

	// 获取rerank
	shareKnowledgeBases := imp.App.GetKnowledgeQa().GetShareKnowledgeBases()
	shareKnowledgeBaseMap := make(map[uint64]*admin.APIShareKnowledgeBase)
	if len(shareKnowledgeBases) > 0 {
		for _, shareKnowledgeBase := range shareKnowledgeBases {
			shareKnowledgeBaseMap[shareKnowledgeBase.GetKnowledgeBizId()] = shareKnowledgeBase
		}
	}

	for i, shareKG := range shareKGs {
		var shareKGFilters *admin.AppFilters
		if shareKnowledgeBase, ok := shareKnowledgeBaseMap[shareKG.KnowledgeBizID]; ok && shareKnowledgeBase != nil {
			searchStrategy := shareKnowledgeBase.GetSearchStrategy()
			if searchStrategy != nil {
				shareKGs[i].Rerank = &retrieval.Rerank{
					Model:  searchStrategy.GetRerankModel(),
					TopN:   imp.getRerankTopN(ctx),
					Enable: searchStrategy.GetRerankModelSwitch() == "on",
				}
				shareKGs[i].SearchStrategy = &retrieval.SearchStrategy{
					StrategyType:     retrieval.SearchStrategyTypeEnum(searchStrategy.GetStrategyType()),
					TableEnhancement: searchStrategy.GetTableEnhancement(),
				}
			}
			// 共享知识库理论上只有评测端，需要同时兼容AppInfo返回评测或者发布端的filter
			if shareKnowledgeBase.GetFilters() != nil {
				filterMap := shareKnowledgeBase.GetFilters()
				switch shareKG.FilterKey {
				case model.AppPreviewQuestionFilterKey:
					// 0.97直出
					shareKGFilters, ok = filterMap[model.AppPreviewQuestionFilterKey]
					if !ok || shareKGFilters == nil {
						shareKGFilters, ok = filterMap[model.AppReleaseQuestionFilterKey]
					}
				case model.AppReleaseQuestionFilterKey:
					// 0.97直出
					shareKGFilters, ok = filterMap[model.AppReleaseQuestionFilterKey]
					if !ok || shareKGFilters == nil {
						shareKGFilters, ok = filterMap[model.AppPreviewQuestionFilterKey]
					}
				case model.AppSearchPreviewFilterKey:
					shareKGFilters, ok = filterMap[model.AppSearchPreviewFilterKey]
					if !ok || shareKGFilters == nil {
						shareKGFilters, ok = filterMap[model.AppSearchReleaseFilterKey]
					}
				case model.AppSearchReleaseFilterKey:
					shareKGFilters, ok = filterMap[model.AppSearchReleaseFilterKey]
					if !ok || shareKGFilters == nil {
						shareKGFilters, ok = filterMap[model.AppSearchPreviewFilterKey]
					}
				}
			}
		} else {
			// 默认情况，使用应用级的配置
			appSearchStrategy := imp.App.GetKnowledgeQa().GetSearchStrategy()
			if appSearchStrategy != nil {
				shareKGs[i].Rerank = &retrieval.Rerank{
					Model:  appSearchStrategy.GetRerankModel(),
					TopN:   imp.getRerankTopN(ctx),
					Enable: appSearchStrategy.GetRerankModelSwitch() == "on",
				}
			}
			shareKGs[i].SearchStrategy = imp.HandleSearchStrategy(ctx)
		}
		// 转换filter
		shareKGs[i].Filters, err = imp.HandleRetrievalFilters(ctx, shareKGFilters.GetFilter())
		if err != nil {
			log.ErrorContextf(ctx, "HandleRetrievalFilters err: %v", err)
		}
	}

	return shareKGs, nil
}

// genFilterKey 构建filterkey
func (imp *BotContext) genFilterKey() string {
	var filterKey string
	if imp.SearchScope == search.EnumSearchScopeQAPriority { // qa 0.97特殊逻辑
		imp.SearchScope = search.EnumSearchScopeAll
		if imp.SceneType == pb.SceneType_PROD {
			filterKey = model.AppReleaseQuestionFilterKey
		} else {
			filterKey = model.AppPreviewQuestionFilterKey
		}
	} else {
		if imp.SceneType == pb.SceneType_PROD {
			filterKey = model.AppSearchReleaseFilterKey
		} else {
			filterKey = model.AppSearchPreviewFilterKey
		}
	}
	return filterKey
}

// genVisitorLabels 构建自定义标签（已废弃，只是兼容老数据）-新版都走custom_variables
func (imp *BotContext) genVisitorLabels() []*pb.VectorLabel {
	// 只有searchKnowledge接口才会有这个数据
	// labels 里面的name是attrKey
	if imp.FuncType == search.EnumFuncTypeSearchBatchKG {
		return nil
	}
	if len(imp.SearchKGReq.GetReq().GetLabels()) > 0 {
		return imp.SearchKGReq.GetReq().GetLabels()
	}
	return nil
}

// getRoleLabels 获取角色的标签
func (imp *BotContext) getRoleLabels(ctx context.Context) map[uint64]*retrieval.LabelExpression {
	lkeUserID := imp.GetLKEUserID()
	mKGLabel, notSearch, err := imp.permisLogic.GetRoleSearchLabel(ctx, imp.App.GetCorpBizId(), imp.App.GetId(),
		imp.App.GetAppBizId(), lkeUserID)
	if err != nil {
		log.WarnContextf(ctx, "getRoleLabels err: %v", err)
	}
	log.InfoContextf(ctx, "getRoleLabels: %s", utils.Any2String(mKGLabel))
	imp.RoleNotAllowedSearch = notSearch
	return mKGLabel
}

// getRerankTopN 获取rerank topN
func (imp *BotContext) getRerankTopN(ctx context.Context) uint32 {
	f, ok := imp.App.GetKnowledgeQa().GetFilters()[model.AppRerankFilterKey]
	if !ok {
		log.ErrorContextf(ctx, "rerank filter not found")
		return utilConfig.GetMainConfig().RetrievalConfig.DefaultRerankTopN
	}
	return f.GetTopN()
}

// GetLKEUserID 获取访客id标识
func (imp *BotContext) GetLKEUserID() string {
	lkeUserID := imp.CustomVariables["lke_userid"]
	if lkeUserID != "" { // isearch
		return lkeUserID
	}
	return imp.VisitorBizID
}

// GetRetrievalKGList 获取需要检索的知识库列表
func (imp *BotContext) GetRetrievalKGList(ctx context.Context) []search.RetrievalKGConfig {
	var finalList []search.RetrievalKGConfig
	var kgs []search.RetrievalKGConfig
	if imp.DefaultKG.KnowledgeID != 0 && imp.DefaultKG.KnowledgeBizID != 0 { // 只要勾选了默认知识库的才能添加
		kgs = append(kgs, imp.DefaultKG)
	}
	kgs = append(kgs, imp.ShareKGs...)
	for _, kgInfo := range kgs {
		if imp.RoleLabels != nil { // 为nil ，有全部权限，不需要过滤
			if _, ok := imp.RoleLabels[kgInfo.KnowledgeBizID]; !ok { // 角色没有知识库权限
				log.WarnContextf(ctx, "role does not have kg permission,kgBizId:%d", kgInfo.KnowledgeBizID)
				continue
			}
		}
		finalList = append(finalList, kgInfo)
	}
	return finalList
}

// HandleRetrievalFilters 处理检索过滤
func (imp *BotContext) HandleRetrievalFilters(ctx context.Context, filters []*admin.AppFiltersInfo) (
	[]*retrieval.SearchFilter, error) {
	log.InfoContextf(ctx, "HandleRetrievalFilters filterKey:%s, filters:%s", imp.FilterKey, utils.Any2String(filters))
	// 默认使用默认知识库配置
	var appFilter *admin.AppFilters
	var ok bool
	appFilter, ok = imp.App.GetKnowledgeQa().GetFilters()[imp.FilterKey]
	if !ok {
		return nil, fmt.Errorf("robot %s filter not found scenes %d", imp.FilterKey, imp.SceneType)
	}
	defaultKgFilters := imp.covertAdminFiltersToRetrievalFilters(ctx, appFilter.GetFilter())
	if imp.FilterKey == model.AppPreviewQuestionFilterKey || imp.FilterKey == model.AppReleaseQuestionFilterKey {
		// 0.97问答直出场景，不管是默认知识库还是共享知识库，都从默认知识库配置中取0.97问答的filter
		return defaultKgFilters, nil
	}

	workflowFilters := make([]*retrieval.SearchFilter, 0)
	if imp.KnowledgeType == pb.KnowledgeType_WORKFLOW {
		//工作流场景，取工作流节点设置的filter
		for _, filter := range imp.SearchBatchReq.GetWorkflowSearchParam().GetFilters() {
			workflowFilters = append(workflowFilters, &retrieval.SearchFilter{
				IndexId:    model.GetType(filter.GetDocType()),
				Confidence: filter.GetConfidence(),
				TopN:       filter.GetTopN(),
				DocType:    filter.GetDocType(),
			})
		}
		log.InfoContextf(ctx, "HandleRetrievalFilters workflowFilters:%s", utils.Any2String(workflowFilters))
		return workflowFilters, nil
	}

	if filters != nil && len(filters) != 0 {
		// 共享知识库有独立的应用级配置
		return imp.covertAdminFiltersToRetrievalFilters(ctx, filters), nil
	}

	// 返回默认使用应用配置：1、默认知识库非0.97场景；2、共享知识库兜底场景
	return defaultKgFilters, nil
}

// covertFilters 协议转换
func (imp *BotContext) covertAdminFiltersToRetrievalFilters(ctx context.Context, filters []*admin.AppFiltersInfo) []*retrieval.SearchFilter {
	retrievalFilters := make([]*retrieval.SearchFilter, 0)
	for _, f := range filters {
		if f.GetDocType() == model.DocTypeSearchEngine {
			// 检索服务中不再支持搜索引擎
			continue
		}
		if f.GetDocType() == model.DocTypeTaskFlow {
			continue
		}
		if (imp.FilterKey == model.AppSearchPreviewFilterKey || imp.FilterKey == model.AppSearchReleaseFilterKey) &&
			!f.GetIsEnable() {
			continue
		}
		retrievalFilters = append(retrievalFilters, &retrieval.SearchFilter{
			IndexId:    uint64(f.GetIndexId()),
			Confidence: f.GetConfidence(),
			TopN:       f.GetTopN(),
			DocType:    f.GetDocType(),
		})
	}
	// 基于检索范围二次过滤
	retrievalFilters = filterSearchScope(ctx, uint32(imp.SearchScope), retrievalFilters)
	return retrievalFilters
}

// HandleRetrievalRecallNum 处理检索召回的数量
func (imp *BotContext) HandleRetrievalRecallNum(ctx context.Context) (uint32, error) {
	recallNum := uint32(0)
	if imp.KnowledgeType == pb.KnowledgeType_WORKFLOW { //工作流场景，取自己设置的filter
		recallNum = imp.SearchBatchReq.GetWorkflowSearchParam().GetTopN()
		return recallNum, nil
	}
	// 默认使用应用高级设置
	recallNum = uint32(imp.App.GetKnowledgeQa().GetKnowledgeAdvancedConfig().GetRerankRecallNum())
	if recallNum != 0 {
		return recallNum, nil
	}
	// 其次使用应用默认知识库设置，兼容旧版本
	if appFilter, ok := imp.App.GetKnowledgeQa().GetFilters()[imp.FilterKey]; ok && appFilter.GetTopN() != 0 {
		return appFilter.GetTopN(), nil
	}
	// 兜底使用配置文件
	recallNum = utilConfig.GetMainConfig().RetrievalConfig.DefaultRecallNum
	return recallNum, nil
}

// HandleSearchStrategy 处理检索策略
func (imp *BotContext) HandleSearchStrategy(ctx context.Context) *retrieval.SearchStrategy {
	searchStrategy := &retrieval.SearchStrategy{}
	appSearchStrategy := imp.App.GetKnowledgeQa().GetSearchStrategy()
	if appSearchStrategy != nil { // 应用配置的策略
		searchStrategy = &retrieval.SearchStrategy{
			StrategyType:     retrieval.SearchStrategyTypeEnum(appSearchStrategy.GetStrategyType()),
			TableEnhancement: appSearchStrategy.GetTableEnhancement(),
		}
	}
	workflowStrategy := imp.SearchBatchReq.GetWorkflowSearchParam().GetSearchStrategy()
	if workflowStrategy != nil { // 工作流单独配置的策略
		searchStrategy = &retrieval.SearchStrategy{
			StrategyType:     retrieval.SearchStrategyTypeEnum(workflowStrategy.GetStrategyType()),
			TableEnhancement: workflowStrategy.GetTableEnhancement(),
		}
	}
	return searchStrategy
}

// HandleRerank 处理rerank模型
func (imp *BotContext) HandleRerank(ctx context.Context) (*retrieval.Rerank, error) {
	rerank := &retrieval.Rerank{}
	advancedConfig := imp.App.GetKnowledgeQa().GetKnowledgeAdvancedConfig()
	rerank.Model = advancedConfig.GetRerankModel()
	if rerank.Model == "" {
		rerank.Model = utilConfig.GetMainConfig().RetrievalConfig.DefaultModelName
	}
	rerank.TopN = imp.getRerankTopN(ctx)
	rerank.Enable = imp.App.GetKnowledgeQa().GetEnableRerank()
	return rerank, nil
}

// convertSearchKnowledgeReq2SearchKnowledgeBatchReq 协议转换
func convertSearchKnowledgeReq2SearchKnowledgeBatchReq(req *pb.SearchKnowledgeReq) *pb.SearchKnowledgeBatchReq {
	target := new(pb.SearchKnowledgeBatchReq)
	target.AppBizId = req.GetReq().GetBotBizId()
	target.SceneType = req.GetSceneType()
	target.Question = req.GetReq().GetQuestion()
	target.SubQuestions = req.GetReq().GetSubQuestions()
	target.ImageUrls = req.GetReq().GetImageUrls()
	target.UsePlaceholder = req.GetReq().GetUsePlaceholder()
	target.ModelName = req.GetReq().GetModelName()
	target.KnowledgeType = req.GetKnowledgeType()
	target.SearchScope = req.GetReq().GetSearchScope()
	if req.GetReq().GetWorkflowSearchExtraParam() != nil { // 工作流场景的请求
		target.WorkflowSearchParam = &pb.WorkflowSearchParam{
			Filters:        convertPbFilters(req.GetReq().GetWorkflowSearchExtraParam().GetFilters()),
			TopN:           req.GetReq().GetWorkflowSearchExtraParam().GetTopN(),
			SearchStrategy: req.GetReq().GetWorkflowSearchExtraParam().GetSearchStrategy(),
		}
		target.SearchConfig = append(target.SearchConfig, &pb.SearchKnowledgeConfig{
			KnowledgeBizId: req.GetReq().GetBotBizId(),
			WorkflowKnowledgeParam: &pb.WorkflowKnowledgeParam{
				Labels:         nil, // 这里需要特殊处理，先不赋值，seachknowledge接口传进来的是attrKey
				KnowledgeScope: nil,
				LabelLogicOpr:  req.GetReq().GetWorkflowSearchExtraParam().GetLabelLogicOpr(),
				CloseKnowledge: nil,
			},
		})
	}
	target.CustomVariables = req.GetReq().GetCustomVariables()
	target.VisitorBizId = req.GetReq().GetVisitorBizId()
	// todo 	Labels:          bs.Labels,
	return target
}

func convertPbFilters(src []*pb.WorkflowSearchExtraParam_Filter) []*pb.Filter {
	var filters []*pb.Filter
	for _, val := range src {
		filters = append(filters, &pb.Filter{
			DocType:    val.GetDocType(),
			Confidence: val.GetConfidence(),
			TopN:       val.GetTopN(),
		})
	}
	return filters
}

func (imp *BotContext) GetDBTableID() uint64 {
	for _, customVariableValue := range imp.getGetCustomVariables() {
		if dbTableIDStr, found := strings.CutPrefix(customVariableValue, "db_table_"); found {
			dbTableID, err := cast.ToUint64E(dbTableIDStr)
			if err == nil {
				return dbTableID
			}
		}
	}
	return 0
}
