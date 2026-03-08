package search

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	adpCommon "git.woa.com/adp/pb-go/common"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_common"

	"github.com/spf13/cast"
	"golang.org/x/net/context"

	"git.code.oa.com/trpc-go/trpc-go/codec"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	kbdao "git.woa.com/adp/kb/kb-config/internal/dao/kb"
	labeldao "git.woa.com/adp/kb/kb-config/internal/dao/label"
	releasedao "git.woa.com/adp/kb/kb-config/internal/dao/release"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	financeEntity "git.woa.com/adp/kb/kb-config/internal/entity/finance"
	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/internal/entity/search"
	"git.woa.com/adp/kb/kb-config/internal/logic/category"
	"git.woa.com/adp/kb/kb-config/internal/logic/finance"
	kbLogic "git.woa.com/adp/kb/kb-config/internal/logic/kb"
	cacheLogic "git.woa.com/adp/kb/kb-config/internal/logic/localcache"
	"git.woa.com/adp/kb/kb-config/internal/logic/user"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

// BotContext 上下文信息，包括全局变量等
type BotContext struct {
	rpc        *rpc.RPC
	kbDao      kbdao.Dao
	labelDao   labeldao.Dao
	releaseDao releasedao.Dao
	userLogic  *user.Logic
	cateLogic  *category.Logic
	cacheLogic *cacheLogic.Logic
	kbLogic    *kbLogic.Logic

	FuncType search.FuncType

	App        *entity.App
	ReplaceApp *entity.App

	SearchBatchReq       *pb.SearchKnowledgeBatchReq
	SearchKGReq          *pb.SearchKnowledgeReq
	KnowledgeType        pb.SearchKnowledgeType     // 检索知识类型
	SceneType            pb.SceneType               // 1:评测，2：线上
	AppPrimaryID         uint64                     // 应用自增id
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
	APISearchRange       map[uint64]*entity.SearchRange        // 知识库的api参数配置
	CustomVariables      map[string]string                     // api请求时，设置的自定义参数custom_variables
	VisitorLabels        []*pb.VectorLabel                     // api请求时，设置的自定义参数visitor_labels[已废弃，兼容存量数据]
	RoleLabels           map[uint64]*retrieval.LabelExpression // 角色标签<key:knowledgeBizId，value：labelExpre>
	RoleNotAllowedSearch bool                                  // 角色是否允许检索
	VisitorBizID         string                                // lke_userid,api传入的访客id,用于控制用户权限
	SearchDocBizIDs      []uint64                              // 指定文档id检索
	BillingInfo          search.BillingInfo                    // 计费上报相关信息
	StartTime            time.Time                             // 开始时间
}

// Init 接口初始化
func (imp *BotContext) Init(ctx context.Context, rpc *rpc.RPC,
	searchKGBatchReq *pb.SearchKnowledgeBatchReq, searchKGReq *pb.SearchKnowledgeReq,
	kbDao kbdao.Dao, labelDao labeldao.Dao, releaseDao releasedao.Dao, cateLogic *category.Logic, cacheLogic *cacheLogic.Logic,
	userLogic *user.Logic, kbLogic *kbLogic.Logic) error {
	imp.rpc = rpc
	imp.kbDao = kbDao
	imp.labelDao = labelDao
	imp.releaseDao = releaseDao
	imp.cateLogic = cateLogic
	imp.cacheLogic = cacheLogic
	imp.userLogic = userLogic
	imp.kbLogic = kbLogic

	imp.APISearchRange = make(map[uint64]*entity.SearchRange)
	imp.CustomVariables = make(map[string]string)
	imp.RoleLabels = make(map[uint64]*retrieval.LabelExpression)
	imp.BillingInfo.ModelBillingStatus = make(map[string]bool)
	imp.BillingInfo.BillingTags = make(map[string]string)

	imp.StartTime = time.Now()
	if searchKGReq != nil {
		imp.FuncType = search.EnumFuncTypeSearchKG
		imp.SearchKGReq = searchKGReq
		imp.SearchBatchReq = convertSearchKnowledgeReq2SearchKnowledgeBatchReq(searchKGReq)
	} else {
		imp.FuncType = search.EnumFuncTypeSearchBatchKG
		imp.SearchBatchReq = searchKGBatchReq
	}
	app, replaceApp, err := imp.getRealAppInfo(ctx, imp.SearchBatchReq.GetAppBizId(),
		uint32(imp.SearchBatchReq.GetSceneType()))
	if err != nil {
		logx.E(ctx, "getRealAppInfo failed, req: %+v, err: %+v", imp.SearchBatchReq, err)
		return err
	}
	logx.I(ctx, "getRealAppInfo app: %+v, replaceApp: %+v", app, replaceApp)
	logx.I(ctx, "getRealApp.QaConfig, replaceApp.QaConfig",
		jsonx.MustMarshalToString(app.QaConfig), jsonx.MustMarshalToString(replaceApp.QaConfig))
	imp.App = app
	imp.ReplaceApp = replaceApp
	imp.KnowledgeType = imp.SearchBatchReq.GetKnowledgeType()
	imp.SceneType = imp.SearchBatchReq.GetSceneType()
	imp.AppPrimaryID = app.PrimaryId
	imp.AppBizID = app.BizId
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
	imp.APISearchRange, err = imp.getAPISearchRange(ctx)
	imp.BillingInfo.FinanceType = imp.SearchBatchReq.GetFinanceSubBizType()
	imp.BillingInfo.CallSource = imp.SearchBatchReq.GetCallSource()
	if err != nil {
		logx.E(ctx, "getAPISearchRange failed, req: %+v, err: %+v", imp.SearchBatchReq, err)
		return err
	}
	imp.FilterKey = imp.genFilterKey()
	logx.I(ctx, "BotContext Impl: %s", jsonx.MustMarshalToString(imp))
	imp.DefaultKG = imp.genDefaultKG(ctx, imp.ReplaceApp)
	imp.ShareKGs, err = imp.genShareKGs(ctx)
	if err != nil {
		logx.E(ctx, "genShareKGs err: %+v", err)
		return err
	}
	imp.BillingInfo.BillingTags = imp.SearchBatchReq.GetBillingTags()
	imp.BillingInfo.NeedReportEmbeddingAndRerank = imp.isNeedReportEmbeddingAndRerank(ctx)
	logx.I(ctx, "BotContext Init NeedReportEmbeddingAndRerank: %+v ", imp.BillingInfo.NeedReportEmbeddingAndRerank)
	if imp.BillingInfo.NeedReportEmbeddingAndRerank { // 如果需要上报计费，就需要获取计费状态和裁剪知识库
		imp.BillingInfo.SubBizType = imp.getModelBillingSubBizType(ctx)
		// 遍历imp.DefaultKG和imp.ShareKGs，取出所有embedding和rerank的模型名称
		imp.BillingInfo.ModelBillingStatus = imp.getModelBillingStatus(ctx, imp.BillingInfo.SubBizType)
		// 根据模型名计费状态判断是否要裁剪,遍历imp.DefaultKG和imp.ShareKGs
		err = imp.pruneModelsByBillingStatus(ctx)
		if err != nil {
			logx.E(ctx, "pruneModelsByBillingStatus err: %+v", err)
			return err
		}
	}
	imp.RoleLabels = imp.getRoleLabels(ctx)

	logx.I(ctx, "BotContext Init End. (appID:%d, appBizID:%d, replaceAppID:%d, replaceAppBizID:%d, KnowledgeType:%d, ModelName:%s)",
		app.PrimaryId, app.BizId, replaceApp.PrimaryId, replaceApp.BizId, imp.KnowledgeType, imp.ModelName)
	logx.I(ctx, "BotContext Init End. DefaultKG: %+s ", jsonx.MustMarshalToString(imp.DefaultKG))
	logx.I(ctx, "BotContext Init End. ShareKGs: %+s ", jsonx.MustMarshalToString(imp.ShareKGs))
	return nil
}

func (imp *BotContext) isNeedReportEmbeddingAndRerank(ctx context.Context) bool {
	calleeMethod := codec.Message(ctx).CalleeMethod()
	logx.I(ctx, "isNeedReportEmbeddingAndRerank calleeMethod: %+v ", calleeMethod)
	if dbTableID := imp.GetDBTableID(); dbTableID > 0 { // 如果有指定数据库表，则不进行上报
		return false
	}
	if calleeMethod == "/SearchKnowledgeRelease" { // 需要特殊处理
		return config.App().Finance.EmbeddingAndRerank.SearchReleaseSwitch
	} else {
		return config.App().Finance.EmbeddingAndRerank.ReportSwitch
	}
}

func (imp *BotContext) EnableScopeValues() []string {
	vals := []string{
		entity.EnableScopeDb2Label[entity.EnableScopeAll],
	}
	if imp.SceneType == pb.SceneType_PROD {
		vals = append(vals, entity.EnableScopeDb2Label[entity.EnableScopePublish])
	} else {
		vals = append(vals, entity.EnableScopeDb2Label[entity.EnableScopeDev])
	}
	return vals
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
					logx.W(ctx, "getSearchDocBizIDs docBizId invalid: %+v", err)
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
func (imp *BotContext) getRealAppInfo(ctx context.Context, srcAppBizID uint64, sceneType uint32) (*entity.App, *entity.App, error) {
	var err error
	app := entity.AppFromContext(ctx, sceneType)
	if app == nil {
		app, err = imp.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, srcAppBizID, sceneType)
		if err != nil {
			return nil, nil, errs.ErrAppNotFound
		}
	}
	replaceApp := app
	if newAppBizID, ok := config.GetMainConfig().SearchKnowledgeAppIdReplaceMap[srcAppBizID]; ok {
		replaceApp, err = imp.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, newAppBizID, sceneType)
		if err != nil {
			logx.E(ctx, "Get replace App failed, %v", err)
			return nil, nil, errs.ErrAppNotFound
		}
		// iSearch项目的bot_biz_id替换，临时实现共享知识库功能
		logx.I(ctx, "iSearch项目的bot_biz_id替换，临时实现共享知识库功能 srcApp:%+v replaceApp:%+v",
			srcAppBizID, replaceApp.BizId)
		return app, replaceApp, nil
	}
	return app, replaceApp, nil
}

// getAPISearchRange 获取api参数的配置信息
func (imp *BotContext) getAPISearchRange(ctx context.Context) (map[uint64]*entity.SearchRange, error) {
	if imp.App == nil || imp.App.QaConfig == nil || imp.App.QaConfig.SearchRange == nil ||
		imp.ReplaceApp == nil || imp.ReplaceApp.QaConfig == nil || imp.ReplaceApp.QaConfig.SearchRange == nil {
		logx.E(ctx, "getAPISearchRange failed, app: %+v, replaceApp: %+v", imp.App, imp.ReplaceApp)
		return nil, errs.ErrSystem
	}
	searchRange := make(map[uint64]*entity.SearchRange)
	searchRange[imp.AppBizID] = imp.App.QaConfig.SearchRange
	searchRange[imp.ReplaceApp.BizId] = imp.ReplaceApp.QaConfig.SearchRange
	for shareKbBizID, info := range imp.App.ShareKnowledgeBases {
		if info == nil {
			logx.E(ctx, "getAPISearchRange failed, shareKbBizID:%d SearchRange is nil", shareKbBizID)
			return nil, errs.ErrSystem
		}
		searchRange[shareKbBizID] = info.QaConfig.SearchRange
	}
	return searchRange, nil
}

// getShareAPPInfos 获取共享应用的信息
func (imp *BotContext) getShareAppBizIds(ctx context.Context) map[uint64]*pb.WorkflowKnowledgeParam {
	appBizIdMap := make(map[uint64]*pb.WorkflowKnowledgeParam)
	var err error
	needSearchShareKG := false
	if imp.FuncType == search.EnumFuncTypeSearchBatchKG {
		if len(imp.SearchBatchReq.GetSearchConfig()) == 0 { // 如果是批量接口，但是知识库没有配置，说明是要检索全部知识
			needSearchShareKG = true
		}
	}
	if imp.FuncType == search.EnumFuncTypeSearchKG || needSearchShareKG { // 非批量接口，需要自己去获取关联的共享知识库
		var shareKGList []*kbEntity.AppShareKnowledge
		if imp.SceneType == pb.SceneType_PROD { // 发布库
			shareKGList, err = imp.kbDao.GetAppShareKGListProd(ctx, imp.AppBizID)
			if err != nil {
				logx.W(ctx, "SearchKnowledge GetAppShareKGListProd failed, %v", err)
			}
		} else {
			shareKGList, err = imp.kbDao.GetAppShareKGList(ctx, imp.AppBizID)
			if err != nil {
				logx.W(ctx, "SearchKnowledge GetAppShareKGList failed, %v", err)
			}
		}
		for _, share := range shareKGList {
			appBizIdMap[share.KnowledgeBizID] = nil
		}
	} else if imp.FuncType == search.EnumFuncTypeSearchBatchKG {
		for _, info := range imp.SearchBatchReq.GetSearchConfig() {
			if info.GetKnowledgeBizId() != imp.SearchBatchReq.GetAppBizId() { // 说明是共享知识库
				appBizIdMap[info.GetKnowledgeBizId()] = info.GetWorkflowKnowledgeParam()
			}
		}
	}
	return appBizIdMap
}

// genShareKGs 构建应用默认知识库配置
func (imp *BotContext) genDefaultKG(ctx context.Context, app *entity.App) search.RetrievalKGConfig {
	defaultKG := search.RetrievalKGConfig{} // 这里只能用空结构体，因为后续构造检索请求时会通过KnowledgeID和KnowledgeBizID是否为0来判断是否需要检索默认知识库
	var err error
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
		defaultKG.KnowledgeID = app.PrimaryId
		defaultKG.KnowledgeBizID = app.BizId
		defaultKG.KnowledgeName = app.Name
		defaultKG.IsShareKG = false
		defaultKG.EmbeddingVersion = app.Embedding.Version
		if imp.SceneType == pb.SceneType_PROD {
			defaultKG.QAVersion = app.QaVersion
			embeddingModelName, err := imp.kbLogic.GetDefaultKnowledgeBaseConfig(ctx, app.CorpBizId, app.BizId, app.BizId,
				uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL), bot_common.AdpDomain_ADP_DOMAIN_PROD)
			if err != nil {
				logx.E(ctx, "getKnowledgeBaseConfig err: %v", err)
				return defaultKG
			}
			defaultKG.EmbeddingModelName = embeddingModelName
		} else {
			embeddingModelName, err := imp.kbLogic.GetDefaultKnowledgeBaseConfig(ctx, app.CorpBizId, app.BizId, app.BizId,
				uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL), bot_common.AdpDomain_ADP_DOMAIN_DEV)
			if err != nil {
				logx.E(ctx, "getKnowledgeBaseConfig err: %v", err)
				return defaultKG
			}
			defaultKG.EmbeddingModelName = embeddingModelName
		}
		if defaultKG.EmbeddingModelName != "" {
			defaultKG.EmbeddingVersion = entity.GetEmbeddingVersion(defaultKG.EmbeddingModelName)
		}
		for _, val := range imp.SearchBatchReq.GetSearchConfig() {
			if val.GetKnowledgeBizId() == imp.SearchBatchReq.GetAppBizId() {
				defaultKG.WorkflowKGCfg = val.GetWorkflowKnowledgeParam()
			}
		}
		defaultKG.FilterKey = imp.FilterKey

		searchStrategy := app.QaConfig.SearchStrategy
		if searchStrategy != nil {
			defaultKG.Rerank = &retrieval.Rerank{
				Model:  searchStrategy.RerankModel,
				TopN:   imp.getRerankTopN(ctx),
				Enable: searchStrategy.RerankModelSwitch == "on",
			}
		}
		defaultKG.SearchStrategy = imp.HandleSearchStrategy(ctx)
		qaDirectOutput := imp.isQAEnabledForDirectOutput(ctx, imp.App.QaConfig.Filters) // 知识库关闭问答，不能走0.97直出
		defaultKG.Filters, err = imp.HandleRetrievalFilters(ctx, []config.RobotFilterDetail{}, qaDirectOutput)
		if err != nil {
			logx.E(ctx, "HandleRetrievalFilters err: %v", err)
			return defaultKG
		}
	}
	return defaultKG
}

// genShareKGs 构建共享知识库配置
func (imp *BotContext) genShareKGs(ctx context.Context) ([]search.RetrievalKGConfig, error) {
	shareKGs := make([]search.RetrievalKGConfig, 0)
	shareAppBizIdMap := imp.getShareAppBizIds(ctx)
	if len(shareAppBizIdMap) == 0 {
		return shareKGs, nil
	}
	for shareAppBizId, workflowKnowledgeParam := range shareAppBizIdMap {
		shareApp, err := imp.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, shareAppBizId, uint32(pb.SceneType_TEST))
		if err != nil {
			logx.E(ctx, "getAppInfo err: %v", err)
			// 降级
			continue
		}
		if shareApp.CorpPrimaryId != imp.App.CorpPrimaryId {
			logx.E(ctx, "shareApp:%d corpId:%d not match app:%d corpId:%d",
				shareApp.BizId, shareApp.CorpPrimaryId, imp.App.BizId, imp.App.CorpPrimaryId)
			return nil, errs.ErrSharedKnowledgeRecordNotFound
		}
		shareKG := search.RetrievalKGConfig{}
		shareKG.IsShareKG = true
		shareKG.QAVersion = 0                                 // 共享知识库不需要发布，qaversion 为0
		shareKG.EmbeddingVersion = shareApp.Embedding.Version // 共享知识库使用自己的embedding版本
		shareKG.KnowledgeID = shareApp.PrimaryId
		shareKG.KnowledgeBizID = shareApp.BizId
		shareKG.KnowledgeName = shareApp.Name
		shareKG.WorkflowKGCfg = imp.DefaultKG.WorkflowKGCfg
		if workflowKnowledgeParam != nil {
			shareKG.WorkflowKGCfg = workflowKnowledgeParam
		}
		shareKG.FilterKey = entity.AppSearchPreviewFilterKey // 共享知识库，默认检索评测库
		if imp.FilterKey == entity.AppPreviewQuestionFilterKey || imp.FilterKey == entity.AppReleaseQuestionFilterKey {
			shareKG.FilterKey = entity.AppPreviewQuestionFilterKey // qa 0.97特殊逻辑
		}
		shareKGs = append(shareKGs, shareKG)
	}

	// 获取embedding model name
	appBizIds := make([]uint64, 0)
	for _, shareKG := range shareKGs {
		appBizIds = append(appBizIds, shareKG.KnowledgeBizID)
	}
	configs, err := imp.kbLogic.GetShareKBModelNames(ctx, imp.ReplaceApp.CorpBizId, appBizIds,
		[]uint32{uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL)})
	if err != nil {
		// 降级
		logx.E(ctx, "getKnowledgeBaseConfigs err: %v", err)
	}
	if configs != nil && len(configs) > 0 {
		configMap := make(map[uint64]string)
		for _, config := range configs {
			logx.D(ctx, "getKnowledgeBaseConfigs config: %+v", config)
			configMap[config.KnowledgeBizID] = config.Config
		}
		for i, shareKG := range shareKGs {
			if config, ok := configMap[shareKG.KnowledgeBizID]; ok {
				shareKGs[i].EmbeddingModelName = config
				shareKGs[i].EmbeddingVersion = entity.GetEmbeddingVersion(config)
			}
		}
	}

	// 获取rerank
	shareKnowledgeBases := imp.App.ShareKnowledgeBases
	shareKnowledgeBaseMap := make(map[uint64]*entity.App)
	if len(shareKnowledgeBases) > 0 {
		for knowledgeBizId, shareKnowledgeBase := range shareKnowledgeBases {
			shareKnowledgeBaseMap[knowledgeBizId] = shareKnowledgeBase
		}
	}

	for i, shareKG := range shareKGs {
		var shareKGFilters config.RobotFilter
		qaDirectOutput := true // qa>0.97 直出
		if shareKnowledgeBase, ok := shareKnowledgeBaseMap[shareKG.KnowledgeBizID]; ok && shareKnowledgeBase != nil {
			searchStrategy := shareKnowledgeBase.QaConfig.SearchStrategy
			if searchStrategy != nil {
				shareKGs[i].Rerank = &retrieval.Rerank{
					Model:  searchStrategy.RerankModel,
					TopN:   imp.getRerankTopN(ctx),
					Enable: searchStrategy.RerankModelSwitch == "on",
				}
				shareKGs[i].SearchStrategy = &retrieval.SearchStrategy{
					StrategyType:     retrieval.SearchStrategyTypeEnum(searchStrategy.StrategyType),
					TableEnhancement: searchStrategy.TableEnhancement,
				}
			}
			// 共享知识库只有评测端
			if shareKnowledgeBase.QaConfig.Filters != nil {
				filterMap := shareKnowledgeBase.QaConfig.Filters
				qaDirectOutput = imp.isQAEnabledForDirectOutput(ctx, filterMap) // 知识库关闭问答，不能走0.97直出
				switch shareKG.FilterKey {
				case entity.AppPreviewQuestionFilterKey:
					// 0.97直出
					shareKGFilters, ok = filterMap[entity.AppPreviewQuestionFilterKey]
					if !ok {
						shareKGFilters, ok = filterMap[entity.AppReleaseQuestionFilterKey]
					}
				case entity.AppSearchPreviewFilterKey:
					shareKGFilters, ok = filterMap[entity.AppSearchPreviewFilterKey]
					if !ok {
						shareKGFilters, ok = filterMap[entity.AppSearchReleaseFilterKey]
					}
				}
			}
		} else {
			// 默认情况，使用应用级的配置
			appSearchStrategy := imp.App.QaConfig.SearchStrategy
			if appSearchStrategy != nil {
				shareKGs[i].Rerank = &retrieval.Rerank{
					Model:  appSearchStrategy.RerankModel,
					TopN:   imp.getRerankTopN(ctx),
					Enable: appSearchStrategy.RerankModelSwitch == "on",
				}
			}
			shareKGs[i].SearchStrategy = imp.HandleSearchStrategy(ctx)
		}
		if _, ok := imp.APISearchRange[shareKG.KnowledgeBizID]; !ok {
			// 空SearchRange兜底，避免后续panic
			imp.APISearchRange[shareKG.KnowledgeBizID] = &entity.SearchRange{}
		}
		// 转换filter
		shareKGs[i].Filters, err = imp.HandleRetrievalFilters(ctx, shareKGFilters.Filter, qaDirectOutput)
		if err != nil {
			logx.E(ctx, "HandleRetrievalFilters err: %v", err)
		}
	}

	return shareKGs, nil
}

// genFilterKey 构建filterkey
func (imp *BotContext) genFilterKey() string {
	var filterKey string
	if imp.SearchScope == search.EnumSearchScopeQAPriority { // qa 0.97特殊逻辑
		imp.SearchScope = search.EnumSearchScopeAll
		filterKey = entity.AppPreviewQuestionFilterKey
		if imp.SceneType == pb.SceneType_PROD {
			filterKey = entity.AppReleaseQuestionFilterKey
		} else {
			filterKey = entity.AppPreviewQuestionFilterKey
		}
	} else {
		filterKey = entity.AppSearchPreviewFilterKey
		if imp.SceneType == pb.SceneType_PROD {
			filterKey = entity.AppSearchReleaseFilterKey
		} else {
			filterKey = entity.AppSearchPreviewFilterKey
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
	mKGLabel, notSearch, err := imp.userLogic.DescribeRoleSearchLabel(ctx, imp.App.CorpBizId, imp.App.PrimaryId,
		imp.App.BizId, lkeUserID)
	if err != nil {
		logx.W(ctx, "getRoleLabels err: %v", err)
	}
	logx.I(ctx, "getRoleLabels: %s", jsonx.MustMarshalToString(mKGLabel))
	imp.RoleNotAllowedSearch = notSearch
	return mKGLabel
}

// getRerankTopN 获取rerank topN
func (imp *BotContext) getRerankTopN(ctx context.Context) uint32 {
	f, ok := imp.App.QaConfig.Filters[entity.AppRerankFilterKey]
	if !ok {
		logx.E(ctx, "rerank filter not found")
		return config.GetMainConfig().RetrievalConfig.DefaultRerankTopN
	}
	return f.TopN
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
				logx.W(ctx, "role does not have kg permission,kgBizId:%d", kgInfo.KnowledgeBizID)
				continue
			}
		}
		finalList = append(finalList, kgInfo)
	}
	return finalList
}

// HandleRetrievalFilters 处理检索过滤
func (imp *BotContext) HandleRetrievalFilters(ctx context.Context, filters []config.RobotFilterDetail, qaDirectOutput bool) (
	[]*retrieval.SearchFilter, error) {
	logx.I(ctx, "HandleRetrievalFilters filterKey:%s, filters:%s", imp.FilterKey, jsonx.MustMarshalToString(filters))
	// 默认使用默认知识库配置
	appFilter, ok := imp.App.QaConfig.Filters[imp.FilterKey]
	logx.I(ctx, "HandleRetrievalFilters filterKey:%s, appFilter:%s", imp.FilterKey, jsonx.MustMarshalToString(appFilter))
	if !ok {
		return nil, fmt.Errorf("robot %s filter not found scenes %d", imp.FilterKey, imp.SceneType)
	}
	defaultKgFilters := imp.covertAdminFiltersToRetrievalFilters(ctx, appFilter.Filter)
	if imp.FilterKey == entity.AppPreviewQuestionFilterKey || imp.FilterKey == entity.AppReleaseQuestionFilterKey {
		// 0.97问答直出场景，不管是默认知识库还是共享知识库，都从默认知识库配置中取0.97问答的filter
		if !qaDirectOutput { // 如果没有开启问答，则返回空filter
			return []*retrieval.SearchFilter{}, nil
		}
		if imp.KnowledgeType == pb.SearchKnowledgeType_WORKFLOW { // 判断工作流和agent的filter里面是否有配置问答
			qaEnable := false
			for _, filter := range imp.SearchBatchReq.GetWorkflowSearchParam().GetFilters() {
				if filter.GetDocType() == entity.DocTypeQA {
					qaEnable = true
					break
				}
			}
			if !qaEnable {
				return []*retrieval.SearchFilter{}, nil
			}
		}
		return defaultKgFilters, nil
	}
	workflowFilters := make([]*retrieval.SearchFilter, 0)
	if imp.KnowledgeType == pb.SearchKnowledgeType_WORKFLOW {
		// 工作流场景，取工作流节点设置的filter
		for _, filter := range imp.SearchBatchReq.GetWorkflowSearchParam().GetFilters() {
			workflowFilters = append(workflowFilters, &retrieval.SearchFilter{
				IndexId:    entity.GetType(filter.GetDocType()),
				Confidence: filter.GetConfidence(),
				TopN:       filter.GetTopN(),
				DocType:    filter.GetDocType(),
			})
		}
		logx.I(ctx, "HandleRetrievalFilters workflowFilters:%s", jsonx.MustMarshalToString(workflowFilters))
		return workflowFilters, nil
	}

	if filters != nil && len(filters) != 0 {
		// 共享知识库有独立的应用级配置
		return imp.covertAdminFiltersToRetrievalFilters(ctx, filters), nil
	}

	// 返回默认使用应用配置：1、默认知识库非0.97场景；2、共享知识库兜底场景
	return defaultKgFilters, nil
}

// isQAEnabledForDirectOutput 检查0.97问答直出场景的问答功能是否开启
func (imp *BotContext) isQAEnabledForDirectOutput(ctx context.Context, filters config.RobotFilters) bool {
	isEnable := true
	if imp.FilterKey == entity.AppPreviewQuestionFilterKey {
		previewFilters := filters[entity.AppSearchPreviewFilterKey]
		for _, filter := range previewFilters.Filter {
			if filter.DocType == entity.DocTypeQA {
				isEnable = filter.IsEnabled
				break
			}
		}
	} else if imp.FilterKey == entity.AppReleaseQuestionFilterKey {
		releaseFilters := filters[entity.AppSearchReleaseFilterKey]
		for _, filter := range releaseFilters.Filter {
			if filter.DocType == entity.DocTypeQA {
				isEnable = filter.IsEnabled
				break
			}
		}
	}
	return isEnable
}

// covertFilters 协议转换
func (imp *BotContext) covertAdminFiltersToRetrievalFilters(ctx context.Context, filters []config.RobotFilterDetail) []*retrieval.SearchFilter {
	retrievalFilters := make([]*retrieval.SearchFilter, 0)
	for _, f := range filters {
		if f.DocType == entity.DocTypeSearchEngine {
			// 检索服务中不再支持搜索引擎
			continue
		}
		if f.DocType == entity.DocTypeTaskFlow {
			continue
		}
		if (imp.FilterKey == entity.AppSearchPreviewFilterKey || imp.FilterKey == entity.AppSearchReleaseFilterKey) &&
			!f.IsEnabled {
			continue
		}
		retrievalFilters = append(retrievalFilters, &retrieval.SearchFilter{
			IndexId:    uint64(f.IndexID),
			Confidence: f.Confidence,
			TopN:       f.TopN,
			DocType:    f.DocType,
		})
	}
	// 基于检索范围二次过滤
	retrievalFilters = filterSearchScope(ctx, uint32(imp.SearchScope), retrievalFilters)
	logx.D(ctx, "covertAdminFiltersToRetrievalFilters filters:%s retrievalFilters:%s", jsonx.MustMarshalToString(filters), jsonx.MustMarshalToString(retrievalFilters))
	return retrievalFilters
}

// HandleRetrievalRecallNum 处理检索召回的数量
func (imp *BotContext) HandleRetrievalRecallNum(ctx context.Context) (uint32, error) {
	recallNum := uint32(0)
	if imp.KnowledgeType == pb.SearchKnowledgeType_WORKFLOW { // 工作流场景，取自己设置的filter
		recallNum = imp.SearchBatchReq.GetWorkflowSearchParam().GetTopN()
		return recallNum, nil
	}
	// 默认使用应用高级设置
	recallNum = uint32(imp.App.QaConfig.AdvancedConfig.RerankRecallNum)
	if recallNum != 0 {
		return recallNum, nil
	}
	// 其次使用应用默认知识库设置，兼容旧版本
	if appFilter, ok := imp.App.QaConfig.Filters[imp.FilterKey]; ok && appFilter.TopN != 0 {
		return appFilter.TopN, nil
	}
	// 兜底使用配置文件
	recallNum = config.GetMainConfig().RetrievalConfig.DefaultRecallNum
	return recallNum, nil
}

// HandleSearchStrategy 处理检索策略
func (imp *BotContext) HandleSearchStrategy(ctx context.Context) *retrieval.SearchStrategy {
	searchStrategy := &retrieval.SearchStrategy{}
	appSearchStrategy := imp.App.QaConfig.SearchStrategy
	if appSearchStrategy != nil { // 应用配置的策略
		searchStrategy = &retrieval.SearchStrategy{
			StrategyType:     retrieval.SearchStrategyTypeEnum(appSearchStrategy.StrategyType),
			TableEnhancement: appSearchStrategy.TableEnhancement,
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
	advancedConfig := imp.App.QaConfig.AdvancedConfig
	rerank.Model = advancedConfig.RerankModel
	if rerank.Model == "" {
		rerank.Model = config.GetMainConfig().RetrievalConfig.DefaultModelName
	}
	rerank.TopN = imp.getRerankTopN(ctx)
	rerank.Enable = imp.App.QaConfig.EnableRerank
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
	target.BillingTags = req.GetReq().GetBillingTags()
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
	target.CallSource = req.GetCallSource()
	target.FinanceSubBizType = req.GetFinanceSubBizType()
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

// getModelBillingStatus 遍历DefaultKG和ShareKGs，提取所有embedding和rerank的模型名称，去重后返回map
func (imp *BotContext) getModelBillingStatus(ctx context.Context, subBizType string) map[string]bool {
	oriModelNames := make(map[string]bool)
	// 处理DefaultKG
	if imp.DefaultKG.KnowledgeID != 0 && imp.DefaultKG.KnowledgeBizID != 0 {
		// 提取embedding模型名称
		if imp.DefaultKG.EmbeddingModelName != "" {
			oriModelNames[imp.DefaultKG.EmbeddingModelName] = true
		}

		// 提取rerank模型名称
		if imp.DefaultKG.Rerank != nil && imp.DefaultKG.Rerank.Model != "" {
			oriModelNames[imp.DefaultKG.Rerank.Model] = true
		}
	}

	// 处理ShareKGs
	for _, shareKG := range imp.ShareKGs {
		// 提取embedding模型名称
		if shareKG.EmbeddingModelName != "" {
			oriModelNames[shareKG.EmbeddingModelName] = true
		}

		// 提取rerank模型名称
		if shareKG.Rerank != nil && shareKG.Rerank.Model != "" {
			oriModelNames[shareKG.Rerank.Model] = true
		}
	}
	// 将外层的rerank模型也加入
	if imp.App.QaConfig.AdvancedConfig != nil && imp.App.QaConfig.AdvancedConfig.RerankModel != "" {
		oriModelNames[imp.App.QaConfig.AdvancedConfig.RerankModel] = true
	}

	// 遍历去重后的所有模型，检查计费状态
	if len(oriModelNames) > 0 {
		// 1.调用配置文件config.App().Finance.BillingModelMap获取计费模型名映射
		billingModelMap := config.App().Finance.BillingModelMap
		logx.I(ctx, "ori model names:%v, billing model names:%v", jsonx.MustMarshalToString(oriModelNames), jsonx.MustMarshalToString(billingModelMap))
		// 建立原始模型名到计费模型名的映射
		oriToBillingMap := make(map[string]string)                   // key: 原始模型名, value: 计费模型名
		billingModelNames := make([]financeEntity.ModelStatusReq, 0) // 计费模型名列表

		for oriModelName := range oriModelNames {
			// 从BillingModelMap中获取计费模型信息
			if billingInfo, ok := billingModelMap[oriModelName]; ok {
				// 使用计费模型名进行状态检查
				billingModelName := billingInfo.ModelName
				if billingModelName == "" {
					// 如果计费模型名为空，使用原始模型名
					billingModelName = oriModelName
				}
				oriToBillingMap[oriModelName] = billingModelName
				billingModelNames = append(billingModelNames, financeEntity.ModelStatusReq{
					OriModelName:     oriModelName,
					BillingModelName: billingModelName,
					SubBizType:       subBizType})
			} else {
				// 如果没有配置，使用原始模型名和默认subBizType
				oriToBillingMap[oriModelName] = oriModelName
				billingModelNames = append(billingModelNames, financeEntity.ModelStatusReq{
					OriModelName:     oriModelName,
					BillingModelName: oriModelName,
					SubBizType:       subBizType})
			}
		}
		logx.I(ctx, "oriToBillingMap:%v", jsonx.MustMarshalToString(oriToBillingMap))
		// 2.组装好参数，调用CheckModelStatusBatch，传入计费模型名
		if imp.ReplaceApp != nil && imp.ReplaceApp.CorpPrimaryId != 0 {
			// 创建finance逻辑实例
			financeLogic := finance.NewLogic(imp.rpc)

			// 调用批量检查接口，传入ModelStatusReq列表
			billingStatusMap := financeLogic.CheckModelStatusBatch(ctx, imp.App.CorpPrimaryId, billingModelNames)
			// 3.将计费模型的状态结果映射回原始模型名
			oriStatusMap := make(map[string]bool)
			for oriModelName := range oriToBillingMap {
				if status, ok := billingStatusMap[oriModelName]; ok {
					oriStatusMap[oriModelName] = status
				} else {
					// 如果计费模型名没有状态结果，使用默认值true
					oriStatusMap[oriModelName] = true
				}
			}

			logx.I(ctx, "CheckModelStatusBatch result - original models: %+v, billing models: %+v, status: %+v",
				oriModelNames, billingModelNames, oriStatusMap)
			return oriStatusMap
		} else {
			logx.W(ctx, "Cannot check model status: App or CorpPrimaryId is nil")
		}
	}

	logx.I(ctx, "getModelBillingStatus result: %+v", oriModelNames)
	return oriModelNames
}

// getModelBillingSubBizType 获取模型计费的subBizType
func (imp *BotContext) getModelBillingSubBizType(ctx context.Context) string {
	// 根据FinanceType和CallSource确定subBizType
	// 定义场景映射关系：FinanceType -> (工作流场景, 插件场景, 知识库问答场景)
	scenarioMap := map[string]struct {
		workflow string
		plugin   string
		kbQaCall string
	}{
		search.EnumScenarioTypeKnowledgeQA: {
			workflow: rpc.DialogApiWorkflowNode,
			plugin:   rpc.DialogApiPluginCall,
			kbQaCall: rpc.DialogApiKbQaCall,
		},
		search.EnumScenarioTypeKnowledgeQADialogTest: {
			workflow: rpc.DialogTestWorkflowNode,
			plugin:   rpc.DialogTestPluginCall,
			kbQaCall: rpc.DialogTestKbQaCall,
		},
		search.EnumScenarioTypeEvaluateTest: {
			workflow: rpc.AppEvalWorkflowNode,
			plugin:   rpc.AppEvalPluginCall,
			kbQaCall: rpc.DialogEvalKbQaCall,
		},
		search.EnumScenarioTypeKnowledgeQAUser: {
			workflow: rpc.ChannelExpWorkflowNode,
			plugin:   rpc.ChannelExpPluginCall,
			kbQaCall: rpc.ChannelExpKbQaCall,
		},
	}

	// 获取场景配置，如果不存在则使用默认配置（对话API场景）
	scenario, ok := scenarioMap[imp.BillingInfo.FinanceType]
	if !ok {
		logx.W(ctx, "Unknown FinanceType: %s, using default DialogApi scenario", imp.BillingInfo.FinanceType)
		scenario = scenarioMap[search.EnumScenarioTypeKnowledgeQA]
	}

	// 根据CallSource返回对应的场景类型
	switch imp.BillingInfo.CallSource {
	case adpCommon.CallSource_CALL_SOURCE_WORKFLOW:
		return scenario.workflow
	case adpCommon.CallSource_CALL_SOURCE_PLUGIN:
		return scenario.plugin
	default:
		// 默认返回知识库问答调用场景
		return scenario.kbQaCall
	}
}

// pruneModelsByBillingStatus 根据模型计费状态裁剪DefaultKG和ShareKGs的配置
func (imp *BotContext) pruneModelsByBillingStatus(ctx context.Context) error {
	// 记录裁剪前的知识库数量
	originalDefaultKGEnabled := imp.DefaultKG.KnowledgeID != 0 && imp.DefaultKG.KnowledgeBizID != 0
	originalShareKGCount := len(imp.ShareKGs)
	// 裁剪DefaultKG
	if imp.DefaultKG.KnowledgeID != 0 && imp.DefaultKG.KnowledgeBizID != 0 {
		shouldDisable := false
		disableReason := ""

		// 检查embedding模型状态
		if imp.DefaultKG.EmbeddingModelName != "" {
			if status, ok := imp.BillingInfo.ModelBillingStatus[imp.DefaultKG.EmbeddingModelName]; ok && !status {
				shouldDisable = true
				disableReason = fmt.Sprintf("embedding model %s is not available", imp.DefaultKG.EmbeddingModelName)
			}
		}

		// 检查rerank模型状态
		if !shouldDisable && imp.DefaultKG.Rerank != nil && imp.DefaultKG.Rerank.Model != "" {
			if status, ok := imp.BillingInfo.ModelBillingStatus[imp.DefaultKG.Rerank.Model]; ok && !status {
				shouldDisable = true
				disableReason = fmt.Sprintf("rerank model %s is not available", imp.DefaultKG.Rerank.Model)
			}
		}

		if shouldDisable {
			logx.W(ctx, "DefaultKG %s, disabling retrieval", disableReason)
			// 禁用默认知识库的检索
			imp.DefaultKG.KnowledgeID = 0
			imp.DefaultKG.KnowledgeBizID = 0
		}
	}

	// 裁剪ShareKGs
	for i := len(imp.ShareKGs) - 1; i >= 0; i-- {
		shareKG := &imp.ShareKGs[i]
		shouldRemove := false
		removeReason := ""

		// 检查embedding模型状态
		if shareKG.EmbeddingModelName != "" {
			if status, ok := imp.BillingInfo.ModelBillingStatus[shareKG.EmbeddingModelName]; ok && !status {
				shouldRemove = true
				removeReason = fmt.Sprintf("embedding model %s is not available", shareKG.EmbeddingModelName)
			}
		}

		// 检查rerank模型状态
		if !shouldRemove && shareKG.Rerank != nil && shareKG.Rerank.Model != "" {
			if status, ok := imp.BillingInfo.ModelBillingStatus[shareKG.Rerank.Model]; ok && !status {
				shouldRemove = true
				removeReason = fmt.Sprintf("rerank model %s is not available", shareKG.Rerank.Model)
			}
		}

		if shouldRemove {
			logx.W(ctx, "ShareKG %s %s, removing from retrieval list", shareKG.KnowledgeName, removeReason)
			// 从ShareKGs中移除该知识库
			imp.ShareKGs = append(imp.ShareKGs[:i], imp.ShareKGs[i+1:]...)
		}
	}

	// 检查裁剪后是否还有可用的知识库
	currentDefaultKGEnabled := imp.DefaultKG.KnowledgeID != 0 && imp.DefaultKG.KnowledgeBizID != 0
	currentShareKGCount := len(imp.ShareKGs)

	if !currentDefaultKGEnabled && currentShareKGCount == 0 {
		logx.E(ctx, "All knowledge bases have been pruned due to model billing status. Original: DefaultKG enabled=%v, ShareKGs count=%d",
			originalDefaultKGEnabled, originalShareKGCount)
		return errs.ErrNoTokenBalance // 没有资源
	}

	logx.I(ctx, "After pruning by billing status - DefaultKG: %+v, ShareKGs count: %d",
		imp.DefaultKG, len(imp.ShareKGs))
	return nil
}
