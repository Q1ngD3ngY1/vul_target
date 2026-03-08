package kb

import (
	"context"
	"strconv"
	"time"
	"unicode/utf8"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/logx/auditx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	commonLogic "git.woa.com/adp/kb/kb-config/internal/logic/common"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	"git.woa.com/adp/pb-go/common"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// CreateSharedKnowledgeResult 创建共享知识库的结果
type CreateSharedKnowledgeResult struct {
	KnowledgeBizID uint64
}

// CreateSharedKnowledge 创建共享知识库的业务逻辑
func (l *Logic) CreateSharedKnowledge(ctx context.Context, params *kbe.CreateSharedKnowledgeRequest) (*CreateSharedKnowledgeResult, error) {
	logx.I(ctx, "CreateSharedKnowledge logic start, params: %+v", params)

	// 1. 参数校验
	if err := l.validateSharedKnowledgeParams(ctx, params); err != nil {
		return nil, err
	}

	// 2. 获取或设置默认的 embedding 模型
	embeddingModel, err := l.getOrSetDefaultEmbeddingModel(ctx, params.CorpBizID, params.EmbeddingModel)
	if err != nil {
		return nil, err
	}

	// 3. 检查知识库名称是否已存在
	knowledgeNameList := []string{params.Name}
	knowledgeList, err := l.kbDao.RetrieveSharedKnowledgeByName(ctx, params.CorpBizID, knowledgeNameList, params.SpaceID)
	if err != nil {
		return nil, errs.ErrSharedKnowledgeNameQueryFailed
	}

	logx.I(ctx, "CreateSharedKnowledge, knowledgeList: %+v", knowledgeList)
	if len(knowledgeList) > 0 {
		return nil, errs.ErrSharedKnowledgeExist
	}

	// 4. 获取用户信息
	staffNameResponse, err := l.rpc.PlatformAdmin.GetStaffByID(ctx, params.StaffPrimaryID)
	if err != nil {
		return nil, errs.ErrGetUserNameFailed
	}

	// 5. 创建或使用已有的共享知识库应用
	spaceID := gox.IfElse(params.SpaceID != "", params.SpaceID, kbe.DefaultSpaceID)
	var appBizID uint64

	if params.SharedKnowledgeAppBizID > 0 {
		// 使用已有应用
		appBizID = params.SharedKnowledgeAppBizID
		logx.I(ctx, "Use existing app, appBizID: %d", appBizID)
	} else {
		// 创建新应用
		createResponse, err := l.rpc.AppAdmin.CreateShareKnowledgeBaseApp(ctx, params.Uin,
			GenerateSharedKnowledgeAppName(&pb.CreateSharedKnowledgeReq{
				KnowledgeName: params.Name,
			}),
			SharedKnowledgeAppAvatar,
			spaceID)
		if err != nil {
			return nil, errs.ErrCreateSharedKnowledgeAppFailed
		}
		appBizID = createResponse.GetAppBizId()
		logx.I(ctx, "Created new app, appBizID: %d", appBizID)
	}

	// 6. 添加知识库记录
	createParams := &kbe.CreateSharedKnowledgeParams{
		CorpBizID:      params.CorpBizID,
		KnowledgeBizID: appBizID,
		Name:           params.Name,
		Description:    params.Description,
		UserBizID:      params.StaffBizID,
		UserName:       staffNameResponse.NickName,
		EmbeddingModel: embeddingModel.ModelName,
		SpaceID:        params.SpaceID,
		OwnerStaffID:   contextx.Metadata(ctx).StaffID(),
	}
	insertID, err := l.kbDao.CreateSharedKnowledge(ctx, createParams)
	if err != nil {
		// 回滚：删除应用
		if _, deleteError := l.rpc.AppAdmin.DeleteShareKnowledgeBaseApp(ctx, params.Uin, appBizID); deleteError != nil {
			logx.E(ctx, "DeleteShareKnowledgeBaseApp failed, err: %+v", deleteError)
		}
		return nil, errs.ErrCreateSharedKnowledgeRecordFailed
	}
	logx.I(ctx, "CreateSharedKnowledge, insertID: %d", insertID)

	// 7. 设置共享知识库的 embedding 模型配置
	logx.I(ctx, "SetSharedKnowledge: %d, embeddingModel: %+v", appBizID, embeddingModel)
	if err := l.setShareKnowledgeEmbeddingModelConfig(ctx, appBizID, embeddingModel); err != nil {
		return nil, errs.ErrSystem
	}
	app, err := l.rpc.DescribeAppById(ctx, appBizID)
	if params.SharedKnowledgeAppBizID == 0 {
		// 8. 初始化默认分类
		if err == nil {
			if initErr := l.cateLogic.InitDefaultCategory(ctx, app.CorpPrimaryId, app.PrimaryId); initErr != nil {
				logx.W(ctx, "InitDefaultCategory failed, err: %+v", initErr)
			}
		} else {
			logx.E(ctx, "DescribeAppById failed, app biz id: %d, err: %+v", appBizID, err)
		}
	}
	// 9. 创建向量索引
	if app != nil {
		if err := l.OperateAllVectorIndex(ctx, app.PrimaryId, app.BizId, app.Embedding.Version, embeddingModel.ModelName, kbe.OperatorCreate); err != nil {
			logx.E(ctx, "OperateAllVectorIndex (create) failed, err: %+v", err)
			return nil, errs.ErrSystem
		}
	}

	// 10. 上报操作日志
	auditx.Create(auditx.BizKB).Corp(params.CorpBizID).Space(spaceID).Log(ctx, appBizID, params.Name)

	// 11. 异步上报统计数据
	go func(newCtx context.Context) {
		defer gox.Recover()
		counterInfo := &commonLogic.CounterInfo{
			CorpBizId:       params.CorpBizID,
			SpaceId:         spaceID,
			StatisticObject: common.StatObject_STAT_OBJECT_KB,
			StatisticType:   common.StatType_STAT_TYPE_CREATE,
			ObjectId:        strconv.FormatUint(appBizID, 10),
			ObjectName:      params.Name,
			Count:           1,
		}
		commonLogic.Counter(newCtx, counterInfo, l.rpc)
	}(trpc.CloneContext(ctx))

	return &CreateSharedKnowledgeResult{
		KnowledgeBizID: appBizID,
	}, nil
}

// validateSharedKnowledgeParams 校验创建共享知识库的参数
func (l *Logic) validateSharedKnowledgeParams(ctx context.Context, params *kbe.CreateSharedKnowledgeRequest) error {
	if !VerifyData([]*DataValidation{
		{
			Data: utf8.RuneCountInString(params.Name),
			Validator: NewRangeValidator(
				WithMin(1),
				WithMax(kbe.ShareKnowledgeNameLength),
			),
		},
		{
			Data: utf8.RuneCountInString(params.Description),
			Validator: NewRangeValidator(
				WithMax(kbe.ShareKnowledgeDescriptionLength),
			),
		},
	}) {
		return errs.ErrParameterInvalid
	}
	return nil
}

// getOrSetDefaultEmbeddingModel 获取或设置默认的 embedding 模型
func (l *Logic) getOrSetDefaultEmbeddingModel(ctx context.Context, corpBizID uint64, modelName string) (kbe.KnowledgeModel, error) {
	embeddingModel := kbe.KnowledgeModel{
		ModelName: modelName,
	}

	if embeddingModel.ModelName == "" {
		// 获取默认模型
		modelRsp, err := l.rpc.Resource.GetDefaultModelConfig(ctx, entity.ModelCategoryEmbedding)
		if err != nil {
			logx.E(ctx, "GetDefaultModelConfig failed, err: %+v", err)
			return embeddingModel, errs.ErrSystem
		}
		embeddingModel.ModelName = modelRsp.GetModelName()
		embeddingModel.ModelAliasName = modelRsp.GetAliasName()
	} else {
		// 获取指定模型信息
		modelRsp, err := l.rpc.Resource.GetModelInfo(ctx, corpBizID, embeddingModel.ModelName)
		if err != nil {
			logx.E(ctx, "GetModelInfo failed, err: %+v", err)
			return embeddingModel, errs.ErrSystem
		}
		embeddingModel.ModelName = modelRsp.GetModelName()
		embeddingModel.ModelAliasName = modelRsp.GetAliasName()
	}

	return embeddingModel, nil
}

// setShareKnowledgeEmbeddingModelConfig 设置共享知识库的 embedding 模型配置
func (l *Logic) setShareKnowledgeEmbeddingModelConfig(ctx context.Context, knowledgeBizID uint64, embeddingModel kbe.KnowledgeModel) error {
	// 构建配置
	config := &kbe.KnowledgeConfig{
		CorpBizID:      contextx.Metadata(ctx).CorpBizID(),
		KnowledgeBizID: knowledgeBizID,
		AppBizID:       0,
		Type:           uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL),
		IsDeleted:      false,
	}

	// 序列化模型信息
	modelInfo := &pb.EmbeddingModel{
		ModelName: embeddingModel.ModelName,
		AliasName: embeddingModel.ModelAliasName,
	}

	configJSON, err := jsonx.Marshal(modelInfo)
	if err != nil {
		logx.E(ctx, "marshal embedding model config failed, err: %+v", err)
		return errs.ErrSystem
	}

	// 共享知识库只填 Config 字段
	config.Config = string(configJSON)

	// 保存配置
	if err := l.kbDao.SetKnowledgeConfig(ctx, config, l.kbDao.TDSQLQuery(), true); err != nil {
		logx.E(ctx, "SetKnowledgeConfig failed, err: %+v", err)
		return errs.ErrSystem
	}

	return nil
}

// ReferShareKnowledge 引用共享知识库的业务逻辑
func (l *Logic) ReferShareKnowledge(ctx context.Context, params *kbe.ReferShareKnowledgeRequest) error {
	logx.I(ctx, "ReferShareKnowledge logic start, params: %+v", params)

	// 1. 校验引用的知识库id是否合法
	shareKnowledgeFilter := kbe.ShareKnowledgeFilter{
		CorpBizID: params.CorpBizID,
		BizIds:    params.KnowledgeBizIDs,
	}
	checkList, err := l.kbDao.RetrieveBaseSharedKnowledge(ctx, &shareKnowledgeFilter)
	if err != nil {
		logx.E(ctx, "RetrieveBaseSharedKnowledge err:%v", err)
		return errs.ErrPermissionDenied
	}
	if len(checkList) != len(params.KnowledgeBizIDs) { // 说明有不合法的id
		logx.E(ctx, "invalid knowledgeBizId")
		return errs.ErrPermissionDenied
	}

	// 2. 获取应用引用共享库列表
	shareKGList, err := l.kbDao.GetAppShareKGList(ctx, params.AppBizID)
	if err != nil {
		logx.E(ctx, "GetAppShareKGList failed, err: %+v", err)
		return errs.ErrGetAppShareKGListFailed
	}
	var oldList []uint64
	for _, shareKG := range shareKGList {
		oldList = append(oldList, shareKG.KnowledgeBizID)
	}
	newList := params.KnowledgeBizIDs

	// 3. 对比差异，看哪些是新增，哪些是删除
	addList := l.diffSlice(newList, oldList)
	delList := l.diffSlice(oldList, newList)

	// 4. 新增
	if len(addList) > 0 {
		if err := l.addShareKnowledgeReferences(ctx, params, addList); err != nil {
			return err
		}
	}

	// 5. 删除
	if len(delList) > 0 {
		logx.I(ctx, "ReferShareKnowledge delList : %+v", delList)
		err = l.MultiUnbindShareKb(ctx, params.CorpPrimaryID, params.CorpBizID, params.AppBizID, delList)
		if err != nil {
			logx.E(ctx, "DeleteKnowledgeAssociation failed, err: %+v", err)
			return errs.ErrSetAppShareKGFailed
		}
	}

	// 6. 更新App(需要触发app-config更新缓存)
	if err := l.updateAppCache(ctx, params.AppPrimaryID); err != nil {
		logx.E(ctx, "updateAppCache failed, err: %+v", err)
	}

	// 7. 异步上报统计数据
	go func(newCtx context.Context) {
		defer gox.Recover()
		counterInfo := &commonLogic.CounterInfo{
			CorpBizId:       params.CorpBizID,
			SpaceId:         params.SpaceID,
			AppBizId:        params.AppBizID,
			StatisticObject: common.StatObject_STAT_OBJECT_KB,
			StatisticType:   common.StatType_STAT_TYPE_EDIT,
			ObjectId:        strconv.FormatUint(params.AppBizID, 10),
			ObjectName:      params.AppName,
			Count:           1,
		}
		commonLogic.Counter(newCtx, counterInfo, l.rpc)
	}(trpc.CloneContext(ctx))

	return nil
}

// addShareKnowledgeReferences 添加共享知识库引用
func (l *Logic) addShareKnowledgeReferences(ctx context.Context, params *kbe.ReferShareKnowledgeRequest, addList []uint64) error {
	var addShareKGList []*kbe.AppShareKnowledge
	var addKnowledgeConfigList []*kbe.KnowledgeConfig
	now := time.Now()
	defaultRetrievalConfig := l.getDefaultRetrievalConfigStr()

	for _, knowledgeBizID := range addList {
		addShareKGList = append(addShareKGList, &kbe.AppShareKnowledge{
			AppBizID:       params.AppBizID,
			KnowledgeBizID: knowledgeBizID,
			CorpBizID:      params.CorpBizID,
			UpdateTime:     now,
			CreateTime:     now,
		})
		addKnowledgeConfigList = append(addKnowledgeConfigList, &kbe.KnowledgeConfig{
			CorpBizID:      params.CorpBizID,
			KnowledgeBizID: knowledgeBizID,
			Type:           uint32(pb.KnowledgeBaseConfigType_RETRIEVAL_SETTING),
			IsDeleted:      false,
			CreateTime:     now,
			UpdateTime:     now,
			AppBizID:       params.AppBizID,
			PreviewConfig:  defaultRetrievalConfig,
		})
	}

	logx.I(ctx, "ReferShareKnowledge addShareKGList : %+v", addShareKGList)
	err := l.MultiBindShareKb(ctx, addShareKGList, addKnowledgeConfigList)
	if err != nil {
		logx.E(ctx, "CreateAppShareKG failed, err: %+v", err)
		return errs.ErrSetAppShareKGFailed
	}

	return nil
}

// diffSlice 计算两个切片的差集（在 a 中但不在 b 中的元素）
func (l *Logic) diffSlice(a, b []uint64) []uint64 {
	bMap := make(map[uint64]struct{}, len(b))
	for _, item := range b {
		bMap[item] = struct{}{}
	}

	var diff []uint64
	for _, item := range a {
		if _, exists := bMap[item]; !exists {
			diff = append(diff, item)
		}
	}
	return diff
}

// getDefaultRetrievalConfigStr 获取默认的检索配置字符串
func (l *Logic) getDefaultRetrievalConfigStr() string {
	retrievalConfig := &pb.RetrievalConfig{
		SearchStrategy: &pb.SearchStrategy{
			StrategyType:      pb.SearchStrategyTypeEnum_Mixing,
			TableEnhancement:  false,
			RerankModelSwitch: "on",
			RerankModel:       "sn-llm-reranker-2b-v0.2",
		},
	}
	retrievalInfos := make([]*pb.RetrievalInfo, 0, 4)
	for _, filter := range config.App().RobotDefault.Filters[entity.AppSearchPreviewFilterKey].Filter {
		isEnable := false // 非文档/问答/数据库默认关闭
		switch common.KnowledgeType(filter.DocType) {
		case common.KnowledgeType_KnowledgeTypeQa,
			common.KnowledgeType_KnowledgeTypeDoc,
			common.KnowledgeType_KnowledgeTypeDB:
			isEnable = true
		}
		switch common.KnowledgeType(filter.DocType) {
		case common.KnowledgeType_KnowledgeTypeQa,
			common.KnowledgeType_KnowledgeTypeSearch,
			common.KnowledgeType_KnowledgeTypeDoc,
			common.KnowledgeType_KnowledgeTypeTaskFlow,
			common.KnowledgeType_KnowledgeTypeDB:
			retrievalInfos = append(retrievalInfos, &pb.RetrievalInfo{
				RetrievalType: common.KnowledgeType(filter.DocType),
				IndexId:       filter.IndexID,
				Confidence:    filter.Confidence,
				TopN:          filter.TopN,
				IsEnable:      isEnable,
			})
		}
	}
	retrievalConfig.Retrievals = retrievalInfos
	// 将配置转换为JSON字符串
	configStr, _ := jsonx.Marshal(retrievalConfig)
	return string(configStr)
}

// updateAppCache 更新App缓存
func (l *Logic) updateAppCache(ctx context.Context, appPrimaryID uint64) error {
	modifyAppReq := appconfig.ModifyAppReq{
		Inner: &appconfig.ModifyAppInner{
			AppPrimaryId: appPrimaryID,
		},
	}
	if _, err := l.rpc.AppAdmin.ModifyApp(ctx, &modifyAppReq); err != nil {
		return err
	}
	return nil
}
