package corp

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/data_statistics"
	logicKnowConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model/billing"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	statistics "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_data_statistics_server"

	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
)

// GetTokenDosage 初始化token用量
func GetTokenDosage(ctx context.Context, appBizID uint64, modelName string, configType uint32) (*billing.TokenDosage, error) {
	app, err := client.GetAppInfo(ctx, appBizID, model.AppTestScenes)
	if err != nil {
		log.ErrorContextf(ctx, "GetTokenDosage GetAppByID failed, err:%+v", err)
		return nil, err
	}
	if app.GetIsDelete() {
		log.DebugContextf(ctx, "GetTokenDosage appDB.HasDeleted()|ignore")
		return nil, errs.ErrAppNotFound
	}
	if modelName == "" {
		modelName, err = logicKnowConfig.GetKnowledgeBaseConfig(ctx, app.GetCorpBizId(), app.GetAppBizId(), configType)
		if err != nil {
			log.ErrorContextf(ctx, "GetTokenDosage GetKnowledgeBaseConfig err: %+v", err)
			return nil, err
		}
	}

	log.DebugContextf(ctx, "GetTokenDosage GetAppNormalModelName modelName:%s", modelName)
	if modelName == "" {
		log.ErrorContextf(ctx, "GetTokenDosage GetAppModelName failed, modelName is empty")
		return nil, errs.ErrNotInvalidModel
	}

	aliasName := ""
	rsp, err := knowClient.GetModelInfo(ctx, 0, modelName)
	if err != nil {
		// 降级处理，获取 模型别名 AliasName
		log.WarnContextf(ctx, "GetTokenDosage GetModelInfo failed, modelName:%s, err:%+v", modelName, err)
	}
	aliasName = rsp.GetAliasName()

	dosage := &billing.TokenDosage{
		AppID:         app.GetAppBizId(),
		AppType:       app.GetAppType(),
		ModelName:     modelName,
		AliasName:     aliasName,
		InputDosages:  []int{},
		OutputDosages: []int{},
	}
	if app.GetIsShareKnowledgeBase() {
		dosage.AppType = client.TokenDosageAppTypeSharedKnowledge
	}
	log.DebugContextf(ctx, "GetTokenDosage, dosage%+v", dosage)
	return dosage, nil
}

// CheckModelStatus 检查模型用量是否可用
func CheckModelStatus(ctx context.Context, db dao.Dao, corpID uint64, modelName string, subBizType string) bool {
	if config.IsFinanceDisabled() {
		// 关闭计费返回可用
		return true
	}
	modelMap, err := client.GetModelFinanceInfo(ctx, []string{modelName})
	if err != nil {
		log.ErrorContextf(ctx, "CheckModelStatus failed, modelName:%s, err:%+v", modelName, err)
		// 降级处理
		return true
	}
	// 安全处理modelMap为nil或GetModelFinanceInfo()为nil的情况降级处理
	if modelMap == nil || modelMap.GetModelFinanceInfo() == nil {
		log.ErrorContextf(ctx, "CheckModelStatus model finance info is nil for model:%s", modelName)
		// 降级处理
		return true
	}
	// 检查模型是否免费
	if info, ok := modelMap.GetModelFinanceInfo()[modelName]; ok && info.GetIsFree() {
		// https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800122766473
		// 预置模型有可能是免费的，所以有is_free，另外就是之前自定义模型也是免费，但是现在自定义模型不免费
		return true
	}
	corp, err := db.GetCorpByID(ctx, corpID)
	if err != nil {
		log.ErrorContextf(ctx, "CheckModelStatusBySubBizType GetCorpByBusinessID failed, err=%+v", err)
		// 降级处理
		return true
	}
	if corp == nil {
		log.ErrorContextf(ctx, "CheckModelStatusBySubBizType GetCorpByID is nil , corpID=%+v", corpID)
		// 降级处理
		return true
	}
	status, err := client.DescribeAccountStatus(ctx, corp.Uin, corp.SID, modelName, subBizType)
	if err != nil {
		log.ErrorContextf(ctx, "CheckModelStatus DescribeAccountStatus err:%+v", err)
		// 降级处理
		return true
	}
	if status != 0 {
		log.DebugContextf(ctx, "CheckModelStatus DescribeAccountStatus status:%d|err:%+v", status, errs.ErrNoTokenBalance)
		return false
	}
	return true
}

// ReportTokenDosage 上报模型用量
func ReportTokenDosage(ctx context.Context, tokenStatisticInfo *client.StatisticInfo,
	dosage *billing.TokenDosage, dao dao.Dao, corpID uint64, subBizType string, appBizId uint64) error {
	if dosage == nil {
		log.ErrorContextf(ctx, "ReportTokenDosage, dosage is nil")
		// 降级处理
		return nil
	}
	if tokenStatisticInfo == nil {
		log.ErrorContextf(ctx, "ReportTokenDosage, tokenStatisticInfo is nil")
		// 降级处理
		return nil
	}
	modelMap, err := client.GetModelFinanceInfo(ctx, []string{dosage.ModelName})
	if err != nil {
		log.ErrorContextf(ctx, "ReportTokenDosage Failed, modelNames:%s, err:%+v", dosage.ModelName, err)
		// 降级处理
		return nil
	}
	// 安全处理modelMap为nil或GetModelFinanceInfo()为nil的情况降级处理
	if modelMap == nil || modelMap.GetModelFinanceInfo() == nil {
		log.ErrorContextf(ctx, "ReportTokenDosage model finance info is nil for model:%s", dosage.ModelName)
		// 降级处理
		return nil
	}
	// 检查模型是否免费
	if info, ok := modelMap.GetModelFinanceInfo()[dosage.ModelName]; ok && info.GetIsFree() {
		// https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800122766473
		// 预置模型有可能是免费的，所以有is_free，另外就是之前自定义模型也是免费，但是现在自定义模型不免费
		return nil
	}
	dosage.InputDosages = []int{int(tokenStatisticInfo.InputTokens)}
	dosage.OutputDosages = []int{int(tokenStatisticInfo.OutputTokens)}
	dosage.RecordID = fmt.Sprintf("%d", dao.GenerateSeqID()) // 每次上报生成唯一ID
	dosage.EndTime = time.Now()
	log.DebugContextf(ctx, "ReportTokenDosage task reportTokenFinance, dosage%+v", dosage)

	if len(dosage.InputDosages) > 0 || len(dosage.OutputDosages) > 0 {
		corp, err := dao.GetCorpByID(ctx, corpID)
		if err != nil {
			log.ErrorContextf(ctx, "ReportTokenDosage task reportTokenFinance, GetCorpByID failed, err:%+v", err)
			// 降级处理
			return nil
		}
		if err = client.ReportTokenDosage(ctx, corp, dosage, subBizType); err != nil {
			log.ErrorContextf(ctx, "ReportTokenDosage failed, err:%+v", err)
			// 降级处理
			return nil
		}
		// 上报统计数据
		go func(newCtx context.Context) { //异步上报
			counterInfo := &data_statistics.CounterInfo{
				CorpBizId:       pkg.CorpBizID(newCtx),
				AppBizId:        appBizId,
				StatisticObject: statistics.StatObject_STAT_OBJECT_MODEL,
				StatisticType:   statistics.StatType_STAT_TYPE_CALL,
				ObjectId:        dosage.ModelName,
				ObjectName:      dosage.ModelName,
				Count:           uint64(tokenStatisticInfo.TotalTokens),
			}
			data_statistics.Counter(newCtx, counterInfo)
		}(trpc.CloneContext(ctx))
	}
	return nil
}
