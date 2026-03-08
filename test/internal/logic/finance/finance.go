package finance

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/finance"
	"git.woa.com/adp/kb/kb-config/internal/logic/common"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	adpCommon "git.woa.com/adp/pb-go/common"
	pc "git.woa.com/adp/pb-go/platform/platform_charger"
	pm "git.woa.com/adp/pb-go/platform/platform_manager"
	dataStat "git.woa.com/adp/pb-go/platform/platform_metrology"
)

func NewLogic(rpc *rpc.RPC) *Logic {
	return &Logic{
		rpc: rpc,
	}
}

type Logic struct {
	rpc *rpc.RPC
}

// CheckKnowledgeBaseQuota 检查知识库是否超过配额
// 需要区分新老用户, 新用户配额是容量，老用户配额是字符数
// 新用户，如果超配额（容量），则需要判断是否有资源，有的话允许继续使用，但需要上报超量
// 老用户，如果超配额（字符数），则直接返回不可用
func (l *Logic) CheckKnowledgeBaseQuota(ctx context.Context, req finance.CheckQuotaReq) error {
	if req.App == nil {
		return errs.ErrSystem
	}
	newCtx := util.SetMultipleMetaData(ctx, req.App.SpaceId, req.App.Uin)
	// 判断单应用下是否超限
	appUsedCharSize := req.App.UsedCharSize + req.NewCharSize
	if appUsedCharSize > config.GetBotMaxCharSize(newCtx, req.App.BizId) {
		return errs.ErrBotOverCharacterSizeLimit
	}
	// 获取企业信息
	corp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(newCtx, req.App.CorpPrimaryId)
	if err != nil {
		logx.E(ctx, "CheckKnowledgeBaseQuota DescribeCorpByPrimaryId failed, corpID:%d, err:%+v", req.App.CorpPrimaryId, err)
		return errs.ErrCorpNotFound
	}
	if corp == nil {
		logx.E(ctx, "CheckKnowledgeBaseQuota corp is nil, corpID:%d", req.App.CorpPrimaryId)
		return errs.ErrCorpNotFound
	}
	// 调用DescribeAccountQuota获取配额信息
	quotaRsp, err := l.rpc.Finance.DescribeAccountQuota(newCtx, corp.GetUin(), corp.GetSid())
	if err != nil {
		logx.E(ctx, "CheckKnowledgeBaseQuota DescribeAccountQuota failed, uin:%s, sid:%d, err:%+v", corp.GetUin(), corp.GetSid(), err)
		// 降级处理，返回不超限
		return nil
	}
	if quotaRsp == nil {
		logx.E(ctx, "CheckKnowledgeBaseQuota quotaRsp is nil")
		return nil
	}
	var checkRsp finance.CheckQuotaResp
	// 根据IsPurchasePackage判断新老用户
	if quotaRsp.GetIsPackageScene() {
		// 新用户：基于容量判断
		checkRsp, err = l.CheckNewUserQuota(newCtx, req, corp, quotaRsp)
	} else {
		// 老用户：沿用字符数判断
		checkRsp, err = l.checkOldUserQuota(newCtx, req, corp)
	}
	if err != nil {
		return err
	}
	logx.I(ctx, "CheckKnowledgeBaseQuota checkRsp:%+v", checkRsp)
	if checkRsp.Status == finance.QuotaStatusExceeded { // 超量不可用
		return common.ConvertErrMsg(ctx, l.rpc, 0, req.App.CorpPrimaryId, errs.ErrOverCharacterSizeLimit)
	} else if checkRsp.Status == finance.QuotaStatusTolerated && req.NewKnowledgeCapacity > 0 { // 超量但有资源允许继续使用，需要做超量上报
		err = l.rpc.PlatformApi.ModifyCorpKnowledgeOverCapacity(ctx, req.App.CorpBizId, entity.CapacityUsage{
			KnowledgeCapacity: int64(checkRsp.KnowledgeCapacityExceeded),
			StorageCapacity:   int64(checkRsp.StorageCapacityExceeded),
			ComputeCapacity:   int64(checkRsp.ComputeCapacityExceeded),
		})
		if err != nil {
			logx.E(ctx, "CheckKnowledgeBaseQuota ModifyCorpKnowledgeOverCapacity failed, corpID:%d, err:%+v", req.App.CorpPrimaryId, err)
		}
	}
	return nil
}

// CheckNewUserQuota 新用户配额检查（基于容量）
func (l *Logic) CheckNewUserQuota(ctx context.Context, req finance.CheckQuotaReq, corp *pm.DescribeCorpRsp, quotaRsp *pc.DescribeAccountQuotaRsp) (finance.CheckQuotaResp, error) {
	var rsp finance.CheckQuotaResp

	// 获取企业下所有知识库的容量使用情况
	capacityUsage, err := l.rpc.AppApi.DescribeCorpKnowledgeCapacity(ctx, corp.GetCorpId(), []uint64{})
	if err != nil {
		logx.E(ctx, "CheckNewUserQuota DescribeCorpKnowledgeCapacity failed, corpID:%d, err:%+v", corp.GetCorpId(), err)
		return rsp, errs.ErrSystem
	}
	logx.D(ctx, "quota is %v, capacityUsage is %v", jsonx.MustMarshalToString(quotaRsp), jsonx.MustMarshalToString(capacityUsage))

	// 获取配额信息
	maxCapacity := uint64(quotaRsp.GetPackageDetail().GetKnowledgeCapacity()) * entity.ByteToGB // GB转字节

	// 计算新增后的使用量
	usedKnowledgeCapacity := uint64(capacityUsage.KnowledgeCapacity) + req.NewKnowledgeCapacity
	usedStorageCapacity := uint64(capacityUsage.StorageCapacity) + req.NewStorageCapacity
	usedComputeCapacity := uint64(capacityUsage.ComputeCapacity) + req.NewComputeCapacity

	// 分别检查三种容量是否超限
	knowledgeExceeded := usedKnowledgeCapacity > maxCapacity
	storageExceeded := usedStorageCapacity > maxCapacity
	computeExceeded := usedComputeCapacity > maxCapacity
	logx.I(ctx, "checkNewUserQuota knowledgeExceeded:%v, storageExceeded:%v, computeExceeded:%v", knowledgeExceeded, storageExceeded, computeExceeded)
	// 记录超限量
	if knowledgeExceeded {
		rsp.KnowledgeCapacityExceeded = usedKnowledgeCapacity - maxCapacity
	}
	if storageExceeded {
		rsp.StorageCapacityExceeded = usedStorageCapacity - maxCapacity
	}
	if computeExceeded {
		rsp.ComputeCapacityExceeded = usedComputeCapacity - maxCapacity
	}
	// 获取知识库容量状态
	rsp.Status = l.GetCapacityStatus(ctx, corp, entity.CapacityUsage{
		KnowledgeCapacity: int64(usedKnowledgeCapacity),
		StorageCapacity:   int64(usedStorageCapacity),
		ComputeCapacity:   int64(usedComputeCapacity),
	}, int64(maxCapacity))
	return rsp, nil
}

// GetCapacityStatus 获取知识库容量状态
func (l *Logic) GetCapacityStatus(ctx context.Context, corp *pm.DescribeCorpRsp, corpUsage entity.CapacityUsage, maxCapacity int64) finance.QuotaStatus {
	logx.I(ctx, "GetCapacityStatus corpUsage:%+v, maxCapacity:%d", corpUsage, maxCapacity)
	storageExceeded := corpUsage.StorageCapacity > maxCapacity
	computeExceeded := corpUsage.ComputeCapacity > maxCapacity

	// 如果存储容量或计算容量超限
	if storageExceeded || computeExceeded {
		logx.W(ctx, "GetNewUserCapacityStatus corpID:%d, knowledge:%d/%d GB, storage:%d/%d GB, compute:%d/%d GB",
			corp.GetCorpId(),
			corpUsage.KnowledgeCapacity/(entity.ByteToGB), maxCapacity/(entity.ByteToGB),
			corpUsage.StorageCapacity/(entity.ByteToGB), maxCapacity/(entity.ByteToGB),
			corpUsage.ComputeCapacity/(entity.ByteToGB), maxCapacity/(entity.ByteToGB))

		// 检查账户状态，判断是否允许继续使用（有资源则容忍超量）
		status, err := l.rpc.Finance.DescribeAccountStatus(ctx, corp.GetUin(), corp.GetSid(), "", rpc.KbCapacityOverStorage)
		if err == nil && status == 0 {
			return finance.QuotaStatusTolerated // 超量，但有pu资源，可以继续使用
		}
		return finance.QuotaStatusExceeded // 超量，无pu资源，不可继续使用
	}
	return finance.QuotaStatusAvailable
}

// checkOldUserQuota 老用户配额检查（基于字符数）
func (l *Logic) checkOldUserQuota(ctx context.Context, req finance.CheckQuotaReq, corp *pm.DescribeCorpRsp) (finance.CheckQuotaResp, error) {
	var rsp finance.CheckQuotaResp
	// 获取企业下所有应用的已使用字符数
	usedCharSize, err := l.rpc.AppAdmin.CountCorpAppCharSize(ctx, corp.GetCorpPrimaryId())
	if err != nil {
		logx.E(ctx, "checkOldUserQuota CountCorpAppCharSize failed, corpID:%d, err:%+v", corp.GetCorpId(), err)
		return rsp, errs.ErrSystem
	}
	usedCharSize += req.NewCharSize
	maxCharSize := uint64(0)
	// 获取企业的最大字符数
	if config.IsFinanceDisabled() { // 私有化场景，就使用企业信息中的最大字符数
		logx.D(ctx, "checkOldUserQuota private cloud, usedCharSize:%d", usedCharSize)
		maxCharSize = corp.GetMaxCharSize()
	} else {
		maxCharSize, err = l.rpc.Finance.GetCorpMaxCharSize(ctx, corp.GetSid(), corp.GetUin())
		if err != nil {
			logx.E(ctx, "checkOldUserQuota GetCorpMaxCharSize failed, corpID:%d, err:%+v", corp.GetCorpId(), err)
			return rsp, errs.ErrCorpNotFound
		}
	}
	// 检查是否超过字符数配额
	if usedCharSize > maxCharSize {
		logx.W(ctx, "checkOldUserQuota char size exceeded, corpID:%d, usedCharSize:%d, maxCharSize:%d",
			corp.GetCorpId(), usedCharSize, maxCharSize)
		rsp.Status = finance.QuotaStatusExceeded
		return rsp, nil
	}
	return rsp, nil
}

// GetTokenDosage 初始化token用量
func (l *Logic) GetTokenDosage(ctx context.Context, app *entity.App, modelName string) (*finance.TokenDosage, error) {
	logx.D(ctx, "GetTokenDosage modelName:%s", modelName)
	if app == nil {
		logx.E(ctx, "GetTokenDosage app is nil")
		return nil, errs.ErrAppNotFound
	}
	if modelName == "" {
		logx.E(ctx, "GetTokenDosage GetAppModelName failed, modelName is empty")
		return nil, errs.ErrNotInvalidModel
	}
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
	aliasName := ""
	rsp, err := l.rpc.Resource.GetModelInfo(newCtx, 0, modelName)
	if err != nil {
		// 降级处理，获取 模型别名 AliasName
		logx.W(ctx, "GetTokenDosage GetModelInfo failed, modelName:%s, err:%+v", modelName, err)
	}
	aliasName = rsp.GetAliasName()

	dosage := &finance.TokenDosage{
		AppID:           app.BizId,
		AppType:         app.AppType,
		SpaceID:         app.SpaceId,
		ModelName:       modelName,
		AliasName:       aliasName,
		InputDosages:    []int{},
		OutputDosages:   []int{},
		KnowledgeBaseID: app.BizId,
	}
	if app.IsShared {
		dosage.AppType = rpc.TokenDosageAppTypeSharedKnowledge
	}
	logx.D(ctx, "GetTokenDosage, dosage%+v", dosage)
	return dosage, nil
}

// CheckModelIsFree 检查模型是否是免费的
func (l *Logic) CheckModelIsFree(ctx context.Context, modelName string, uin string, sid uint64) bool {
	getModelInfoByModelNameRsp, err := l.rpc.Resource.GetModelFreeStatus(ctx, []string{modelName}, uin, sid)
	if err != nil {
		logx.E(ctx, "CheckModelStatus failed, modelName:%s, err:%+v", modelName, err)
		// 降级处理
		return true
	}
	// 安全处理modelMap为nil或GetModelFinanceInfo()为nil的情况降级处理
	if getModelInfoByModelNameRsp == nil || getModelInfoByModelNameRsp.GetModelInfo() == nil {
		logx.E(ctx, "CheckModelStatus model finance info is nil for model:%s", modelName)
		// 降级处理
		return true
	}
	// 检查模型是否免费
	info, ok := getModelInfoByModelNameRsp.GetModelInfo()[modelName]
	if !ok || info.GetIsFree() {
		// 结果中不存或明确标记免费，则返回true
		// https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800122766473
		// 预置模型有可能是免费的，所以有is_free，另外就是之前自定义模型也是免费，但是现在自定义模型不免费
		return true
	}
	return false
}

// CheckModelIsFreeBatch 批量检查模型是否是免费的
func (l *Logic) CheckModelIsFreeBatch(ctx context.Context, modelNames []string, uin string, sid uint64) map[string]finance.ModelStatus {
	result := make(map[string]finance.ModelStatus)

	// 如果传入空数组，直接返回空结果
	if len(modelNames) == 0 {
		return result
	}

	// 批量获取模型信息
	getModelInfoByModelNameRsp, err := l.rpc.Resource.GetModelFreeStatus(ctx, modelNames, uin, sid)
	if err != nil {
		logx.E(ctx, "CheckModelIsFreeBatch GetModelFreeStatus failed, modelNames:%v, err:%+v", modelNames, err)
		// 降级处理，所有模型返回免费
		for _, modelName := range modelNames {
			result[modelName] = finance.ModelStatus{
				IsFree: true,
			}
		}
		return result
	}

	// 安全处理modelMap为nil或GetModelFinanceInfo()为nil的情况降级处理
	if getModelInfoByModelNameRsp == nil || getModelInfoByModelNameRsp.GetModelInfo() == nil {
		logx.E(ctx, "CheckModelIsFreeBatch model finance info is nil for models:%v", modelNames)
		// 降级处理，所有模型返回免费
		for _, modelName := range modelNames {
			result[modelName] = finance.ModelStatus{
				IsFree: true,
			}
		}
		return result
	}

	// 检查每个模型是否免费
	modelInfoMap := getModelInfoByModelNameRsp.GetModelInfo()
	for _, modelName := range modelNames {
		info, ok := modelInfoMap[modelName]
		if !ok || info.GetIsFree() {
			// 结果中不存在或明确标记免费，则返回true
			result[modelName] = finance.ModelStatus{
				IsFree:       true,
				ProviderType: info.GetProviderType(),
			}
		} else {
			result[modelName] = finance.ModelStatus{
				IsFree:       false,
				ProviderType: info.GetProviderType(),
			}
		}
	}

	return result
}

// CheckModelStatus 检查模型用量是否可用
func (l *Logic) CheckModelStatus(ctx context.Context, corpID uint64, modelName string, subBizType string) bool {
	if config.IsFinanceDisabled() {
		// 关闭计费返回可用
		return true
	}
	corp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpID)
	if err != nil {
		logx.E(ctx, "CheckModelStatusBySubBizType GetCorpByBusinessID failed, err=%+v", err)
		// 降级处理
		return true
	}
	if corp == nil {
		logx.E(ctx, "CheckModelStatusBySubBizType GetCorpByID is nil , corpID=%+v", corpID)
		// 降级处理
		return true
	}
	if l.CheckModelIsFree(ctx, modelName, corp.GetUin(), corp.GetSid()) {
		// 模型是免费的，返回可用
		return true
	}
	financeModelName := modelName
	if subBizType == rpc.RealTimeDocParseFinanceBizType && strings.HasPrefix(modelName, "Youtu/") {
		financeModelName = rpc.DocParseCommonModel
	}
	status, err := l.rpc.Finance.DescribeAccountStatus(ctx, corp.Uin, corp.GetSid(), financeModelName, subBizType)
	if err != nil {
		logx.E(ctx, "CheckModelStatus DescribeAccountStatus err:%+v", err)
		// 降级处理
		return true
	}
	if status != 0 {
		logx.D(ctx, "CheckModelStatus DescribeAccountStatus status:%d|err:%+v", status, errs.ErrNoTokenBalance)
		return false
	}
	return true
}

// CheckModelStatusBatch 批量检查模型用量是否可用
func (l *Logic) CheckModelStatusBatch(ctx context.Context, corpID uint64, reqs []finance.ModelStatusReq) map[string]bool {
	logx.I(ctx, "CheckModelStatusBatch corpID:%d, reqs:%+v", corpID, jsonx.MustMarshalToString(reqs))
	result := make(map[string]bool)
	// 如果关闭计费，所有模型都返回可用
	if config.IsFinanceDisabled() {
		for _, req := range reqs {
			result[req.OriModelName] = true
		}
		return result
	}

	// 提取模型名称用于批量检查
	modelNames := make([]string, 0, len(reqs))
	for _, req := range reqs {
		modelNames = append(modelNames, req.OriModelName)
	}

	// 获取企业信息
	corp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpID)
	if err != nil {
		logx.E(ctx, "CheckModelStatusBatch GetCorpByBusinessID failed, err=%+v", err)
		// 降级处理，所有模型返回可用
		for _, req := range reqs {
			result[req.OriModelName] = true
		}
		return result
	}
	if corp == nil {
		logx.E(ctx, "CheckModelStatusBatch GetCorpByID is nil , corpID=%+v", corpID)
		// 降级处理，所有模型返回可用
		for _, req := range reqs {
			result[req.OriModelName] = true
		}
		return result
	}

	// 使用批量接口检查所有模型是否免费
	freeModels := l.CheckModelIsFreeBatch(ctx, modelNames, corp.GetUin(), corp.GetSid())

	// 并行检查非免费模型的账户状态
	var wg sync.WaitGroup
	resultChan := make(chan struct {
		modelName string
		status    bool
	}, len(reqs))
	for _, req := range reqs {
		status := freeModels[req.OriModelName]
		if status.IsFree {
			result[req.OriModelName] = true // 免费模型直接设置为true
			continue
		}
		wg.Add(1)
		go func(req finance.ModelStatusReq) {
			defer gox.Recover()
			defer wg.Done()

			status, err := l.rpc.Finance.DescribeAccountStatus(ctx, corp.Uin, corp.GetSid(), req.BillingModelName, req.SubBizType)
			if err != nil {
				logx.E(ctx, "CheckModelStatusBatch DescribeAccountStatus err for model %s: %+v", req.OriModelName, err)
				// 降级处理，返回可用
				resultChan <- struct {
					modelName string
					status    bool
				}{modelName: req.OriModelName, status: true}
				return
			}
			if status != 0 {
				logx.D(ctx, "CheckModelStatusBatch DescribeAccountStatus status:%d for model %s", status, req.OriModelName)
				resultChan <- struct {
					modelName string
					status    bool
				}{modelName: req.OriModelName, status: false}
			} else {
				resultChan <- struct {
					modelName string
					status    bool
				}{modelName: req.OriModelName, status: true}
			}
		}(req)
	}

	// 启动goroutine等待所有任务完成并关闭channel
	go func() {
		defer gox.Recover()
		wg.Wait()
		close(resultChan)
	}()

	// 收集结果
	for res := range resultChan {
		result[res.modelName] = res.status
	}
	logx.I(ctx, "CheckModelStatusBatch result:%v", jsonx.MustMarshalToString(result))
	return result
}

// ReportTokenDosage 上报模型用量
func (l *Logic) ReportTokenDosage(ctx context.Context, tokenStatisticInfo *rpc.StatisticInfo,
	dosage *finance.TokenDosage, corpID uint64, subBizType string, app *entity.App) error {
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)
	if dosage == nil {
		logx.E(ctx, "ReportTokenDosage, dosage is nil")
		// 降级处理
		return nil
	}
	if tokenStatisticInfo == nil {
		logx.E(ctx, "ReportTokenDosage, tokenStatisticInfo is nil")
		// 降级处理
		return nil
	}
	corp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpID)
	if err != nil {
		logx.E(ctx, "ReportTokenDosage GetCorpByBusinessID failed, err=%+v", err)
		// 降级处理
		return nil
	}
	if corp == nil {
		logx.E(ctx, "ReportTokenDosage GetCorpByID is nil , corpID=%+v", corpID)
		// 降级处理
		return nil
	}
	if l.CheckModelIsFree(newCtx, dosage.ModelName, corp.GetUin(), corp.GetSid()) {
		dosage.SkipBilling = true // 【重要】模型是免费的，不用上报计费，但需要上报数据统计
	}
	dosage.InputDosages = []int{int(tokenStatisticInfo.InputTokens)}
	dosage.OutputDosages = []int{int(tokenStatisticInfo.OutputTokens)}
	dosage.RecordID = fmt.Sprintf("%d", idgen.GetId()) // 每次上报生成唯一ID
	dosage.EndTime = time.Now()
	logx.D(ctx, "ReportTokenDosage task reportTokenFinance, dosage%+v", dosage)

	if len(dosage.InputDosages) > 0 || len(dosage.OutputDosages) > 0 {
		if err = l.rpc.DataStat.ReportDosage(ctx, corp, dosage, subBizType); err != nil {
			logx.E(ctx, "ReportTokenDosage` failed, err:%+v", err)
			// 降级处理
			return nil
		}
		// 上报统计数据
		go func(newCtx context.Context) { // 异步上报
			defer gox.Recover()
			counterInfo := &common.CounterInfo{
				CorpBizId:       contextx.Metadata(ctx).CorpBizID(),
				AppBizId:        app.BizId,
				StatisticObject: adpCommon.StatObject_STAT_OBJECT_MODEL,
				StatisticType:   adpCommon.StatType_STAT_TYPE_CALL,
				ObjectId:        dosage.ModelName,
				ObjectName:      dosage.ModelName,
				Count:           uint64(tokenStatisticInfo.TotalTokens),
			}
			common.Counter(newCtx, counterInfo, l.rpc)
		}(trpc.CloneContext(ctx))
	}
	return nil
}

// FinanceReportToken 上报模型计费用量
func (l *Logic) FinanceReportToken(ctx context.Context, dosage *finance.TokenDosage, corpID uint64, subBizType string, app *entity.App) error {
	contextx.Metadata(ctx).WithSpaceID(app.SpaceId)
	if dosage == nil {
		logx.E(ctx, "FinanceReportToken, dosage is nil")
		// 降级处理
		return nil
	}
	dosage.RecordID = fmt.Sprintf("%d", idgen.GetId()) // 每次上报生成唯一ID
	dosage.EndTime = time.Now()
	logx.D(ctx, "FinanceReportToken task reportTokenFinance, dosage%+v", dosage)
	if len(dosage.InputDosages) > 0 || len(dosage.OutputDosages) > 0 {
		corp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpID)
		if err != nil {
			logx.E(ctx, "FinanceReportToken task reportTokenFinance, GetCorpByID failed, err:%+v", err)
			// 降级处理
			return nil
		}
		if err = l.rpc.Finance.ReportTokenDosage(ctx, corp, dosage, subBizType); err != nil {
			logx.E(ctx, "FinanceReportToken failed, err:%+v", err)
			// 降级处理
			return nil
		}
	}
	return nil
}

func (l *Logic) ReportBusinessUsage(ctx context.Context, appBizID uint64) error {
	gox.GoWithContext(ctx, func(ctx context.Context) {
		l.rpc.DataStat.ReportBusinessUsage(ctx, &dataStat.ReportBusinessUsageReq{
			UsageType:  dataStat.BusinessUsageType_BUSINESS_USAGE_TYPE_COMPONENT_CALL,
			AppBizId:   strconv.FormatUint(appBizID, 10),
			RecordId:   uuid.NewString(),
			RecordTime: time.Now().Unix(),
			ComponentCallUsage: &dataStat.ComponentCallUsage{
				ComponentType: dataStat.ComponentType_COMPONENT_TYPE_KNOWLEDGE,
				Count:         1,
			},
		})
	})
	return nil
}

// UpdateAppCapacityUsage 更新应用容量使用情况
func (l *Logic) UpdateAppCapacityUsage(ctx context.Context, usage entity.CapacityUsage, appPrimaryID, corpPrimaryID uint64) error {
	logx.I(ctx, "UpdateAppCapacityUsage, appPrimaryID:%d, usage:%+v", appPrimaryID, usage)
	// 更新应用容量使用情况
	if err := l.rpc.AppAdmin.UpdateAppUsage(ctx, usage, appPrimaryID); err != nil {
		logx.E(ctx, "UpdateAppCapacityUsage failed, appPrimaryID:%d, err:%+v", appPrimaryID, err)
		return err
	}
	// 如果有减容量，需要再判断下是否需要更新超量失效容量
	if usage.CharSize < 0 || usage.KnowledgeCapacity < 0 {
		// 先查下现在是否有超量上报
		corp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpPrimaryID)
		if err != nil {
			logx.E(ctx, "DescribeCorpByPrimaryId failed, corpPrimaryID:%d, err:%+v", corpPrimaryID, err)
			return err
		}
		if corp.OverKnowledgeCapacity == 0 && corp.OverStorageCapacity == 0 && corp.OverComputeCapacity == 0 {
			// 没有超量上报，不需要更新
			return nil
		}
		// 重置超量信息
		resetOverCapacity := entity.CapacityUsage{
			KnowledgeCapacity: max(0, int64(corp.OverKnowledgeCapacity)+usage.KnowledgeCapacity),
			StorageCapacity:   max(0, int64(corp.OverStorageCapacity)+usage.StorageCapacity),
			ComputeCapacity:   max(0, int64(corp.OverComputeCapacity)+usage.ComputeCapacity),
		}
		logx.I(ctx, "ResetCorpKnowledgeOverCapacity, corpPrimaryID:%d, resetOverCapacity:%+v", corpPrimaryID, resetOverCapacity)
		if err := l.rpc.PlatformApi.ResetCorpKnowledgeOverCapacity(ctx, corp.CorpId, resetOverCapacity); err != nil {
			logx.E(ctx, "ResetCorpKnowledgeOverCapacity failed, corpPrimaryID:%d, err:%+v", corpPrimaryID, err)
			return err
		}
	}
	return nil
}
