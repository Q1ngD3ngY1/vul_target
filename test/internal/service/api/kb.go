package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cast"

	"git.woa.com/adp/common/x/syncx/errgroupx"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/logx/auditx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	kbLogic "git.woa.com/adp/kb/kb-config/internal/logic/kb"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	appCommon "git.woa.com/adp/pb-go/app/common"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// DescribeKnowledgeBase 查询知识库信息
func (s *Service) DescribeKnowledgeBase(ctx context.Context, req *pb.DescribeKnowledgeBaseReq) (*pb.DescribeKnowledgeBaseRsp, error) {
	return s.svc.DescribeKnowledgeBase(ctx, req)
}

// InnerGetKnowledgeBaseConfig 获取知识库配置
// Deprecated: 3.0 大重构完全上线后随时可以删除，就不可能有调用方了
func (s *Service) InnerGetKnowledgeBaseConfig(ctx context.Context, req *pb.InnerGetKnowledgeBaseConfigReq) (*pb.InnerGetKnowledgeBaseConfigRsp, error) {
	rsp := new(pb.InnerGetKnowledgeBaseConfigRsp)
	wg, wgCtx := errgroupx.WithContext(ctx)
	wg.SetLimit(10)
	for _, knowledgeBizId := range req.GetKnowledgeBizIds() {
		wg.Go(func() error {
			configRsp, err := s.svc.GetKnowledgeBaseConfig(wgCtx, &pb.GetKnowledgeBaseConfigReq{
				KnowledgeBizId: knowledgeBizId,
				ConfigTypes:    req.GetConfigTypes(),
			})
			if err != nil {
				return err
			}
			rsp.KnowledgeBaseConfigs = append(rsp.KnowledgeBaseConfigs, configRsp.GetKnowledgeBaseConfig())
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return nil, err
	}
	return rsp, nil
}

// InnerSetKnowledgeBaseConfig 设置知识库配置
// Deprecated: 3.0 大重构完全上线后随时可以删除，就不可能有调用方了
func (s *Service) InnerSetKnowledgeBaseConfig(ctx context.Context, req *pb.InnerSetKnowledgeBaseConfigReq) (*pb.InnerSetKnowledgeBaseConfigRsp, error) {
	wg, wgCtx := errgroupx.WithContext(ctx)
	wg.SetLimit(10)
	for _, v := range req.GetKnowledgeBaseConfigs() {
		wg.Go(func() error {
			_, err := s.svc.SetKnowledgeBaseConfig(wgCtx, &pb.SetKnowledgeBaseConfigReq{
				KnowledgeBizId:       v.GetKnowledgeBizId(),
				ConfigTypes:          v.GetConfigTypes(),
				ThirdAclConfig:       v.GetThirdAclConfig(),
				EmbeddingModel:       v.GetEmbeddingModel(),
				QaExtractModel:       v.GetQaExtractModel(),
				KnowledgeSchemaModel: v.GetKnowledgeSchemaModel(),
			})
			return err
		})
	}
	if err := wg.Wait(); err != nil {
		return nil, err
	}
	return &pb.InnerSetKnowledgeBaseConfigRsp{}, nil
}

// ReferShareKnowledge 引用共享知识库
func (s *Service) ReferShareKnowledge(ctx context.Context, req *pb.ReferSharedKnowledgeReq) (*pb.ReferSharedKnowledgeRsp, error) {
	return s.svc.ReferShareKnowledge(ctx, req)
}

// ListReferShareKnowledge 列出引用的共享知识库
func (s *Service) ListReferShareKnowledge(ctx context.Context, req *pb.ListReferSharedKnowledgeReq) (*pb.ListReferSharedKnowledgeRsp, error) {
	return s.svc.ListReferShareKnowledge(ctx, req)
}

// GetModelAssociatedApps 获取模型关联的应用
// NOTE(ericjwang): 有调用， /trpc.KEP.bot_admin_config_server.Admin/DescribeModelProviderApps --> here
func (s *Service) GetModelAssociatedApps(ctx context.Context, req *pb.GetModelAssociatedAppsReq) (*pb.GetModelAssociatedAppsRsp, error) {
	resp := &pb.GetModelAssociatedAppsRsp{}
	spaceID := contextx.Metadata(ctx).SpaceID()
	logx.I(ctx, "GetModelAssociatedApps, request: %+v, spaceID:%s", req, spaceID)
	knowledgeBaseInfoList, err := s.kbLogic.GetModelAssociatedApps(ctx, req.GetCorpBizId(), req.GetModelKeyword(), spaceID)
	if err != nil {
		return nil, err
	}
	resp.KnowledgeBases = knowledgeBaseInfoList
	logx.I(ctx, "GetModelAssociatedApps response: %+v", resp)
	return resp, nil
}

// BatchGetSharedKnowledge 批量获取共享知识库
func (s *Service) BatchGetSharedKnowledge(ctx context.Context, req *pb.BatchGetSharedKnowledgeReq) (
	*pb.BatchGetSharedKnowledgeRsp, error) {
	start := time.Now()

	var err error
	rsp := new(pb.BatchGetSharedKnowledgeRsp)

	logx.I(ctx, "BatchGetSharedKnowledge, request: %+v", req)
	defer func() {
		logx.I(ctx, "BatchGetSharedKnowledge, response: %+v, elapsed: %d, error: %+v",
			rsp, time.Since(start).Milliseconds(), err)
	}()
	if !kbLogic.VerifyData([]*kbLogic.DataValidation{
		{
			Data:      req.GetCorpBizId(),
			Validator: kbLogic.NewRangeValidator(kbLogic.WithMin(1))},
		{
			Data:      len(req.GetKnowledgeBizIdList()),
			Validator: kbLogic.NewRangeValidator(kbLogic.WithMin(1))},
	}) {
		err = errs.ErrParameterInvalid
		return rsp, err
	}
	// NOTICE: 内部服务接口不做账号鉴权
	shareKnowledgeFilter := kbe.ShareKnowledgeFilter{
		CorpBizID: req.GetCorpBizId(),
		BizIds:    req.GetKnowledgeBizIdList(),
	}
	knowledgeList, err := s.kbDao.RetrieveBaseSharedKnowledge(ctx, &shareKnowledgeFilter)
	if err != nil {
		if errors.Is(err, errx.ErrNotFound) {
			// NOTICE: 未找到任何记录 不报错pkg.ErrSharedKnowledgeRecordNotFound
			logx.W(ctx, "RetrieveBaseSharedKnowledge without any record: err: %+v", err)
			return rsp, nil
		} else {
			return rsp, errs.ErrQuerySharedKnowledgeRecordFailed
		}
	}
	logx.I(ctx, "BatchGetSharedKnowledge, GetKnowledgeBizIdList.size: %d, knowledgeList(%d): %+v",
		len(req.GetKnowledgeBizIdList()), len(knowledgeList), knowledgeList)

	// NOTICE: 检索模型配置
	knowledgeList, err = s.kbLogic.RetrieveModelConfig(ctx, req.GetCorpBizId(), knowledgeList)
	if err != nil {
		return rsp, errs.ErrQueryKnowledgeModelConfigFailed
	}

	infoList := make([]*pb.KnowledgeBaseInfo, 0)
	for _, item := range knowledgeList {
		knowledge, _ := kbLogic.ConvertSharedKnowledgeBaseInfo(ctx, item)
		infoList = append(infoList, knowledge)
	}

	rsp.InfoList = infoList
	return rsp, nil
}

// ClearSpaceKnowledge 清理空间知识库，删除空间时调用
func (s *Service) ClearSpaceKnowledge(ctx context.Context, req *pb.ClearSpaceKnowledgeReq) (*pb.ClearSpaceKnowledgeRsp, error) {
	rsp := &pb.ClearSpaceKnowledgeRsp{}
	logx.I(ctx, "ClearSpaceKnowledge, request: %+v", req)

	err := s.kbLogic.ClearSpaceKnowledge(ctx, req.GetCorpBizId(), req.GetSpaceId())
	if err != nil {
		logx.E(ctx, "ClearSpaceKnowledge failed, err=%+v", err)
		return rsp, err
	}

	return rsp, nil
}

// GetSpaceShareKnowledgeList 获取空间共享知识列表
func (s *Service) GetSpaceShareKnowledgeList(
	ctx context.Context,
	req *pb.GetSpaceShareKnowledgeListReq) (*pb.GetSpaceShareKnowledgeListRsp, error) {
	rsp := &pb.GetSpaceShareKnowledgeListRsp{}
	logx.I(ctx, "GetSpaceShareKnowledgeList, request: %+v", req)
	total, list, err := s.kbLogic.GetSpaceShareKnowledgeListExSelf(ctx, req.GetCorpBizId(), req.GetExcludeStaffId(), req.GetSpaceId(),
		req.GetKeyword(), req.GetPageNumber(), req.GetPageSize())
	if err != nil {
		logx.E(ctx, "GetSpaceShareKnowledgeListExSelf fail, err=%+v", err)
		return rsp, err
	}
	rsp.Total = uint32(total)
	rsp.ShareKnowledgeList = list
	return rsp, nil
}

// ModifyKBConfigList 设置知识配置
func (s *Service) ModifyKBConfigList(ctx context.Context, req *pb.ModifyKBConfigListReq) (*pb.ModifyKBConfigListRsp, error) {
	logx.I(ctx, "Api ModifyKBConfigList, request: %+v", req)
	var err error
	// 如果需要审计，先获取原有配置
	corpBizId := contextx.Metadata(ctx).CorpBizID()
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByBizId(ctx, corpBizId)
	if err != nil {
		logx.E(ctx, "Failed to get corp (corpBizId:%d). err:%+v,", corpBizId, err)
		return nil, errs.ErrCorpNotFound
	}
	ctx = contextx.SetServerMetaData(ctx, contextx.MDUin, corp.Uin)
	appBizId := uint64(0)
	originKbConfigs := make([]*kbe.KnowledgeConfig, 0)
	updatedKBConfigs := make([]*kbe.KnowledgeConfig, 0)
	if req.NeedAudit {
		for _, pbKbConfig := range req.KnowledgeBaseConfigs {
			if pbKbConfig.AppBizId != "" && pbKbConfig.Scenes == entity.AppTestScenes {
				// 只有应用下的默认知识库的评测场景才需要审计
				appBizId = cast.ToUint64(pbKbConfig.AppBizId)
				knowledgeConfigs, err := s.svc.KnowledgeConfigPB2DO(ctx, pbKbConfig)
				if err != nil {
					logx.E(ctx, "ModifyKBConfigList KnowledgeConfigPB2DO fail, err=%+v", err)
					continue
				}
				updatedKBConfigs = append(updatedKBConfigs, knowledgeConfigs...)
			}
		}
		if appBizId > 0 {
			originKbConfigs, err = s.kbLogic.DescribeAppKnowledgeBaseConfigList(ctx, corpBizId, []uint64{appBizId}, true, 0)
			if err != nil {
				logx.E(ctx, "ModifyKBConfigList DescribeAppKnowledgeBaseConfigList fail, err=%+v", err)
			}
		}
	}

	// 设置新的配置
	err = s.svc.SetKnowledgeBaseConfigInfo(ctx, req.KnowledgeBaseConfigs)
	if err != nil {
		return nil, err
	}

	// 如果需要审计，记录配置变更
	if req.NeedAudit && len(updatedKBConfigs) > 0 {
		spaceId := ""
		appInfo, err := s.rpc.AppAdmin.GetAppBaseInfo(ctx, appBizId)
		if err != nil || appInfo == nil {
			logx.W(ctx, "ModifyKBConfigList GetAppBaseInfo fail, err=%+v, appInfo=%+v", err, appInfo)
			return &pb.ModifyKBConfigListRsp{}, nil
		} else {
			spaceId = appInfo.SpaceId
		}
		newCtx := util.SetMultipleMetaData(ctx, spaceId, appInfo.Uin)
		diffs, err := s.kbLogic.AppKnowledgeConfigAuditDiff(newCtx, corpBizId, originKbConfigs, updatedKBConfigs, spaceId)
		if err != nil {
			logx.E(newCtx, "ModifyKBConfigList AppKnowledgeConfigAuditDiff fail, err=%+v", err)
		} else {
			for _, diff := range diffs {
				logx.D(newCtx, "ModifyKBConfigList diff item: %+v", diff)
				content := diff.Content
				if content == "" {
					content = diff.LastValue + i18n.Translate(newCtx, kbe.KeyEdit) + diff.NewValue
				}
				auditx.Modify(auditx.BizApp).Space(appInfo.SpaceId).App(appInfo.BizId, appInfo.Name).
					Log(newCtx, appInfo.BizId, i18n.Translate(newCtx, diff.ConfigItem), content)
			}
		}
	}

	return &pb.ModifyKBConfigListRsp{}, nil
}

// DescribeKBConfigList 获取知识配置
func (s *Service) DescribeKBConfigList(ctx context.Context, req *pb.DescribeKBConfigListReq) (*pb.DescribeKBConfigListRsp, error) {
	logx.I(ctx, "DescribeKBConfigList, request: %+v", req)
	corpId := contextx.Metadata(ctx).CorpID()
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpId)
	if err != nil {
		logx.E(ctx, "Failed to get corp (corpId:%d). err:%+v,", corpId, err)
		return nil, errs.ErrCorpNotFound
	}
	newCtx := contextx.SetServerMetaData(ctx, contextx.MDUin, corp.Uin)
	newCtx = contextx.SetServerMetaData(newCtx, contextx.MDCorpBizID, cast.ToString(corp.CorpId))
	configList, err := s.svc.GetKnowledgeBaseConfigInfo(newCtx, req.GetKnowledgeBizIds(), req.GetAppBizId(),
		req.GetConfigTypes(), req.GetScenes(), req.GetReleasePrimaryId())
	if err != nil {
		return nil, err
	}
	rsp := &pb.DescribeKBConfigListRsp{
		KnowledgeBaseConfigs: configList,
	}
	logx.D(newCtx, "DescribeKBConfigList, response: %+v", rsp)
	return rsp, nil
}

func (s *Service) GetKnowledgeSchema(ctx context.Context, req *pb.GetKnowledgeSchemaReq) (*pb.GetKnowledgeSchemaRsp, error) {
	return s.svc.GetKnowledgeSchema(ctx, req)
}

// GlobalKnowledge 全局知识库
// 没有跟着机器人走, 也就意味着无法找到关联的 embedding 版本, 只能使用当前版本, 因此当 embedding 模型更新时, 需要刷新所有的全局知识
// NOTE(ericjwang): 看日志没有调用了，bot-op-server 里也没有调用
func (s *Service) GlobalKnowledge(ctx context.Context, req *pb.GlobalKnowledgeReq) (*pb.GlobalKnowledgeRsp, error) {
	return nil, errors.New("全局知识库功能已经下线")
}

// ListGlobalKnowledge 全局知识列表
// NOTE(ericjwang): 有零星调用，bot-op-server:/opapi/global_knowledge/list --> here
func (s *Service) ListGlobalKnowledge(ctx context.Context, req *pb.ListGlobalKnowledgeReq) (*pb.ListGlobalKnowledgeRsp, error) {
	return nil, errors.New("全局知识库功能已经下线")
}

// InitKB 初始化知识库
func (s *Service) InitKB(ctx context.Context, req *pb.InitKBReq) (*pb.InitKBRsp, error) {
	app, err := s.rpc.DescribeAppById(ctx, req.GetAppBizId())
	if err != nil {
		return nil, fmt.Errorf("DescribeAppById fail, invaild req:%+v err:%+v", req, err)
	}
	err = s.cateLogic.InitDefaultCategory(ctx, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		return nil, err
	}
	return &pb.InitKBRsp{}, nil
}

// CheckAppConfig 检查应用下知识库的配置有没有依赖缺失
func (s *Service) CheckAppConfig(ctx context.Context, req *appCommon.CheckAppConfigReq) (*appCommon.CheckAppConfigRsp, error) {
	logx.I(ctx, "CheckAppConfig, appBizId: %d", req.GetAppBizId())
	rsp := new(appCommon.CheckAppConfigRsp)
	shareKGExceptionList, err := s.kbLogic.CheckShareKG(ctx, req.GetAppBizId())
	if err != nil {
		return rsp, err
	}
	rsp.Kb = append(rsp.Kb, shareKGExceptionList...)
	kgModelExceptionList, err := s.kbLogic.CheckKGModel(ctx, req.GetAppBizId())
	if err != nil {
		return rsp, err
	}
	rsp.Kb = append(rsp.Kb, kgModelExceptionList...)
	logx.I(ctx, "CheckAppConfig done, appBizId: %d, rsp: %s", req.GetAppBizId(), rsp)
	return rsp, err
}

// DescribeExceededKnowledgeList 获取超量知识库列表
func (s *Service) DescribeExceededKnowledgeList(ctx context.Context,
	req *pb.InternalDescribeExceededKnowledgeListReq) (*pb.DescribeExceededKnowledgeListRsp, error) {
	corpID := req.GetCorpPrimaryId()
	return s.kbLogic.DescribeExceededKnowledgeList(ctx, corpID, req.GetSpaceId(),
		req.GetPageNumber(), req.GetPageSize(), false)
}
