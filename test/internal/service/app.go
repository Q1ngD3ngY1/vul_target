package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	stdhttp "net/http"
	"strings"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx/validx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	appConfig "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// ListNonRoleModel 获取不展示的角色模型列表
func (s *Service) ListNonRoleModel(ctx context.Context, req *pb.ListNonRoleModelReq) (*pb.ListNonRoleModelRsp, error) {
	corpID := contextx.Metadata(ctx).CorpID()
	if corpID == 0 {
		return nil, errs.ErrCorpNotFound
	}
	roleCustomModels := config.App().RobotDefault.RoleCustomModels
	logx.I(ctx, "ListNonRoleModel role_custom_models|%+v|req|%+v", roleCustomModels, req)
	rsp := &pb.ListNonRoleModelRsp{}
	if len(roleCustomModels) > 0 {
		for _, role := range roleCustomModels {
			rsp.List = append(rsp.List, role)
		}
	}
	logx.I(ctx, "ListNonRoleModel rsp|%+v", rsp)
	return rsp, nil
}

// GetLikeDataCount 点踩点赞数据统计
func (s *Service) GetLikeDataCount(ctx context.Context, req *pb.GetLikeDataCountReq) (*pb.GetLikeDataCountRsp, error) {
	newReq := appConfig.DescribeLikeDataCountReq{
		StartTime: req.GetStartTime(),
		EndTime:   req.GetEndTime(),
		AppBizId:  req.GetAppBizId(),
		Type:      req.GetType(),
	}
	newRsp, err := s.rpc.AppAdmin.DescribeLikeDataCount(ctx, &newReq)
	if newRsp != nil {
		return &pb.GetLikeDataCountRsp{
			Total:             newRsp.GetTotal(),
			AppraisalTotal:    newRsp.GetAppraisalTotal(),
			ParticipationRate: newRsp.GetParticipationRate(),
			LikeTotal:         newRsp.GetLikeTotal(),
			LikeRate:          newRsp.GetLikeRate(),
			DislikeTotal:      newRsp.GetDislikeTotal(),
			DislikeRate:       newRsp.GetDislikeRate(),
		}, err
	}
	return nil, err
}

// GetAnswerTypeDataCount 回答类型数据统计
func (s *Service) GetAnswerTypeDataCount(ctx context.Context, req *pb.GetAnswerTypeDataCountReq) (
	*pb.GetAnswerTypeDataCountRsp, error) {
	newReq := appConfig.DescribeAnswerTypeDataCountReq{
		StartTime: req.GetStartTime(),
		EndTime:   req.GetEndTime(),
		AppBizId:  req.GetAppBizId(),
		Type:      req.GetType(),
	}
	newRsp, err := s.rpc.AppAdmin.DescribeAnswerTypeDataCount(ctx, &newReq)
	if newRsp != nil {
		return &pb.GetAnswerTypeDataCountRsp{
			Total:                   newRsp.GetTotal(),
			ModelReplyCount:         newRsp.GetModelReplyCount(),
			KnowledgeCount:          newRsp.GetKnowledgeCount(),
			TaskFlowCount:           newRsp.GetTaskFlowCount(),
			SearchEngineCount:       newRsp.GetSearchEngineCount(),
			ImageUnderstandingCount: newRsp.GetImageUnderstandingCount(),
			RejectCount:             newRsp.GetRejectCount(),
			SensitiveCount:          newRsp.GetSensitiveCount(),
			ConcurrentLimitCount:    newRsp.GetConcurrentLimitCount(),
			UnknownIssuesCount:      newRsp.GetUnknownIssuesCount(),
		}, err
	}
	return nil, err
}

// GetAppKnowledgeCount 获取知识问答个数
func (s *Service) GetAppKnowledgeCount(ctx context.Context, req *pb.GetAppKnowledgeCountReq) (*pb.GetAppKnowledgeCountRsp, error) {
	logx.I(ctx, "GetAppKnowledgeCount Req:%+v", req)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetAppBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	var rsp pb.GetAppKnowledgeCountRsp
	switch req.GetType() {
	case "doc":
		rsp.Total, err = s.getValidityDocCount(ctx, app.PrimaryId, app.CorpPrimaryId)
		if err != nil {
			logx.E(ctx, "get app knowledge count type:%s, err:%v", req.GetType(), err)
			return &rsp, errs.ErrSystem
		}
		shareTotal, err := s.getShareKnowledgeValidityDocCount(ctx, app.BizId)
		if err != nil {
			logx.W(ctx, "getShareKnowledgeValidityDocCount err:%v", err)
			return &rsp, errs.ErrSystem
		}
		rsp.Total += shareTotal
	case "qa":
		rsp.Total, err = s.getValidityQaCount(ctx, app.PrimaryId, app.CorpPrimaryId)
		if err != nil {
			logx.E(ctx, "get app knowledge count type:%s, err:%v", req.GetType(), err)
			return &rsp, errs.ErrSystem
		}
		shareTotal, err := s.getShareKnowledgeValidityQACount(ctx, app.BizId)
		if err != nil {
			logx.W(ctx, "getShareKnowledgeValidityQACount err:%v", err)
			return &rsp, errs.ErrSystem
		}
		rsp.Total += shareTotal
	case "db":
		rsp.Total, err = s.getValidityDBTableCount(ctx, app.BizId)
		if err != nil {
			logx.E(ctx, "get app knowledge count type:%s, err:%v", req.GetType(), err)
			return &rsp, errs.ErrSystem
		}
	default:
		return nil, errs.ErrParams
	}
	return &rsp, nil
}

// DescribeAppByScene 通过 应用ID 获取应用信息
func (s *Service) DescribeAppByScene(ctx context.Context, appBizIDStr string, scene uint32) (*entity.App, error) {
	appBizID, err := validx.CheckAndParseUint64(appBizIDStr)
	if err != nil {
		return nil, errs.ErrParams
	}
	appInfo, err := s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, appBizID, scene)
	if err != nil {
		return nil, errs.ErrAppNotFound
	}
	if appInfo == nil {
		return nil, errs.ErrAppNotFound
	}
	if appInfo.IsDeleted {
		return nil, errs.ErrAppNotFound
	}
	if appInfo.AppType != entity.KnowledgeQaAppType {
		return nil, errs.ErrGetAppFail
	}
	return appInfo, nil
}

// DescribeAppAndCheckCorp 通过 应用ID 获取应用信息，并且检查当前登录企业和应用归属企业是否一致
func (s *Service) DescribeAppAndCheckCorp(ctx context.Context, appBizIDStr string) (*entity.App, error) {
	appInfo, err := s.DescribeAppByScene(ctx, appBizIDStr, entity.AppTestScenes)
	if err != nil {
		return nil, err
	}
	corpID := contextx.Metadata(ctx).CorpID()
	if corpID != 0 && corpID != appInfo.CorpPrimaryId {
		logx.W(ctx, "当前企业与应用归属企业不一致 businessID:%s corpID:%d robot:%+v", appBizIDStr, corpID, appInfo)
		// 给C端分享出去的链接使用上传图片或者文档，此时当前登录的Corp和App可能不是归属关系
		// 这里将app信息返回，可以针对ErrCorpAppNotEqual处理
		return appInfo, errs.ErrCorpAppNotEqual
	}
	return appInfo, nil
}

func (s *Service) DescribeAppBySceneAndCheckCorp(ctx context.Context, appBizIDStr string, scene uint32) (*entity.App, error) {
	appInfo, err := s.DescribeAppByScene(ctx, appBizIDStr, scene)
	if err != nil {
		return nil, err
	}
	corpID := contextx.Metadata(ctx).CorpID()
	if corpID != 0 && corpID != appInfo.CorpPrimaryId {
		logx.W(ctx, "当前企业与应用归属企业不一致 businessID:%s corpID:%d robot:%+v", appBizIDStr, corpID, appInfo)
		// 给C端分享出去的链接使用上传图片或者文档，此时当前登录的Corp和App可能不是归属关系
		// 这里将app信息返回，可以针对ErrCorpAppNotEqual处理
		return appInfo, errs.ErrCorpAppNotEqual
	}
	return appInfo, nil
}

// DescribeAppBaseInfoAndCheckCorp 通过 应用ID 获取应用基础信息，并且检查当前登录企业和应用归属企业是否一致
func (s *Service) DescribeAppBaseInfoAndCheckCorp(ctx context.Context, appBizId uint64) (*entity.AppBaseInfo, error) {
	app, err := s.rpc.AppAdmin.GetAppBaseInfo(ctx, appBizId)
	if err != nil || app == nil {
		return nil, errs.ErrRobotNotFound
	}
	corpID := contextx.Metadata(ctx).CorpID()
	if corpID != 0 && corpID != app.CorpPrimaryId {
		logx.W(ctx, "当前企业与应用归属企业不一致 businessID:%d corpID:%d robot:%+v", appBizId, corpID, app)
		return nil, errs.ErrCorpAppNotEqual
	}
	return app, nil
}

// SyncRetrievalConfigFromDB 应用的检索配置从DB同步到redis，如果redis重启，需要手动调用这个接口恢复数据。本接口长期保留
// TODO(ericjwang): 运维工具。需要确认是否需要保留
func (s *Service) SyncRetrievalConfigFromDB(w stdhttp.ResponseWriter, r *stdhttp.Request) {

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if strings.ToUpper(r.Method) != "POST" {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("only POST is allowed"))
		return
	}
	ctx := r.Context()
	reqBody, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("数据读取失败 err:%+v", err)))
		return
	}
	req := &entity.SyncRetrievalConfigFromDBReq{}
	if err = jsonx.Unmarshal(reqBody, req); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("请求数据解析失败,err:%+v", err)))
		return
	}
	if (len(req.RobotIDs) == 0 && !req.IsAllConfigApp) || (len(req.RobotIDs) > 0 && req.IsAllConfigApp) {
		_, _ = w.Write([]byte("请求数据参数错误，请输入正确的参数"))
		return
	}
	logx.I(ctx, "SyncRetrievalConfigFromDB req:%s", jsonx.MustMarshalToString(req))
	// err = s.dao.SyncRetrievalConfigFromDB(ctx, req.RobotIDs)
	err = s.kbLogic.SyncRetrievalConfigFromDBToCache(ctx, req.RobotIDs)
	if err != nil {
		logx.I(ctx, "SyncRetrievalConfigFromDB err:%+v, RobotIDs:%+v", err, req.RobotIDs)
		_, _ = w.Write([]byte(fmt.Sprintf("SyncRetrievalConfigFromDB err:%+v", err)))
		return
	}

	logx.I(ctx, "SyncRetrievalConfigFromDB success, RobotIDs:%+v", req.RobotIDs)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// DescribeAppCharSize 获取应用的字符数 implements bot_knowledge_config_server.AdminService.
func (s *Service) DescribeAppCharSize(ctx context.Context, req *pb.DescribeAppCharSizeReq) (*pb.DescribeAppCharSizeRsp, error) {
	logx.I(ctx, "DescribrAppCharSize req:%+v", req)
	rsp := &pb.DescribeAppCharSizeRsp{}
	sizeType := req.CharSizeType
	var charSzie uint64
	var err error

	switch sizeType {
	case pb.CharSizeType_DocCharSize:
		docUsage, err := s.docLogic.GetRobotDocUsage(ctx, req.GetAppId(), req.GetCorpId())
		if err == nil {
			charSzie = uint64(docUsage.CharSize)
		}
	case pb.CharSizeType_QACharSize:
		qaUsage, err := s.qaLogic.GetRobotQAUsage(ctx, req.GetAppId(), req.GetCorpId())
		if err == nil {
			charSzie = uint64(qaUsage.CharSize)
		}
	default:
		err = errs.ErrParamsNotExpected
	}

	if err != nil {
		return nil, err
	}
	rsp.TotalCharSize = charSzie
	return rsp, nil

}

func (s *Service) ClearRealtimeAppResourceReleaseSegment(ctx context.Context, req *pb.ClearRealtimeAppResourceReleaseSegmentReq) (*pb.ClearRealtimeAppResourceReleaseSegmentRsp, error) {
	rsp := new(pb.ClearRealtimeAppResourceReleaseSegmentRsp)
	err := s.releaseLogic.ClearRealtimeAppResourceReleaseSegment(ctx, req.GetRemoveTimeStamp())
	if err != nil {
		logx.W(ctx, "pb.ClearRealtimeAppResourceReleaseSegmentRsp failed, err =%v", err)
		return nil, err
	}
	return rsp, nil
}
