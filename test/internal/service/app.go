package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	stdhttp "net/http"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/utils"
	appImpl "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/go-comm/json0"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	jsoniter "github.com/json-iterator/go"
)

// ListNonRoleModel 获取不展示的角色模型列表
func (s *Service) ListNonRoleModel(ctx context.Context, req *pb.ListNonRoleModelReq) (*pb.ListNonRoleModelRsp, error) {
	corpID := pkg.CorpID(ctx)
	if corpID == 0 {
		return nil, errs.ErrCorpNotFound
	}
	roleCustomModels := config.App().RobotDefault.RoleCustomModels
	log.InfoContextf(ctx, "ListNonRoleModel role_custom_models|%+v|req|%+v", roleCustomModels, req)
	rsp := &pb.ListNonRoleModelRsp{}
	if len(roleCustomModels) > 0 {
		for _, role := range roleCustomModels {
			rsp.List = append(rsp.List, role)
		}
	}
	log.InfoContextf(ctx, "ListNonRoleModel rsp|%+v", rsp)
	return rsp, nil
}

// GetLikeDataCount 点踩点赞数据统计
func (s *Service) GetLikeDataCount(ctx context.Context, req *pb.GetLikeDataCountReq) (*pb.GetLikeDataCountRsp, error) {
	corpID := pkg.CorpID(ctx)
	if corpID == 0 {
		return nil, errs.ErrCorpNotFound
	}
	log.InfoContextf(ctx, "GetLikeData req|%+v", req)
	rsp := new(pb.GetLikeDataCountRsp)

	param := &model.MsgDataCountReq{
		Type: req.GetType(),
	}

	if len(req.GetAppBizId()) == 0 {
		return rsp, errs.ErrAppBizIDParams
	} else {
		for _, v := range req.GetAppBizId() {
			if v == "" {
				return rsp, errs.ErrAppBizIDParams
			}
			if _, err := util.CheckReqBotBizIDUint64(ctx, v); err != nil {
				return rsp, errs.ErrAppBizIDParams
			}
		}
	}

	appBizids, err := util.CheckReqSliceUint64(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	param.AppBizIds = append(param.AppBizIds, appBizids...)

	err, done := checkGetLikeDataCountReq(req.GetStartTime(), req.GetEndTime(), param)
	if done {
		return rsp, err
	}
	log.InfoContextf(ctx, "GetLikeData param|%+v", param)
	data, err := s.dao.GetLikeData(ctx, param)
	if err != nil {
		log.ErrorContextf(ctx, "GetLikeData dao err:%v", err)
		return rsp, errs.ErrSystem
	}
	if data != nil {
		log.InfoContextf(ctx, "GetLikeData data|%+v", data)
		if data.TotalCount.Valid {
			rsp.Total = uint32(data.TotalCount.Int32)
			rsp.AppraisalTotal = uint32(data.LikeCount.Int32 + data.DislikeCount.Int32)
			rsp.LikeTotal = uint32(data.LikeCount.Int32)
			rsp.DislikeTotal = uint32(data.DislikeCount.Int32)
			if rsp.Total == 0 {
				per, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(0)), 64)
				rsp.ParticipationRate = float32(per)
			} else {
				per, _ := strconv.ParseFloat(fmt.Sprintf("%.2f",
					float64(rsp.AppraisalTotal)/float64(rsp.Total)), 64)
				rsp.ParticipationRate = float32(per)
			}
			if rsp.AppraisalTotal == 0 {
				per, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(0)), 64)
				rsp.LikeRate = float32(per)
				rsp.DislikeRate = float32(per)
			} else {
				likePer, _ := strconv.ParseFloat(fmt.Sprintf("%.2f",
					float64(rsp.LikeTotal)/float64(rsp.AppraisalTotal)), 64)
				rsp.LikeRate = float32(likePer)
				dislikePer, _ := strconv.ParseFloat(fmt.Sprintf("%.2f",
					float64(rsp.DislikeTotal)/float64(rsp.AppraisalTotal)), 64)
				rsp.DislikeRate = float32(dislikePer)
			}
		}
	}
	log.InfoContextf(ctx, "GetLikeData rsp|%+v", rsp)
	return rsp, nil
}

// GetAnswerTypeDataCount 回答类型数据统计
func (s *Service) GetAnswerTypeDataCount(ctx context.Context, req *pb.GetAnswerTypeDataCountReq) (
	*pb.GetAnswerTypeDataCountRsp, error) {
	corpID := pkg.CorpID(ctx)
	if corpID == 0 {
		return nil, errs.ErrCorpNotFound
	}
	log.InfoContextf(ctx, "GetAnswerTypeData req|%+v", req)
	rsp := new(pb.GetAnswerTypeDataCountRsp)
	param := &model.MsgDataCountReq{
		Type: req.GetType(),
	}
	if len(req.GetAppBizId()) > 0 {
		for _, v := range req.GetAppBizId() {
			botBizID, err := util.CheckReqParamsIsUint64(ctx, v)
			if err != nil {
				return nil, err
			}
			param.AppBizIds = append(param.AppBizIds, botBizID)
		}
	}
	err, done := checkGetLikeDataCountReq(req.GetStartTime(), req.GetEndTime(), param)
	if done {
		return rsp, err
	}
	log.InfoContextf(ctx, "GetAnswerTypeData param|%+v", param)
	data, err := s.dao.GetAnswerTypeData(ctx, param)
	if err != nil {
		log.ErrorContextf(ctx, "GetAnswerTypeData dao err:%v", err)
		return rsp, errs.ErrSystem
	}
	if data != nil {
		log.InfoContextf(ctx, "GetAnswerTypeData data|%+v", data)
		if data.TotalCount.Valid {
			rsp.Total = uint32(data.TotalCount.Int32)
			rsp.ModelReplyCount = uint32(data.ModelReplyCount.Int32)
			rsp.KnowledgeCount = uint32(data.KnowledgeCount.Int32)
			rsp.TaskFlowCount = uint32(data.TaskFlowCount.Int32)
			rsp.SearchEngineCount = uint32(data.SearchEngineCount.Int32)
			rsp.ImageUnderstandingCount = uint32(data.ImageUnderstandingCount.Int32)
			rsp.RejectCount = uint32(data.RejectCount.Int32)
			rsp.SensitiveCount = uint32(data.SensitiveCount.Int32)
			rsp.ConcurrentLimitCount = uint32(data.ConcurrentLimitCount.Int32)
			rsp.UnknownIssuesCount = uint32(data.UnknownIssuesCount.Int32)
		}
	}
	log.InfoContextf(ctx, "GetAnswerTypeData rsp|%+v", rsp)
	return rsp, nil
}

// checkGetLikeDataCountReq 校验请求参数
func checkGetLikeDataCountReq(startTime uint64, endTime uint64, param *model.MsgDataCountReq) (error, bool) {
	if startTime != 0 {
		param.StartTime = time.Unix(int64(startTime), 0)
	} else {
		return errs.TMsgDataCountReqError("开始日期没传"), true
	}
	if endTime != 0 {
		param.EndTime = time.Unix(int64(endTime), 0)
	} else {
		return errs.TMsgDataCountReqError("结束日期没传"), true
	}
	// 获取当前时间
	currentTime := time.Now()
	currentTime = time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(),
		23, 59, 59, 59, currentTime.Location())
	// 计算时间差
	duration := currentTime.Sub(param.StartTime)
	// 判断时间差是否超过180天
	if duration.Hours() > 24*180 {
		return errs.TMsgDataCountReqError("开始日期距今超过180天"), true
	}
	if param.StartTime.After(param.EndTime) {
		return errs.TMsgDataCountReqError("开始时间晚于结束时间"), true
	}
	if param.EndTime.After(currentTime) {
		return errs.TMsgDataCountReqError("结束时间是未来时间"), true
	}
	return nil, false
}

// GetAppKnowledgeCount 获取知识问答个数
func (s *Service) GetAppKnowledgeCount(ctx context.Context,
	req *pb.GetAppKnowledgeCountReq) (*pb.GetAppKnowledgeCountRsp, error) {
	log.InfoContextf(ctx, "GetAppKnowledgeCount Req:%+v", req)
	var rsp pb.GetAppKnowledgeCountRsp
	corpID := pkg.CorpID(ctx)
	if req.GetAppBizId() == "" {
		return nil, errs.ErrParams
	}
	appBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, appBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	switch req.GetType() {
	case "doc":
		rsp.Total, err = s.getValidityDocCount(ctx, app.ID, corpID)
		if err != nil {
			log.ErrorContextf(ctx, "get app knowledge count type:%s, err:%v", req.GetType(), err)
			return &rsp, errs.ErrSystem
		}
		shareTotal, err := s.getShareKnowledgeValidityDocCount(ctx, appBizID)
		if err != nil {
			log.WarnContextf(ctx, "getShareKnowledgeValidityDocCount err:%v", err)
			return &rsp, errs.ErrSystem
		}
		rsp.Total += shareTotal
	case "qa":
		rsp.Total, err = s.getValidityQaCount(ctx, app.ID, corpID)
		if err != nil {
			log.ErrorContextf(ctx, "get app knowledge count type:%s, err:%v", req.GetType(), err)
			return &rsp, errs.ErrSystem
		}
		shareTotal, err := s.getShareKnowledgeValidityQACount(ctx, appBizID)
		if err != nil {
			log.WarnContextf(ctx, "getShareKnowledgeValidityQACount err:%v", err)
			return &rsp, errs.ErrSystem
		}
		rsp.Total += shareTotal
	case "db":
		rsp.Total, err = s.getValidityDBTableCount(ctx, appBizID)
		if err != nil {
			log.ErrorContextf(ctx, "get app knowledge count type:%s, err:%v", req.GetType(), err)
			return &rsp, errs.ErrSystem
		}
	default:
		return nil, errs.ErrParams
	}
	return &rsp, nil
}

// getAppByAppBizID 通过业务ID获取应用
func (s *Service) getAppByAppBizID(ctx context.Context, appBizID uint64) (*model.App, error) {
	appDB, err := s.dao.GetAppByAppBizID(ctx, appBizID)
	if err != nil {
		return nil, errs.ErrAppNotFound
	}
	if appDB == nil {
		return nil, errs.ErrAppNotFound
	}
	if appDB.HasDeleted() {
		return nil, errs.ErrAppNotFound
	}
	instance := appImpl.GetApp(appDB.AppType)
	if instance == nil {
		return nil, errs.ErrAppTypeInvalid
	}
	app, err := instance.AnalysisDescribeApp(ctx, appDB)
	if err != nil {
		return nil, errs.ErrSystem
	}
	corpID := pkg.CorpID(ctx)
	if corpID != 0 && corpID != app.CorpID {
		log.WarnContextf(ctx, "当前企业与应用归属企业不一致 businessID:%d corpID:%d robot:%+v",
			appBizID, corpID, app)
		// 给C端分享出去的链接使用上传图片或者文档，此时当前登录的Corp和App可能不是归属关系
		// 这里将app信息返回，可以针对ErrCorpAppNotEqual处理
		return app, errs.ErrCorpAppNotEqual
	}
	if err = s.dao.CreateAppVectorIndex(ctx, appDB); err != nil {
		return nil, errs.ErrRobotInitFail
	}
	return app, nil
}

// SyncKnowledgeQaAppData 同步知识库问答应用历史数据
func (s *Service) SyncKnowledgeQaAppData(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	log.ErrorContextf(r.Context(), "准备删除的接口收到了请求 deprecated interface req:%+v", r)

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
	req := &model.SyncKnowledgeQaAppDataReq{}
	if err = jsoniter.Unmarshal(reqBody, req); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("请求数据解析失败,err:%+v", err)))
		return
	}
	if req.StartID == 0 || req.EndID == 0 || req.StartID > req.EndID {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("请求数据参数错误，请输入正确的参数"))
		return
	}
	startID, endID := req.StartID, req.EndID
	for {
		apps, err := s.dao.GetAppByIDRange(ctx, startID, endID, model.SyncToAppLimit)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(fmt.Sprintf("查询应用数据失败 err:%+v", err)))
			return
		}
		if len(apps) == 0 {
			break
		}
		syncApps := make([]*model.AppDB, 0, len(apps))
		for _, v := range apps {
			ok, err := fillKnowledgeQaAppConfigOfSync(ctx, v)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(fmt.Sprintf("转换问答知识库应用数据失败 err:%+v", err)))
				return
			}
			if ok {
				syncApps = append(syncApps, v)
			}
		}
		if err = s.dao.SyncAppData(ctx, syncApps); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(fmt.Sprintf("同步问答知识库应用数据失败 err:%+v", err)))
			return
		}
		if len(apps) < model.SyncToAppLimit {
			break
		}
		startID = apps[len(apps)-1].ID
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// SyncRobotRetrievalConfig 初始化同步存量机器人的检索配置
func (s *Service) SyncRobotRetrievalConfig(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	log.ErrorContextf(r.Context(), "准备删除的接口收到了请求 deprecated interface req:%+v", r)

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
	req := &model.SyncRobotRetrievalConfigReq{}
	if err = jsoniter.Unmarshal(reqBody, req); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("请求数据解析失败,err:%+v", err)))
		return
	}
	if len(req.RobotIDs) == 0 || req.Config == nil {
		_, _ = w.Write([]byte("请求数据参数错误，请输入正确的参数"))
		return
	}
	log.InfoContextf(ctx, "SyncRobotRetrievalConfig req:%s", json0.Marshal2StringNoErr(req))
	for _, robotID := range req.RobotIDs {
		err = s.dao.SaveRetrievalConfig(ctx, robotID, *req.Config, "")
		if err != nil {
			log.InfoContextf(ctx, "SyncRobotRetrievalConfig err:%+v, robotID:%d", err, robotID)
			_, _ = w.Write([]byte(fmt.Sprintf("SaveRetrievalConfig err:%+v, robotID:%d", err, robotID)))
			return
		}
	}
	log.InfoContextf(ctx, "SyncRobotRetrievalConfig success, robotID len:%d, config:%+v", len(req.RobotIDs), req.Config)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// SyncRetrievalConfigFromDB 应用的检索配置从DB同步到redis，如果redis重启，需要手动调用这个接口恢复数据。本接口长期保留
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
	req := &model.SyncRetrievalConfigFromDBReq{}
	if err = jsoniter.Unmarshal(reqBody, req); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("请求数据解析失败,err:%+v", err)))
		return
	}
	if (len(req.RobotIDs) == 0 && !req.IsAllConfigApp) || (len(req.RobotIDs) > 0 && req.IsAllConfigApp) {
		_, _ = w.Write([]byte("请求数据参数错误，请输入正确的参数"))
		return
	}
	log.InfoContextf(ctx, "SyncRetrievalConfigFromDB req:%s", json0.Marshal2StringNoErr(req))
	err = s.dao.SyncRetrievalConfigFromDB(ctx, req.RobotIDs)
	if err != nil {
		log.InfoContextf(ctx, "SyncRetrievalConfigFromDB err:%+v, RobotIDs:%+v", err, req.RobotIDs)
		_, _ = w.Write([]byte(fmt.Sprintf("SyncRetrievalConfigFromDB err:%+v", err)))
		return
	}

	log.InfoContextf(ctx, "SyncRetrievalConfigFromDB success, RobotIDs:%+v", req.RobotIDs)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// FlushKnowledgeQaAppConfig 刷新知识库问答应用配置的存量数据
func (s *Service) FlushKnowledgeQaAppConfig(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	log.ErrorContextf(r.Context(), "准备删除的接口收到了请求 deprecated interface req:%+v", r)

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
	req := &model.FlushKnowledgeQaAppConfigReq{}
	if err = jsoniter.Unmarshal(reqBody, req); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("请求数据解析失败,err:%+v", err)))
		return
	}
	if req.StartID == 0 || req.EndID == 0 || req.BatchSize == 0 || req.StartID > req.EndID || req.FlushConfig == nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("请求数据参数错误，请输入正确的参数"))
		return
	}
	startID, endID := req.StartID, req.EndID
	var flushAppCount int
	for {
		apps, err := s.dao.GetAppByIDRange(ctx, startID, endID, req.BatchSize)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(fmt.Sprintf("查询应用数据失败 err:%+v", err)))
			return
		}
		log.InfoContextf(ctx, "Flush|GetAppByIDRange len:%d|startID:%d|endID:%d", len(apps), startID, endID)
		if len(apps) == 0 {
			break
		}
		flushApps := make([]*model.AppDB, 0, len(apps))
		for _, v := range apps {
			if v.AppType != model.KnowledgeQaAppType {
				// 只刷知识问答的应用
				log.InfoContextf(ctx, "Flush|NOT knowledge_qa app|%d|%s", v.ID, v.AppType)
				continue
			}
			isUpdate, err := FlushSingleAppConfig(ctx, v, req.FlushConfig)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(fmt.Sprintf("flushKnowledgeQaAppConfig err:%+v", err)))
				return
			}
			log.InfoContextf(ctx, "Flush|flushKnowledgeQaAppConfig isUpdate:%t|ID:%d", isUpdate, v.ID)
			if isUpdate {
				flushApps = append(flushApps, v)
			}
		}
		if err = s.dao.FlushAppData(ctx, flushApps); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(fmt.Sprintf("FlushAppData err:%+v", err)))
			return
		}
		flushAppCount += len(flushApps)
		log.InfoContextf(ctx, "Flush|batch flush success|apps len:%d|flushApps len:%d|startID:%d|endID:%d",
			len(apps), len(flushApps), startID, endID)
		if len(apps) < int(req.BatchSize) {
			break
		}
		startID = apps[len(apps)-1].ID + 1
	}
	log.InfoContextf(ctx, "Flush|FlushKnowledgeQaAppConfig done|flushAppCount:%d", flushAppCount)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// FlushSingleAppConfig 刷新单个应用的配置
func FlushSingleAppConfig(ctx context.Context, appDB *model.AppDB, flushConfig *model.KnowledgeQaConfig) (
	bool, error) {
	var isUpdatePreview, isUpdateRelease bool
	if len(appDB.PreviewJSON) != 0 {
		newPreviewJSON, err := flushAppDetailsConfig(ctx, appDB.PreviewJSON, flushConfig)
		if err != nil {
			log.ErrorContextf(ctx, "Flush|flushAppDetailsConfig err:%+v|ID:%d", err, appDB.ID)
			return false, err
		}
		if appDB.PreviewJSON != newPreviewJSON {
			log.InfoContextf(ctx, "Flush|preview_json update|%d|old|%s", appDB.ID, appDB.PreviewJSON)
			log.InfoContextf(ctx, "Flush|new|%s", newPreviewJSON)
			appDB.PreviewJSON = newPreviewJSON
			isUpdatePreview = true
		} else {
			log.InfoContextf(ctx, "Flush|preview_json NOT update|%d|%s", appDB.ID, appDB.PreviewJSON)
		}
	} else {
		log.WarnContextf(ctx, "Flush|preview_json empty|%d", appDB.ID)
	}
	if appDB.AppStatus != model.AppStatusInit && len(appDB.ReleaseJSON) != 0 {
		newReleaseJSON, err := flushAppDetailsConfig(ctx, appDB.ReleaseJSON, flushConfig)
		if err != nil {
			log.ErrorContextf(ctx, "Flush|flushAppDetailsConfig err:%+v|ID:%d", err, appDB.ID)
			return false, err
		}
		if appDB.ReleaseJSON != newReleaseJSON {
			log.InfoContextf(ctx, "Flush|release_json update|%d|old|%s", appDB.ID, appDB.ReleaseJSON)
			log.InfoContextf(ctx, "Flush|new|%s", newReleaseJSON)
			appDB.ReleaseJSON = newReleaseJSON
			isUpdateRelease = true
		} else {
			log.InfoContextf(ctx, "Flush|release_json NOT update|%d|%s", appDB.ID, appDB.ReleaseJSON)
		}
	}
	return isUpdatePreview || isUpdateRelease, nil
}

// flushAppDetailsConfig 刷新preview_json和release_json配置数据
func flushAppDetailsConfig(ctx context.Context, configJSON string,
	flushConfig *model.KnowledgeQaConfig) (string, error) {
	// 当前应用配置
	var appDetailsConfig model.AppDetailsConfig
	err := model.UnmarshalStr(configJSON, &appDetailsConfig)
	if err != nil {
		log.ErrorContextf(ctx, "Flush|unmarshal app config json err:%+v|%s", err, configJSON)
		return configJSON, err
	}

	// 默认model配置
	defaultModel := utilConfig.GetMainConfig().RobotDefault.AppModelConfig.KnowledgeQaAppModel

	appKnowledgeQaConfig := appDetailsConfig.AppConfig.KnowledgeQaConfig
	if appKnowledgeQaConfig == nil {
		log.ErrorContextf(ctx, "Flush|appKnowledgeQaConfig is nil|%s", configJSON)
		return configJSON, errors.New("appKnowledgeQaConfig is nil")
	}
	if len(flushConfig.Model) > 0 {
		// 更新model配置
		for k, v := range flushConfig.Model {
			defaultAppModel, ok := defaultModel[k]
			if !ok {
				log.ErrorContextf(ctx, "Flush|NOT find model|%s|%s", k, configJSON)
				continue
			}
			appModel, ok := appKnowledgeQaConfig.Model[k]
			if !ok {
				log.ErrorContextf(ctx, "Flush|NOT find model|%s|%s", k, configJSON)
				continue
			}
			if v.Prompt != "" && v.Prompt != appModel.Prompt &&
				appModel.Prompt == defaultAppModel.Prompt {
				appModel.Prompt = v.Prompt
			}
			if v.PromptWordsLimit > 0 {
				appModel.PromptWordsLimit = v.PromptWordsLimit
			}
			if v.HistoryLimit > 0 {
				appModel.HistoryLimit = v.HistoryLimit
			}
			if v.HistoryWordsLimit > 0 {
				appModel.HistoryWordsLimit = v.HistoryWordsLimit
			}
			appKnowledgeQaConfig.Model[k] = appModel
		}
	}
	if len(flushConfig.Filters) > 0 {
		// 更新filter配置
		for k, v := range flushConfig.Filters {
			if appFilters, ok := appKnowledgeQaConfig.Filters[k]; ok {
				// topN总数小于待刷的值则刷成新值
				if v.TopN > 0 && appFilters.TopN < v.TopN {
					appFilters.TopN = v.TopN
				}
				for _, flushFilter := range v.Filter {
					for i := range appFilters.Filter {
						if flushFilter.DocType == appFilters.Filter[i].DocType {
							// 置信度阈值大于待刷的值才会刷成新值
							if flushFilter.Confidence > 0 && appFilters.Filter[i].Confidence > flushFilter.Confidence {
								appFilters.Filter[i].Confidence = flushFilter.Confidence
							}
						}
					}
				}
				appKnowledgeQaConfig.Filters[k] = appFilters
			}
		}
	}
	newConfigJSON := appDetailsConfig.ToJSON()
	return newConfigJSON, nil
}

// fillKnowledgeQaAppConfigOfSync TODO
func fillKnowledgeQaAppConfigOfSync(ctx context.Context, appDB *model.AppDB) (bool, error) {
	if (len(appDB.AppType) != 0 && appDB.AppType != model.KnowledgeQaAppType) || len(appDB.PreviewJSON) != 0 {
		return false, nil
	}
	instance := appImpl.GetApp(appDB.AppType)
	if instance == nil {
		return false, errs.ErrAppTypeInvalid
	}
	app, err := instance.AnalysisDescribeApp(ctx, appDB)
	if err != nil {
		return false, errs.ErrSystem
	}
	modelInfo, modelAliasName, err := fillAppModelsOfSync(ctx, app)
	if err != nil {
		return false, err
	}
	previewFilters, releaseFilters, err := fillAppFiltersOfSync(ctx, app)
	if err != nil {
		return false, err
	}
	docSplit, err := fillAppDocSplitOfSync(ctx, app)
	if err != nil {
		return false, err
	}
	searchVector, err := fillAppSearchVectorOfSync(ctx, app)
	if err != nil {
		return false, err
	}
	appDB.AppType = model.KnowledgeQaAppType
	appDB.AppStatus = utils.When(appDB.QAVersion == 0, uint32(model.AppStatusInit),
		uint32(model.AppStatusRunning))
	appDB.ModelName = modelAliasName
	configDeatails := model.AppDetailsConfig{
		BaseConfig: model.BaseConfig{
			Name:        appDB.Name,
			Avatar:      appDB.Avatar,
			Description: appDB.Description,
		},
		AppConfig: model.AppConfig{
			KnowledgeQaConfig: &model.KnowledgeQaConfig{
				Greeting:            appDB.Greeting,
				RoleDescription:     appDB.RoleDescription,
				Method:              model.AppMethodStream,
				UseGeneralKnowledge: appDB.UseGeneralKnowledge,
				BareAnswer:          appDB.BareAnswer,
				ReplyFlexibility:    appDB.ReplyFlexibility,
				UseSearchEngine:     appDB.UseSearchEngine,
				ShowSearchEngine:    appDB.ShowSearchEngine,
				Model:               modelInfo,
				Filters:             previewFilters,
				DocSplit:            docSplit,
				SearchVector:        searchVector,
			},
		},
	}
	appDB.PreviewJSON = configDeatails.ToJSON()
	if appDB.AppStatus == model.AppStatusRunning {
		configDeatails.AppConfig.KnowledgeQaConfig.Filters = releaseFilters
		appDB.ReleaseJSON = configDeatails.ToJSON()
	}
	return true, nil
}

func fillAppFiltersOfSync(ctx context.Context, app *model.App) (config.RobotFilters, config.RobotFilters,
	error) {
	filters, _, err := app.GetOldFilters()
	if err != nil {
		log.ErrorContextf(ctx, "parse sync filters err:%v", err)
		return nil, nil, err
	}
	previewFilters := make(config.RobotFilters)
	releaseFilters := make(config.RobotFilters)
	for k, v := range filters {
		if k != model.AppSearchPreviewFilterKey && k != model.AppSearchReleaseFilterKey {
			continue
		}
		robotFilter := config.RobotFilter{
			TopN:   v.TopN,
			Filter: make([]config.RobotFilterDetail, 0),
		}
		for i := range v.Filter {
			robotFilterDetail := v.Filter[i]
			switch robotFilterDetail.DocType {
			case model.QaFilterType, model.DocFilterType:
				robotFilterDetail.IsEnabled = true
			case model.SearchFilterType:
				if app.UseSearchEngine {
					robotFilterDetail.IsEnabled = true
				}
			}
			robotFilter.Filter = append(robotFilter.Filter, robotFilterDetail)
		}
		if k == model.AppSearchPreviewFilterKey {
			previewFilters[k] = robotFilter
		}
		if k == model.AppSearchReleaseFilterKey {
			releaseFilters[k] = robotFilter
		}
	}
	return previewFilters, releaseFilters, nil
}

func fillAppModelsOfSync(ctx context.Context, app *model.App) (config.AppModel, string, error) {
	var modelAliasName string
	appModel, _, err := app.GetOldModels()
	if err != nil {
		log.ErrorContextf(ctx, "parse sync model err:%v", err)
		return appModel, modelAliasName, err
	}
	nameToAliasNameMap, err := model.GetNameToAliasNameMap(model.KnowledgeQaAppType)
	if err != nil {
		return appModel, modelAliasName, err
	}
	newAppModels := make(config.AppModel)
	for i := range appImpl.KQaModelTypes {
		modelInfo, ok := appModel[appImpl.KQaModelTypes[i]]
		if !ok {
			continue
		}
		if appImpl.KQaModelTypes[i] == model.AppModelNormal {
			modelAliasName = nameToAliasNameMap[modelInfo.ModelName]
		}
		newAppModels[appImpl.KQaModelTypes[i]] = modelInfo
	}
	return newAppModels, modelAliasName, nil
}

func fillAppDocSplitOfSync(ctx context.Context, app *model.App) (config.RobotDocSplit, error) {
	splitConf, isDocSplitDefault, err := app.GetOldDocSplitConf()
	if err != nil {
		log.ErrorContextf(ctx, "parse sync doc split err:%v", err)
		return splitConf, err
	}
	if isDocSplitDefault {
		return nil, err
	}
	return splitConf, nil
}

func fillAppSearchVectorOfSync(ctx context.Context, app *model.App) (*config.SearchVector, error) {
	searchVector, isSearchVectorDefault, err := app.GetOldSearchVector()
	if err != nil {
		log.ErrorContextf(ctx, "parse sync search vector err:%v", err)
		return nil, err
	}
	if isSearchVectorDefault {
		return nil, nil
	}
	return searchVector, nil

}
