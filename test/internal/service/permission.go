package service

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"net/http"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"
)

const (
	PermissionInfinityAttributeLabel = "infinity_attribute_label"
)

// UserPermission 查询用户权限
func (s *Service) UserPermission(w http.ResponseWriter, r *http.Request) {
	log.ErrorContextf(r.Context(), "准备删除的接口收到了请求 deprecated interface req:%+v", r)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx := r.Context()
	uin := r.FormValue("uin")
	subAccountUin := r.FormValue("sub_account_uin")
	permissionID := r.FormValue("permission_id")
	perms, err := s.dao.GetUserPermission(ctx, uin, subAccountUin, []string{permissionID})
	if err != nil {
		log.ErrorContextf(ctx, "UserPermission err:%+v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	rsp, _ := jsoniter.Marshal(perms)
	_, _ = w.Write(rsp)
}

// UserResource 查询用户资源
func (s *Service) UserResource(w http.ResponseWriter, r *http.Request) {
	log.ErrorContextf(r.Context(), "准备删除的接口收到了请求 deprecated interface req:%+v", r)
}

// VerifyUserPermission 校验用户权限
func (s *Service) VerifyUserPermission(w http.ResponseWriter, r *http.Request) {
	log.ErrorContextf(r.Context(), "准备删除的接口收到了请求 deprecated interface req:%+v", r)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx := r.Context()
	uin := r.FormValue("uin")
	subAccountUin := r.FormValue("sub_account_uin")
	action := r.FormValue("action")
	ok, err := s.dao.VerifyPermission(ctx, uin, subAccountUin, action)
	if err != nil {
		log.ErrorContextf(ctx, "VerifyPermission err:%+v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	rsp, _ := jsoniter.Marshal(ok)
	_, _ = w.Write(rsp)
}

// DescribeNickname 获取用户昵称
func (s *Service) DescribeNickname(w http.ResponseWriter, r *http.Request) {
	log.ErrorContextf(r.Context(), "准备删除的接口收到了请求 deprecated interface req:%+v", r)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx := r.Context()
	uin := r.FormValue("uin")
	subAccountUin := r.FormValue("subAccountUin")
	ok, err := s.dao.DescribeNickname(ctx, uin, subAccountUin)
	if err != nil {
		log.ErrorContextf(ctx, "GetAuthInfoByUin err:%+v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	rsp, _ := jsoniter.Marshal(ok)
	_, _ = w.Write(rsp)
}

// BatchCheckWhitelist 批量检查白名单
func (s *Service) BatchCheckWhitelist(w http.ResponseWriter, r *http.Request) {
	log.ErrorContextf(r.Context(), "准备删除的接口收到了请求 deprecated interface req:%+v", r)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx := r.Context()
	uin := r.FormValue("uin")
	key := r.FormValue("key")
	ok, err := s.dao.BatchCheckWhitelist(ctx, key, uin)
	if err != nil {
		log.ErrorContextf(ctx, "BatchCheckWhitelist err:%+v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	rsp, _ := jsoniter.Marshal(ok)
	_, _ = w.Write(rsp)
}

// ModifyAppTokenUsage 更新应用token使用情况
func (s *Service) ModifyAppTokenUsage(w http.ResponseWriter, r *http.Request) {
	log.ErrorContextf(r.Context(), "准备删除的接口收到了请求 deprecated interface req:%+v", r)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx := r.Context()
	appBizID := r.FormValue("app_biz_id")
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(appBizID))
	if err != nil {
		log.ErrorContextf(ctx, "ModifyAppTokenUsage err:%+v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	if err = s.dao.ModifyAppTokenUsage(ctx, app); err != nil {
		log.ErrorContextf(ctx, "ModifyAppTokenUsage err:%+v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	usage, err := s.dao.GetTrailCorpTokenUsage(ctx, app.CorpID)
	if err != nil {
		log.ErrorContextf(ctx, "ModifyAppTokenUsage err:%+v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	log.DebugContextf(ctx, "GetTrailCorpTokenUsage corpID:%d 已使用token量:%d", app.CorpID, usage)
	corp, err := s.dao.GetCorpByID(ctx, app.CorpID)
	if err != nil {
		log.ErrorContextf(ctx, "ModifyAppTokenUsage err:%+v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	if corp.IsUsedTokenUsageExceeded(usage) {
		if err = s.dao.UpdateTrialCorpAppStatus(ctx, app.CorpID, model.AppStatusDeactivate, "已欠费"); err != nil {
			log.ErrorContextf(ctx, "ModifyAppTokenUsage err:%+v", err)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	rsp, _ := jsoniter.Marshal("token_usage:" + cast.ToString(usage))
	_, _ = w.Write(rsp)
}

// CheckKnowledgePermission 检查知识库白名单
func (s *Service) CheckKnowledgePermission(ctx context.Context, req *pb.CheckKnowledgePermissionReq) (*pb.CheckKnowledgePermissionRsp, error) {
	rsp := &pb.CheckKnowledgePermissionRsp{}
	if len(req.GetOperations()) == 0 {
		return rsp, nil
	}
	uin := pkg.Uin(ctx)
	for _, op := range req.GetOperations() {
		if op == "" {
			continue
		}
		flag := false
		switch op {
		case PermissionInfinityAttributeLabel:
			appBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
			if err != nil {
				return nil, err
			}
			flag = utilConfig.IsInWhiteList(uin, appBizID, utilConfig.GetWhitelistConfig().InfinityAttributeLabel)
		default:
			flag = false
		}
		res := &pb.KnowledgePermission{
			Operation: op,
			Allowed:   flag,
		}
		rsp.CheckResult = append(rsp.CheckResult, res)
	}
	return rsp, nil
}
