package service

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/share_knowledge"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/common/v3/errors"
	jsoniter "github.com/json-iterator/go"
	"io"
	"net/http"
	"strings"
)

func (s *Service) flushParamsParse(w http.ResponseWriter, r *http.Request) (model.FlushKnowledgeBaseDataReq, bool) {
	var req model.FlushKnowledgeBaseDataReq
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if strings.ToUpper(r.Method) != "POST" {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("only POST is allowed"))
		return req, false
	}
	reqBody, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("数据读取失败 err:%+v", err)))
		return req, false
	}
	if err = jsoniter.Unmarshal(reqBody, &req); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("请求数据解析失败,err:%+v", err)))
		return req, false
	}
	if req.StartID == 0 || req.EndID == 0 || req.StartID > req.EndID {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("请求数据参数错误，请输入正确的参数"))
		return req, false
	}
	return req, true

}

// FlushShareKbUserResourcePermission 刷新共享知识库用户资源权限
func (s *Service) FlushShareKbUserResourcePermission(w http.ResponseWriter, r *http.Request) {
	req, ok := s.flushParamsParse(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	go func(rCtx context.Context, startID, endID uint64) {
		defer errors.PanicHandler()
		if err := share_knowledge.FlushShareKbUserResourcePermission(rCtx, s.dao, startID, endID); err != nil {
			log.ErrorContextf(rCtx, "FlushShareKbUserResourcePermission err:%+v", err)
		}
	}(trpc.CloneContext(ctx), req.StartID, req.EndID)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}
