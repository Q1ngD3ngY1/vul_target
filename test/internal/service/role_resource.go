// Package service 业务逻辑层-客户COS文档
package service

import (
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"io"
	"net/http"

	"git.code.oa.com/trpc-go/trpc-go/log"
	jsoniter "github.com/json-iterator/go"
)

// CheckResourceByRoleName 检查角色业务资源
func (s *Service) CheckResourceByRoleName(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.ErrorContextf(ctx, "CheckResourceByRoleName failed, method: %s, error: %+v",
			r.Method, http.StatusText(http.StatusMethodNotAllowed))
		return
	}

	if r.Body == nil {
		w.WriteHeader(http.StatusBadRequest)
		log.ErrorContextf(ctx, "CheckResourceByRoleName failed, Request.Body nil, error: %+v",
			http.StatusText(http.StatusBadRequest))
		return
	}

	defer func(body io.ReadCloser) {
		err := body.Close()
		if err != nil {
			log.WarnContextf(ctx, "CheckResourceByRoleName, body.Close failed, error: %+v", err)
		}
	}(r.Body)

	requestBody, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.ErrorContextf(ctx, "CheckResourceByRoleName, io.ReadAll failed, error: %+v", err)
		return
	}

	log.InfoContextf(ctx, "CheckResourceByRoleName, requestBody: %s", string(requestBody))
	var req model.CheckResourceByRoleNameRequest
	if err = jsoniter.Unmarshal(requestBody, &req); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.ErrorContextf(ctx, "CheckResourceByRoleName, jsoniter.Unmarshal failed, requestBody:%s, error: %+v",
			string(requestBody), err)
		return
	}

	rsp := &model.CheckResourceByRoleNameResponse{
		Response: model.RoleResourceInfo{
			RequestId: req.RequestId,
			List:      []string{},
		},
	}
	responseBody, err := jsoniter.Marshal(rsp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.ErrorContextf(ctx, "CheckResourceByRoleName, jsoniter.Marshal failed, rsp: %+v, error:%+v",
			rsp, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	result, err := w.Write(responseBody)
	if err != nil {
		log.ErrorContextf(ctx, "CheckResourceByRoleName, Write failed, error: %+v, responseBody:%s",
			err, string(responseBody))
		return
	}

	log.InfoContextf(ctx, "CheckResourceByRoleName succeed, responseBody: %s, result: %d",
		string(responseBody), result)
}
