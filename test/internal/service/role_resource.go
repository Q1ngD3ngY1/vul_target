package service

import (
	"io"
	"net/http"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	jsoniter "github.com/json-iterator/go"
)

// CheckResourceByRoleName 检查角色业务资源(供CAM调用)
// [NOTICE] 部署要求: 仅内网访问, 固定URI, HTTP协议, 非鉴权接口
// [NOTICE] 变更要求: 变更调整需要提前与CAM同步
func (s *Service) CheckResourceByRoleName(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		logx.E(ctx, "CheckResourceByRoleName failed, method: %s, error: %+v",
			r.Method, http.StatusText(http.StatusMethodNotAllowed))
		return
	}

	if r.Body == nil {
		w.WriteHeader(http.StatusBadRequest)
		logx.E(ctx, "CheckResourceByRoleName failed, Request.Body nil, error: %+v",
			http.StatusText(http.StatusBadRequest))
		return
	}

	defer func(body io.ReadCloser) {
		err := body.Close()
		if err != nil {
			logx.W(ctx, "CheckResourceByRoleName, body.Close failed, error: %+v", err)
		}
	}(r.Body)

	requestBody, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		logx.E(ctx, "CheckResourceByRoleName, io.ReadAll failed, error: %+v", err)
		return
	}

	logx.I(ctx, "CheckResourceByRoleName, requestBody: %s", string(requestBody))
	var req entity.CheckResourceByRoleNameRequest
	if err = jsoniter.Unmarshal(requestBody, &req); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		logx.E(ctx, "CheckResourceByRoleName, jsoniter.Unmarshal failed, requestBody: %s, error: %+v",
			string(requestBody), err)
		return
	}

	rsp := &entity.CheckResourceByRoleNameResponse{
		Response: entity.RoleResourceInfo{
			RequestId: req.RequestId,
			List:      []string{},
		},
	}
	responseBody, err := jsoniter.Marshal(rsp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		logx.E(ctx, "CheckResourceByRoleName, jsoniter.Marshal failed, rsp: %+v, error:%+v",
			rsp, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	result, err := w.Write(responseBody)
	if err != nil {
		logx.E(ctx, "CheckResourceByRoleName, Write failed, error: %+v, responseBody: %s",
			err, string(responseBody))
		return
	}

	logx.I(ctx, "CheckResourceByRoleName succeed, responseBody: %s, result: %d",
		string(responseBody), result)
}
