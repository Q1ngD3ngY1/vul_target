package pkg

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/filter"
	"git.code.oa.com/trpc-go/trpc-go/log"
	mosslog "git.woa.com/baicaoyuan/moss/filters/log"
	"git.woa.com/dialogue-platform/common/v3/errors"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	galileo "git.woa.com/galileo/trpc-go-galileo"
)

func init() {
	filter.Register("user-ext", UserExtFilter, nil)
	filter.Register("actionLog", ActionLogFilter, nil)
}

// LogFilter 日志filter
var LogFilter = mosslog.ServerFilter(
	mosslog.WithCtxFields("uin", func(ctx context.Context) string {
		return Uin(ctx)
	}),
	mosslog.WithCtxFields("sub_account_uin", func(ctx context.Context) string {
		return SubAccountUin(ctx)
	}),
	mosslog.WithCtxFields("login_uin", func(ctx context.Context) string {
		return LoginUin(ctx)
	}),
	mosslog.WithCtxFields("login_sub_account_uin", func(ctx context.Context) string {
		return LoginSubAccountUin(ctx)
	}),
	mosslog.WithCtxFields("request_id", func(ctx context.Context) string {
		return RequestID(ctx)
	}),
)

// UserExtFilter 用户维度上报filter
var UserExtFilter filter.ServerFilter = func(ctx context.Context, req interface{},
	next filter.ServerHandleFunc) (rsp interface{}, err error) {
	galileo.SetRPCExtLabels(trpc.Message(ctx), galileo.WithUserExt1(Uin(ctx)))
	return next(ctx, req)
}

var ActionLogFilter filter.ServerFilter = func(ctx context.Context, req interface{},
	next filter.ServerHandleFunc) (rsp interface{}, err error) {
	rsp, err = next(ctx, req)
	go func(newCtx context.Context) { //异步上报操作日志
		defer errors.PanicHandler()
		reqStr, err1 := json.Marshal(req)
		if err1 != nil { //柔性放过
			log.ErrorContextf(newCtx, "CreateAccountOperationLog json marshal req:%+v,err:%v", req, err1)
			return
		}
		rspStr, err1 := json.Marshal(rsp)
		if err1 != nil {
			log.ErrorContextf(newCtx, "CreateAccountOperationLog json marshal rsp:%+v,err:%v", rsp, err1)
			return
		}
		proxy := admin.NewApiClientProxy()
		params := &admin.CreateAccountOperationLogReq{
			Request:  string(reqStr),
			Response: string(rspStr),
		}
		if err != nil {
			params.Message = err.Error()
		}
		log.DebugContextf(newCtx, "CreateAccountOperationLog params:%+v", params)
		_, err1 = proxy.CreateAccountOperationLog(newCtx, params)
		if err1 != nil {
			log.ErrorContextf(newCtx, "CreateAccountOperationLog params:%+v,err:%v", params, err1)
		}
	}(trpc.CloneContext(ctx))
	return rsp, err
}

// getAppBizID 获取请求中的应用id
func getAppBizID(ctx context.Context, req any) uint64 {
	var appBizID uint64
	defer func() {
		log.InfoContextf(ctx, "getAppBizID :%d", appBizID)
	}()
	t := reflect.Indirect(reflect.ValueOf(req)).FieldByName("AppBizId")
	if !t.IsValid() {
		t = reflect.Indirect(reflect.ValueOf(req)).FieldByName("BotBizId")
	}
	if t.IsValid() {
		switch t.Interface().(type) {
		case uint64:
			appBizID = t.Interface().(uint64)
		case string:
			idStr := t.Interface().(string)
			idInt, err := strconv.ParseUint(idStr, 10, 64)
			if err != nil {
				log.WarnContextf(ctx, "getAppBizID strconv.ParseUint err:%v", err)
				return 0
			}
			appBizID = idInt
		default:
			log.WarnContextf(ctx, "appbizid type unknown")
		}
		return appBizID
	}
	return 0
}
