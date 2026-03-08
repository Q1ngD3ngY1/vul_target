package main

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/filter"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/pb-go/common"
	platformManager "git.woa.com/adp/pb-go/platform/platform_manager"
	dataStat "git.woa.com/adp/pb-go/platform/platform_metrology"
	"github.com/google/uuid"
)

// updateKbActionMap 更新知识库的action集合
var updateKbActionMap = map[string]struct{}{
	"SaveDoc":              {},
	"DeleteDoc":            {},
	"ReferDoc":             {},
	"ModifyDoc":            {},
	"BatchModifyDoc":       {},
	"ModifyDocStatus":      {},
	"ModifyDocAttrRange":   {},
	"RenameDoc":            {},
	"CreateDocCate":        {},
	"CreateQACate":         {},
	"CreateSynonymsCate":   {},
	"ModifyDocCate":        {},
	"ModifyQACate":         {},
	"ModifySynonymsCate":   {},
	"DeleteDocCate":        {},
	"DeleteQACate":         {},
	"DeleteSynonymsCate":   {},
	"ModifyAttributeLabel": {},
	"CreateAttributeLabel": {},
	"DeleteAttributeLabel": {},
	"UploadAttributeLabel": {},
	"CreateQA":             {},
	"ModifyQA":             {},
	"DeleteQA":             {},
	"VerifyQA":             {},
	"ModifyQAStatus":       {},
	"ModifyQAAttrRange":    {},
	"BatchModifyQaExpire":  {},
	"BatchModifyQaDoc":     {},
}

var rpcInstance *rpc.RPC

func init() {
	filter.Register("actionLog", ActionLogFilter, nil)
}

var ActionLogFilter filter.ServerFilter = func(ctx context.Context, req any,
	next filter.ServerHandleFunc) (rsp any, err error) {
	rsp, err = next(ctx, req)
	// 异步上报操作日志
	go func(newCtx context.Context) {
		defer gox.Recover()
		reqStr, err1 := json.Marshal(req)
		if err1 != nil { // 柔性放过
			logx.E(newCtx, "ActionLogFilter json marshal req:%+v,err:%v", req, err1)
			return
		}
		rspStr, err1 := json.Marshal(rsp)
		if err1 != nil {
			logx.E(newCtx, "ActionLogFilter json marshal rsp:%+v,err:%v", rsp, err1)
			return
		}
		params := &platformManager.CreateOpLogReq{
			Request:  string(reqStr),
			Response: string(rspStr),
		}
		if err != nil {
			params.Message = err.Error()
		}
		logx.D(newCtx, "ActionLogFilter CreateOpLog params:%+v", params)
		err1 = rpcInstance.CreateOpLog(newCtx, params)
		if err1 != nil {
			logx.E(newCtx, "ActionLogFilter CreateOpLog params:%+v,err:%v", params, err1)
		}
	}(trpc.CloneContext(ctx))

	if err != nil {
		// 失败的情况不需要上报数据统计
		return rsp, err
	}
	// 异步上报数据统计
	action := contextx.Metadata(ctx).Action()
	if action == "" {
		logx.E(ctx, "ActionLogFilter action is empty")
		return rsp, err
	}
	if _, ok := updateKbActionMap[action]; !ok {
		return rsp, err
	}
	appBizId := getAppBizId(ctx, req)
	if appBizId == 0 {
		logx.E(ctx, "ActionLogFilter action:%s appBizId is empty", action)
		return rsp, err
	}
	logx.D(ctx, "ActionLogFilter action:%s appBizId:%d", action, appBizId)
	app, err := rpcInstance.DescribeAppById(ctx, appBizId)
	if err != nil {
		logx.E(ctx, "ActionLogFilter DescribeAppById err: %+v", err)
		return nil, err
	}
	go func(newCtx context.Context) {
		defer gox.Recover()
		counter(newCtx, app)
	}(trpc.CloneContext(ctx))
	return rsp, err
}

// getAppBizID 获取请求中的应用id
func getAppBizId(ctx context.Context, req any) uint64 {
	var appBizID uint64
	defer func() {
		logx.I(ctx, "getAppBizID :%d", appBizID)
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
				logx.W(ctx, "getAppBizID strconv.ParseUint err:%v", err)
				return 0
			}
			appBizID = idInt
		default:
			logx.W(ctx, "appbizid type unknown")
		}
		return appBizID
	}
	return 0
}

// counter 上报知识库变更计数
func counter(ctx context.Context, app *entity.App) {
	counterItemList := []*dataStat.CounterItem{
		{
			Statistic: &dataStat.Statistic{
				StatObject: common.StatObject_STAT_OBJECT_KB,
				StatType:   common.StatType_STAT_TYPE_EDIT,
			},
			AppBizId:   app.BizId,
			ObjectId:   strconv.FormatUint(app.BizId, 10),
			ObjectName: app.Name,
			Count:      1,
		},
	}
	counterReq := &dataStat.CounterReq{
		UniqueKey:       uuid.NewString(),
		CorpBizId:       app.CorpBizId,
		SpaceId:         app.SpaceId,
		StatTime:        time.Now().Format(time.RFC3339),
		CounterItemList: counterItemList,
	}
	err := rpcInstance.Counter(ctx, counterReq)
	if err != nil {
		logx.E(ctx, "Counter, counterReq: %+v, error: %+v", counterReq, err)
	}
}
