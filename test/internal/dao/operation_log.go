package dao

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/data_statistics"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	statistics "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_data_statistics_server"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
)

const (
	operationLogFields = `
		id,corp_id,staff_id,robot_id,content,event,release_id,create_time,update_time
	`
	addOperationLog = `
		INSERT INTO 
		    t_operation_log (%s) 
		VALUES 
		    (:id,:corp_id,:staff_id,:robot_id,:content,:event,:release_id,:create_time,:update_time)`
)

// AddOperationLog 添加写操作记录
func (d *dao) AddOperationLog(ctx context.Context, event string, corpID, robotID uint64, req, rsp, before,
	after any) error {
	appBizId, err := GetAppBizIDByAppID(ctx, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "GetAppBizIDByAppID err: %+v", err)
		return err
	}
	// 上报统计数据
	go func(newCtx context.Context) { //异步上报
		counterInfo := &data_statistics.CounterInfo{
			CorpBizId:       pkg.CorpBizID(newCtx),
			AppBizId:        appBizId,
			StatisticObject: statistics.StatObject_STAT_OBJECT_KB,
			StatisticType:   statistics.StatType_STAT_TYPE_EDIT,
			ObjectId:        strconv.FormatUint(appBizId, 10),
			Count:           1,
		}
		data_statistics.Counter(newCtx, counterInfo)
	}(trpc.CloneContext(ctx))

	releaseDetail, err := d.GetLatestSuccessRelease(ctx, corpID, robotID)
	if err != nil {
		return err
	}
	contentByte, err := json.Marshal(&model.Snapshot{
		Req:    req,
		Rsp:    rsp,
		Before: before,
		After:  after,
	})
	if err != nil {
		log.ErrorContextf(ctx, "序列化失败 err:%+v", err)
		return err
	}
	content := string(contentByte)
	// 对内容进行截断，避免超过数据库字段长度限制
	if len(content) > MaxTextLength {
		prefix := util.GetPrefixByUTF8Length(content, MaxTextLength)
		if len(prefix) > 0 {
			content = prefix
		} else {
			log.ErrorContextf(ctx, "分割操作记录失败 length:%d 内容:%s", len(content), content)
		}
	}
	operation := &model.OperationLog{
		CorpID:     corpID,
		StaffID:    pkg.StaffID(ctx),
		RobotID:    robotID,
		Content:    content,
		Event:      event,
		ReleaseID:  releaseDetail.ID,
		CreateTime: time.Now(),
		UpdateTime: time.Now(),
	}
	querySQL := fmt.Sprintf(addOperationLog, operationLogFields)
	if _, err = d.db.NamedExec(ctx, querySQL, operation); err != nil {
		log.ErrorContextf(ctx, "添加写操作日志失败 sql:%s args:%+v err:%+v", querySQL, operation, err)
		return err
	}
	return nil
}
