package dao

import (
	"context"
	"strings"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	jsoniter "github.com/json-iterator/go"
)

// ExportUnsatisfiedReplyTask 导出不满意回复任务定义
type ExportUnsatisfiedReplyTask struct {
	Dao Dao
}

// GetExportTotal 获取导出数据总数
func (eurt *ExportUnsatisfiedReplyTask) GetExportTotal(ctx context.Context, corpID, robotID uint64,
	params string) (uint64, error) {
	req := &pb.ExportUnsatisfiedReplyReq{}
	if err := jsoniter.UnmarshalFromString(params, req); err != nil {
		log.ErrorContextf(ctx, "任务参数解析失败 corpID:%d,robotID:%d,params:%s,err:%+v", corpID, robotID,
			params, err)
		return 0, err
	}
	ids, err := util.CheckReqSliceUint64(ctx, req.GetIds())
	if err != nil {
		return 0, err
	}
	replyBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetReplyBizIds())
	if err != nil {
		return 0, err
	}
	unsatisfiedReplyListReq := fillUnsatisfiedReplyListReqOfExport(req, corpID, robotID, ids, replyBizIDs, 0, 0)
	total, err := eurt.Dao.GetUnsatisfiedReplyTotal(ctx, unsatisfiedReplyListReq)
	if err != nil {
		log.ErrorContextf(ctx, "export get unsatisfied reply total orpID:%d,robotID:%d,params:%s,err:%+v",
			corpID, robotID, params, err)
		return 0, err
	}
	return total, nil
}

// fillUnsatisfiedReplyListReqOfExport TODO
func fillUnsatisfiedReplyListReqOfExport(req *pb.ExportUnsatisfiedReplyReq, corpID, robotID uint64, ids, replyBizIDs []uint64, page,
	pageSize uint32) *model.UnsatisfiedReplyListReq {
	return &model.UnsatisfiedReplyListReq{
		CorpID:   corpID,
		RobotID:  robotID,
		Query:    req.GetFilters().GetQuery(),
		Reasons:  req.GetFilters().GetReasons(),
		IDs:      ids,
		BizIDs:   replyBizIDs,
		Page:     page,
		PageSize: pageSize,
	}
}

// GetExportData 分页获取导出数据
func (eurt *ExportUnsatisfiedReplyTask) GetExportData(ctx context.Context, corpID, robotID uint64, params string,
	page, pageSize uint32) ([][]string, error) {
	req := &pb.ExportUnsatisfiedReplyReq{}
	if err := jsoniter.UnmarshalFromString(params, req); err != nil {
		log.ErrorContextf(ctx, "任务参数解析失败 corpID:%d,robotID:%d,params:%s,err:%+v", corpID, robotID,
			params, err)
		return nil, err
	}
	ids, err := util.CheckReqSliceUint64(ctx, req.GetIds())
	if err != nil {
		return nil, err
	}
	replyBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetReplyBizIds())
	if err != nil {
		return nil, err
	}
	unsatisfiedReplyListReq := fillUnsatisfiedReplyListReqOfExport(req, corpID, robotID, ids, replyBizIDs, page, pageSize)
	list, err := eurt.Dao.GetUnsatisfiedReplyList(ctx, unsatisfiedReplyListReq)
	if err != nil {
		log.ErrorContextf(ctx, "export get unsatisfied reply list orpID:%d,robotID:%d,params:%s,err:%+v",
			corpID, robotID, params, err)
		return nil, err
	}

	var staffIDs []uint64
	for _, v := range list {
		staffIDs = append(staffIDs, v.StaffID)
	}
	staffNickNameMap, err := eurt.Dao.GetStaffNickNameMapByIDs(ctx, staffIDs)
	if err != nil {
		return nil, err
	}

	rows := make([][]string, 0)
	for _, v := range list {
		var statusDesc, operation string
		statusDesc = i18n.Translate(ctx, i18nkey.KeyUnsatisfiedStatusProcessedDesc)
		if v.Status == 0 {
			statusDesc = i18n.Translate(ctx, i18nkey.KeyUnsatisfiedStatusPendingDesc)
		} else if v.Status == 1 {
			operation = i18n.Translate(ctx, i18nkey.KeyUnsatisfiedReject)
		} else if v.Status == 2 {
			operation = i18n.Translate(ctx, i18nkey.KeyUnsatisfiedIgnore)
		} else if v.Status == 3 {
			operation = i18n.Translate(ctx, i18nkey.KeyUnsatisfiedCorrect)
		}

		rows = append(rows, []string{
			v.Question,
			v.Answer,
			strings.Join(v.Reasons, ","),
			staffNickNameMap[v.StaffID],
			statusDesc,
			operation,
			v.CreateTime.String(),
			v.UpdateTime.String(),
		})
	}
	return rows, nil
}

// GetExportHeader 获取导出数据表头
func (eurt *ExportUnsatisfiedReplyTask) GetExportHeader(ctx context.Context) []string {
	return []string{
		i18n.Translate(ctx, i18nkey.KeyUserQuestion),
		i18n.Translate(ctx, i18nkey.KeyBotReply),
		i18n.Translate(ctx, i18nkey.KeyErrorType),
		i18n.Translate(ctx, i18nkey.KeyUnsatisfiedOperator),
		i18n.Translate(ctx, i18nkey.KeyUnsatisfiedStatus),
		i18n.Translate(ctx, i18nkey.KeyUnsatisfiedOperation),
		i18n.Translate(ctx, i18nkey.KeyUnsatisfiedCreateTime),
		i18n.Translate(ctx, i18nkey.KeyUnsatisfiedUpdateTime),
	}
}
