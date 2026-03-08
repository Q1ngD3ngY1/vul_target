// bot-knowledge-config-server
//
// @(#)doc_diff_task.go  星期二, 一月 21, 2025
// Copyright(c) 2025, zrwang@Tencent. All rights reserved.

package service

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/common"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc_diff_task"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"

	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// ListDocDiffTask 获取对比任务列表
func (s *Service) ListDocDiffTask(ctx context.Context, req *pb.ListDocDiffTaskReq) (*pb.ListDocDiffTaskRsp, error) {
	log.InfoContextf(ctx, "ListDocDiffTask Req:%+v", req)
	rsp := new(pb.ListDocDiffTaskRsp)
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corp, err := s.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		log.ErrorContextf(ctx, "GetCorpByID err: %+v", err)
		return rsp, err
	}
	deletedFlag := dao.IsNotDeleted
	filter := &dao.DocDiffTaskFilter{
		CorpBizId:      corp.BusinessID,
		RobotBizId:     app.BusinessID,
		IsDeleted:      &deletedFlag,
		Statuses:       req.GetStatuses(),
		OrderColumn:    []string{dao.DocDiffTaskTblColCreateTime},
		OrderDirection: []string{dao.SqlOrderByDesc},
		Offset:         common.GetOffsetByPage(req.GetPageNumber(), req.GetPageSize()),
		Limit:          req.GetPageSize(),
	}
	selectColumns := []string{
		dao.DocDiffTaskTblColBusinessId,
		dao.DocDiffTaskTblColNewDocBizId,
		dao.DocDiffTaskTblColOldDocBizId,
		dao.DocDiffTaskTblColNewDocRename,
		dao.DocDiffTaskTblColComparisonReason,
		dao.DocDiffTaskTblColDiffType,
		dao.DocDiffTaskTblColDocOperation,
		dao.DocDiffTaskTblColQaOperation,
		dao.DocDiffTaskTblColStatus,
		dao.DocDiffTaskTblColDiffDataProcessStatus,
		dao.DocDiffTaskTblColCreateTime}
	list, total, err := dao.GetDocDiffTaskDao().GetDocDiffTaskCountAndList(ctx, selectColumns, filter)
	if err != nil {
		log.ErrorContextf(ctx, "ListDocDiffTask err:%+v", err)
		return rsp, err
	}
	if total == 0 {
		return rsp, nil
	}
	docBizIDs := make([]uint64, 0, len(list))
	for _, item := range list {
		docBizIDs = append(docBizIDs, item.OldDocBizID)
		docBizIDs = append(docBizIDs, item.NewDocBizID)
	}
	docFilter := &dao.DocFilter{
		CorpId:      corpID,
		RobotId:     app.ID,
		BusinessIds: docBizIDs,
	}
	docs, err := dao.GetDocDao().GetDocDiffTaskDocs(ctx, docFilter)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocDiffTaskDocs err: %+v", err)
		return rsp, err
	}
	docIDs := make([]uint64, 0, len(list))
	for _, item := range docs {
		docIDs = append(docIDs, item.ID)
	}
	qaNums, err := s.dao.GetDocQANum(ctx, corpID, app.ID, docIDs)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	rsp.Total = uint64(total)
	rsp.PageNumber = req.GetPageNumber()
	rsp.List = make([]*pb.ListDocDiffTaskRsp_DocDiffTask, 0, len(list))
	for _, docDiff := range list {
		docDiffRsp := &pb.ListDocDiffTaskRsp_DocDiffTask{
			DocDiffTaskBizId:      strconv.FormatUint(docDiff.BusinessID, 10),
			NewDoc:                model.FormatDocDiffDocInfo(docDiff.NewDocBizID, docs, qaNums),
			OldDoc:                model.FormatDocDiffDocInfo(docDiff.OldDocBizID, docs, qaNums),
			ComparisonReason:      docDiff.ComparisonReason,
			DiffType:              docDiff.DiffType,
			DocOperation:          docDiff.DocOperation,
			QaOperation:           docDiff.QaOperation,
			Status:                docDiff.Status,
			DiffDataProcessStatus: docDiff.DiffDataProcessStatus,
			CreateTime:            docDiff.CreateTime.Unix(),
		}
		rsp.List = append(rsp.List, docDiffRsp)
	}
	return rsp, nil
}

// CreateDocDiffTask 创建对比任务
func (s *Service) CreateDocDiffTask(ctx context.Context, req *pb.CreateDocDiffTaskReq) (*pb.CreateDocDiffTaskRsp, error) {
	log.InfoContextf(ctx, "CreateDocDiffTask Req:%+v", req)
	rsp := new(pb.CreateDocDiffTaskRsp)
	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corp, err := s.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		log.ErrorContextf(ctx, "GetCorpByID err: %+v", err)
		return rsp, err
	}
	staff, err := s.dao.GetStaffByID(ctx, staffID)
	if err != nil || staff == nil {
		return rsp, errs.ErrStaffNotFound
	}

	newDocID, err := util.CheckReqBotBizIDUint64(ctx, req.GetNewDocBizId())
	if err != nil {
		return nil, err
	}
	oldDocID, err := util.CheckReqBotBizIDUint64(ctx, req.GetOldDocBizId())
	if err != nil {
		return nil, err
	}
	if newDocID == 0 || oldDocID == 0 {
		return nil, errs.ErrParams
	}

	diffID, err := doc_diff_task.CreateDocDiffTask(ctx, corpID, app.ID, corp.BusinessID, app.BusinessID, newDocID,
		oldDocID, staff.BusinessID, req.GetComparisonReason(), s.dao, false)
	if err != nil {
		return rsp, err
	}

	rsp.DocDiffTaskBizId = strconv.FormatUint(diffID, 10)
	return rsp, nil
}

// DescribeDocDiffTask 获取对比任务
func (s *Service) DescribeDocDiffTask(ctx context.Context, req *pb.DescribeDocDiffTaskReq) (*pb.DescribeDocDiffTaskRsp, error) {
	log.InfoContextf(ctx, "DescribeDocDiffTask Req:%+v", req)
	rsp := new(pb.DescribeDocDiffTaskRsp)
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corp, err := s.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		log.ErrorContextf(ctx, "GetCorpByID err: %+v", err)
		return rsp, err
	}
	log.InfoContextf(ctx, "DescribeDocDiffTask corpID:%d,app.BusinessIds:%d", corpID, app.BusinessID)
	diffID, err := util.CheckReqBotBizIDUint64(ctx, req.GetDocDiffTaskBizId())
	if err != nil {
		return nil, err
	}
	selectColumns := []string{
		dao.DocDiffTaskTblColBusinessId,
		dao.DocDiffTaskTblColNewDocBizId,
		dao.DocDiffTaskTblColOldDocBizId,
		dao.DocDiffTaskTblColNewDocRename,
		dao.DocDiffTaskTblColOldDocRename,
		dao.DocDiffTaskTblColComparisonReason,
		dao.DocDiffTaskTblColDiffType,
		dao.DocDiffTaskTblColDocOperation,
		dao.DocDiffTaskTblColDocOperationStatus,
		dao.DocDiffTaskTblColQaOperation,
		dao.DocDiffTaskTblColQaOperationStatus,
		dao.DocDiffTaskTblColQaOperationResult,
		dao.DocDiffTaskTblColStatus,
		dao.DocDiffTaskTblColCreateTime}
	docDiffTask, err := dao.GetDocDiffTaskDao().GetDocDiffTask(ctx, selectColumns, corp.BusinessID, app.BusinessID,
		diffID)
	if err != nil {
		log.WarnContextf(ctx, "GetDocDiffTaskDocs err: %+v", err)
		return rsp, err
	}
	if docDiffTask == nil || docDiffTask.BusinessID == 0 {
		return rsp, nil
	}
	docBizIDs := make([]uint64, 0)
	docBizIDs = append(docBizIDs, docDiffTask.NewDocBizID)
	docBizIDs = append(docBizIDs, docDiffTask.OldDocBizID)
	docFilter := &dao.DocFilter{
		CorpId:      corpID,
		RobotId:     app.ID,
		BusinessIds: docBizIDs,
	}
	docs, err := dao.GetDocDao().GetDocDiffTaskDocs(ctx, docFilter)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocDiffTaskDocs err: %+v", err)
		return rsp, err
	}
	docIDs := make([]uint64, 0)
	for _, item := range docs {
		docIDs = append(docIDs, item.ID)
	}
	qaNums, err := s.dao.GetDocQANum(ctx, corpID, app.ID, docIDs)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	docDiffRsp := &pb.DescribeDocDiffTaskRsp{
		DocDiffTaskBizId: docDiffTask.BusinessID,
		NewDoc:           model.FormatDocDiffDocInfo(docDiffTask.NewDocBizID, docs, qaNums),
		OldDoc:           model.FormatDocDiffDocInfo(docDiffTask.OldDocBizID, docs, qaNums),
		ComparisonReason: docDiffTask.ComparisonReason,
		DiffType:         docDiffTask.DiffType,
		DocResultInfo: &pb.DescribeDocDiffTaskRspDocResult{
			DocOperation:       docDiffTask.DocOperation,
			DocOperationResult: docDiffTask.DocOperationStatus,
			NewDocRename:       docDiffTask.NewDocRename,
			OldDocRename:       docDiffTask.OldDocRename,
		},
		QaResultInfo: &pb.DescribeDocDiffTaskRspQaResult{
			QaOperation:       docDiffTask.QaOperation,
			QaOperationStatus: docDiffTask.QaOperationStatus,
			QaOperationResult: docDiffTask.QaOperationResult,
		},
		Status:     docDiffTask.Status,
		CreateTime: docDiffTask.CreateTime.Unix(),
	}
	return docDiffRsp, nil
}

// DeleteDocDiffTask 删除对比任务
func (s *Service) DeleteDocDiffTask(ctx context.Context, req *pb.DeleteDocDiffTaskReq) (*pb.DeleteDocDiffTaskRsp, error) {
	log.InfoContextf(ctx, "DeleteDocDiffTask Req:%+v", req)
	rsp := new(pb.DeleteDocDiffTaskRsp)
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corp, err := s.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		log.ErrorContextf(ctx, "GetCorpByID err: %+v", err)
		return rsp, err
	}
	log.InfoContextf(ctx, "DeleteDocDiffTask corpID:%d,app.BusinessIds:%d", corpID, app.BusinessID)
	err = doc_diff_task.DeleteDocDiffTask(ctx, corp.BusinessID, app.BusinessID, req.GetDocDiffTaskBizIds())
	if err != nil {
		return rsp, err
	}
	return rsp, nil
}

// HandleDocDiffTask 处理对比任务
func (s *Service) HandleDocDiffTask(ctx context.Context, req *pb.HandleDocDiffTaskReq) (*pb.HandleDocDiffTaskRsp, error) {
	log.InfoContextf(ctx, "HandleDocDiffTask Req:%+v", req)
	rsp := new(pb.HandleDocDiffTaskRsp)
	key := fmt.Sprintf(dao.LockHandleDocDiffTask, req.GetAppBizId())
	if err := s.dao.Lock(ctx, key, 10*time.Second); err != nil {
		return rsp, errs.ErrHandleDocDiffNotFail
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()
	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corp, err := s.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		log.ErrorContextf(ctx, "GetCorpByID err: %+v", err)
		return rsp, err
	}
	if len(req.List) == 0 {
		return nil, errs.ErrParams
	}

	log.InfoContextf(ctx, "HandleDocDiffTask corpID:%d,app.BusinessIds:%d", corpID, app.BusinessID)

	err = doc_diff_task.HandleDocDiffTaskAction(ctx, s.dao, staffID, corpID, app.ID, corp.BusinessID, app.BusinessID,
		req.List, rsp)
	if err != nil {
		return rsp, err
	}
	return rsp, nil
}
