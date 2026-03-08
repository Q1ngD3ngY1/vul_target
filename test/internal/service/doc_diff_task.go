package service

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// ListDocDiffTask 获取对比任务列表
func (s *Service) ListDocDiffTask(ctx context.Context, req *pb.ListDocDiffTaskReq) (*pb.ListDocDiffTaskRsp, error) {
	logx.I(ctx, "ListDocDiffTask Req:%+v", req)
	rsp := new(pb.ListDocDiffTaskRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetAppBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, app.CorpPrimaryId)
	// corp, err := s.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		logx.E(ctx, "GetCorpByID err: %+v", err)
		return rsp, err
	}
	offset, limit := utilx.Page(req.GetPageNumber(), req.GetPageSize())
	filter := &docEntity.DocDiffTaskFilter{
		CorpBizId:      corp.GetCorpId(),
		RobotBizId:     app.BizId,
		IsDeleted:      ptrx.Bool(false),
		Statuses:       req.GetStatuses(),
		OrderColumn:    []string{docEntity.DocDiffTaskTblColCreateTime},
		OrderDirection: []string{util.SqlOrderByDesc},
		Offset:         offset,
		Limit:          limit,
	}
	selectColumns := []string{
		docEntity.DocDiffTaskTblColBusinessId,
		docEntity.DocDiffTaskTblColNewDocBizId,
		docEntity.DocDiffTaskTblColOldDocBizId,
		docEntity.DocDiffTaskTblColNewDocRename,
		docEntity.DocDiffTaskTblColComparisonReason,
		docEntity.DocDiffTaskTblColDiffType,
		docEntity.DocDiffTaskTblColDocOperation,
		docEntity.DocDiffTaskTblColQaOperation,
		docEntity.DocDiffTaskTblColStatus,
		docEntity.DocDiffTaskTblColDiffDataProcessStatus,
		docEntity.DocDiffTaskTblColCreateTime}
	list, total, err := s.taskLogic.GetDocDiffTaskCountAndList(ctx, selectColumns, filter)
	if err != nil {
		logx.E(ctx, "ListDocDiffTask err:%+v", err)
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
	docFilter := &docEntity.DocFilter{
		CorpId:      app.CorpPrimaryId,
		RobotId:     app.PrimaryId,
		BusinessIds: docBizIDs,
	}
	docs, err := s.docLogic.GetDocDiffTaskDocs(ctx, docFilter)
	if err != nil {
		logx.E(ctx, "GetDocDiffTaskDocs err: %+v", err)
		return rsp, err
	}
	docIDs := make([]uint64, 0, len(list))
	for _, item := range docs {
		docIDs = append(docIDs, item.ID)
	}
	qaNums, err := s.qaLogic.GetDocQANum(ctx, app.CorpPrimaryId, app.PrimaryId, docIDs)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	rsp.Total = uint64(total)
	rsp.PageNumber = req.GetPageNumber()
	rsp.List = make([]*pb.ListDocDiffTaskRsp_DocDiffTask, 0, len(list))
	for _, docDiff := range list {
		docDiffRsp := &pb.ListDocDiffTaskRsp_DocDiffTask{
			DocDiffTaskBizId:      strconv.FormatUint(docDiff.BusinessID, 10),
			NewDoc:                docEntity.FormatDocDiffDocInfo(docDiff.NewDocBizID, docs, qaNums),
			OldDoc:                docEntity.FormatDocDiffDocInfo(docDiff.OldDocBizID, docs, qaNums),
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
	logx.I(ctx, "CreateDocDiffTask Req:%+v", req)
	rsp := new(pb.CreateDocDiffTaskRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetAppBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, app.CorpPrimaryId)
	// corp, err := s.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		logx.E(ctx, "GetCorpByID err: %+v", err)
		return rsp, err
	}
	staffID := contextx.Metadata(ctx).StaffID()
	staff, err := s.rpc.PlatformAdmin.GetStaffByID(ctx, staffID)
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

	diffID, err := s.taskLogic.CreateDocDiffTask(ctx, app.CorpPrimaryId, app.PrimaryId, corp.GetCorpId(), app.BizId, newDocID,
		oldDocID, staff.BusinessID, req.GetComparisonReason(), false)
	if err != nil {
		return rsp, err
	}

	rsp.DocDiffTaskBizId = strconv.FormatUint(diffID, 10)
	return rsp, nil
}

// DescribeDocDiffTask 获取对比任务
func (s *Service) DescribeDocDiffTask(ctx context.Context, req *pb.DescribeDocDiffTaskReq) (*pb.DescribeDocDiffTaskRsp, error) {
	logx.I(ctx, "DescribeDocDiffTask Req:%+v", req)
	rsp := new(pb.DescribeDocDiffTaskRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetAppBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, app.CorpPrimaryId)
	// corp, err := s.dao.GetCorpByID(ctx, app.CorpPrimaryId)
	if err != nil {
		logx.E(ctx, "GetCorpByID err: %+v", err)
		return rsp, err
	}
	logx.I(ctx, "DescribeDocDiffTask app.CorpPrimaryId:%d,app.BusinessIds:%d", app.CorpPrimaryId, app.BizId)
	diffID, err := util.CheckReqBotBizIDUint64(ctx, req.GetDocDiffTaskBizId())
	if err != nil {
		return nil, err
	}
	selectColumns := []string{
		docEntity.DocDiffTaskTblColBusinessId,
		docEntity.DocDiffTaskTblColNewDocBizId,
		docEntity.DocDiffTaskTblColOldDocBizId,
		docEntity.DocDiffTaskTblColNewDocRename,
		docEntity.DocDiffTaskTblColOldDocRename,
		docEntity.DocDiffTaskTblColComparisonReason,
		docEntity.DocDiffTaskTblColDiffType,
		docEntity.DocDiffTaskTblColDocOperation,
		docEntity.DocDiffTaskTblColDocOperationStatus,
		docEntity.DocDiffTaskTblColQaOperation,
		docEntity.DocDiffTaskTblColQaOperationStatus,
		docEntity.DocDiffTaskTblColQaOperationResult,
		docEntity.DocDiffTaskTblColStatus,
		docEntity.DocDiffTaskTblColCreateTime}
	docDiffTask, err := s.taskLogic.GetDocDiffTask(ctx, selectColumns, corp.GetCorpId(), app.BizId, diffID)
	if err != nil {
		logx.W(ctx, "GetDocDiffTaskDocs err: %+v", err)
		return rsp, err
	}
	if docDiffTask == nil || docDiffTask.BusinessID == 0 {
		return rsp, nil
	}
	docBizIDs := make([]uint64, 0)
	docBizIDs = append(docBizIDs, docDiffTask.NewDocBizID)
	docBizIDs = append(docBizIDs, docDiffTask.OldDocBizID)
	docFilter := &docEntity.DocFilter{
		CorpId:      app.CorpPrimaryId,
		RobotId:     app.PrimaryId,
		BusinessIds: docBizIDs,
	}
	docs, err := s.docLogic.GetDocDiffTaskDocs(ctx, docFilter)
	if err != nil {
		logx.E(ctx, "GetDocDiffTaskDocs err: %+v", err)
		return rsp, err
	}
	docIDs := make([]uint64, 0)
	for _, item := range docs {
		docIDs = append(docIDs, item.ID)
	}
	qaNums, err := s.qaLogic.GetDocQANum(ctx, app.CorpPrimaryId, app.PrimaryId, docIDs)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	docDiffRsp := &pb.DescribeDocDiffTaskRsp{
		DocDiffTaskBizId: docDiffTask.BusinessID,
		NewDoc:           docEntity.FormatDocDiffDocInfo(docDiffTask.NewDocBizID, docs, qaNums),
		OldDoc:           docEntity.FormatDocDiffDocInfo(docDiffTask.OldDocBizID, docs, qaNums),
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
	logx.I(ctx, "DeleteDocDiffTask Req:%+v", req)
	rsp := new(pb.DeleteDocDiffTaskRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetAppBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, app.CorpPrimaryId)
	// corp, err := s.dao.GetCorpByID(ctx, app.CorpPrimaryId)
	if err != nil {
		logx.E(ctx, "GetCorpByID err: %+v", err)
		return rsp, err
	}
	logx.I(ctx, "DeleteDocDiffTask app.CorpPrimaryId:%d,app.BusinessIds:%d", app.CorpPrimaryId, app.BizId)
	err = s.taskLogic.DeleteDocDiffTask(ctx, corp.GetCorpId(), app.BizId, req.GetDocDiffTaskBizIds())
	if err != nil {
		return rsp, err
	}
	return rsp, nil
}

// HandleDocDiffTask 处理对比任务
func (s *Service) HandleDocDiffTask(ctx context.Context, req *pb.HandleDocDiffTaskReq) (*pb.HandleDocDiffTaskRsp, error) {
	logx.I(ctx, "HandleDocDiffTask Req:%+v", req)
	rsp := new(pb.HandleDocDiffTaskRsp)
	key := fmt.Sprintf(dao.LockHandleDocDiffTask, req.GetAppBizId())
	if err := s.dao.Lock(ctx, key, 10*time.Second); err != nil {
		return rsp, errs.ErrHandleDocDiffNotFail
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetAppBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, app.CorpPrimaryId)
	// corp, err := s.dao.GetCorpByID(ctx, app.CorpPrimaryId)
	if err != nil {
		logx.E(ctx, "GetCorpByID err: %+v", err)
		return rsp, err
	}
	if len(req.List) == 0 {
		return nil, errs.ErrParams
	}

	logx.I(ctx, "HandleDocDiffTask app.CorpPrimaryId:%d,app.BusinessIds:%d", app.CorpPrimaryId, app.BizId)

	staffID := contextx.Metadata(ctx).StaffID()
	err = s.taskLogic.HandleDocDiffTaskAction(ctx, staffID, app.CorpPrimaryId, app.PrimaryId, corp.GetCorpId(), app.BizId,
		req.List, rsp)
	if err != nil {
		return rsp, err
	}
	return rsp, nil
}
