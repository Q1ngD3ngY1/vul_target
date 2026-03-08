package service

import (
	"context"
	"crypto/md5"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	logicCorp "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/corp"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"strconv"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	logicApp "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

const (
	maxCount = 50
)

// CreateTest 创建评测任务
func (s *Service) CreateTest(ctx context.Context, req *pb.CreateTestReq) (*pb.CreateTestRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.CreateTestRsp)
	return rsp, nil
}

// CreateEvaluateTest 创建评测任务
func (s *Service) CreateEvaluateTest(ctx context.Context, req *pb.CreateEvaluateTestReq) (
	*pb.CreateEvaluateTestRsp, error) {
	rsp := new(pb.CreateEvaluateTestRsp)
	key := fmt.Sprintf(dao.LockForCreateTest, md5.Sum([]byte(req.GetTestName())))
	if err := s.dao.Lock(ctx, key, 10*time.Second); err != nil {
		return nil, errs.ErrSameCreateTest
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()
	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	app, err := client.GetAppInfo(ctx, req.GetBotBizId(), model.AppTestScenes)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	isEdit, err := s.dao.IsDocInEditState(ctx, corpID, app.Id)
	if err != nil {
		return nil, errs.ErrSystem
	}
	if isEdit {
		return nil, errs.ErrCreateTest
	}
	test, err := s.dao.GetTestByName(ctx, corpID, app.Id, req.GetTestName())
	if err != nil {
		log.DebugContextf(ctx, "CreateTest get test %s, err :%v", req.GetTestName(), err)
		return nil, errs.ErrSystem
	}
	if test != nil {
		log.DebugContextf(ctx, "CreateTest check same name test %v", test)
		return nil, errs.ErrSameCreateTest
	}
	sets, err := s.dao.GetSampleSetsByBizIDs(ctx, corpID, app.Id, []uint64{req.GetSetBizId()})
	if err != nil {
		return nil, errs.ErrSystem
	}
	if len(sets) == 0 {
		return nil, errs.ErrSystem
	}
	robotTest := &model.RobotTest{
		CorpID:        corpID,
		RobotID:       app.Id,
		BusinessID:    s.dao.GenerateSeqID(),
		TestName:      req.GetTestName(),
		SetID:         sets[req.GetSetBizId()].ID,
		CreateStaffID: staffID,
		Status:        model.TestStatusRunning,
	}
	modelName, err := logicApp.GetAppNormalModelName(ctx, app)
	if err != nil {
		return nil, err
	}
	if !logicCorp.CheckModelStatus(ctx, s.dao, corpID, modelName, client.KnowledgeQAFinanceBizType) {
		return nil, errs.ErrNoTokenBalance
	}
	log.DebugContextf(ctx, "CreateTest create test %v", robotTest)
	if err = s.dao.CreateTest(ctx, robotTest); err != nil {
		return rsp, errs.ErrSystem
	}
	taskID, err := dao.NewTestTask(ctx, corpID, app.Id, robotTest.ID, staffID)
	if err != nil {
		log.ErrorContextf(ctx, "CreateTest new task err :%v", err)
		return rsp, errs.ErrSystem
	}
	robotTest.TaskID = taskID
	// 更新任务ID，作用中断执行中定时任务，非核心逻辑，可以不做错误响应
	_ = s.dao.UpdateTestTaskID(ctx, taskID, robotTest.ID)
	rsp = &pb.CreateEvaluateTestRsp{
		TestBizId: robotTest.BusinessID,
	}
	_ = s.dao.AddOperationLog(ctx, model.TestEventCreate, corpID, app.Id, req, rsp, nil, robotTest)
	return rsp, nil
}

// QueryTestList 条件查询任务列表
func (s *Service) QueryTestList(ctx context.Context, req *pb.QueryTestReq) (*pb.QueryTestRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.QueryTestRsp)
	return rsp, nil
}

// ListEvaluateTest 查询任务列表
func (s *Service) ListEvaluateTest(ctx context.Context, req *pb.ListEvaluateTestReq) (
	*pb.ListEvaluateTestRsp, error) {
	rsp := new(pb.ListEvaluateTestRsp)
	corpID := pkg.CorpID(ctx)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	total, list, err := s.dao.GetTests(ctx, corpID, app.ID, req.GetTestName(), uint64(req.GetPageNumber()),
		uint64(req.GetPageSize()))
	if err != nil {
		log.ErrorContextf(ctx, "QueryTestList err :%v", err)
		return rsp, errs.ErrSystem
	}
	rsp = &pb.ListEvaluateTestRsp{
		Total: uint32(total),
		List:  slicex.Map(list, (*model.RobotTest).ToRspList),
	}
	return rsp, nil
}

// DeleteTest 任务删除
func (s *Service) DeleteTest(ctx context.Context, req *pb.DeleteTestReq) (*pb.DeleteTestRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.DeleteTestRsp)
	return rsp, nil
}

// StopTest 任务停止
func (s *Service) StopTest(ctx context.Context, req *pb.StopTestReq) (*pb.StopTestRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.StopTestRsp)
	return rsp, nil
}

// RetryTest 任务重试
func (s *Service) RetryTest(ctx context.Context, req *pb.RetryTestReq) (*pb.RetryTestRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.RetryTestRsp)
	return rsp, nil
}

// DeleteEvaluateTest 任务删除
func (s *Service) DeleteEvaluateTest(ctx context.Context, req *pb.DeleteEvaluateTestReq) (
	*pb.DeleteEvaluateTestRsp, error) {
	rsp := new(pb.DeleteEvaluateTestRsp)
	corpID := pkg.CorpID(ctx)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	var ids []uint64
	tests, err := s.dao.GetTestByTestBizIDs(ctx, corpID, app.ID, req.GetTestBizIds())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	for _, test := range tests {
		ids = append(ids, test.ID)
	}
	if len(ids) > maxCount {
		return rsp, errs.ErrTestNum
	}
	key := fmt.Sprintf(dao.LockForOperateTest, req.GetBotBizId())
	if err := s.dao.Lock(ctx, key, 30*time.Second); err != nil {
		log.ErrorContextf(ctx, "test is operating, err:%v", err)
		return rsp, errs.ErrSystem
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()
	if err := s.dao.DeleteTest(ctx, app.ID, corpID, ids); err != nil {
		return rsp, err
	}
	_ = s.dao.AddOperationLog(ctx, model.TestEventDelete, corpID, app.ID, req, rsp, nil, nil)
	return rsp, nil
}

// StopEvaluateTest 任务停止
func (s *Service) StopEvaluateTest(ctx context.Context, req *pb.StopEvaluateTestReq) (*pb.StopEvaluateTestRsp, error) {
	rsp := new(pb.StopEvaluateTestRsp)
	corpID := pkg.CorpID(ctx)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	var ids []uint64
	tests, err := s.dao.GetTestByTestBizIDs(ctx, corpID, app.ID, req.GetTestBizIds())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	for _, test := range tests {
		if test.Status != model.TestStatusRunning && test.Status != model.TestStatusInit {
			log.ErrorContextf(ctx, "任务ID %d 当前状态 %d,不可停止", test.ID, test.Status)
			return rsp, errs.ErrStopTestStatus
		}
		ids = append(ids, test.ID)
	}
	if len(ids) > maxCount {
		return rsp, errs.ErrTestNum
	}
	key := fmt.Sprintf(dao.LockForOperateTest, req.GetBotBizId())
	if err := s.dao.Lock(ctx, key, 10*time.Second); err != nil {
		log.ErrorContextf(ctx, "test is operating, err:%v", err)
		return rsp, errs.ErrSystem
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()
	if err := s.dao.StopTest(ctx, app.ID, corpID, ids); err != nil {
		return rsp, errs.ErrSystem
	}
	_ = s.dao.AddOperationLog(ctx, model.TestEventStop, corpID, app.ID, req, rsp, nil, nil)
	return rsp, nil

}

// RetryEvaluateTest 任务重试
func (s *Service) RetryEvaluateTest(ctx context.Context, req *pb.RetryEvaluateTestReq) (
	*pb.RetryEvaluateTestRsp, error) {
	rsp := new(pb.RetryEvaluateTestRsp)
	corpID := pkg.CorpID(ctx)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	var ids []uint64
	tests, err := s.dao.GetTestByTestBizIDs(ctx, corpID, app.ID, req.GetTestBizIds())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	for _, test := range tests {
		if test.Status != model.TestStatusStop && test.Status != model.TestStatusFail {
			log.ErrorContextf(ctx, "任务ID %d 当前状态 %d,不可重试", test.ID, test.Status)
			return rsp, errs.ErrRetryTestStatus
		}
		ids = append(ids, test.ID)
	}
	if len(ids) > maxCount {
		return rsp, errs.ErrTestNum
	}
	key := fmt.Sprintf(dao.LockForOperateTest, req.GetBotBizId())
	if err := s.dao.Lock(ctx, key, 10*time.Second); err != nil {
		log.ErrorContextf(ctx, "test is operating, err:%v", err)
		return rsp, errs.ErrSystem
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()
	if err := s.dao.RetryTest(ctx, app.ID, corpID, ids); err != nil {
		return rsp, errs.ErrTestNum
	}
	_ = s.dao.AddOperationLog(ctx, model.TestEventRetry, corpID, app.ID, req, rsp, nil, nil)
	return rsp, nil
}

// GetOneWaitJudging 待标注测试记录详情
func (s *Service) GetOneWaitJudging(ctx context.Context, req *pb.GetOneJudgingReq) (*pb.GetOneJudgingRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.GetOneJudgingRsp)
	return rsp, nil
}

// DescribeWaitJudgeRecord 待标注测试记录详情
func (s *Service) DescribeWaitJudgeRecord(ctx context.Context, req *pb.DescribeWaitJudgeRecordReq) (
	*pb.DescribeWaitJudgeRecordRsp, error) {
	rsp := new(pb.DescribeWaitJudgeRecordRsp)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	test, err := s.dao.GetTestByTestBizID(ctx, req.GetTestBizId())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	if test == nil || test.Status != model.TestStatusJudging {
		return rsp, errs.ErrTestNotExist
	}
	record, _, err := s.dao.GetRecordToJudge(ctx, test.ID, app.AppKey)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	if record == nil {
		return nil, errs.ErrNoWaitJudgeRecords
	}
	rsp = &pb.DescribeWaitJudgeRecordRsp{
		JudgeNumber: uint32(test.JudgeNum),
		TestNumber:  uint32(test.TestNum),
		RecordBizId: record.BusinessID,
		Question:    record.Question,
		Answer:      record.Answer,
		ReplyMethod: uint32(record.ReplyMethod),
		Record:      record.MsgRecord,
	}
	return rsp, nil
}

// GetRecord 查询标注记录详情
func (s *Service) GetRecord(ctx context.Context, req *pb.GetRecordReq) (*pb.GetRecordRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.GetRecordRsp)
	return rsp, nil
}

// DescribeRecord 查询标注记录详情
func (s *Service) DescribeRecord(ctx context.Context, req *pb.DescribeRecordReq) (*pb.DescribeRecordRsp, error) {
	rsp := new(pb.DescribeRecordRsp)
	record, err := s.dao.GetTestRecordByBizID(ctx, req.GetRecordBizId())
	if err != nil {
		return rsp, err
	}
	test, err := s.dao.GetTestByTestBizID(ctx, req.GetTestBizId())
	if err != nil {
		return rsp, err
	}
	if test == nil {
		return rsp, errs.ErrTestNotExist
	}
	rsp = &pb.DescribeRecordRsp{
		JudgeNumber: uint32(test.JudgeNum),
		TestNumber:  uint32(test.TestNum),
		RecordBizId: record.BusinessID,
		Question:    record.Question,
		Answer:      record.Answer,
	}
	return rsp, nil
}

// JudgeRecord 标注会话
func (s *Service) JudgeRecord(ctx context.Context, req *pb.JudgeReq) (*pb.JudgeRsp, error) {
	rsp := new(pb.JudgeRsp)
	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	// API3.0逻辑 上云后删除if判断
	if req.GetTestBizId() > 0 && req.GetRecordBizId() > 0 {
		test, err := s.dao.GetTestByTestBizID(ctx, req.GetTestBizId())
		if err != nil {
			return rsp, errs.ErrSystem
		}
		record, err := s.dao.GetTestRecordByBizID(ctx, req.GetRecordBizId())
		if err != nil {
			return rsp, errs.ErrSystem
		}
		err = s.dao.JudgeTestRecord(ctx, test.ID, record.ID, staffID, uint64(req.GetAnswerJudge()))
		if err != nil {
			return rsp, errs.ErrSystem
		}
		_ = s.dao.AddOperationLog(ctx, model.TestRecordJudge, corpID, app.ID, req, rsp, nil, nil)
		return rsp, nil
	}
	err = s.dao.JudgeTestRecord(ctx, req.GetTestId(), req.GetRecordId(), staffID, uint64(req.GetAnswerJudge()))
	if err != nil {
		return rsp, errs.ErrSystem
	}
	_ = s.dao.AddOperationLog(ctx, model.TestRecordJudge, corpID, app.ID, req, rsp, nil, nil)
	return rsp, nil
}

func (s *Service) isInTestMode(ctx context.Context, corpID uint64, robotID uint64, setIDs []uint64) error {
	ids := make([]string, 0, len(setIDs))
	for _, setID := range setIDs {
		// 后续可能从一个任务对应一个样本集合改成一个任务多个样本集，所以数据库字段设计的string
		ids = append(ids, strconv.FormatInt(int64(setID), 10))
	}
	list, err := s.dao.GetRunningTests(ctx, corpID, robotID, ids)
	if err != nil {
		log.ErrorContextf(ctx, "查询正在执行的评测任务列表失败 %+v", err)
		return errs.ErrSystem
	}
	if len(list) != 0 {
		return errs.ErrTestRunning
	}
	return nil
}

// ExportEvaluateTask 导出评测任务
func (s *Service) ExportEvaluateTask(ctx context.Context, req *pb.ExportEvaluateTaskReq) (
	*pb.ExportEvaluateTaskRsp, error) {
	rsp := new(pb.ExportEvaluateTaskRsp)
	corpID := pkg.CorpID(ctx)
	staffID := pkg.StaffID(ctx)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrExportQA
	}
	log.DebugContextf(ctx, "CreateExportEvaluateTask corpID:%d, staffID:%d, appID:%d", corpID, staffID, app.ID)
	cosURL, err := s.dao.CreateExportEvaluateTask(ctx, corpID, req.GetTestBizId(), app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "导出评测任务失败, 获取机器人ID失败, req: %+v, err: %+v", req, err)
		return rsp, errs.ErrExportQA
	}
	rsp.CosPath = cosURL
	log.DebugContextf(ctx, "CreateExportEvaluateTask cosURL:%s", cosURL)
	return rsp, nil
}
