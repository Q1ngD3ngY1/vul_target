package dao

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"

	"google.golang.org/protobuf/encoding/protojson"

	"google.golang.org/protobuf/proto"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/chat"
	"github.com/jmoiron/sqlx"
)

// DB SQL
const (
	batchRecordLimit = 700
	testFields       = `id,business_id,corp_id,robot_id,test_name,test_num,set_id,judge_num,judge_right_num,
						message,status,task_id,create_staff_id,create_time,update_time,test_done_num`
	testRecordFields = `id,business_id,test_id,set_id,question,answer,answer_judge,reference,reply_method,
						role_description,trace_id,create_time,update_time,record_id,related_record_id,custom_variables`
	createTest = `
		INSERT INTO 
		    t_evaluate_test (business_id,corp_id,robot_id,test_name,test_num,set_id,status,create_staff_id,test_done_num)
		VALUES 
		    (:business_id,:corp_id,:robot_id,:test_name,:test_num,:set_id,:status,:create_staff_id,:test_done_num)`
	createRecords = `
		INSERT INTO t_evaluate_test_record (business_id,test_id,set_id,question,answer,reference,prompt,
        role_description,custom_variables)
		VALUES
        (:business_id,:test_id,:set_id,:question,'','','',:role_description,:custom_variables)`
	getTestCount = `
		SELECT 
    		COUNT(*) 
		FROM 
		    t_evaluate_test 
		WHERE 
		    corp_id = ? AND robot_id =? AND status != 4 %s 
	`
	getTests = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_test 
		WHERE 
		    corp_id = ? AND robot_id = ? AND status != 4 %s 
		ORDER BY 
		    id DESC 
		LIMIT ?,?
		`
	getTestsBySetID = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_test 
		WHERE 
		    corp_id = ? AND robot_id = ? AND status IN (0,1) %s
		LIMIT 1
		`
	getTestByTestID = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_test 
		WHERE 
		    id = ?
		LIMIT 1
		`
	getTestByTestBizID = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_test 
		WHERE 
		    business_id = ?
		LIMIT 1
		`
	getTestByTestIDs = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_test 
		WHERE 
		    corp_id = ? AND robot_id = ? AND id IN (?)
		`
	getTestByTestBizIDs = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_test 
		WHERE 
		    corp_id = ? AND robot_id = ? AND business_id IN (%s)
		`
	getTestByName = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_test 
		WHERE 
		    corp_id = ? AND robot_id = ? AND test_name = ? AND status != 4
		LIMIT 1
		`
	getOneRecord = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_test_record 
		WHERE 
		    test_id = ? AND answer_judge = 0
		ORDER BY id ASC 
		LIMIT 1
		`
	getTestRecordByID = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_test_record 
		WHERE 
		    test_id = ? AND id = ?
		ORDER BY id DESC 
		LIMIT 1
		`
	getTestRecordByTestID = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_test_record 
		WHERE 
		    test_id = ? 
		`
	getTestRecordByBizID = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_test_record 
		WHERE 
		    business_id = ?
		`
	updateTestStateByID = `
		UPDATE 
		    t_evaluate_test 
		SET 
		    status = :status,
		    message = :message
		WHERE 
		    corp_id = :corp_id AND robot_id = :robot_id AND id = :id
	`

	updateTestDoneNum = `
		UPDATE 
		    t_evaluate_test 
		SET test_done_num = CASE 
			WHEN test_done_num + 1 > test_num THEN test_num 
			ELSE test_done_num + 1  
		END 
		WHERE 
		    corp_id = ? AND robot_id = ? AND id = ? 
	`

	updateTaskIDByID = `
		UPDATE 
		    t_evaluate_test 
		SET 
		    task_id = ?
		WHERE 
		    id = ?
	`

	updateRetryTaskIDByID = `
		UPDATE 
		    t_evaluate_test 
		SET 
		    task_id = ?,
		    status = 1,
		    message = "" 
		WHERE 
		    id = ?
	`

	updateTestJudgeState = `
		UPDATE 
		    t_evaluate_test 
		SET 
		    judge_num = ?,
		    judge_right_num = ?
		WHERE 
		    id = ?
	`
	updateTestStatusByID = `
		UPDATE 
		    t_evaluate_test 
		SET 
		    status = ?
		WHERE 
		    id IN (?)
	`
	countRecordJudge = `
		SELECT 
    		COUNT(*) 
		FROM 
		    t_evaluate_test_record 
		WHERE 
		    test_id = ? AND answer_judge = ?
		`
	countJudgeGroup = `
		SELECT 
    		answer_judge,COUNT(*) AS judge_count
		FROM 
		    t_evaluate_test_record 
		WHERE 
		    test_id = ? GROUP BY answer_judge
		`

	judgeRecord = `
		UPDATE 
    		t_evaluate_test_record
		SET 
		    answer_judge = ?,
		    judge_staff_id = ?
		WHERE 
		    test_id = ? AND id = ?
		`
	updateTestRecord = `
		UPDATE 
    		t_evaluate_test_record
		SET 
		    question = :question,
			answer = :answer,
		    reference = :reference,
		    prompt = :prompt,
		    reply_method = :reply_method,
		    record_id = :record_id,
		    trace_id = :trace_id,
		    related_record_id = :related_record_id
		WHERE 
		    test_id = :test_id AND id = :id
		`
	updateTestRecordFromSelf = `
		UPDATE 
    		t_evaluate_test_record
		SET 
		    trace_id = :trace_id,
		    related_record_id = :related_record_id,
		    reply_method = :reply_method 
		WHERE 
		    test_id = :test_id AND id = :id
		`

	deleteTestsByIDs = `
		UPDATE 
		    t_evaluate_test 
		SET 
		    status = 4
		WHERE 
		    corp_id = ? AND robot_id = ? AND id IN (?)
	`
	getTestRecords = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_test_record 
		WHERE 
		    test_id = ? AND related_record_id = "" 
		ORDER BY id DESC
		LIMIT ?,?
		`
	getDeleteTestsBySampleSetID = `
		SELECT 
    		%s 
		FROM 
		    t_evaluate_test 
		WHERE 
		    corp_id = ? AND robot_id = ? AND set_id = ? 
		`
	deleteEvaluateTests = `
		DELETE FROM 
		           t_evaluate_test 
		       WHERE corp_id = ? AND robot_id = ? AND set_id = ?  `
	deleteTestRecords = `
		DELETE FROM 
		           t_evaluate_test_record 
		       WHERE set_id = ?  `
)

// CreateTest 创建评测任务
func (d *dao) CreateTest(ctx context.Context, test *model.RobotTest) error {
	return d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		samples, err := d.GetSampleRecordsBySetIDs(ctx, []uint64{test.SetID})
		if err != nil {
			return err
		}
		test.TestNum = uint64(len(samples))
		res, err := tx.NamedExecContext(ctx, createTest, test)
		if err != nil {
			log.ErrorContextf(ctx, "创建评测任务失败, sql:%s, args:%+v, err:%+v", createTest, test, err)
			return err
		}
		id, _ := res.LastInsertId()
		test.ID = uint64(id)
		records := make([]*model.RobotTestRecord, 0, len(samples))
		for _, sample := range samples {
			if sample == nil {
				continue
			}
			records = append(records, &model.RobotTestRecord{
				BusinessID:      d.GenerateSeqID(),
				TestID:          test.ID,
				SetID:           sample.SetID,
				Question:        sample.Content,
				RoleDescription: sample.RoleDescription,
				CustomVariables: sample.CustomVariables,
			})
		}
		preRecordsList := slicex.Chunk(records, batchRecordLimit)
		for _, preRecords := range preRecordsList {
			if _, err := tx.NamedExecContext(ctx, createRecords, preRecords); err != nil {
				log.ErrorContextf(ctx, "批量插入任务样本列表失败, sql:%s, err:%+v", createRecords, err)
				return err
			}
		}
		return nil
	})
}

// GetTestByTestID 查询一个评测任务
func (d *dao) GetTestByTestID(ctx context.Context, testID uint64) (*model.RobotTest, error) {
	querySQL := fmt.Sprintf(getTestByTestID, testFields)
	var tests []*model.RobotTest
	log.DebugContextf(ctx, "根据任务ID获取任务列表 sql:%s args:%+v", querySQL, testID)
	if err := d.db.QueryToStructs(ctx, &tests, querySQL, testID); err != nil {
		log.ErrorContextf(ctx, "获取任务列表失败 sql:%s args:%+v err:%+v", querySQL, testID, err)
		return nil, err
	}
	if len(tests) == 0 {
		return nil, nil
	}
	return tests[0], nil
}

// GetTestByTestBizID 根据业务ID查询评测任务
func (d *dao) GetTestByTestBizID(ctx context.Context, testBizID uint64) (*model.RobotTest, error) {
	querySQL := fmt.Sprintf(getTestByTestBizID, testFields)
	var tests []*model.RobotTest
	log.DebugContextf(ctx, "根据任务ID获取任务列表 sql:%s args:%+v", querySQL, testBizID)
	if err := d.db.QueryToStructs(ctx, &tests, querySQL, testBizID); err != nil {
		log.ErrorContextf(ctx, "获取任务列表失败 sql:%s args:%+v err:%+v", querySQL, testBizID, err)
		return nil, err
	}
	if len(tests) == 0 {
		return nil, nil
	}
	return tests[0], nil
}

// GetTestByTestBizIDs 根据业务ID查询评测任务
func (d *dao) GetTestByTestBizIDs(ctx context.Context, corpID, robotID uint64, testBizIDs []uint64) ([]*model.RobotTest,
	error) {
	if len(testBizIDs) == 0 {
		return nil, nil
	}
	querySQL := fmt.Sprintf(getTestByTestBizIDs, testFields, placeholder(len(testBizIDs)))
	args := []any{corpID, robotID}
	for _, id := range testBizIDs {
		args = append(args, id)
	}
	var tests []*model.RobotTest
	log.DebugContextf(ctx, "根据任务ID获取任务列表 sql:%s args:%+v", querySQL, args)
	if err := d.db.QueryToStructs(ctx, &tests, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取任务列表失败 sql:%s args:%+v err:%+v", querySQL, testBizIDs, err)
		return nil, err
	}
	return tests, nil
}

// StopTest 事务批量停止评测任务
func (d *dao) StopTest(ctx context.Context, robotID uint64, corpID uint64, testIDs []uint64) error {
	tests, err := d.getTestsByIDs(ctx, robotID, corpID, testIDs)
	if err != nil {
		return err
	}
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		if err := updateTestsStatusByIDs(ctx, tx, model.TestStatusStop, testIDs); err != nil {
			return err
		}
		for _, test := range tests {
			if test.TaskID > 0 && (test.Status == model.TestStatusRunning || test.Status == model.TestStatusInit) {
				log.InfoContextf(ctx, "根据任务ID %d 终止任务", test.TaskID)
				err = stopTestTask(ctx, test.TaskID)
				if err != nil {
					log.ErrorContextf(ctx, "任务ID %d 终止任务,失败 %+v", test.TaskID, err)
					return err
				}
				test.Status = model.TestStatusFail
				test.Message = model.ReasonStop
				err = d.CreateNotice(ctx, test.CreateTestNotice(ctx))
				if err != nil {
					// 通知为非必要逻辑，失败不导致整个事务回滚
					log.ErrorContextf(ctx, "任务ID %d 发送通知,失败 %+v", test.TaskID, err)
				}
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "批量停止评测任务失败 err:%+v", err)
		return err
	}
	return nil
}

// RetryTest 事务批量重试评测任务
func (d *dao) RetryTest(ctx context.Context, robotID uint64, corpID uint64, testIDs []uint64) error {
	tests, err := d.getTestsByIDs(ctx, robotID, corpID, testIDs)
	if err != nil {
		return err
	}
	staffID := pkg.StaffID(ctx)
	if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		for _, test := range tests {
			taskID, err := NewTestTask(ctx, corpID, robotID, test.ID, staffID)
			if err != nil {
				return errs.ErrSystem
			}
			args := []any{taskID, test.ID}
			if _, err := tx.ExecContext(ctx, updateRetryTaskIDByID, args...); err != nil {
				log.ErrorContextf(ctx, "更新任务taskID失败 sql:%s args:%+v err:%+v", updateRetryTaskIDByID, args, err)
				return err
			}
		}
		return nil
	}); err != nil {
		log.ErrorContextf(ctx, "批量重试评测任务失败 err:%+v", err)
		return err
	}
	return nil
}

// DeleteTest 事务批量删除评测任务
func (d *dao) DeleteTest(ctx context.Context, robotID uint64, corpID uint64, testIDs []uint64) error {
	tests, err := d.getTestsByIDs(ctx, robotID, corpID, testIDs)
	if err != nil {
		return errs.ErrSystem
	}
	for _, test := range tests {
		if test.Status == model.TestStatusRunning {
			return errs.ErrDeleteRunningTest
		}
	}
	sql, args, err := sqlx.In(updateTestStatusByID, model.TestStatusDeleted, testIDs)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteTest 拼接 更新sql 和 参数 异常，err:%v", err)
		return errs.ErrSystem
	}
	if _, err := d.db.Exec(ctx, sql, args...); err != nil {
		log.ErrorContextf(ctx, "批量停止任务失败 sql:%s args:%+v err:%+v", sql, args, err)
		return errs.ErrSystem
	}
	return nil
}

// GetTestByName 查询一个在线同名评测任务
func (d *dao) GetTestByName(ctx context.Context, corpID, robotID uint64, testName string) (*model.RobotTest, error) {
	querySQL := fmt.Sprintf(getTestByName, testFields)
	var tests []model.RobotTest
	args := []any{corpID, robotID, testName}
	log.DebugContextf(ctx, "根据任务Name获取任务 sql:%s args:%+v", querySQL, args)
	if err := d.db.QueryToStructs(ctx, &tests, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取任务列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	if len(tests) == 0 {
		return nil, nil
	}
	return &tests[0], nil
}

// GetRunningTests 查询使用该样本集的评测中的任务列表（查询到一个就行）
func (d *dao) GetRunningTests(ctx context.Context, corpID, robotID uint64,
	setIDs []string) (tests []*model.RobotTest, err error) {
	sql := fmt.Sprintf(getTestsBySetID, testFields, "")
	args := []any{corpID, robotID}
	if len(setIDs) > 0 {
		querySQL := fmt.Sprintf(getTestsBySetID, testFields, "AND set_id IN (?) ")
		sql, args, err = sqlx.In(querySQL, corpID, robotID, setIDs)
		if err != nil {
			log.ErrorContextf(ctx, "查询样本集任务列表参数异常, sql:%s, setIDs:%+v, err:%+v", sql, setIDs, err)
			return nil, err
		}
	}
	log.DebugContextf(ctx, "根据样本集ID获取任务列表 sql:%s args:%+v", sql, args)
	if err := d.db.QueryToStructs(ctx, &tests, sql, args...); err != nil {
		log.ErrorContextf(ctx, "获取样本集列表失败 sql:%s args:%+v err:%+v", sql, args, err)
		return nil, err
	}
	return tests, nil
}

// GetTests 分页查询机器人评测任务
func (d *dao) GetTests(ctx context.Context, corpID, robotID uint64, testName string, page,
	pageSize uint64) (uint64, []*model.RobotTest, error) {
	args := []any{corpID, robotID}
	condition := ""
	if testName != "" {
		condition = " AND test_name LIKE ?"
		args = append(args, fmt.Sprintf("%%%s%%", special.Replace(testName)))
	}
	querySQL := fmt.Sprintf(getTestCount, condition)
	var total uint64
	if err := d.db.Get(ctx, &total, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取任务总数失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, nil, err
	}
	querySQL = fmt.Sprintf(getTests, testFields, condition)
	offset := (page - 1) * pageSize
	args = append(args, offset, pageSize)
	tests := make([]*model.RobotTest, 0)
	log.DebugContextf(ctx, "获取任务列表 sql:%s args:%+v", querySQL, args)
	if err := d.db.QueryToStructs(ctx, &tests, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "获取样本集列表失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return 0, nil, err
	}
	return total, tests, nil
}

// GetRecordToJudge 根据评测任务ID查询一个可以标注的记录
func (d *dao) GetRecordToJudge(ctx context.Context, testID uint64, appKey string) (*model.RobotTestRecord,
	*chat.MsgRecord, error) {
	var records []model.RobotTestRecord
	querySQL := fmt.Sprintf(getOneRecord, testRecordFields)
	if err := d.db.QueryToStructs(ctx, &records, querySQL, testID); err != nil {
		log.ErrorContextf(ctx, "获取可标注记录失败 sql:%s args:%+v err:%+v", querySQL, testID, err)
		return nil, nil, err
	}
	if len(records) == 0 {
		log.ErrorContextf(ctx, "没有待标注记录 sql:%s args:%+v", querySQL, testID)
		return nil, nil, nil
	}

	msgRecord, err := d.getRecordToJudgeMsgRecord(ctx, records[0], appKey)
	if err != nil {
		log.ErrorContextf(ctx, "getRecordToJudgeMsgRecord  根据评测记录查询消息详情 fail err:%+v", err)
		return &records[0], nil, err
	}
	if msgRecord == nil || msgRecord.Content == "" || records[0].RecordID == "" {
		log.InfoContextf(ctx, "getRecordToJudgeMsgRecord 历史消息记录为空 records:%v", records[0])
		return &records[0], nil, nil
	}

	msgRecordStr, err := toJsonString(msgRecord)
	if err != nil {
		log.ErrorContextf(ctx, "toJsonString 评测消息记录转换json fail err:%+v", err)
		return &records[0], nil, err
	}
	log.InfoContextf(ctx, "GetMsgRecord msgRecordStr:%s", msgRecordStr)
	records[0].MsgRecord = msgRecordStr

	forMsgRecord := &chat.MsgRecord{}
	err = jsonToPb(msgRecordStr, forMsgRecord)
	if err != nil {
		log.ErrorContextf(ctx, "UnmarshalFromString 评测消息记录json序列化 fail err:%+v", err)
		return &records[0], nil, err
	}

	log.InfoContextf(ctx, "formatMsgRecord msgRsp:%v", forMsgRecord)
	return &records[0], forMsgRecord, nil
}

// jsonToPb Json转换pb
func jsonToPb(jsonStr string, pb proto.Message) error {
	unmarshaler := protojson.UnmarshalOptions{}
	err := unmarshaler.Unmarshal([]byte(jsonStr), pb)
	if err != nil {
		return err
	}
	return nil
}

// toJsonString pb转换Json
func toJsonString(msg proto.Message) (string, error) {
	marshaler := protojson.MarshalOptions{
		EmitUnpopulated: true, // 设置为 true，这样即使字段的值为零值，它们也会被编码为 JSON
	}
	bytes, err := marshaler.Marshal(msg)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// getRecordToJudgeMsgRecord 根据评测记录查询消息详情
func (d *dao) getRecordToJudgeMsgRecord(ctx context.Context, record model.RobotTestRecord, appKey string) (
	*chat.MsgRecord, error) {
	traceID := ""
	if record.TraceID.Valid {
		traceID = record.TraceID.String
	}
	if traceID == "" || appKey == "" || record.RecordID == "" || record.RelatedRecordID == "" {
		log.InfoContextf(ctx, "历史评测没有消息记录|traceID:%s|appKey:%s|RecordID:%s|RelatedRecordID:%s|records:%v",
			traceID, appKey, record.RecordID, record.RelatedRecordID, record)
		return nil, nil
	}
	req := &chat.GetMsgRecordReq{
		SessionId: traceID,
		Count:     2,
		Scene:     1,
		Type:      5,
		BotAppKey: appKey,
	}
	msgRsp, err := d.chatCli.GetMsgRecord(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "getRecordToJudgeMsgRecord|GetMsgRecord|err:%+v", err)
		return nil, nil
	}
	if msgRsp == nil {
		log.InfoContextf(ctx, "getRecordToJudgeMsgRecord 历史消息记录为空 msgRsp:%v", msgRsp)
		return nil, nil
	}
	log.InfoContextf(ctx, "getRecordToJudgeMsgRecord record count:%v", len(msgRsp.Records))
	for _, recordInfo := range msgRsp.Records {
		if recordInfo.RecordId == record.RecordID { // 通过recordID查询到对应的回答消息记录
			return recordInfo, nil
		}
	}
	return nil, nil
}

// GetTestRecordByID 查询标注的记录
func (d *dao) GetTestRecordByID(ctx context.Context, testID uint64, id uint64) (*model.RobotTestRecord, error) {
	var records []model.RobotTestRecord
	querySQL := fmt.Sprintf(getTestRecordByID, testRecordFields)
	if err := d.db.QueryToStructs(ctx, &records, querySQL, testID, id); err != nil {
		log.ErrorContextf(ctx, "获取可标注记录失败 sql:%s args:%+v err:%+v", querySQL, id, err)
		return nil, errs.ErrSystem
	}
	if len(records) == 0 {
		log.ErrorContextf(ctx, "没有待标注记录 sql:%s args:%+v", querySQL, id)
		return nil, errs.ErrNoWaitJudgeRecords
	}
	return &records[0], nil
}

// GetTestRecordByTestID 根据testID查询标注的记录
func (d *dao) GetTestRecordByTestID(ctx context.Context, testID uint64) ([]model.RobotTestRecord, error) {
	var records []model.RobotTestRecord
	querySQL := fmt.Sprintf(getTestRecordByTestID, testRecordFields)
	if err := d.db.QueryToStructs(ctx, &records, querySQL, testID); err != nil {
		log.ErrorContextf(ctx, "获取标注记录失败 sql:%s args:%+v err:%+v", querySQL, err)
		return nil, errs.ErrSystem
	}
	if len(records) == 0 {
		log.ErrorContextf(ctx, "没有标注记录 sql:%s args:%+v", querySQL, testID)
		return nil, errs.ErrRecords
	}
	return records, nil
}

// GetTestRecordByBizID 查询标注的记录
func (d *dao) GetTestRecordByBizID(ctx context.Context, id uint64) (*model.RobotTestRecord, error) {
	var records []model.RobotTestRecord
	querySQL := fmt.Sprintf(getTestRecordByBizID, testRecordFields)
	if err := d.db.QueryToStructs(ctx, &records, querySQL, id); err != nil {
		log.ErrorContextf(ctx, "获取可标注记录失败 sql:%s args:%+v err:%+v", querySQL, id, err)
		return nil, errs.ErrSystem
	}
	if len(records) == 0 {
		log.ErrorContextf(ctx, "没有待标注记录 sql:%s args:%+v", querySQL, id)
		return nil, errs.ErrNoWaitJudgeRecords
	}
	return &records[0], nil
}

// UpdateTestStatus 更新任务状态，必要参数：RobotID、CorpID、ID、Status、message
func (d *dao) UpdateTestStatus(ctx context.Context, test *model.RobotTest) error {
	if _, err := d.db.NamedExec(ctx, updateTestStateByID, test); err != nil {
		log.ErrorContextf(ctx, "更新任务状态失败 sql:%s args:%+v err:%+v", updateTestStateByID, test, err)
		return err
	}
	return nil
}

// UpdateTestDoneNum 更新任务完成数  必要参数：CorpID、RobotID、TestID
func (d *dao) UpdateTestDoneNum(ctx context.Context, corpID, robotID, testID uint64) error {
	args := []any{corpID, robotID, testID}
	if _, err := d.db.Exec(ctx, updateTestDoneNum, args...); err != nil {
		log.ErrorContextf(ctx, "更新任务完成数失败 sql:%s args:%+v err:%+v", updateTestDoneNum, args, err)
		return err
	}
	return nil
}

// UpdateTestTaskID 更新任务
func (d *dao) UpdateTestTaskID(ctx context.Context, taskID, testID uint64) error {
	args := []any{taskID, testID}
	if _, err := d.db.Exec(ctx, updateTaskIDByID, args...); err != nil {
		log.ErrorContextf(ctx, "更新任务状态失败 sql:%s args:%+v err:%+v", updateTaskIDByID, args, err)
		return err
	}
	return nil
}

// JudgeTestRecord 标注记录
func (d *dao) JudgeTestRecord(ctx context.Context, testID, recordID, staffID, judge uint64) error {
	return d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		args := []any{judge, staffID, testID, recordID}
		if _, err := tx.ExecContext(ctx, judgeRecord, args...); err != nil {
			log.ErrorContextf(ctx, "标注记录失败 sql:%s args:%+v err:%+v", judgeRecord, args, err)
			return err
		}
		var rightNum, errorNum, waitNum uint64
		var countList []model.JudgeCount
		if err := tx.SelectContext(ctx, &countList, countJudgeGroup, testID); err != nil {
			log.ErrorContextf(ctx, "计算标注数量记录失败 sql:%s args:%+v err:%+v", countRecordJudge, testID, err)
			return err
		}
		for _, count := range countList {
			switch count.Judge {
			case model.JudgeWait:
				waitNum = count.Count
			case model.JudgeRight:
				rightNum = count.Count
			case model.JudgeError:
				errorNum = count.Count
			}
		}
		args = []any{rightNum + errorNum, rightNum, testID}
		if _, err := tx.ExecContext(ctx, updateTestJudgeState, args...); err != nil {
			log.ErrorContextf(ctx, "更新标注记录后任务状况失败 sql:%s args:%+v err:%+v",
				updateTestJudgeState, args, err)
			return err
		}
		if waitNum == 0 {
			_, err := tx.ExecContext(ctx, updateTestStatusByID, model.TestStatusFinish, testID)
			if err != nil {
				log.ErrorContextf(ctx, "更新标注完成状态失败, sql:%s, testID:%d, err:%+v",
					updateTestStatusByID, testID, err)
				return err
			}
		}
		return nil
	})

}

// UpdateTestRecord 更新记录
func (d *dao) UpdateTestRecord(ctx context.Context, record *model.RobotTestRecord) error {
	if _, err := d.db.NamedExec(ctx, updateTestRecord, record); err != nil {
		log.ErrorContextf(ctx, "更新记录评测结果失败 sql:%s args:%+v err:%+v", updateTestRecord, record, err)
		return err
	}
	return nil
}

// UpdateTestRecordFromSelf 更新首条记录
func (d *dao) UpdateTestRecordFromSelf(ctx context.Context, record *model.RobotTestRecord) error {
	if _, err := d.db.NamedExec(ctx, updateTestRecordFromSelf, record); err != nil {
		log.ErrorContextf(ctx, "更新首条记录评测结果失败 sql:%s args:%+v err:%+v",
			updateTestRecordFromSelf, record, err)
		return err
	}
	return nil
}

// DeleteRobotTests 批量删除评测任务
func (d *dao) DeleteRobotTests(ctx context.Context, corpID, robotID uint64, ids []uint64) error {
	sql, args, err := sqlx.In(deleteTestsByIDs, corpID, robotID, ids)
	if err != nil {
		log.ErrorContextf(ctx, "删除评测任务参数异常, sql:%s, ids:%+v, err:%+v", deleteTestsByIDs, ids, err)
		return err
	}
	if _, err = d.db.Exec(ctx, sql, args...); err != nil {
		log.ErrorContextf(ctx, "删除评测任务失败, sql:%s, args:%+v, err:%+v", deleteTestsByIDs, ids, err)
		return err
	}
	return nil
}

// GetRecordByTestIDs 查询评测任务的全部待评测记录
func (d *dao) GetRecordByTestIDs(ctx context.Context, testID uint64) ([]*model.RobotTestRecord, error) {
	querySQL := fmt.Sprintf(getTestRecords, testRecordFields)
	var records []*model.RobotTestRecord
	for start := 0; ; start += batchRecordLimit {
		var subList []*model.RobotTestRecord
		err := d.db.QueryToStructs(ctx, &subList, querySQL, testID, start, batchRecordLimit)
		if err != nil {
			log.ErrorContextf(ctx, "获取样本列表失败 sql:%s args:%+v err:%+v", querySQL, testID, err)
			return nil, err
		}
		records = append(records, subList...)
		if len(subList) < batchRecordLimit {
			break
		}
	}
	return records, nil
}

func (d *dao) getTestsByIDs(
	ctx context.Context, robotID uint64, corpID uint64, testIDs []uint64,
) ([]*model.RobotTest, error) {
	querySQL := fmt.Sprintf(getTestByTestIDs, testFields)
	sql, args, err := sqlx.In(querySQL, corpID, robotID, testIDs)
	if err != nil {
		log.ErrorContextf(ctx, "StopTest 拼接 查询sql 和 参数 异常，err:%v", err)
		return nil, err
	}
	var tests []*model.RobotTest
	log.DebugContextf(ctx, "根据任务ID获取任务列表 sql:%s args:%+v", sql, args)
	if err := d.db.QueryToStructs(ctx, &tests, sql, args...); err != nil {
		log.ErrorContextf(ctx, "获取任务列表失败 sql:%s args:%+v err:%+v", sql, args, err)
		return nil, err
	}
	if len(testIDs) > len(tests) {
		log.ErrorContextf(ctx, "获取任务列表数量不符合")
		return nil, errs.ErrTestNum
	}
	return tests, nil
}

func updateTestsStatusByIDs(ctx context.Context, tx *sqlx.Tx, status uint64, testIDs []uint64) error {
	sql, args, err := sqlx.In(updateTestStatusByID, status, testIDs)
	if err != nil {
		log.ErrorContextf(ctx, "StopTest 拼接 更新sql 和 参数 异常，err:%v", err)
		return err
	}
	if _, err := tx.ExecContext(ctx, sql, args...); err != nil {
		log.ErrorContextf(ctx, "批量更新任务状态失败 sql:%s args:%+v err:%+v", sql, args, err)
		return err
	}
	return nil
}

// GetDeleteTestsBySampleSetID 查询评测集下所有待删除的评测任务
func (d *dao) GetDeleteTestsBySampleSetID(ctx context.Context, corpID, robotID, setID uint64) ([]*model.RobotTest,
	error) {
	args := []any{corpID, robotID, setID}
	querySQL := fmt.Sprintf(getDeleteTestsBySampleSetID, testFields)
	tests := make([]*model.RobotTest, 0)
	log.DebugContextf(ctx, "查询评测集下所有待删除的评测任务 sql:%s args:%+v", querySQL, args)
	if err := d.db.QueryToStructs(ctx, &tests, querySQL, args...); err != nil {
		log.ErrorContextf(ctx, "查询评测集下所有待删除的评测任务失败 sql:%s args:%+v err:%+v", querySQL, args, err)
		return nil, err
	}
	return tests, nil
}

// DeleteEvaluateTests 删除样本集下所有评测任务
func (d *dao) DeleteEvaluateTests(ctx context.Context, corpID, robotID, setID uint64) error {
	deleteSQL := deleteEvaluateTests
	args := []any{corpID, robotID, setID}
	result, err := d.db.Exec(ctx, deleteSQL, args...)
	if err != nil {
		log.ErrorContextf(ctx, "删除样本集下所有评测任务失败, sql:%s,corpID:%+v,robotID:%+v, setID:%+v, err:%+v",
			deleteSQL, corpID, robotID, setID, err)
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.ErrorContextf(ctx, "DeleteEvaluateTests|GetRowsAffected Failed result:%+v,err:%+v", result, err)
		return err
	}
	log.DebugContextf(ctx, "删除样本集下所有评测任务 corpID:%+v,robotID:%+v, setID:%v, rowsAffected:%d",
		corpID, robotID, setID, rowsAffected)
	return nil
}

// DeleteTestRecords 删除样本集下所有评测结果
func (d *dao) DeleteTestRecords(ctx context.Context, setID uint64) error {
	deleteSQL := deleteTestRecords
	result, err := d.db.Exec(ctx, deleteSQL, setID)
	if err != nil {
		log.ErrorContextf(ctx, "删除样本集下所有评测结果失败, sql:%s, setID:%+v, err:%+v",
			deleteSQL, setID, err)
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.ErrorContextf(ctx, "DeleteTestRecords|GetRowsAffected Failed result:%+v,err:%+v", result, err)
		return err
	}
	log.DebugContextf(ctx, "删除样本集下所有评测结果 setID:%v, rowsAffected:%d",
		setID, rowsAffected)
	return nil
}
