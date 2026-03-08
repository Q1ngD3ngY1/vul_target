// Package task 送审任务
package async

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/sergi/go-diff/diffmatchpatch"
)

const (
	taskKvKeyOldDocParseRes = "oldDocParseRes"
	taskKvKeyNewDocParseRes = "newDocParseRes"

	docDiffDataMaxTextLength = 100000000 // 约100M的长度
)

// DocDiffDataTaskHandler 文档比较任务
type DocDiffDataTaskHandler struct {
	*taskCommon

	task   task_scheduler.Task
	params entity.DocDiffParams
}

func registerDocDiffTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.DocDiffDataTask,
		func(t task_scheduler.Task, params entity.DocDiffParams) task_scheduler.TaskHandler {
			return &DocDiffDataTaskHandler{
				taskCommon: tc,
				task:       t,
				params:     params,
			}
		},
	)
}

// Prepare 数据准备
func (d *DocDiffDataTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.params.Language)
	logx.D(ctx, "task(DocDiffData) Prepare, task: %+v, params: %+v", d.task, d.params)
	kv := make(task_scheduler.TaskKV)
	// 更新问对比对任务中比对状态
	err := d.updateDiffDataProcessStatus(ctx, d.params.CorpBizID, d.params.RobotBizID, d.params.DiffBizID,
		docEntity.DiffDataProcessStatusProcessing)
	if err != nil {
		return kv, err
	}
	app, err := d.rpc.AppAdmin.DescribeAppById(ctx, d.params.RobotBizID)
	// app, err := d.dao.GetAppByAppBizID(ctx, d.params.RobotBizID)
	if err != nil {
		return kv, err
	}
	if app == nil {
		return kv, errs.ErrRobotNotFound
	}

	selectColumns := []string{docEntity.DocDiffTaskTblColNewDocBizId, docEntity.DocDiffTaskTblColOldDocBizId}
	docDiff, err := d.taskLogic.GetDocDiffTask(ctx, selectColumns, d.params.CorpBizID, d.params.RobotBizID,
		d.params.DiffBizID)
	if err != nil {
		return kv, err
	}
	// 获取新旧文档解析结果json
	oldDocParseRes, err := d.getDocParseRes(ctx, docDiff.OldDocBizID, app.PrimaryId)
	if err != nil {
		return kv, err
	}
	kv[taskKvKeyOldDocParseRes] = oldDocParseRes
	newDocParseRes, err := d.getDocParseRes(ctx, docDiff.NewDocBizID, app.PrimaryId)
	if err != nil {
		return kv, err
	}
	kv[taskKvKeyNewDocParseRes] = newDocParseRes

	return kv, nil
}

// Init 初始化
func (d *DocDiffDataTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.params.Language)
	return nil
}

// Process 任务处理
func (d *DocDiffDataTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(DocDiffData) Process, task: %+v, params: %+v", d.task, d.params)
	taskKvMap := progress.TaskKV(ctx)
	oldDocParseRes, ok := taskKvMap[taskKvKeyOldDocParseRes]
	if !ok {
		err := errors.New(fmt.Sprintf("taskKvKeyOldDocParseRes not exist, kv:%+v", taskKvMap))
		logx.E(ctx, "%s", err.Error())
		return err
	}
	newDocParseRes, ok := taskKvMap[taskKvKeyNewDocParseRes]
	if !ok {
		err := errors.New(fmt.Sprintf("taskKvKeyNewDocParseRes not exist, kv:%+v", taskKvMap))
		logx.E(ctx, "%s", err.Error())
		return err
	}

	// 通过文档解析结果中的cos地址获取文档解析内容
	var oldFileData string
	var newFileData string
	var err error
	beginTime := time.Now()
	g := errgroupx.New()
	g.Go(func() error {
		oldFileData, err = d.getDocParseData(ctx, oldDocParseRes)
		return err
	})
	g.Go(func() error {
		newFileData, err = d.getDocParseData(ctx, newDocParseRes)
		return err
	})
	if err := g.Wait(); err != nil {
		logx.E(ctx, "group wait() error:%+v", err)
		return errs.ErrSystem
	}
	logx.D(ctx, "download file old doc length:%d new doc length:%d cost:%dms",
		len(oldFileData), len(newFileData), time.Since(beginTime).Milliseconds())
	if len(oldFileData) > docDiffDataMaxTextLength || len(newFileData) > docDiffDataMaxTextLength {
		err := errors.New(fmt.Sprintf("oldFileData length:%d or newFileData length:%d > "+
			"docDiffDataMaxTextLength:%d", len(oldFileData), len(newFileData), docDiffDataMaxTextLength))
		logx.E(ctx, "%s", err.Error())
		return err
	}

	// 比对
	beginTime = time.Now()
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(oldFileData, newFileData, false)
	logx.D(ctx, "diff file has %d diffs, cost:%dms", len(diffs), time.Since(beginTime).Milliseconds())

	textDiffList := make([]*docEntity.TextDiff, 0)
	textLength := 0
	index := 0
	for _, diff := range diffs {
		// logx.D(ctx, "diffs[%d] type:%d length:%d", i, diff.Type, len(diff.Text))
		if textLength+len(diff.Text) > util.MaxTextLength {
			// 超过长度限制，需要先把前面累积的数据插入数据库
			if len(textDiffList) > 0 {
				err = d.insertDocDiffRes(ctx, &d.params, textDiffList, index)
				if err != nil {
					return err
				}
				textDiffList = []*docEntity.TextDiff{}
				textLength = 0
				index++
			}
		}
		// 再继续处理当前diff
		text := diff.Text
		if len(text) > util.MaxTextLength {
			// 如果单个diff长度超过限制，需要拆分下
			count := int(math.Ceil(float64(len(text)) / float64(util.MaxTextLength)))
			for i := 0; i < count-1; i++ {
				// 最后一个diff可能长度较小，需要特殊处理
				textDiff := &docEntity.TextDiff{
					Type: int(diff.Type),
					Text: text[i*util.MaxTextLength : (i+1)*util.MaxTextLength],
				}
				err = d.insertDocDiffRes(ctx, &d.params, []*docEntity.TextDiff{textDiff}, index)
				if err != nil {
					return err
				}
				index++
			}
			// 最后一个diff片段继续走通用的逻辑处理
			text = text[(count-1)*util.MaxTextLength:]
		}

		textDiff := &docEntity.TextDiff{
			Type: int(diff.Type),
			Text: text,
		}
		textDiffList = append(textDiffList, textDiff)
		textStr, err := jsonx.Marshal(textDiff)
		if err != nil {
			logx.E(ctx, "marshal doc diff res failed, err:%+v", err)
			return err
		}
		textLength += len(string(textStr))
	}
	if len(textDiffList) > 0 {
		// 最后一个diff片段需要插入数据库
		err = d.insertDocDiffRes(ctx, &d.params, textDiffList, index)
		if err != nil {
			return err
		}
	}

	if err = progress.Finish(ctx, taskKvKeyOldDocParseRes); err != nil {
		logx.E(ctx, "task(DocDiffData) Finish kv:%s err:%+v", taskKvKeyOldDocParseRes, err)
		return err
	}
	if err = progress.Finish(ctx, taskKvKeyNewDocParseRes); err != nil {
		logx.E(ctx, "task(DocDiffData) Finish kv:%s err:%+v", taskKvKeyNewDocParseRes, err)
		return err
	}
	logx.D(ctx, "task(DocDiffData) Finish kv:[%s,%s]",
		taskKvKeyOldDocParseRes, taskKvKeyNewDocParseRes)

	return nil
}

// Fail 任务失败
func (d *DocDiffDataTaskHandler) Fail(ctx context.Context) error {
	logx.D(ctx, "task(DocDiffData) fail, doc diff id: %v", d.params.DiffBizID)
	// 更新问对比对任务中比对状态
	err := d.updateDiffDataProcessStatus(ctx, d.params.CorpBizID, d.params.RobotBizID, d.params.DiffBizID,
		docEntity.DiffDataProcessStatusFailed)
	if err != nil {
		return err
	}
	return nil
}

// Stop 任务停止
func (d *DocDiffDataTaskHandler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocDiffDataTaskHandler) Done(ctx context.Context) error {
	logx.D(ctx, "task(DocDiffData) done, doc diff id: %v", d.params.DiffBizID)
	// 更新问对比对任务中比对状态
	err := d.updateDiffDataProcessStatus(ctx, d.params.CorpBizID, d.params.RobotBizID, d.params.DiffBizID,
		docEntity.DiffDataProcessStatusSuccess)
	if err != nil {
		return err
	}
	return nil
}

func (d *DocDiffDataTaskHandler) getDocParseRes(ctx context.Context, docBizId uint64, robotID uint64) (string, error) {
	docId, err := d.docLogic.GetDocIDByBusinessID(ctx, docBizId, robotID)
	if err != nil {
		return "", err
	}
	docParse, err := d.docLogic.GetDocParseByDocIDAndTypeAndStatus(ctx, docId, docEntity.DocParseTaskTypeSplitSegment,
		docEntity.DocParseSuccess, robotID)
	if err != nil {
		return "", err
	}
	return docParse.Result, nil
}

func (d *DocDiffDataTaskHandler) getDocParseData(ctx context.Context, docParseResJson string) (string, error) {
	result := &pb.FileParserCallbackReq{}
	err := jsonx.UnmarshalFromString(docParseResJson, result)
	if err != nil {
		logx.E(ctx, "getDocParseContent|jsonx.UnmarshalFromString failed, err:%+v", err)
		return "", err
	}
	logx.I(ctx, "getDocParseContent|file parse result:%+v", result)
	resultDataMap := result.GetResults()
	docParseRes := resultDataMap[int32(pb.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_PARSE)]
	fileData := ""
	for _, res := range docParseRes.GetResult() {
		data, err := d.docLogic.GetFileDataFromCosURL(ctx, res.GetResult())
		if err != nil {
			return "", err
		}
		fileData += data
	}
	return fileData, nil
}

func (d *DocDiffDataTaskHandler) insertDocDiffRes(ctx context.Context, params *entity.DocDiffParams,
	textDiffList []*docEntity.TextDiff, index int) error {
	diffStr, err := jsonx.Marshal(textDiffList)
	if err != nil {
		logx.E(ctx, "marshal doc diff res failed, err:%+v", err)
		return err
	}
	docDiffRes := &docEntity.DocDiffData{
		CorpBizID:  params.CorpBizID,
		RobotBizID: params.RobotBizID,
		DiffBizID:  params.DiffBizID,
		DiffIndex:  index,
		DiffData:   string(diffStr),
	}
	if err := d.docLogic.CreateDocDiffData(ctx, docDiffRes); err != nil {
		logx.E(ctx, "insert doc diff data failed, err:%+v", err)
		return err
	}
	return nil
}

// updateDiffDataProcessStatus 更新问对比对任务中比对状态
func (s *DocDiffDataTaskHandler) updateDiffDataProcessStatus(ctx context.Context, corpBizId, robotBizId, diffBizId uint64, status uint32) error {
	updateColumns := []string{docEntity.DocDiffTaskTblColDiffDataProcessStatus}
	updateDocDiff := &docEntity.DocDiff{
		DiffDataProcessStatus: status,
	}
	err := s.taskLogic.UpdateDocDiffTasks(ctx, updateColumns, corpBizId, robotBizId, []uint64{diffBizId}, updateDocDiff)
	if err != nil {
		return err
	}
	return nil
}
