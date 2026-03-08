// Package task 送审任务
package task

import (
	"context"
	"errors"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"git.woa.com/dialogue-platform/common/v3/sync/errgroupx"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	jsoniter "github.com/json-iterator/go"
	"github.com/sergi/go-diff/diffmatchpatch"
	"math"
	"time"
)

const (
	taskKvKeyOldDocParseRes = "oldDocParseRes"
	taskKvKeyNewDocParseRes = "newDocParseRes"

	docDiffDataMaxTextLength = 100000000 // 约100M的长度
)

// DocDiffDataScheduler 文档比较任务
type DocDiffDataScheduler struct {
	dao      dao.Dao
	task     task_scheduler.Task
	instance app.Base
	params   model.DocDiffParams
}

func initDocDiffScheduler() {
	task_scheduler.Register(
		model.DocDiffDataTask,
		func(t task_scheduler.Task, params model.DocDiffParams) task_scheduler.TaskHandler {
			return &DocDiffDataScheduler{
				dao:    dao.New(),
				task:   t,
				params: params,
			}
		},
	)
}

// Prepare 数据准备
func (d *DocDiffDataScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.params.Language)
	log.DebugContextf(ctx, "task(DocDiffData) Prepare, task: %+v, params: %+v", d.task, d.params)
	kv := make(task_scheduler.TaskKV)
	// 更新问对比对任务中比对状态
	err := updateDiffDataProcessStatus(ctx, d.params.CorpBizID, d.params.RobotBizID, d.params.DiffBizID,
		model.DiffDataProcessStatusProcessing)
	if err != nil {
		return kv, err
	}
	appDB, err := d.dao.GetAppByAppBizID(ctx, d.params.RobotBizID)
	if err != nil {
		return kv, err
	}
	if appDB == nil {
		return kv, errs.ErrRobotNotFound
	}

	selectColumns := []string{dao.DocDiffTaskTblColNewDocBizId, dao.DocDiffTaskTblColOldDocBizId}
	docDiff, err := dao.GetDocDiffTaskDao().GetDocDiffTask(ctx, selectColumns, d.params.CorpBizID, d.params.RobotBizID,
		d.params.DiffBizID)
	if err != nil {
		return kv, err
	}
	// 获取新旧文档解析结果json
	oldDocParseRes, err := getDocParseRes(ctx, d.dao, docDiff.OldDocBizID, appDB.ID)
	if err != nil {
		return kv, err
	}
	kv[taskKvKeyOldDocParseRes] = oldDocParseRes
	newDocParseRes, err := getDocParseRes(ctx, d.dao, docDiff.NewDocBizID, appDB.ID)
	if err != nil {
		return kv, err
	}
	kv[taskKvKeyNewDocParseRes] = newDocParseRes

	return kv, nil
}

// Init 初始化
func (d *DocDiffDataScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.params.Language)
	return nil
}

// Process 任务处理
func (d *DocDiffDataScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(DocDiffData) Process, task: %+v, params: %+v", d.task, d.params)
	taskKvMap := progress.TaskKV(ctx)
	oldDocParseRes, ok := taskKvMap[taskKvKeyOldDocParseRes]
	if !ok {
		err := errors.New(fmt.Sprintf("taskKvKeyOldDocParseRes not exist, kv:%+v", taskKvMap))
		log.ErrorContextf(ctx, "%s", err.Error())
		return err
	}
	newDocParseRes, ok := taskKvMap[taskKvKeyNewDocParseRes]
	if !ok {
		err := errors.New(fmt.Sprintf("taskKvKeyNewDocParseRes not exist, kv:%+v", taskKvMap))
		log.ErrorContextf(ctx, "%s", err.Error())
		return err
	}

	// 通过文档解析结果中的cos地址获取文档解析内容
	var oldFileData string
	var newFileData string
	var err error
	beginTime := time.Now()
	g := errgroupx.Group{}
	g.Go(func() error {
		oldFileData, err = getDocParseData(ctx, d.dao, oldDocParseRes)
		return err
	})
	g.Go(func() error {
		newFileData, err = getDocParseData(ctx, d.dao, newDocParseRes)
		return err
	})
	if err := g.Wait(); err != nil {
		log.ErrorContextf(ctx, "group wait() error:%+v", err)
		return errs.ErrSystem
	}
	log.DebugContextf(ctx, "download file old doc length:%d new doc length:%d cost:%dms",
		len(oldFileData), len(newFileData), time.Since(beginTime).Milliseconds())
	if len(oldFileData) > docDiffDataMaxTextLength || len(newFileData) > docDiffDataMaxTextLength {
		err := errors.New(fmt.Sprintf("oldFileData length:%d or newFileData length:%d > "+
			"docDiffDataMaxTextLength:%d", len(oldFileData), len(newFileData), docDiffDataMaxTextLength))
		log.ErrorContextf(ctx, "%s", err.Error())
		return err
	}

	// 比对
	beginTime = time.Now()
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(oldFileData, newFileData, false)
	log.DebugContextf(ctx, "diff file has %d diffs, cost:%dms", len(diffs), time.Since(beginTime).Milliseconds())

	textDiffList := make([]*model.TextDiff, 0)
	textLength := 0
	index := 0
	for _, diff := range diffs {
		//log.DebugContextf(ctx, "diffs[%d] type:%d length:%d", i, diff.Type, len(diff.Text))
		if textLength+len(diff.Text) > dao.MaxTextLength {
			// 超过长度限制，需要先把前面累积的数据插入数据库
			if len(textDiffList) > 0 {
				err = insertDocDiffRes(ctx, &d.params, textDiffList, index)
				if err != nil {
					return err
				}
				textDiffList = []*model.TextDiff{}
				textLength = 0
				index++
			}
		}
		// 再继续处理当前diff
		text := diff.Text
		if len(text) > dao.MaxTextLength {
			// 如果单个diff长度超过限制，需要拆分下
			count := int(math.Ceil(float64(len(text)) / float64(dao.MaxTextLength)))
			for i := 0; i < count-1; i++ {
				// 最后一个diff可能长度较小，需要特殊处理
				textDiff := &model.TextDiff{
					Type: int(diff.Type),
					Text: text[i*dao.MaxTextLength : (i+1)*dao.MaxTextLength],
				}
				err = insertDocDiffRes(ctx, &d.params, []*model.TextDiff{textDiff}, index)
				if err != nil {
					return err
				}
				index++
			}
			// 最后一个diff片段继续走通用的逻辑处理
			text = text[(count-1)*dao.MaxTextLength:]
		}

		textDiff := &model.TextDiff{
			Type: int(diff.Type),
			Text: text,
		}
		textDiffList = append(textDiffList, textDiff)
		textStr, err := jsoniter.Marshal(textDiff)
		if err != nil {
			log.ErrorContextf(ctx, "marshal doc diff res failed, err:%+v", err)
			return err
		}
		textLength += len(string(textStr))
	}
	if len(textDiffList) > 0 {
		// 最后一个diff片段需要插入数据库
		err = insertDocDiffRes(ctx, &d.params, textDiffList, index)
		if err != nil {
			return err
		}
	}

	if err = progress.Finish(ctx, taskKvKeyOldDocParseRes); err != nil {
		log.ErrorContextf(ctx, "task(DocDiffData) Finish kv:%s err:%+v", taskKvKeyOldDocParseRes, err)
		return err
	}
	if err = progress.Finish(ctx, taskKvKeyNewDocParseRes); err != nil {
		log.ErrorContextf(ctx, "task(DocDiffData) Finish kv:%s err:%+v", taskKvKeyNewDocParseRes, err)
		return err
	}
	log.DebugContextf(ctx, "task(DocDiffData) Finish kv:[%s,%s]",
		taskKvKeyOldDocParseRes, taskKvKeyNewDocParseRes)

	return nil
}

// Fail 任务失败
func (d *DocDiffDataScheduler) Fail(ctx context.Context) error {
	log.DebugContextf(ctx, "task(DocDiffData) fail, doc diff id: %v", d.params.DiffBizID)
	// 更新问对比对任务中比对状态
	err := updateDiffDataProcessStatus(ctx, d.params.CorpBizID, d.params.RobotBizID, d.params.DiffBizID,
		model.DiffDataProcessStatusFailed)
	if err != nil {
		return err
	}
	return nil
}

// Stop 任务停止
func (d *DocDiffDataScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocDiffDataScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(DocDiffData) done, doc diff id: %v", d.params.DiffBizID)
	// 更新问对比对任务中比对状态
	err := updateDiffDataProcessStatus(ctx, d.params.CorpBizID, d.params.RobotBizID, d.params.DiffBizID,
		model.DiffDataProcessStatusSuccess)
	if err != nil {
		return err
	}
	return nil
}

func getDocParseRes(ctx context.Context, dao dao.Dao, docBizId uint64, robotID uint64) (string, error) {
	docId, err := dao.GetDocIDByBusinessID(ctx, docBizId, robotID)
	if err != nil {
		return "", err
	}
	docParse, err := dao.GetDocParseByDocIDAndTypeAndStatus(ctx, docId, model.DocParseTaskTypeSplitSegment,
		model.DocParseSuccess, robotID)
	if err != nil {
		return "", err
	}
	return docParse.Result, nil
}

func getDocParseData(ctx context.Context, dao dao.Dao, docParseResJson string) (string, error) {
	result := &pb.FileParserCallbackReq{}
	err := jsoniter.UnmarshalFromString(docParseResJson, result)
	if err != nil {
		log.ErrorContextf(ctx, "getDocParseContent|jsoniter.UnmarshalFromString failed, err:%+v", err)
		return "", err
	}
	log.InfoContextf(ctx, "getDocParseContent|file parse result:%+v", result)
	resultDataMap := result.GetResults()
	docParseRes := resultDataMap[int32(pb.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_PARSE)]
	fileData := ""
	for _, res := range docParseRes.GetResult() {
		data, err := dao.GetFileDataFromCosURL(ctx, res.GetResult())
		if err != nil {
			return "", err
		}
		fileData += data
	}
	return fileData, nil
}

func insertDocDiffRes(ctx context.Context, params *model.DocDiffParams, textDiffList []*model.TextDiff, index int) error {
	diffStr, err := jsoniter.Marshal(textDiffList)
	if err != nil {
		log.ErrorContextf(ctx, "marshal doc diff res failed, err:%+v", err)
		return err
	}
	docDiffRes := &model.DocDiffData{
		CorpBizID:  params.CorpBizID,
		RobotBizID: params.RobotBizID,
		DiffBizID:  params.DiffBizID,
		DiffIndex:  index,
		DiffData:   string(diffStr),
	}
	if err := dao.GetDocDiffDataDao().CreateDocDiffData(ctx, docDiffRes); err != nil {
		log.ErrorContextf(ctx, "insert doc diff data failed, err:%+v", err)
		return err
	}
	return nil
}

// updateDiffDataProcessStatus 更新问对比对任务中比对状态
func updateDiffDataProcessStatus(ctx context.Context, corpBizId, robotBizId, diffBizId uint64, status uint32) error {
	updateColumns := []string{dao.DocDiffTaskTblColDiffDataProcessStatus}
	updateDocDiff := &model.DocDiff{
		DiffDataProcessStatus: status,
	}
	err := dao.GetDocDiffTaskDao().UpdateDocDiffTasks(ctx, nil, updateColumns, corpBizId, robotBizId,
		[]uint64{diffBizId}, updateDocDiff)
	if err != nil {
		return err
	}
	return nil
}
