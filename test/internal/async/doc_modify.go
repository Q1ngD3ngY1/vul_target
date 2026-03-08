package async

import (
	"context"
	"fmt"
	"strings"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

const (
	segModifyPrefix = "seg:modify:"
	qaModifyPrefix  = "qa:modify:"
)

// DocModifyTaskHandler 文档修改任务
type DocModifyTaskHandler struct {
	*taskCommon

	task                task_scheduler.Task
	p                   entity.DocModifyParams
	VectorLabels        []*retrieval.VectorLabel
	Text2SQLSegmentMeta segEntity.Text2SQLSegmentMeta
}

func registerDocModifyTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.DocModifyTask,
		func(t task_scheduler.Task, params entity.DocModifyParams) task_scheduler.TaskHandler {
			return &DocModifyTaskHandler{
				taskCommon:   tc,
				task:         t,
				p:            params,
				VectorLabels: make([]*retrieval.VectorLabel, 0),
			}
		},
	)
}

// Prepare 数据准备
func (d *DocModifyTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(DocModify) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return kv, err
	}
	if doc == nil {
		return kv, errs.ErrDocNotFound
	}
	qas := make([]*qaEntity.DocQA, 0)
	if !d.p.NotChangeQA {
		tmp, err := getDocNotDeleteQA(ctx, doc, d.qaLogic)
		if err != nil {
			return kv, err
		}
		qas = append(qas, tmp...)
	}
	segs, err := getDocNotDeleteSegment(ctx, doc, d.segLogic)
	if err != nil {
		return kv, err
	}

	// 问答切片
	for index, qa := range qas {
		if qa.OriginDocID != doc.ID {
			continue
		}
		logx.D(ctx, "task(DocModify) Prepare index:%d, qa.ID: %+v", index, qa.ID)
		kv[fmt.Sprintf("%s%d", qaModifyPrefix, qa.ID)] = fmt.Sprintf("%d", qa.ID)
	}

	// 文档切片
	batchSize := config.GetMainConfig().OfflineConfig.SyncVectorAddBatchSize
	if batchSize <= 0 {
		batchSize = defaultSyncAddVectorBatchSize
	}
	for index, segChunks := range slicex.Chunk(segs, batchSize) {
		var idsStr []string
		for _, seg := range segChunks {
			idsStr = append(idsStr, cast.ToString(seg.ID))
		}
		idChunksStr, err := jsonx.MarshalToString(idsStr)
		if err != nil {
			logx.E(ctx, "task(DocModify) Prepare|jsonx.MarshalToString err:%+v", err)
			kv = make(task_scheduler.TaskKV) // 重置kv
			return kv, err
		}
		logx.D(ctx, "task(DocModify) Prepare index:%d, seg.IDs: %+v", index, idChunksStr)
		kv[fmt.Sprintf("%s%d", segModifyPrefix, index)] = fmt.Sprintf("%s", idChunksStr)
	}
	return kv, nil
}

// Init 初始化
func (d *DocModifyTaskHandler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(DocModify) Init, task: %+v, params: %+v, kvs: %d", d.task, d.p, len(kv))
	doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	d.VectorLabels, err = getDocVectorLabels(ctx, doc, d.rpc, d.userLogic, d.cateLogic, d.labelLogic)
	if err != nil {
		return err
	}
	d.Text2SQLSegmentMeta, err = getText2sqlSegmentMeta(ctx, doc, d.segLogic)
	if err != nil {
		return err
	}
	logx.D(ctx, "task(DocModify) Init success, VectorLabels: %+v, Text2SQLSegmentMeta: %+v",
		d.VectorLabels, d.Text2SQLSegmentMeta)
	return nil
}

// Process 任务处理
func (d *DocModifyTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(DocModify) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(DocModify) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			logx.D(ctx, "task(DocModify) appDB.HasDeleted()|appID:%d", d.p.RobotID)
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(DocModify) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		newCtx := util.SetMultipleMetaData(ctx, appDB.SpaceId, appDB.Uin)
		if strings.HasPrefix(key, qaModifyPrefix) {
			id := cast.ToUint64(v)
			if err := d.updateQARelatedInfo(newCtx, id, d.p.ExpireStart, d.p.ExpireEnd, d.p.EnableScope); err != nil {
				return err
			}
		}
		if strings.HasPrefix(key, segModifyPrefix) {
			value := make([]string, 0)
			err := jsonx.Unmarshal([]byte(v), &value)
			if err != nil {
				logx.E(ctx, "task(DocModify) jsonx.Unmarshal  err:%+v", err)
				return err
			}
			ids := make([]uint64, 0)
			for _, idStr := range value {
				ids = append(ids, cast.ToUint64(idStr))
			}

			docSegmentMap, err := d.getSegments(ctx, ids)
			if err != nil {
				logx.E(ctx, "task(DocModify) getSegments err:%+v", err)
				return err
			}

			embeddingVersion := appDB.Embedding.Version
			embeddingName, err :=
				d.kbLogic.GetKnowledgeEmbeddingModel(ctx, appDB.CorpBizId, appDB.BizId, appDB.BizId, appDB.IsShared)
			if err != nil {
				logx.E(ctx, "task(DocModify) GetShareKnowledgeBaseConfig err:%+v", err)
				return err
			}
			logx.I(ctx, "task(DocModify) kb "+
				" embeddingModelName:%s, app embeddingVersion:%d", embeddingName, embeddingVersion)

			for segmentType, segments := range docSegmentMap {
				switch segmentType {
				case segEntity.SegmentTypeText2SQLMeta:
					// ignore, do nothing
				case segEntity.SegmentTypeText2SQLContent:
					if err = addText2sqlSegmentBatch(newCtx, d.segLogic, d.rpc, segments, d.Text2SQLSegmentMeta,
						d.VectorLabels, 0); err != nil {
						return err
					}
				default:
					if err = d.qaLogic.GetVectorSyncLogic().BatchDirectAddSegmentKnowledge(newCtx, appDB.PrimaryId, appDB.BizId,
						segments, embeddingVersion, embeddingName, d.VectorLabels); err != nil {
						return err
					}
				}
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			logx.E(ctx, "task(DocModify) Finish kv:%s err:%+v", key, err)
			return err
		}
		logx.D(ctx, "task(DocModify) Finish kv:%s", key)
	}
	return nil
}

// getSegments 获取切片
func (d *DocModifyTaskHandler) getSegments(ctx context.Context, segmentIDs []uint64) (
	docSegmentMap map[string][]*segEntity.DocSegmentExtend, err error) {
	logx.I(ctx, "task(DocModify) getSegments|segmentIDs: %+v", segmentIDs)
	segments, err := d.segLogic.GetSegmentByIDs(ctx, segmentIDs, d.p.RobotID)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, errs.ErrSegmentNotFound
	}

	robotID, docID := uint64(0), uint64(0)

	modifySegments := make([]*segEntity.DocSegmentExtend, 0)

	docSegmentMap = make(map[string][]*segEntity.DocSegmentExtend)

	for _, segment := range segments {
		if !segment.IsSegmentForQA() {
			if robotID == 0 {
				robotID = segment.RobotID
			}
			if docID == 0 {
				docID = segment.DocID
			}
			if robotID != segment.RobotID || robotID != d.p.RobotID || docID != segment.DocID || docID != d.p.DocID {
				logx.E(ctx, "task(DocModify) getSegments|seg illegal|segment: %+v", segment)
				return nil, errs.ErrSegmentNotFound
			}

			// 重新继承文档有效期
			segment.ExpireStart = d.p.ExpireStart
			segment.ExpireEnd = d.p.ExpireEnd

			docSegments, ok := docSegmentMap[segment.SegmentType]
			if !ok {
				docSegments = make([]*segEntity.DocSegmentExtend, 0)
			}
			docSegments = append(docSegments, segment)
			docSegmentMap[segment.SegmentType] = docSegments

			modifySegments = append(modifySegments, segment)
		}
	}

	if len(docSegmentMap) == 0 {
		logx.D(ctx, "task(DocModify) getSegments|len(docSegmentMap):%d", len(docSegmentMap))
		return nil, nil
	}

	if err := d.segLogic.BatchUpdateSegment(ctx, modifySegments, d.p.RobotID); err != nil {
		return nil, err
	}

	return docSegmentMap, nil
}

// Fail 任务失败
func (d *DocModifyTaskHandler) Fail(ctx context.Context) error {
	logx.D(ctx, "task(DocModify) Done")
	doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	if err = d.docLogic.ModifyDocFail(ctx, doc, d.p.StaffID); err != nil {
		return err
	}
	return nil
}

// Stop 任务停止
func (d *DocModifyTaskHandler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocModifyTaskHandler) Done(ctx context.Context) error {
	logx.D(ctx, "task(DocModify) Done")
	doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if err = d.docLogic.ModifyDocSuccess(ctx, doc, d.p.StaffID); err != nil {
		return err
	}
	return nil
}

// attributeLabel, expireTime, enableScope

func (d *DocModifyTaskHandler) updateQARelatedInfo(ctx context.Context, qaID uint64,
	expireStart, expireEnd time.Time, enableScope uint32) error {
	qa, err := d.qaLogic.GetQAByID(ctx, qaID)
	if err != nil || qa == nil {
		return err
	}
	if qa.IsDelete() {
		return nil
	}
	// 非手动问答对，才做有效期修改
	if qa.Source != docEntity.SourceFromManual {
		qa.ExpireStart = expireStart
		qa.ExpireEnd = expireEnd
	}
	qa.SimilarStatus = docEntity.SimilarStatusInit
	qa.ReleaseStatus = qaEntity.QAReleaseStatusLearning
	qa.IsAuditFree = qaEntity.QAIsAuditFree
	if !qa.IsNextActionAdd() {
		qa.NextAction = qaEntity.NextActionUpdate
	}
	if enableScope != uint32(pb.RetrievalEnableScope_ENABLE_SCOPE_TYPE_UNKNOWN) {
		qa.EnableScope = enableScope
	}
	// 处理相似问
	sqs, err := d.qaLogic.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		logx.E(ctx, "GetSimilarQuestionsByQAID failed, qaID: %d, err: %+v", qaID, err)
		// 柔性放过
	}
	for _, sq := range sqs {
		sq.ReleaseStatus = qaEntity.QAReleaseStatusLearning
		sq.IsAuditFree = qaEntity.QAIsAuditFree
		if !sq.IsNextActionAdd() {
			sq.NextAction = qaEntity.NextActionUpdate
		}
	}
	sqm := &qaEntity.SimilarQuestionModifyInfo{
		UpdateQuestions: sqs,
	}
	attributeLabelReq := &labelEntity.UpdateQAAttributeLabelReq{IsNeedChange: false}
	if err = d.qaLogic.UpdateQA(ctx, qa, sqm, true, false, 0, 0, attributeLabelReq); err != nil {
		return err
	}
	return nil
}
