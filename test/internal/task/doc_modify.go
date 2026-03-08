package task

import (
	"context"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	logicKnowConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"strings"
	"time"

	"git.woa.com/baicaoyuan/moss/types/slicex"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	jsoniter "github.com/json-iterator/go"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"github.com/spf13/cast"
)

const (
	segModifyPrefix = "seg:modify:"
	qaModifyPrefix  = "qa:modify:"
)

// DocModifyScheduler 文档修改任务
type DocModifyScheduler struct {
	dao                 dao.Dao
	task                task_scheduler.Task
	p                   model.DocModifyParams
	VectorLabels        []*retrieval.VectorLabel
	Text2SQLSegmentMeta model.Text2SQLSegmentMeta
}

func initDocModifyScheduler() {
	task_scheduler.Register(
		model.DocModifyTask,
		func(t task_scheduler.Task, params model.DocModifyParams) task_scheduler.TaskHandler {
			return &DocModifyScheduler{
				dao:          dao.New(),
				task:         t,
				p:            params,
				VectorLabels: make([]*retrieval.VectorLabel, 0),
			}
		},
	)
}

// Prepare 数据准备
func (d *DocModifyScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(DocModify) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return kv, err
	}
	if doc == nil {
		return kv, errs.ErrDocNotFound
	}
	qas := make([]*model.DocQA, 0)
	if !d.p.NotChangeQA {
		tmp, err := getDocNotDeleteQA(ctx, doc, d.dao)
		if err != nil {
			return kv, err
		}
		qas = append(qas, tmp...)
	}
	segs, err := getDocNotDeleteSegment(ctx, doc, d.dao)
	if err != nil {
		return kv, err
	}

	// 问答切片
	for index, qa := range qas {
		if qa.OriginDocID != doc.ID {
			continue
		}
		log.DebugContextf(ctx, "task(DocModify) Prepare index:%d, qa.ID: %+v", index, qa.ID)
		kv[fmt.Sprintf("%s%d", qaModifyPrefix, qa.ID)] = fmt.Sprintf("%d", qa.ID)
	}

	// 文档切片
	batchSize := utilConfig.GetMainConfig().OfflineConfig.SyncVectorAddBatchSize
	if batchSize <= 0 {
		batchSize = defaultSyncAddVectorBatchSize
	}
	for index, segChunks := range slicex.Chunk(segs, batchSize) {
		var idsStr []string
		for _, seg := range segChunks {
			idsStr = append(idsStr, cast.ToString(seg.ID))
		}
		idChunksStr, err := jsoniter.MarshalToString(idsStr)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocModify) Prepare|jsoniter.MarshalToString err:%+v", err)
			kv = make(task_scheduler.TaskKV) // 重置kv
			return kv, err
		}
		log.DebugContextf(ctx, "task(DocModify) Prepare index:%d, seg.IDs: %+v", index, idChunksStr)
		kv[fmt.Sprintf("%s%d", segModifyPrefix, index)] = fmt.Sprintf("%s", idChunksStr)
	}
	return kv, nil
}

// Init 初始化
func (d *DocModifyScheduler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(DocModify) Init, task: %+v, params: %+v, kvs: %d", d.task, d.p, len(kv))
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	d.VectorLabels, err = getDocVectorLabels(ctx, doc, d.dao)
	if err != nil {
		return err
	}
	d.Text2SQLSegmentMeta, err = getText2sqlSegmentMeta(ctx, doc, d.dao)
	if err != nil {
		return err
	}
	log.DebugContextf(ctx, "task(DocModify) Init success, VectorLabels: %+v, Text2SQLSegmentMeta: %+v",
		d.VectorLabels, d.Text2SQLSegmentMeta)
	return nil
}

// Process 任务处理
func (d *DocModifyScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(DocModify) Process, task: %+v, params: %+v", d.task, d.p)
	corpBizId, err := dao.GetCorpBizIDByCorpID(ctx, d.p.CorpID)
	if err != nil {
		return err
	}
	appBizId, err := dao.GetAppBizIDByAppID(ctx, d.p.RobotID)
	if err != nil {
		return err
	}
	embeddingModelName, err := logicKnowConfig.GetKnowledgeBaseConfig(ctx, corpBizId, appBizId,
		uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL))
	if err != nil {
		return err
	}
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(DocModify) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			log.DebugContextf(ctx, "task(DocModify) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(DocModify) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		ctx = pkg.WithSpaceID(ctx, appDB.SpaceID)
		if strings.HasPrefix(key, qaModifyPrefix) {
			id := cast.ToUint64(v)
			if err := updateQAAttributeLabelAndExpire(ctx, d.dao, id, d.p.ExpireStart, d.p.ExpireEnd); err != nil {
				return err
			}
		}
		if strings.HasPrefix(key, segModifyPrefix) {
			value := make([]string, 0)
			err := jsoniter.Unmarshal([]byte(v), &value)
			if err != nil {
				log.ErrorContextf(ctx, "task(DocModify) jsoniter.Unmarshal  err:%+v", err)
				return err
			}
			ids := make([]uint64, 0)
			for _, idStr := range value {
				ids = append(ids, cast.ToUint64(idStr))
			}

			docSegmentMap, err := d.getSegments(ctx, ids)
			if err != nil {
				log.ErrorContextf(ctx, "task(DocModify) getSegments err:%+v", err)
				return err
			}

			embeddingConf, _, err := appDB.GetEmbeddingConf()
			if err != nil {
				log.ErrorContextf(ctx, "task(DocModify) GetEmbeddingConf() err:%+v", err)
				return err
			}
			embeddingVersion := embeddingConf.Version

			for segmentType, segments := range docSegmentMap {
				switch segmentType {
				case model.SegmentTypeText2SQLMeta:
					// ignore, do nothing
				case model.SegmentTypeText2SQLContent:
					if err = addText2sqlSegmentBatch(ctx, d.dao, segments, d.Text2SQLSegmentMeta,
						d.VectorLabels, 0); err != nil {
						return err
					}
				default:
					if err = d.dao.BatchDirectAddSegmentKnowledge(ctx, appDB.ID,
						segments, embeddingVersion, d.VectorLabels, embeddingModelName); err != nil {
						return err
					}
				}
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			log.ErrorContextf(ctx, "task(DocModify) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(DocModify) Finish kv:%s", key)
	}
	return nil
}

// getSegments 获取切片
func (d *DocModifyScheduler) getSegments(ctx context.Context, segmentIDs []uint64) (
	docSegmentMap map[string][]*model.DocSegmentExtend, err error) {
	log.InfoContextf(ctx, "task(DocModify) getSegments|segmentIDs: %+v", segmentIDs)
	segments, err := d.dao.GetSegmentByIDs(ctx, segmentIDs, d.p.RobotID)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, errs.ErrSegmentNotFound
	}

	robotID, docID := uint64(0), uint64(0)

	modifySegments := make([]*model.DocSegmentExtend, 0)

	docSegmentMap = make(map[string][]*model.DocSegmentExtend)

	for _, segment := range segments {
		if !segment.IsSegmentForQA() {
			if robotID == 0 {
				robotID = segment.RobotID
			}
			if docID == 0 {
				docID = segment.DocID
			}
			if robotID != segment.RobotID || robotID != d.p.RobotID || docID != segment.DocID || docID != d.p.DocID {
				log.ErrorContextf(ctx, "task(DocModify) getSegments|seg illegal|segment: %+v", segment)
				return nil, errs.ErrSegmentNotFound
			}

			// 重新继承文档有效期
			segment.ExpireStart = d.p.ExpireStart
			segment.ExpireEnd = d.p.ExpireEnd

			docSegments, ok := docSegmentMap[segment.SegmentType]
			if !ok {
				docSegments = make([]*model.DocSegmentExtend, 0)
			}
			docSegments = append(docSegments, segment)
			docSegmentMap[segment.SegmentType] = docSegments

			modifySegments = append(modifySegments, segment)
		}
	}

	if len(docSegmentMap) == 0 {
		log.DebugContextf(ctx, "task(DocModify) getSegments|len(docSegmentMap):%d", len(docSegmentMap))
		return nil, nil
	}

	if err := d.dao.BatchUpdateSegment(ctx, modifySegments, d.p.RobotID); err != nil {
		return nil, err
	}

	return docSegmentMap, nil
}

// Fail 任务失败
func (d *DocModifyScheduler) Fail(ctx context.Context) error {
	log.DebugContextf(ctx, "task(DocModify) Done")
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	if err = d.dao.ModifyDocFail(ctx, doc, d.p.StaffID); err != nil {
		return err
	}
	return nil
}

// Stop 任务停止
func (d *DocModifyScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocModifyScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(DocModify) Done")
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if err = d.dao.ModifyDocSuccess(ctx, doc, d.p.StaffID); err != nil {
		return err
	}
	return nil
}

func updateQAAttributeLabelAndExpire(ctx context.Context, dao dao.Dao, qaID uint64,
	expireStart, expireEnd time.Time) error {
	qa, err := dao.GetQAByID(ctx, qaID)
	if err != nil || qa == nil {
		return err
	}
	if qa.IsDelete() {
		return nil
	}
	// 非手动问答对，才做有效期修改
	if qa.Source != model.SourceFromManual {
		qa.ExpireStart = expireStart
		qa.ExpireEnd = expireEnd
	}
	qa.SimilarStatus = model.SimilarStatusInit
	qa.ReleaseStatus = model.QAReleaseStatusLearning
	qa.IsAuditFree = model.QAIsAuditFree
	if !qa.IsNextActionAdd() {
		qa.NextAction = model.NextActionUpdate
	}
	// 处理相似问
	sqs, err := dao.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		log.ErrorContextf(ctx, "GetSimilarQuestionsByQAID failed, qaID: %d, err: %+v", qaID, err)
		// 柔性放过
	}
	for _, sq := range sqs {
		sq.ReleaseStatus = model.QAReleaseStatusLearning
		sq.IsAuditFree = model.QAIsAuditFree
		if !sq.IsNextActionAdd() {
			sq.NextAction = model.NextActionUpdate
		}
	}
	sqm := &model.SimilarQuestionModifyInfo{
		UpdateQuestions: sqs,
	}
	attributeLabelReq := &model.UpdateQAAttributeLabelReq{IsNeedChange: false}
	if err = dao.UpdateQA(ctx, qa, sqm, true, false, 0, attributeLabelReq); err != nil {
		return err
	}
	return nil
}
