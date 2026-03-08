package task

import (
	"context"
	"errors"
	"fmt"
	logicKnowConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"strconv"
	"strings"
	"sync"

	logicDoc "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc_diff_task"

	"git.woa.com/baicaoyuan/moss/types/slicex"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"
)

// DocToIndexScheduler 文档删除任务
type DocToIndexScheduler struct {
	dao                 dao.Dao
	task                task_scheduler.Task
	p                   model.DocToIndexParams
	VectorLabels        []*retrieval.VectorLabel
	Text2SQLSegmentMeta model.Text2SQLSegmentMeta
}

func initDocToIndexScheduler() {
	task_scheduler.Register(
		model.DocToIndexTask,
		func(t task_scheduler.Task, params model.DocToIndexParams) task_scheduler.TaskHandler {
			return &DocToIndexScheduler{
				dao:          dao.New(),
				task:         t,
				p:            params,
				VectorLabels: make([]*retrieval.VectorLabel, 0),
			}
		},
	)
}

// Prepare 数据准备
func (d *DocToIndexScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(DocToIndex) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return kv, err
	}
	if doc == nil {
		return kv, errs.ErrDocNotFound
	}
	if doc.IsBatchImport() && doc.IsExcel() {
		return kv, errs.ErrDocTypeForIndex
	}
	if doc.HasDeleted() {
		return kv, nil
	}
	doc.BatchID += 1
	//doc.Status = model.DocStatusCreatingIndex 文档状态由状态机控制，不能直接设置
	doc.IsCreatingIndex = true
	doc.AddProcessingFlag([]uint64{model.DocProcessingFlagCreatingIndex})
	if err = d.dao.UpdateCreatingIndexFlag(ctx, doc); err != nil {
		return kv, err
	}
	docParse, err := d.dao.GetDocParseByDocIDAndTypeAndStatus(ctx, doc.ID, model.DocParseTaskTypeSplitSegment,
		model.DocParseSuccess, doc.RobotID)
	if err != nil {
		return kv, err
	}
	if err = d.createSegment(ctx, doc, docParse); err != nil {
		return kv, err
	}
	ids, err := d.dao.GetSegmentIDByDocIDAndBatchID(ctx, doc.ID, doc.BatchID, d.p.RobotID)
	if err != nil {
		return kv, err
	}
	batchSize := utilConfig.GetMainConfig().OfflineConfig.SyncVectorAddBatchSize
	if batchSize <= 0 {
		batchSize = defaultSyncAddVectorBatchSize
	}
	for index, idChunks := range slicex.Chunk(ids, batchSize) {
		var idsStr []string
		for _, id := range idChunks {
			idsStr = append(idsStr, cast.ToString(id))
		}
		idChunksStr, err := jsoniter.MarshalToString(idsStr)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocToIndex) Prepare|jsoniter.MarshalToString err:%+v", err)
			kv = make(task_scheduler.TaskKV) // 重置kv
			return kv, err
		}
		log.DebugContextf(ctx, "task(DocToIndex) Prepare index:%d, seg.IDs: %+v", index, idChunksStr)
		kv[fmt.Sprintf("%s%d", segGenIndexPrefix, index)] = fmt.Sprintf("%s", idChunksStr)
	}
	return kv, nil
}

// Init 初始化
func (d *DocToIndexScheduler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(DocToIndex) Init, task: %+v, params: %+v, kvs: %d", d.task, d.p, len(kv))
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	if doc.IsBatchImport() && doc.IsExcel() {
		return errs.ErrDocTypeForIndex
	}
	if doc.HasDeleted() {
		return nil
	}
	d.VectorLabels, err = getDocVectorLabels(ctx, doc, d.dao)
	if err != nil {
		return err
	}
	d.Text2SQLSegmentMeta, err = getText2sqlSegmentMeta(ctx, doc, d.dao)
	if err != nil {
		return err
	}
	log.DebugContextf(ctx, "task(DocToIndex) Init success, VectorLabels: %+v, Text2SQLSegmentMeta: %+v",
		d.VectorLabels, d.Text2SQLSegmentMeta)
	return nil
}

// Process 任务处理
func (d *DocToIndexScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(DocToIndex) Process, task: %+v, params: %+v", d.task, d.p)
	corpBizId, err := dao.GetCorpBizIDByCorpID(ctx, d.p.CorpID)
	if err != nil {
		return err
	}
	appBizId, err := dao.GetAppBizIDByAppID(ctx, d.p.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocToIndex) Process, GetAppBizIDByAppID err:%+v", err)
		return err
	}

	embeddingModelName, err := logicKnowConfig.GetKnowledgeBaseConfig(ctx, corpBizId, appBizId,
		uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL))
	if err != nil {
		return err
	}
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(DocToIndex) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			log.DebugContextf(ctx, "task(DocToIndex) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(DocToIndex) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		ctx = pkg.WithSpaceID(ctx, appDB.SpaceID)
		if strings.HasPrefix(key, segGenIndexPrefix) {
			value := make([]string, 0)
			err := jsoniter.Unmarshal([]byte(v), &value)
			if err != nil {
				log.ErrorContextf(ctx, "task(DocToIndex) jsoniter.Unmarshal  err:%+v", err)
				return err
			}
			ids := make([]uint64, 0)
			for _, idStr := range value {
				ids = append(ids, cast.ToUint64(idStr))
			}

			docSegmentMap, err := d.getSegments(ctx, ids)
			if err != nil {
				log.ErrorContextf(ctx, "task(DocToIndex) getSegments err:%+v", err)
				return err
			}

			embeddingConf, _, err := appDB.GetEmbeddingConf()
			if err != nil {
				log.ErrorContextf(ctx, "task(DocToIndex) GetEmbeddingConf() err:%+v", err)
				return err
			}
			embeddingVersion := embeddingConf.Version

			for segmentType, segments := range docSegmentMap {
				switch segmentType {
				case model.SegmentTypeText2SQLMeta:
					// ignore, do nothing
				case model.SegmentTypeText2SQLContent:
					if err = addText2sqlSegmentBatch(ctx, d.dao, segments, d.Text2SQLSegmentMeta,
						d.VectorLabels, d.p.InterveneOriginDocBizID); err != nil {
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
			log.ErrorContextf(ctx, "task(DocToIndex) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(DocToIndex) Finish kv:%s", key)
	}
	return nil
}

// getSegments 获取切片
func (d *DocToIndexScheduler) getSegments(ctx context.Context, segmentIDs []uint64) (
	docSegmentMap map[string][]*model.DocSegmentExtend, err error) {
	log.InfoContextf(ctx, "task(DocToIndex) getSegments|segmentIDs: %+v", segmentIDs)
	segments, err := d.dao.GetSegmentByIDs(ctx, segmentIDs, d.p.RobotID)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, errs.ErrSegmentNotFound
	}

	// 重载文档有效期
	err = d.restateData(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "task(DocToIndex) getSegments|restateData err:%+v", err)
		return nil, err
	}

	robotID, docID := uint64(0), uint64(0)

	docSegmentMap = make(map[string][]*model.DocSegmentExtend)

	for _, segment := range segments {
		if robotID == 0 {
			robotID = segment.RobotID
		}
		if docID == 0 {
			docID = segment.DocID
		}
		if robotID != segment.RobotID || robotID != d.p.RobotID || docID != segment.DocID || docID != d.p.DocID {
			log.ErrorContextf(ctx, "task(DocToIndex) getSegments|seg illegal|segment: %+v", segment)
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
	}

	return docSegmentMap, nil
}

// Fail 任务失败
func (d *DocToIndexScheduler) Fail(ctx context.Context) error {
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc.HasDeleted() {
		return nil
	}

	// 更新文档状态
	event := model.EventProcessFailed
	err = updateDocStatus(ctx, doc, event)
	if err != nil {
		return err
	}
	err = d.failNotice(ctx, doc)
	if err != nil {
		return err
	}
	return nil
}

func (d *DocToIndexScheduler) failNotice(ctx context.Context, doc *model.Doc) error {
	log.DebugContextf(ctx, "failNotice task: %+v, doc: %+v", d.task, doc)
	operations := []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}}}
	noticeOptions := []model.NoticeOption{
		model.WithGlobalFlag(),
		model.WithPageID(model.NoticeDocPageID),
		model.WithLevel(model.LevelError),
		model.WithSubject(i18n.Translate(ctx, i18nkey.KeyDocumentLearningFailure)),
		model.WithContent(i18n.Translate(ctx, i18nkey.KeyDocumentLearningFailureWithName, doc.FileName)),
	}
	notice := model.NewNotice(model.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, doc.StaffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		log.ErrorContextf(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := d.dao.CreateNotice(ctx, notice); err != nil {
		log.ErrorContextf(ctx, "CreateNotice err:%+v err:%+v", notice, err)
		return err
	}
	return nil
}

// Stop 任务停止
func (d *DocToIndexScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocToIndexScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(DocToIndex) Done|newDocID:%d|oldDocID:%d", d.p.DocID,
		d.p.InterveneOriginDocBizID)
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc.HasDeleted() {
		return nil
	}

	// 更新文档状态
	event := model.EventProcessSuccess
	err = updateDocStatus(ctx, doc, event)
	if err != nil {
		return err
	}

	// 干预相关逻辑
	if d.p.InterveneOriginDocBizID != 0 {
		// 干预中标记移除
		doc.RemoveProcessingFlag([]uint64{model.DocProcessingFlagSegmentIntervene})
		// sheet停用启用标记入库
		if doc.FileType == model.FileTypeXlsx || doc.FileType == model.FileTypeXls ||
			doc.FileType == model.FileTypeCsv {
			corpBizID, appBizID, staffBizID, docBizID, err := d.dao.SegmentCommonIDsToBizIDs(ctx,
				d.p.CorpID, d.p.RobotID, d.p.StaffID, d.p.DocID)
			if err != nil {
				log.ErrorContextf(ctx, "SegmentCommonIDsToBizIDs|newDocID:%d|oldDocID:%d|err:%v", d.p.DocID,
					d.p.InterveneOriginDocBizID, err)
				return err
			}
			docCommon := &model.DocSegmentCommon{
				AppBizID:   appBizID,
				AppID:      d.p.RobotID,
				CorpBizID:  corpBizID,
				CorpID:     d.p.CorpID,
				StaffID:    d.p.StaffID,
				StaffBizID: staffBizID,
				DocBizID:   docBizID,
				DocID:      d.p.DocID,
			}
			log.DebugContextf(ctx, "StoreSheetByDocParseAndCompareOriginDocuments|newDocID:%d|oldDocID:%d", d.p.DocID,
				d.p.InterveneOriginDocBizID)
			err = logicDoc.StoreSheetByDocParseAndCompareOriginDocuments(ctx, d.dao, docCommon, d.p.InterveneOriginDocBizID)
			if err != nil {
				log.ErrorContextf(ctx, "ListTableSheet|StoreSheetByDocParse|err:%+v", err)
				return errs.ErrSystem
			}
		}
	}

	if err := doc_diff_task.AutoRunDocDiffTask(ctx, doc, d.p.StaffID, d.dao); err != nil {
		// 创建自动diff任务流程不要影响文档的正常导入流程
		log.WarnContextf(ctx, "task(DocToIndex) checkAutoRunDiff err:%+v", err)
	}

	return nil
}

func (d *DocToIndexScheduler) createSegment(ctx context.Context, doc *model.Doc,
	docParse model.DocParse) error {
	intervene := false
	if d.p.InterveneOriginDocBizID != 0 {
		intervene = true
	}
	// 干预任务学习失败orgData回退，物理删除（可能）已经插入成功的OrgData，之前逻辑删除的老OrgData恢复
	corpBizID, appBizID, _, _, err := d.dao.SegmentCommonIDsToBizIDs(ctx, doc.CorpID,
		doc.RobotID, 0, 0)
	if err != nil {
		log.ErrorContextf(ctx, "createSegment|SegmentCommonIDsToBizIDs failed, err:%+v", err)
		return err
	}
	// 清理旧数据
	embeddingModelName, err := logicKnowConfig.GetKnowledgeBaseConfig(ctx, corpBizID, appBizID,
		uint32(pb.KnowledgeBaseConfigType_EMBEDDING_MODEL))
	if err != nil {
		return err
	}
	if err := d.dao.DeleteSegmentsForIndex(ctx, doc, embeddingModelName); err != nil {
		log.ErrorContextf(ctx, "createSegment|DeleteSegmentsForIndex failed, err:%+v", err)
		return err
	}
	err = d.dao.ParseOfflineDocTaskResult(ctx, doc, docParse, model.SegmentTypeIndex, intervene)
	if err != nil {
		log.ErrorContextf(ctx, "createSegment|ParseOfflineDocTaskResult failed, err:%+v", err)
		if d.p.InterveneOriginDocBizID != 0 {
			err := logicDoc.RecoverSegmentsForIndex(ctx, corpBizID, appBizID, d.p.InterveneOriginDocBizID)
			if err != nil {
				log.ErrorContextf(ctx, "createSegment|RecoverSegmentsForIndex failed, err:%+v", err)
				return err
			}
		}
		return err
	}
	// 干预成功，物理删除历史OrgData信息
	if d.p.InterveneOriginDocBizID != 0 {
		err := logicDoc.CleanSegmentsForIndex(ctx, corpBizID, appBizID, d.p.InterveneOriginDocBizID)
		if err != nil {
			log.ErrorContextf(ctx, "createSegment|CleanSegmentsForIndex failed, err:%+v", err)
			return err
		}
	}

	return nil
}

func (d *DocToIndexScheduler) restateData(ctx context.Context) error {
	if d.p.ExpireEnd.Unix() < 0 {
		doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
		if err != nil {
			return err
		}
		if doc == nil {
			return errs.ErrDocNotFound
		}
		if doc.IsExcel() {
			return errs.ErrDocTypeForIndex
		}
		d.p.ExpireStart = doc.ExpireStart
		d.p.ExpireEnd = doc.ExpireEnd
	}
	return nil
}

// getDocVectorLabels 文档向量标签统一处理函数
func getDocVectorLabels(ctx context.Context, doc *model.Doc, d dao.Dao) ([]*retrieval.VectorLabel, error) {
	labels, err := getDocVectorWithoutDocIDLabels(ctx, doc, d)
	if err != nil {
		return nil, err
	}
	// 添加上默认的文档ID标签
	labels = append(labels, &retrieval.VectorLabel{
		Name:  model.SysLabelDocID,
		Value: strconv.FormatUint(doc.ID, 10),
	})
	//feature_permission 文档还需要角色和分类向量标签
	//1.先获取app_biz_id
	app, err := d.GetRobotInfo(ctx, doc.CorpID, doc.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "feature_permission getDocVectorLabels GetRobotInfo err:%+v,doc_id:%v,corp_id:%v,app_id:%v",
			err, doc.ID, doc.CorpID, doc.RobotID)
		return nil, err
	}
	//2.取文档的分类和角色标签
	tmp, err := getDocCateAndRoleLabels(ctx, app.BusinessID, doc, d)
	if err != nil {
		return nil, err
	}
	if len(tmp) > 0 {
		labels = append(labels, tmp...)
	}
	return labels, nil
}

// getDocCateAndRoleLabels 取文档的分类和角色标签
func getDocCateAndRoleLabels(ctx context.Context, appBizId uint64, doc *model.Doc, d dao.Dao) (
	labels []*retrieval.VectorLabel, err error) {
	//1.取分类信息
	cateInfo := &model.CateInfo{}
	if doc.CategoryID > 0 {
		cateInfo, err = d.GetCateByID(ctx, model.DocCate, uint64(doc.CategoryID), doc.CorpID, doc.RobotID)
		if err != nil {
			log.ErrorContextf(ctx, "getDocCateAndRoleLabels getCateInfo err:%v,doc:%+v", err, doc)
			return nil, err
		}
	}
	labels = append(labels, &retrieval.VectorLabel{
		Name:  utilConfig.GetMainConfig().Permissions.CateRetrievalKey, //分类向量统一key
		Value: cast.ToString(cateInfo.BusinessID),
	})
	//3.取角色信息
	roleBizIds, err := dao.GetRoleDao(nil).GetRoleByDocBiz(ctx, appBizId, doc.BusinessID)
	if err != nil {
		log.ErrorContextf(ctx, "getDocCateAndRoleLabels getRoleList err:%v,appBizId:%v,docBizId:%v",
			err, appBizId, doc.BusinessID)
		return nil, err
	}
	for _, v := range roleBizIds {
		labels = append(labels, &retrieval.VectorLabel{
			Name:  utilConfig.GetMainConfig().Permissions.RoleRetrievalKey, //角色向量统一key
			Value: cast.ToString(v),
		})
	}
	return labels, nil
}

func getDocVectorWithoutDocIDLabels(ctx context.Context, doc *model.Doc, dao dao.Dao) ([]*retrieval.VectorLabel, error) {
	var vectorLabels []*retrieval.VectorLabel
	if doc.AttrRange == model.AttrRangeAll {
		vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
			Name:  config.App().AttributeLabel.GeneralVectorAttrKey,
			Value: config.App().AttributeLabel.FullLabelValue,
		})
		return vectorLabels, nil
	}
	mapAttrLabels, err := dao.GetDocAttributeLabelDetail(ctx, doc.RobotID, []uint64{doc.ID})
	if err != nil {
		return nil, err
	}
	attrLabels, ok := mapAttrLabels[doc.ID]
	if !ok {
		return nil, nil
	}
	return model.FillVectorLabels(attrLabels), nil
}

func getText2sqlSegmentMeta(ctx context.Context, doc *model.Doc, dao dao.Dao) (model.Text2SQLSegmentMeta, error) {
	meta := model.Text2SQLSegmentMeta{}
	segments, err := dao.GetText2SqlSegmentMeta(ctx, doc.ID, doc.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "getText2sqlSegmentMeta|GetText2SqlSegmentMeta|docID:%d|err:%+v", doc.ID, err)
		return meta, err
	}
	if len(segments) == 0 {
		log.WarnContextf(ctx, "getText2sqlSegmentMeta|GetText2SqlSegmentMeta|docID:%d|segments.len is empty", doc.ID)
		return meta, nil
	}
	seg := segments[0]
	if len(seg.PageContent) == 0 {
		log.ErrorContextf(ctx, "getText2sqlSegmentMeta|GetText2SqlSegmentMeta|docID:%d|PageContent nil", doc.ID)
		return meta, fmt.Errorf("getText2sqlSegmentMeta.len:%d|doc.ID:%d|PageContent is empty", len(segments), doc.ID)
	}
	err = jsoniter.Unmarshal([]byte(seg.PageContent), &meta)
	if err != nil {
		log.ErrorContextf(ctx, "getText2sqlSegmentMeta|Unmarshal|docID:%d|err:%+v", doc.ID, err)
		return meta, err
	}

	// 因为text2sql的 cos pb可能会拆分成多个
	// 所以可能会把meta数据存入了多条数据
	for i, segment := range segments {
		if i == 0 {
			continue
		}
		metaTemp := model.Text2SQLSegmentMeta{}
		err = jsoniter.Unmarshal([]byte(segment.PageContent), &metaTemp)
		if err != nil {
			log.ErrorContextf(ctx, "getText2sqlSegmentMeta|Unmarshal|docID:%d|err:%+v", doc.ID, err)
			return meta, err
		}
		mergeMissingTableIDsToLeft(&meta, &metaTemp)
	}

	return meta, nil
}

// mergeMissingTableIDsToLeft 查找 left 中不存在的 right 的 tableID 并添加到 left 的 TableMetas 中
func mergeMissingTableIDsToLeft(left, right *model.Text2SQLSegmentMeta) {
	existingTableIDs := make(map[string]bool)
	for _, tableMeta := range left.TableMetas {
		existingTableIDs[tableMeta.TableID] = true
	}

	for _, tableMeta := range right.TableMetas {
		if _, ok := existingTableIDs[tableMeta.TableID]; !ok {
			left.TableMetas = append(left.TableMetas, tableMeta)
		}
	}
}

func addText2sqlSegment(ctx context.Context, dao dao.Dao, seg *model.DocSegmentExtend, meta model.Text2SQLSegmentMeta,
	vectorLabels []*retrieval.VectorLabel, robotID uint64) error {
	content := model.Text2SQLSegmentContent{}
	err := jsoniter.Unmarshal([]byte(seg.PageContent), &content)
	if err != nil {
		log.ErrorContextf(ctx, "addText2sqlSegment|Unmarshal|DocID:%d|PageContent:%s|err:%+v", seg.DocID,
			seg.PageContent, err)
		return err
	}
	for _, tableMeta := range meta.TableMetas {
		// 先判断meta的tableID和content的是否一样，不是同个sheet的就跳过
		if content.TableID != tableMeta.TableID {
			log.InfoContextf(ctx, "addText2sqlSegment|DocID:%d|content.TableID:%s|tableMeta.TableID:%s NOT equal",
				seg.DocID, content.TableID, tableMeta.TableID)
			continue
		}
		text2SQLMeta, rows, err := buildMetaAndRow(ctx, tableMeta, content, seg)
		if err != nil {
			log.WarnContextf(ctx, "addText2sqlSegment|buildMetaAndRow|DocID:%d|SegID:%d|err:%+v",
				seg.DocID, seg.ID, err)
			seg.ReleaseStatus = model.SegmentReleaseStatusNotRequired
			err := dao.UpdateSegmentReleaseStatus(ctx, seg, robotID)
			if err != nil {
				log.ErrorContextf(ctx, "addText2sqlSegment|UpdateSegmentReleaseStatus|DocID:%d|SegID:%d|err:%+v",
					seg.DocID, seg.ID, err)
			}
			continue
		}
		err = dao.AddText2SQL(ctx, seg.RobotID, seg.DocID, seg.GetExpireTime(), text2SQLMeta, rows,
			vectorLabels, meta.FileName, seg.CorpID, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func buildMetaAndRow(ctx context.Context, tableMeta *model.Text2SQLSegmentTableMeta,
	content model.Text2SQLSegmentContent, seg *model.DocSegmentExtend) (*retrieval.Text2SQLMeta,
	[]*retrieval.Text2SQLRowData, error) {

	if tableMeta.DataType != model.TableDataTypeNormal {
		log.WarnContextf(ctx, "buildMetaAndRow|DocID:%d|TableID:%d|DataType:%d", seg.DocID,
			tableMeta.TableID, tableMeta.DataType)
		return nil, nil, fmt.Errorf("wrong DataType:%d", tableMeta.DataType)
	}
	// v2.3.0只支持一个表头的数据
	if len(tableMeta.Headers) != 1 {
		log.WarnContextf(ctx, "buildMetaAndRow|DocID:%d|tableName:%s|unsupported multi header:%d",
			tableMeta.TableName, len(tableMeta.Headers))
		return nil, nil, fmt.Errorf("wrong tableMeta.Headers:%d", len(tableMeta.Headers))
	}
	tableMetaHeader := tableMeta.Headers[0]
	if content.TableID != tableMeta.TableID {
		log.WarnContextf(ctx, "buildMetaAndRow|DocID:%d|content.TableID:%d|tableMeta.TableID:%d",
			seg.DocID, content.TableID, tableMeta.TableID)
		return nil, nil, fmt.Errorf("content.TableID(%s) != tableMeta.TableID(%s)", content.TableID,
			tableMeta.TableID)
	}
	m := new(retrieval.Text2SQLMeta)
	m.TableId = tableMeta.TableID
	m.TableName = tableMeta.TableName
	m.DocType = model.DocTypeSegment
	m.Headers = make([]*retrieval.Text2SQLMeta_Header, 0)

	if tableMetaHeader.Type != model.TableHeaderTypeColumn {
		log.WarnContextf(ctx, "buildMetaAndRow|DocID:%d|TableName:%s|header.Type:%d", seg.DocID,
			m.TableName, tableMetaHeader.Type)
		return nil, nil, fmt.Errorf("wrong tableMetaHeader.Type:%d", tableMetaHeader.Type)
	}
	h := &retrieval.Text2SQLMeta_Header{
		Type: retrieval.Text2SQLMeta_Header_HeaderType(tableMetaHeader.Type),
	}
	headerRows := tableMetaHeader.Rows
	log.InfoContextf(ctx, "buildMetaAndRow|headerRows:%+v", &headerRows)
	if err := checkDuplicateHeaderName(ctx, headerRows); err != nil {
		log.WarnContextf(ctx, "buildMetaAndRow|DocID:%d|TableName:%s|checkDuplicateHeaderName|err:%+v",
			seg.DocID, m.TableName, err)
		return nil, nil, err
	}
	h.Rows = convertRow(headerRows)
	log.InfoContextf(ctx, "buildMetaAndRow|h.Rows:%+v", &h.Rows)
	if len(h.Rows) != 1 {
		log.ErrorContextf(ctx, "buildMetaAndRow|DocID:%d|TableName:%s|len(tableMetaHeader.Rows):%d",
			seg.DocID, m.TableName, len(h.Rows))
		return nil, nil, fmt.Errorf("wrong len(h.Rows)= %d", len(h.Rows))
	}
	m.Headers = append(m.Headers, h)
	//  如果 header的列的个数 和 内容row的列的个数对不上，那就当错误数据处理
	if len(h.Rows[0].Cells) != len(content.Cells) || len(content.Cells) == 0 {
		log.ErrorContextf(ctx, "buildMetaAndRow|DocID:%d|TableName:%s|len(h.Rows):%d|len(content.Cells):%d",
			seg.DocID, m.TableName, len(h.Rows[0].Cells), len(content.Cells))
		return nil, nil, fmt.Errorf("wrong len(h.Rows[0].Cells)(%d) != len(content.Cells)(%d)",
			len(h.Rows[0].Cells), len(content.Cells))
	}
	r := make([]*retrieval.Text2SQLRowData, 0)
	data := &retrieval.Text2SQLRowData{
		Id:          seg.ID,
		SegmentType: model.SegmentTypeText2SQLContent,
		Row:         &retrieval.Row{Cells: convertCell(content.Cells)},
	}
	r = append(r, data)
	return m, r, nil
}

func checkDuplicateHeaderName(ctx context.Context, rows []*model.Text2SQLRow) error {
	// 重复的列名就报错
	var formattedHeader = make(map[string]bool, 0)
	if len(rows) == 0 {
		log.WarnContextf(ctx, "checkDuplicateHeaderName|len(rows) == 0")
		return errors.New("wrong header rows.length")
	}
	for i, cell := range rows[0].Cells {
		if cell == nil {
			log.WarnContextf(ctx, "checkDuplicateHeaderName|%d|cell is nil", i)
			return fmt.Errorf("rows[0][%d] cell is nil", i)
		}
		formatted := util.ReplaceSpecialCharacters(cell.Value)
		if len(formatted) == 0 {
			log.WarnContextf(ctx, "checkDuplicateHeaderName|%d|cell.Value:%s|len(formatted):%d", i,
				cell.Value, len(formatted))
			return fmt.Errorf("formatted header is empty")
		}
		if formattedHeader[formatted] || formattedHeader[cell.Value] { // 判断去掉特殊字符之后的列名是否有重复
			log.WarnContextf(ctx, "checkDuplicateHeaderName|%d|cell.Value:%s|formatted:%s", i, cell.Value, formatted)
			return fmt.Errorf("formatted header has same columns, formatted:%s", formatted)
		}
		formattedHeader[formatted] = true
	}
	return nil
}

func convertRow(rows []*model.Text2SQLRow) []*retrieval.Row {
	r0 := make([]*retrieval.Row, 0, len(rows))
	for _, row := range rows {
		r0 = append(r0, &retrieval.Row{
			Cells: convertCell(row.Cells),
		})
	}
	return r0
}

func convertCell(cells []*model.Text2SQLCell) []*retrieval.Cell {
	c0 := make([]*retrieval.Cell, 0, len(cells))
	for _, c := range cells {
		c0 = append(c0, &retrieval.Cell{
			Value:        c.Value,
			CellDataType: retrieval.CellDataType(c.DataType),
		})
	}
	return c0
}

func addText2sqlSegmentBatch(ctx context.Context, dao dao.Dao, segs []*model.DocSegmentExtend,
	meta model.Text2SQLSegmentMeta, vectorLabels []*retrieval.VectorLabel, interveneOriginDocBizID uint64) error {
	if len(segs) == 0 {
		return errors.New("addText2sqlSegmentBatch|empty segments")
	}
	docID := segs[0].DocID
	robotID := segs[0].RobotID
	expireTime := segs[0].GetExpireTime()
	corpID := segs[0].CorpID
	fileType := segs[0].FileType
	log.InfoContextf(ctx, "addText2sqlSegmentBatch|docID:%d|interveneOriginDocBizID:%d|fileType:%s", docID,
		interveneOriginDocBizID, fileType)
	// tableID -> Text2SQLMeta
	tableMetaMap := make(map[string]*retrieval.Text2SQLMeta)
	for _, tableMeta := range meta.TableMetas {
		text2SQLMeta, err := buildText2sqlMeta(ctx, tableMeta)
		if err != nil {
			log.WarnContextf(ctx, "addText2sqlSegmentBatch|buildText2sqlMeta|DocID:%d|err:%+v|tableMeta:%+v",
				docID, err, tableMeta)
			continue
		}
		tableMetaMap[tableMeta.TableID] = text2SQLMeta
	}
	// 无效的切片，如sheet不符合要求，找不到对应的meta信息，则不会添加到text2sql，需要标记为"不需要发布"
	dirtySegs := make([]*model.DocSegmentExtend, 0)
	// tableID -> rows 按sheet分组 批量添加
	tableRowsMap := make(map[string][]*retrieval.Text2SQLRowData)
	for _, seg := range segs {
		content := model.Text2SQLSegmentContent{}
		err := jsoniter.Unmarshal([]byte(seg.PageContent), &content)
		if err != nil {
			log.ErrorContextf(ctx, "addText2sqlSegmentBatch|Unmarshal|DocID:%d|PageContent:%s|err:%+v",
				seg.DocID, seg.PageContent, err)
			dirtySegs = append(dirtySegs, seg)
			continue
		}
		text2SQLMeta, ok := tableMetaMap[content.TableID]
		if !ok {
			log.WarnContextf(ctx, "addText2sqlSegmentBatch|DocID:%d|TableID:%s|not found",
				seg.DocID, content.TableID)
			dirtySegs = append(dirtySegs, seg)
			continue
		}
		headers := text2SQLMeta.GetHeaders()
		if len(headers) == 0 {
			log.WarnContextf(ctx, "addText2sqlSegmentBatch|GetHeaders|DocID:%d|PageContent:%s",
				seg.DocID, seg.PageContent)
			dirtySegs = append(dirtySegs, seg) // 只是标记为"不需要发布"的数据，不影响整体"解析任务"的状态
			continue
		}

		h := headers[0]
		//  如果 header的列的个数 和 内容row的列的个数对不上，那就当错误数据处理
		if len(h.Rows[0].Cells) != len(content.Cells) || len(content.Cells) == 0 {
			log.ErrorContextf(ctx, "addText2sqlSegmentBatch|DocID:%d|TableName:%s|"+
				"len(h.Rows):%d|len(content.Cells):%d",
				seg.DocID, text2SQLMeta.TableName, len(h.Rows[0].Cells), len(content.Cells))
			dirtySegs = append(dirtySegs, seg) // 只是标记为"不需要发布"的数据，不影响整体"解析任务"的状态
			continue
		}

		data := &retrieval.Text2SQLRowData{
			Id:          seg.ID,
			SegmentType: model.SegmentTypeText2SQLContent,
			Row:         &retrieval.Row{Cells: convertCell(content.Cells)},
		}
		tableRowsMap[content.TableID] = append(tableRowsMap[content.TableID], data)
	}
	sheetSyncMap := &sync.Map{}
	for tableID, rows := range tableRowsMap {
		text2SQLMeta, ok := tableMetaMap[tableID]
		if !ok {
			log.ErrorContextf(ctx, "addText2sqlSegmentBatch|DocID:%d|TableID:%s|not found",
				docID, tableID)
			return fmt.Errorf("addText2sqlSegmentBatch|DocID:%d|TableID:%s|not found", docID, tableID)
		}
		disableEs := false
		// 对于表格类型文档，如果sheet停用，则disableEs为true
		if interveneOriginDocBizID != 0 && (fileType == model.FileTypeXlsx || fileType == model.FileTypeXls ||
			fileType == model.FileTypeCsv) {
			log.InfoContextf(ctx, "addText2sqlSegmentBatch|intervene|docID:%d|interveneOriginDocBizID:%d|fileType:%s",
				docID, interveneOriginDocBizID, fileType)
			corpBizID, appBizID, _, _, err := dao.SegmentCommonIDsToBizIDs(ctx, corpID,
				robotID, 0, 0)
			if err != nil {
				log.ErrorContextf(ctx, "SegmentCommonIDsToBizIDs|interveneOriginDocBizID:%d|SheetData:%s|err:%+v",
					interveneOriginDocBizID, text2SQLMeta.TableName, err)
				return err
			}
			tableName := text2SQLMeta.TableName
			oldSheet, err := dao.GetSheetByNameWithCache(ctx, corpBizID, appBizID, interveneOriginDocBizID,
				text2SQLMeta.TableName, sheetSyncMap)
			if err != nil {
				log.ErrorContextf(ctx, "GetSheetByNameWithCache|interveneOriginDocBizID:%d|SheetData:%s|err:%+v",
					interveneOriginDocBizID, text2SQLMeta.TableName, err)
				return err
			}
			if oldSheet != nil && oldSheet.IsDisabledRetrievalEnhance == model.SheetDisabledRetrievalEnhance {
				log.DebugContextf(ctx, "addText2sqlSegmentBatch|disable es|SheetName:%s|docID:%d",
					tableName, docID)
				disableEs = true
			}
		}
		err := dao.AddText2SQL(ctx, robotID, docID, expireTime, text2SQLMeta, rows,
			vectorLabels, meta.FileName, corpID, disableEs)
		if err != nil {
			return err
		}
	}
	if len(dirtySegs) > 0 {
		log.WarnContextf(ctx, "addText2sqlSegmentBatch|dirtyData|DocID:%d|len(dirtySegs):%d", docID, len(dirtySegs))
		err := dao.BatchUpdateSegmentReleaseStatus(ctx, dirtySegs, model.SegmentReleaseStatusNotRequired, robotID)
		if err != nil {
			log.ErrorContextf(ctx, "addText2sqlSegmentBatch|BatchUpdateSegmentReleaseStatus"+
				"|DocID:%d|err:%+v", docID, err)
			return err
		}
	}

	return nil
}

func buildText2sqlMeta(ctx context.Context, tableMeta *model.Text2SQLSegmentTableMeta) (*retrieval.Text2SQLMeta, error) {
	if tableMeta.DataType != model.TableDataTypeNormal {
		log.WarnContextf(ctx, "buildText2sqlMeta|TableID:%d|DataType:%d",
			tableMeta.TableID, tableMeta.DataType)
		return nil, fmt.Errorf("wrong DataType:%d", tableMeta.DataType)
	}
	// v2.3.0只支持一个表头的数据
	if len(tableMeta.Headers) != 1 {
		log.WarnContextf(ctx, "buildText2sqlMeta|tableName:%s|unsupported multi header:%d",
			tableMeta.TableName, len(tableMeta.Headers))
		return nil, fmt.Errorf("wrong tableMeta.Headers:%d", len(tableMeta.Headers))
	}
	tableMetaHeader := tableMeta.Headers[0]

	m := new(retrieval.Text2SQLMeta)
	m.TableId = tableMeta.TableID
	m.TableName = tableMeta.TableName
	m.DocType = model.DocTypeSegment
	m.Headers = make([]*retrieval.Text2SQLMeta_Header, 0)

	if tableMetaHeader.Type != model.TableHeaderTypeColumn {
		log.WarnContextf(ctx, "buildText2sqlMeta|TableName:%s|header.Type:%d",
			m.TableName, tableMetaHeader.Type)
		return nil, fmt.Errorf("wrong tableMetaHeader.Type:%d", tableMetaHeader.Type)
	}
	h := &retrieval.Text2SQLMeta_Header{
		Type: retrieval.Text2SQLMeta_Header_HeaderType(tableMetaHeader.Type),
	}
	headerRows := tableMetaHeader.Rows
	log.InfoContextf(ctx, "buildText2sqlMeta|headerRows:%+v", &headerRows)
	if err := checkDuplicateHeaderName(ctx, headerRows); err != nil {
		log.WarnContextf(ctx, "buildText2sqlMeta|TableName:%s|checkDuplicateHeaderName|err:%+v",
			m.TableName, err)
		return nil, err
	}
	h.Rows = convertRow(headerRows)
	log.InfoContextf(ctx, "buildText2sqlMeta|h.Rows:%+v", &h.Rows)
	if len(h.Rows) != 1 {
		log.ErrorContextf(ctx, "buildText2sqlMeta|TableName:%s|len(tableMetaHeader.Rows):%d",
			m.TableName, len(h.Rows))
		return nil, fmt.Errorf("wrong len(h.Rows)= %d", len(h.Rows))
	}
	m.Headers = append(m.Headers, h)

	return m, nil
}

// updateDocStatus 更新文档状态
func updateDocStatus(ctx context.Context, doc *model.Doc, event string) error {
	doc.IsCreatingIndex = false
	doc.RemoveProcessingFlag([]uint64{model.DocProcessingFlagCreatingIndex})
	docFilter := &dao.DocFilter{
		RobotId: doc.RobotID,
		IDs:     []uint64{doc.ID},
	}
	updateCols := []string{dao.DocTblColStatus, dao.DocTblColIsCreatingIndex, dao.DocTblColProcessingFlag}
	err := logicDoc.UpdateDoc(ctx, updateCols, docFilter, doc, event)
	if err != nil {
		return errs.ErrUpdateDocStatusFail
	}
	return nil
}
