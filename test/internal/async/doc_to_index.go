package async

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/cast"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/entity/label"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/logic/category"
	labelLogic "git.woa.com/adp/kb/kb-config/internal/logic/label"
	segLogic "git.woa.com/adp/kb/kb-config/internal/logic/segment"
	"git.woa.com/adp/kb/kb-config/internal/logic/user"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
)

// DocToIndexTaskHandler 文档删除任务
type DocToIndexTaskHandler struct {
	*taskCommon

	task                task_scheduler.Task
	p                   entity.DocToIndexParams
	VectorLabels        []*retrieval.VectorLabel
	Text2SQLSegmentMeta segEntity.Text2SQLSegmentMeta
}

func registerDocToIndexTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.DocToIndexTask,
		func(t task_scheduler.Task, params entity.DocToIndexParams) task_scheduler.TaskHandler {
			return &DocToIndexTaskHandler{
				taskCommon:   tc,
				task:         t,
				p:            params,
				VectorLabels: make([]*retrieval.VectorLabel, 0),
			}
		},
	)
}

// Prepare 数据准备
func (d *DocToIndexTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(DocToIndex) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
	if err != nil {
		logx.E(ctx, "task(DocToIndex) DescribeAppByPrimaryIdWithoutNotFoundError err:%+v", err)
		return kv, err
	}
	if appDB == nil {
		logx.W(ctx, "task(DocToIndex) appDB is nil")
		return kv, nil
	}
	if appDB.HasDeleted() {
		logx.W(ctx, "app %v has been deleted", appDB.BizId)
		return kv, nil
	}
	doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
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
	logx.D(ctx, "task(DocToIndex) Prepare|doc: %+v, isFromBatchImport: %v", doc, d.p.IsFromBatchImport)
	doc.IsCreatingIndex = true
	doc.AddProcessingFlag([]uint64{docEntity.DocProcessingFlagCreatingIndex})

	var ids []uint64
	if d.p.IsFromBatchImport {
		// 批量导入：切片已存在，直接获取所有切片ID（分页获取）
		if err = d.docLogic.UpdateCreatingIndexFlag(ctx, doc); err != nil {
			return kv, err
		}
		ids, err = d.getAllSegmentIDsByDocID(ctx, doc.ID)
		if err != nil {
			logx.E(ctx, "task(DocToIndex) getAllSegmentIDsByDocID err:%+v", err)
			return kv, err
		}
	} else {
		// 页面导入：需要创建切片
		doc.BatchID += 1
		if err = d.docLogic.UpdateCreatingIndexFlag(ctx, doc); err != nil {
			return kv, err
		}
		docParse, err := d.docLogic.GetDocParseByDocIDAndTypeAndStatus(ctx, doc.ID, docEntity.DocParseTaskTypeSplitSegment,
			docEntity.DocParseSuccess, doc.RobotID)
		if err != nil {
			return kv, err
		}
		if err = d.createSegment(ctx, appDB, doc, docParse); err != nil {
			return kv, err
		}
		ids, err = d.segLogic.GetSegmentIDByDocIDAndBatchID(ctx, doc.ID, doc.BatchID, d.p.RobotID)
		if err != nil {
			return kv, err
		}
	}

	if len(ids) == 0 {
		logx.W(ctx, "task(DocToIndex) no segments found for doc, docID: %d", d.p.DocID)
		return kv, nil
	}

	batchSize := config.GetMainConfig().OfflineConfig.SyncVectorAddBatchSize
	if batchSize <= 0 {
		batchSize = defaultSyncAddVectorBatchSize
	}
	for index, idChunks := range slicex.Chunk(ids, batchSize) {
		var idsStr []string
		for _, id := range idChunks {
			idsStr = append(idsStr, cast.ToString(id))
		}
		idChunksStr, err := jsonx.MarshalToString(idsStr)
		if err != nil {
			logx.E(ctx, "task(DocToIndex) Prepare|jsonx.MarshalToString err:%+v", err)
			kv = make(task_scheduler.TaskKV) // 重置kv
			return kv, err
		}
		logx.D(ctx, "task(DocToIndex) Prepare index:%d, seg.IDs: %+v", index, idChunksStr)
		kv[fmt.Sprintf("%s%d", segGenIndexPrefix, index)] = fmt.Sprintf("%s", idChunksStr)
	}
	return kv, nil
}

// getAllSegmentIDsByDocID 分页获取文档的所有切片ID（用于批量导入场景）
func (d *DocToIndexTaskHandler) getAllSegmentIDsByDocID(ctx context.Context, docID uint64) ([]uint64, error) {
	var ids []uint64
	page := uint32(1)
	pageSize := uint32(1000)
	for {
		pageIDs, err := d.segLogic.GetPagedSegmentIDsByDocID(ctx, docID, page, pageSize, d.p.RobotID)
		if err != nil {
			return nil, err
		}
		if len(pageIDs) == 0 {
			break
		}
		ids = append(ids, pageIDs...)
		if len(pageIDs) < int(pageSize) {
			break
		}
		page++
	}
	return ids, nil
}

// Init 初始化
func (d *DocToIndexTaskHandler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(DocToIndex) Init, task: %+v, params: %+v, kvs: %d", d.task, d.p, len(kv))
	doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
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
	d.VectorLabels, err = getDocVectorLabels(ctx, doc, d.rpc, d.userLogic, d.cateLogic, d.labelLogic)
	if err != nil {
		return err
	}
	d.Text2SQLSegmentMeta, err = getText2sqlSegmentMeta(ctx, doc, d.segLogic)
	if err != nil {
		return err
	}
	logx.D(ctx, "task(DocToIndex) Init success, VectorLabels: %+v, Text2SQLSegmentMeta: %+v",
		d.VectorLabels, d.Text2SQLSegmentMeta)
	return nil
}

// Process 任务处理
func (d *DocToIndexTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(DocToIndex) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(DocToIndex) Start k:%s, v:%s", k, v)
		key := k
		// 如果一定要查，可以改成调用 appmgr 的应用信息缓存接口（appmgr 要提供）
		appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
		if err != nil {
			logx.E(ctx, "task(DocToIndex) DescribeAppByPrimaryIdWithoutNotFoundError err:%+v", err)
			return err
		}
		if appDB.HasDeleted() {
			logx.D(ctx, "task(DocToIndex) appDB.HasDeleted()|appID:%d", d.p.RobotID)
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(DocToIndex) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		newCtx := util.SetMultipleMetaData(ctx, appDB.SpaceId, appDB.Uin)

		if strings.HasPrefix(key, segGenIndexPrefix) {
			value := make([]string, 0)
			err := jsonx.Unmarshal([]byte(v), &value)
			if err != nil {
				logx.E(ctx, "task(DocToIndex) jsonx.Unmarshal  err:%+v", err)
				return err
			}
			ids := convx.SliceStringToUint64(value)
			docSegmentMap, err := d.getSegments(ctx, ids)
			if err != nil {
				logx.E(ctx, "task(DocToIndex) getSegments err:%+v", err)
				return err
			}

			embeddingVersion := appDB.Embedding.Version
			embeddingName, err :=
				d.kbLogic.GetKnowledgeEmbeddingModel(ctx, appDB.CorpBizId, appDB.BizId, appDB.BizId, appDB.IsShared)

			if err != nil {
				logx.E(ctx, "task(DocToIndex) GetKnowledgeEmbeddingModel err:%+v", err)
				return err
			}

			logx.I(ctx, "task(DocToIndex) kb "+
				" embeddingModelName:%s, app embeddingVersion:%d", embeddingName, embeddingVersion)

			for segmentType, segments := range docSegmentMap {
				switch segmentType {
				case segEntity.SegmentTypeText2SQLMeta:
					// ignore, do nothing
				case segEntity.SegmentTypeText2SQLContent:
					if err = addText2sqlSegmentBatch(newCtx, d.segLogic, d.rpc, segments, d.Text2SQLSegmentMeta,
						d.VectorLabels, d.p.InterveneOriginDocBizID); err != nil {
						return err
					}
				default:
					if err = d.segLogic.GetVectorSyncLogic().BatchDirectAddSegmentKnowledge(
						newCtx, appDB.PrimaryId, appDB.BizId,
						segments, embeddingVersion, embeddingName, d.VectorLabels); err != nil {
						return err
					}
				}
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			logx.E(ctx, "task(DocToIndex) Finish kv:%s err:%+v", key, err)
			return err
		}
		logx.D(ctx, "task(DocToIndex) Finish kv:%s", key)
	}
	return nil
}

// getSegments 获取切片
func (d *DocToIndexTaskHandler) getSegments(ctx context.Context, segmentIDs []uint64) (
	docSegmentMap map[string][]*segEntity.DocSegmentExtend, err error) {
	logx.I(ctx, "task(DocToIndex) getSegments|segmentIDs: %+v", segmentIDs)
	segments, err := d.segLogic.GetSegmentByIDs(ctx, segmentIDs, d.p.RobotID)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, errs.ErrSegmentNotFound
	}

	// 重载文档有效期
	err = d.restateData(ctx)
	if err != nil {
		logx.E(ctx, "task(DocToIndex) getSegments|restateData err:%+v", err)
		return nil, err
	}

	robotID, docID := uint64(0), uint64(0)

	docSegmentMap = make(map[string][]*segEntity.DocSegmentExtend)

	for _, segment := range segments {
		if robotID == 0 {
			robotID = segment.RobotID
		}
		if docID == 0 {
			docID = segment.DocID
		}
		if robotID != segment.RobotID || robotID != d.p.RobotID || docID != segment.DocID || docID != d.p.DocID {
			logx.E(ctx, "task(DocToIndex) getSegments|seg illegal|segment: %+v", segment)
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
	}

	return docSegmentMap, nil
}

// Fail 任务失败
func (d *DocToIndexTaskHandler) Fail(ctx context.Context) error {
	logx.E(ctx, "task(DocToIndex) Fail|newDocID:%d|oldDocID:%d", d.p.DocID, d.p.InterveneOriginDocBizID)
	doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc.HasDeleted() {
		return nil
	}
	logx.E(ctx, "task(DocToIndex) Fail|doc: %+v", doc)

	// 更新文档状态
	event := docEntity.EventProcessFailed
	err = d.updateDocStatus(ctx, doc, event)
	if err != nil {
		return err
	}
	err = d.failNotice(ctx, doc)
	if err != nil {
		return err
	}
	return nil
}

func (d *DocToIndexTaskHandler) failNotice(ctx context.Context, doc *docEntity.Doc) error {
	logx.D(ctx, "failNotice task: %+v, doc: %+v", d.task, doc)
	operations := []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}}}
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithGlobalFlag(),
		releaseEntity.WithPageID(releaseEntity.NoticeDocPageID),
		releaseEntity.WithLevel(releaseEntity.LevelError),
		releaseEntity.WithSubject(i18n.Translate(ctx, i18nkey.KeyDocumentLearningFailure)),
		releaseEntity.WithContent(i18n.Translate(ctx, i18nkey.KeyDocumentLearningFailureWithName, doc.FileName)),
	}
	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, doc.StaffID, noticeOptions...)
	if err := notice.SetOperation(operations); err != nil {
		logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
		return err
	}
	if err := d.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
		logx.E(ctx, "CreateNotice err:%+v err:%+v", notice, err)
		return err
	}
	return nil
}

// Stop 任务停止
func (d *DocToIndexTaskHandler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocToIndexTaskHandler) Done(ctx context.Context) error {
	logx.D(ctx, "task(DocToIndex) Done|newDocID:%d|oldDocID:%d", d.p.DocID,
		d.p.InterveneOriginDocBizID)
	doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc.HasDeleted() {
		return nil
	}

	// 更新文档状态
	event := docEntity.EventProcessSuccess
	err = d.updateDocStatus(ctx, doc, event)
	if err != nil {
		return err
	}

	// 干预相关逻辑
	if d.p.InterveneOriginDocBizID != 0 {
		// 干预中标记移除
		doc.RemoveProcessingFlag([]uint64{docEntity.DocProcessingFlagSegmentIntervene})
		// sheet停用启用标记入库
		if doc.FileType == docEntity.FileTypeXlsx || doc.FileType == docEntity.FileTypeXls ||
			doc.FileType == docEntity.FileTypeCsv || doc.FileType == docEntity.FileTypeNumbers {
			corpBizID, appBizID, staffBizID, docBizID, err := d.segLogic.SegmentCommonIDsToBizIDs(ctx,
				d.p.CorpID, d.p.RobotID, d.p.StaffID, d.p.DocID)
			if err != nil {
				logx.E(ctx, "SegmentCommonIDsToBizIDs|newDocID:%d|oldDocID:%d|err:%v", d.p.DocID,
					d.p.InterveneOriginDocBizID, err)
				return err
			}
			docCommon := &segEntity.DocSegmentCommon{
				AppBizID:   appBizID,
				AppID:      d.p.RobotID,
				CorpBizID:  corpBizID,
				CorpID:     d.p.CorpID,
				StaffID:    d.p.StaffID,
				StaffBizID: staffBizID,
				DocBizID:   docBizID,
				DocID:      d.p.DocID,
			}
			logx.D(ctx, "StoreSheetByDocParseAndCompareOriginDocuments|newDocID:%d|oldDocID:%d", d.p.DocID,
				d.p.InterveneOriginDocBizID)
			err = d.docLogic.StoreSheetByDocParseAndCompareOriginDocuments(ctx, docCommon, d.p.InterveneOriginDocBizID)
			if err != nil {
				logx.E(ctx, "ListTableSheet|StoreSheetByDocParse|err:%+v", err)
				return errs.ErrSystem
			}
		}
	}

	if err := d.taskLogic.AutoRunDocDiffTask(ctx, doc, d.p.StaffID); err != nil {
		// 创建自动diff任务流程不要影响文档的正常导入流程
		logx.W(ctx, "task(DocToIndex) checkAutoRunDiff err:%+v", err)
	}

	return nil
}

func (d *DocToIndexTaskHandler) createSegment(ctx context.Context, appDB *entity.App, doc *docEntity.Doc,
	docParse *docEntity.DocParse) error {
	intervene := false
	if d.p.InterveneOriginDocBizID != 0 {
		intervene = true
	}
	// 干预任务学习失败orgData回退，物理删除（可能）已经插入成功的OrgData，之前逻辑删除的老OrgData恢复
	corpBizID, appBizID, _, _, err := d.segLogic.SegmentCommonIDsToBizIDs(ctx, doc.CorpID,
		doc.RobotID, 0, 0)
	if err != nil {
		logx.E(ctx, "createSegment|SegmentCommonIDsToBizIDs failed, err:%+v", err)
		return err
	}
	// 清理旧数据
	embeddingVersion := appDB.Embedding.Version
	embeddingName, err :=
		d.kbLogic.GetKnowledgeEmbeddingModel(ctx, appDB.CorpBizId, appDB.BizId, appDB.BizId, appDB.IsShared)

	if err != nil {
		logx.E(ctx, "task(DocToIndex) GetKnowledgeEmbeddingModel err:%+v", err)
		return err
	}
	if err := d.segLogic.DeleteSegmentsForIndex(ctx, doc, embeddingName, embeddingVersion); err != nil {
		logx.E(ctx, "createSegment|DeleteSegmentsForIndex failed, err:%+v", err)
		return err
	}
	err = d.docLogic.ParseOfflineDocTaskResult(ctx, doc, docParse, segEntity.SegmentTypeIndex, intervene)
	if err != nil {
		logx.E(ctx, "createSegment|ParseOfflineDocTaskResult failed, err:%+v", err)
		if d.p.InterveneOriginDocBizID != 0 {
			err := d.segLogic.RecoverSegmentsForIndex(ctx, corpBizID, appBizID, d.p.InterveneOriginDocBizID)
			if err != nil {
				logx.E(ctx, "createSegment|RecoverSegmentsForIndex failed, err:%+v", err)
				return err
			}
		}
		return err
	}
	// 干预成功，物理删除历史OrgData信息
	if d.p.InterveneOriginDocBizID != 0 {
		err := d.segLogic.CleanSegmentsForIndex(ctx, corpBizID, appBizID, d.p.InterveneOriginDocBizID)
		if err != nil {
			logx.E(ctx, "createSegment|CleanSegmentsForIndex failed, err:%+v", err)
			return err
		}
	}

	return nil
}

func (d *DocToIndexTaskHandler) restateData(ctx context.Context) error {
	if d.p.ExpireEnd.Unix() < 0 {
		doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
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
func getDocVectorLabels(ctx context.Context, doc *docEntity.Doc, r *rpc.RPC,
	userLogic *user.Logic, cateLogic *category.Logic, labelLogic *labelLogic.Logic) ([]*retrieval.VectorLabel, error) {
	labels, err := getDocVectorWithoutDocIDLabels(ctx, doc, labelLogic)
	if err != nil {
		return nil, err
	}
	// 添加上默认的文档ID标签
	labels = append(labels, &retrieval.VectorLabel{
		Name:  docEntity.SysLabelDocID,
		Value: strconv.FormatUint(doc.ID, 10),
	})
	labels = append(labels, &retrieval.VectorLabel{
		Name:  entity.EnableScopeAttr,
		Value: entity.EnableScopeDb2Label[doc.EnableScope],
	})
	// feature_permission 文档还需要角色和分类向量标签
	// 1.先获取app_biz_id
	app, err := r.DescribeAppByPrimaryId(ctx, doc.RobotID)
	// app, err := d.GetRobotInfo(ctx, doc.CorpPrimaryId, doc.AppPrimaryId)
	if err != nil {
		logx.E(ctx, "feature_permission getDocVectorLabels GetRobotInfo err:%+v,doc_id:%v,corp_id:%v,app_id:%v",
			err, doc.ID, doc.CorpID, doc.RobotID)
		return nil, err
	}
	// 2.取文档的分类和角色标签
	tmp, err := getDocCateAndRoleLabels(ctx, app.BizId, doc, userLogic, cateLogic)
	if err != nil {
		return nil, err
	}
	if len(tmp) > 0 {
		labels = append(labels, tmp...)
	}
	return labels, nil
}

// getDocCateAndRoleLabels 取文档的分类和角色标签
func getDocCateAndRoleLabels(ctx context.Context, appBizId uint64, doc *docEntity.Doc, userLogic *user.Logic, cateLogic *category.Logic) (
	labels []*retrieval.VectorLabel, err error) {
	// 1.取分类信息
	cateInfo := &cateEntity.CateInfo{}
	if doc.CategoryID > 0 {
		cateInfo, err = cateLogic.DescribeCateByID(ctx, cateEntity.DocCate, uint64(doc.CategoryID), doc.CorpID, doc.RobotID)
		if err != nil {
			logx.E(ctx, "getDocCateAndRoleLabels getCateInfo err:%v,doc:%+v", err, doc)
			return nil, err
		}
	}
	labels = append(labels, &retrieval.VectorLabel{
		Name:  config.GetMainConfig().Permissions.CateRetrievalKey, // 分类向量统一key
		Value: cast.ToString(cateInfo.BusinessID),
	})
	// 3.取角色信息
	roleBizIds, err := userLogic.DescribeRoleIDListByDocBiz(ctx, appBizId, doc.BusinessID, 10000)
	if err != nil {
		logx.E(ctx, "getDocCateAndRoleLabels getRoleList err:%v,appBizId:%v,docBizId:%v",
			err, appBizId, doc.BusinessID)
		return nil, err
	}
	for _, v := range roleBizIds {
		labels = append(labels, &retrieval.VectorLabel{
			Name:  config.GetMainConfig().Permissions.RoleRetrievalKey, // 角色向量统一key
			Value: cast.ToString(v),
		})
	}
	return labels, nil
}

func getDocVectorWithoutDocIDLabels(ctx context.Context, doc *docEntity.Doc, labelLogic *labelLogic.Logic) (
	[]*retrieval.VectorLabel, error) {
	var vectorLabels []*retrieval.VectorLabel
	if doc.AttrRange == docEntity.AttrRangeAll {
		vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
			Name:  config.App().AttributeLabel.GeneralVectorAttrKey,
			Value: config.App().AttributeLabel.FullLabelValue,
		})
		return vectorLabels, nil
	}
	mapAttrLabels, err := labelLogic.GetDocAttributeLabelDetail(ctx, doc.RobotID, []uint64{doc.ID})
	if err != nil {
		return nil, err
	}
	attrLabels, ok := mapAttrLabels[doc.ID]
	if !ok {
		return nil, nil
	}
	return label.FillVectorLabels(attrLabels), nil
}

func getText2sqlSegmentMeta(ctx context.Context, doc *docEntity.Doc, segLogic *segLogic.Logic) (segEntity.Text2SQLSegmentMeta, error) {
	meta := segEntity.Text2SQLSegmentMeta{}
	segments, err := segLogic.GetText2SqlSegmentMeta(ctx, doc.ID, doc.RobotID)
	if err != nil {
		logx.E(ctx, "getText2sqlSegmentMeta|GetText2SqlSegmentMeta|docID:%d|err:%+v", doc.ID, err)
		return meta, err
	}
	if len(segments) == 0 {
		logx.W(ctx, "getText2sqlSegmentMeta|GetText2SqlSegmentMeta|docID:%d|segments.len is empty", doc.ID)
		return meta, nil
	}
	seg := segments[0]
	if len(seg.PageContent) == 0 {
		logx.E(ctx, "getText2sqlSegmentMeta|GetText2SqlSegmentMeta|docID:%d|PageContent nil", doc.ID)
		return meta, fmt.Errorf("getText2sqlSegmentMeta.len:%d|doc.ID:%d|PageContent is empty", len(segments), doc.ID)
	}
	err = jsonx.Unmarshal([]byte(seg.PageContent), &meta)
	if err != nil {
		logx.E(ctx, "getText2sqlSegmentMeta|Unmarshal|docID:%d|err:%+v", doc.ID, err)
		return meta, err
	}

	// 因为text2sql的 cos pb可能会拆分成多个
	// 所以可能会把meta数据存入了多条数据
	for i, segment := range segments {
		if i == 0 {
			continue
		}
		metaTemp := segEntity.Text2SQLSegmentMeta{}
		err = jsonx.Unmarshal([]byte(segment.PageContent), &metaTemp)
		if err != nil {
			logx.E(ctx, "getText2sqlSegmentMeta|Unmarshal|docID:%d|err:%+v", doc.ID, err)
			return meta, err
		}
		mergeMissingTableIDsToLeft(&meta, &metaTemp)
	}

	return meta, nil
}

// mergeMissingTableIDsToLeft 查找 left 中不存在的 right 的 tableID 并添加到 left 的 TableMetas 中
func mergeMissingTableIDsToLeft(left, right *segEntity.Text2SQLSegmentMeta) {
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

func buildMetaAndRow(ctx context.Context, tableMeta *segEntity.Text2SQLSegmentTableMeta,
	content segEntity.Text2SQLSegmentContent, seg *segEntity.DocSegmentExtend) (*retrieval.Text2SQLMeta,
	[]*retrieval.Text2SQLRowData, error) {

	if tableMeta.DataType != segEntity.TableDataTypeNormal {
		logx.W(ctx, "buildMetaAndRow|DocID:%d|TableID:%d|DataType:%d", seg.DocID,
			tableMeta.TableID, tableMeta.DataType)
		return nil, nil, fmt.Errorf("wrong DataType:%d", tableMeta.DataType)
	}
	// v2.3.0只支持一个表头的数据
	if len(tableMeta.Headers) != 1 {
		logx.W(ctx, "buildMetaAndRow|DocID:%d|tableName:%s|unsupported multi header:%d",
			tableMeta.TableName, len(tableMeta.Headers))
		return nil, nil, fmt.Errorf("wrong tableMeta.Headers:%d", len(tableMeta.Headers))
	}
	tableMetaHeader := tableMeta.Headers[0]
	if content.TableID != tableMeta.TableID {
		logx.W(ctx, "buildMetaAndRow|DocID:%d|content.TableID:%d|tableMeta.TableID:%d",
			seg.DocID, content.TableID, tableMeta.TableID)
		return nil, nil, fmt.Errorf("content.TableID(%s) != tableMeta.TableID(%s)", content.TableID,
			tableMeta.TableID)
	}
	m := new(retrieval.Text2SQLMeta)
	m.TableId = tableMeta.TableID
	m.TableName = tableMeta.TableName
	m.DocType = entity.DocTypeSegment
	m.Headers = make([]*retrieval.Text2SQLMeta_Header, 0)

	if tableMetaHeader.Type != segEntity.TableHeaderTypeColumn {
		logx.W(ctx, "buildMetaAndRow|DocID:%d|TableName:%s|header.Type:%d", seg.DocID,
			m.TableName, tableMetaHeader.Type)
		return nil, nil, fmt.Errorf("wrong tableMetaHeader.Type:%d", tableMetaHeader.Type)
	}
	h := &retrieval.Text2SQLMeta_Header{
		Type: retrieval.Text2SQLMeta_Header_HeaderType(tableMetaHeader.Type),
	}
	headerRows := tableMetaHeader.Rows
	logx.I(ctx, "buildMetaAndRow|headerRows:%+v", &headerRows)
	if err := checkDuplicateHeaderName(ctx, headerRows); err != nil {
		logx.W(ctx, "buildMetaAndRow|DocID:%d|TableName:%s|checkDuplicateHeaderName|err:%+v",
			seg.DocID, m.TableName, err)
		return nil, nil, err
	}
	h.Rows = convertRow(headerRows)
	logx.I(ctx, "buildMetaAndRow|h.Rows:%+v", &h.Rows)
	if len(h.Rows) != 1 {
		logx.E(ctx, "buildMetaAndRow|DocID:%d|TableName:%s|len(tableMetaHeader.Rows):%d",
			seg.DocID, m.TableName, len(h.Rows))
		return nil, nil, fmt.Errorf("wrong len(h.Rows)= %d", len(h.Rows))
	}
	m.Headers = append(m.Headers, h)
	//  如果 header的列的个数 和 内容row的列的个数对不上，那就当错误数据处理
	if len(h.Rows[0].Cells) != len(content.Cells) || len(content.Cells) == 0 {
		logx.E(ctx, "buildMetaAndRow|DocID:%d|TableName:%s|len(h.Rows):%d|len(content.Cells):%d",
			seg.DocID, m.TableName, len(h.Rows[0].Cells), len(content.Cells))
		return nil, nil, fmt.Errorf("wrong len(h.Rows[0].Cells)(%d) != len(content.Cells)(%d)",
			len(h.Rows[0].Cells), len(content.Cells))
	}
	r := make([]*retrieval.Text2SQLRowData, 0)
	data := &retrieval.Text2SQLRowData{
		Id:          seg.ID,
		SegmentType: segEntity.SegmentTypeText2SQLContent,
		Row:         &retrieval.Row{Cells: convertCell(content.Cells)},
	}
	r = append(r, data)
	return m, r, nil
}

func checkDuplicateHeaderName(ctx context.Context, rows []*segEntity.Text2SQLRow) error {
	// 重复的列名就报错
	var formattedHeader = make(map[string]bool, 0)
	if len(rows) == 0 {
		logx.W(ctx, "checkDuplicateHeaderName|len(rows) == 0")
		return errors.New("wrong header rows.length")
	}
	for i, cell := range rows[0].Cells {
		if cell == nil {
			logx.W(ctx, "checkDuplicateHeaderName|%d|cell is nil", i)
			return fmt.Errorf("rows[0][%d] cell is nil", i)
		}
		formatted := util.ReplaceSpecialCharacters(cell.Value)
		if len(formatted) == 0 {
			logx.W(ctx, "checkDuplicateHeaderName|%d|cell.Value:%s|len(formatted):%d", i,
				cell.Value, len(formatted))
			return fmt.Errorf("formatted header is empty")
		}
		if formattedHeader[formatted] || formattedHeader[cell.Value] { // 判断去掉特殊字符之后的列名是否有重复
			logx.W(ctx, "checkDuplicateHeaderName|%d|cell.Value:%s|formatted:%s", i, cell.Value, formatted)
			return fmt.Errorf("formatted header has same columns, formatted:%s", formatted)
		}
		formattedHeader[formatted] = true
	}
	return nil
}

func convertRow(rows []*segEntity.Text2SQLRow) []*retrieval.Row {
	r0 := make([]*retrieval.Row, 0, len(rows))
	for _, row := range rows {
		r0 = append(r0, &retrieval.Row{
			Cells: convertCell(row.Cells),
		})
	}
	return r0
}

func convertCell(cells []*segEntity.Text2SQLCell) []*retrieval.Cell {
	c0 := make([]*retrieval.Cell, 0, len(cells))
	for _, c := range cells {
		c0 = append(c0, &retrieval.Cell{
			Value:        c.Value,
			CellDataType: retrieval.CellDataType(c.DataType),
		})
	}
	return c0
}

func addText2sqlSegmentBatch(ctx context.Context, segLogic *segLogic.Logic, r *rpc.RPC, segs []*segEntity.DocSegmentExtend,
	meta segEntity.Text2SQLSegmentMeta, vectorLabels []*retrieval.VectorLabel, interveneOriginDocBizID uint64) error {
	if len(segs) == 0 {
		return errors.New("addText2sqlSegmentBatch|empty segments")
	}
	docID := segs[0].DocID
	robotID := segs[0].RobotID
	expireTime := segs[0].GetExpireTime()
	corpID := segs[0].CorpID
	fileType := segs[0].FileType
	logx.I(ctx, "addText2sqlSegmentBatch|docID:%d|interveneOriginDocBizID:%d|fileType:%s", docID,
		interveneOriginDocBizID, fileType)
	// tableID -> Text2SQLMeta
	tableMetaMap := make(map[string]*retrieval.Text2SQLMeta)
	for _, tableMeta := range meta.TableMetas {
		text2SQLMeta, err := buildText2sqlMeta(ctx, tableMeta)
		if err != nil {
			logx.W(ctx, "addText2sqlSegmentBatch|buildText2sqlMeta|DocID:%d|err:%+v|tableMeta:%+v",
				docID, err, tableMeta)
			continue
		}
		tableMetaMap[tableMeta.TableID] = text2SQLMeta
	}
	// 无效的切片，如sheet不符合要求，找不到对应的meta信息，则不会添加到text2sql，需要标记为"不需要发布"
	dirtySegs := make([]*segEntity.DocSegmentExtend, 0)
	// tableID -> rows 按sheet分组 批量添加
	tableRowsMap := make(map[string][]*retrieval.Text2SQLRowData)
	for _, seg := range segs {
		content := segEntity.Text2SQLSegmentContent{}
		err := jsonx.Unmarshal([]byte(seg.PageContent), &content)
		if err != nil {
			logx.E(ctx, "addText2sqlSegmentBatch|Unmarshal|DocID:%d|PageContent:%s|err:%+v",
				seg.DocID, seg.PageContent, err)
			dirtySegs = append(dirtySegs, seg)
			continue
		}
		text2SQLMeta, ok := tableMetaMap[content.TableID]
		if !ok {
			logx.W(ctx, "addText2sqlSegmentBatch|DocID:%d|TableID:%s|not found",
				seg.DocID, content.TableID)
			dirtySegs = append(dirtySegs, seg)
			continue
		}
		headers := text2SQLMeta.GetHeaders()
		if len(headers) == 0 {
			logx.W(ctx, "addText2sqlSegmentBatch|GetHeaders|DocID:%d|PageContent:%s",
				seg.DocID, seg.PageContent)
			dirtySegs = append(dirtySegs, seg) // 只是标记为"不需要发布"的数据，不影响整体"解析任务"的状态
			continue
		}

		h := headers[0]
		//  如果 header的列的个数 和 内容row的列的个数对不上，那就当错误数据处理
		if len(h.Rows[0].Cells) != len(content.Cells) || len(content.Cells) == 0 {
			logx.E(ctx, "addText2sqlSegmentBatch|DocID:%d|TableName:%s|"+
				"len(h.Rows):%d|len(content.Cells):%d",
				seg.DocID, text2SQLMeta.TableName, len(h.Rows[0].Cells), len(content.Cells))
			dirtySegs = append(dirtySegs, seg) // 只是标记为"不需要发布"的数据，不影响整体"解析任务"的状态
			continue
		}

		data := &retrieval.Text2SQLRowData{
			Id:          seg.ID,
			SegmentType: segEntity.SegmentTypeText2SQLContent,
			Row:         &retrieval.Row{Cells: convertCell(content.Cells)},
		}
		tableRowsMap[content.TableID] = append(tableRowsMap[content.TableID], data)
	}
	sheetSyncMap := &sync.Map{}
	for tableID, rows := range tableRowsMap {
		text2SQLMeta, ok := tableMetaMap[tableID]
		if !ok {
			logx.E(ctx, "addText2sqlSegmentBatch|DocID:%d|TableID:%s|not found",
				docID, tableID)
			return fmt.Errorf("addText2sqlSegmentBatch|DocID:%d|TableID:%s|not found", docID, tableID)
		}
		disableEs := false
		// 对于表格类型文档，如果sheet停用，则disableEs为true
		if interveneOriginDocBizID != 0 && (fileType == docEntity.FileTypeXlsx || fileType == docEntity.FileTypeXls ||
			fileType == docEntity.FileTypeCsv || fileType == docEntity.FileTypeNumbers) {
			logx.I(ctx, "addText2sqlSegmentBatch|intervene|docID:%d|interveneOriginDocBizID:%d|fileType:%s",
				docID, interveneOriginDocBizID, fileType)
			corpBizID, appBizID, _, _, err := segLogic.SegmentCommonIDsToBizIDs(ctx, corpID,
				robotID, 0, 0)
			if err != nil {
				logx.E(ctx, "SegmentCommonIDsToBizIDs|interveneOriginDocBizID:%d|SheetData:%s|err:%+v",
					interveneOriginDocBizID, text2SQLMeta.TableName, err)
				return err
			}
			tableName := text2SQLMeta.TableName
			oldSheet, err := segLogic.GetSheetByNameWithCache(ctx, corpBizID, appBizID, interveneOriginDocBizID,
				text2SQLMeta.TableName, sheetSyncMap)
			if err != nil {
				logx.E(ctx, "GetSheetByNameWithCache|interveneOriginDocBizID:%d|SheetData:%s|err:%+v",
					interveneOriginDocBizID, text2SQLMeta.TableName, err)
				return err
			}
			if oldSheet != nil && oldSheet.IsDisabledRetrievalEnhance {
				logx.D(ctx, "addText2sqlSegmentBatch|disable es|SheetName:%s|docID:%d",
					tableName, docID)
				disableEs = true
			}
		}
		req := retrieval.AddText2SQLReq{
			RobotId:    robotID,
			DocId:      docID,
			Meta:       text2SQLMeta,
			Rows:       rows,
			Labels:     vectorLabels,
			ExpireTime: expireTime,
			FileName:   meta.FileName,
			CorpId:     corpID,
			DisableEs:  disableEs,
		}
		_, err := r.AddText2SQL(ctx, &req)
		if err != nil {
			return err
		}
	}
	if len(dirtySegs) > 0 {
		logx.W(ctx, "addText2sqlSegmentBatch|dirtyData|DocID:%d|len(dirtySegs):%d", docID, len(dirtySegs))
		err := segLogic.BatchUpdateSegmentReleaseStatus(ctx, dirtySegs, segEntity.SegmentReleaseStatusNotRequired, robotID)
		if err != nil {
			logx.E(ctx, "addText2sqlSegmentBatch|BatchUpdateSegmentReleaseStatus"+
				"|DocID:%d|err:%+v", docID, err)
			return err
		}
	}

	return nil
}

func buildText2sqlMeta(ctx context.Context, tableMeta *segEntity.Text2SQLSegmentTableMeta) (*retrieval.Text2SQLMeta, error) {
	if tableMeta.DataType != segEntity.TableDataTypeNormal {
		logx.W(ctx, "buildText2sqlMeta|TableID:%d|DataType:%d",
			tableMeta.TableID, tableMeta.DataType)
		return nil, fmt.Errorf("wrong DataType:%d", tableMeta.DataType)
	}
	// v2.3.0只支持一个表头的数据
	if len(tableMeta.Headers) != 1 {
		logx.W(ctx, "buildText2sqlMeta|tableName:%s|unsupported multi header:%d",
			tableMeta.TableName, len(tableMeta.Headers))
		return nil, fmt.Errorf("wrong tableMeta.Headers:%d", len(tableMeta.Headers))
	}
	tableMetaHeader := tableMeta.Headers[0]

	m := new(retrieval.Text2SQLMeta)
	m.TableId = tableMeta.TableID
	m.TableName = tableMeta.TableName
	m.DocType = entity.DocTypeSegment
	m.Headers = make([]*retrieval.Text2SQLMeta_Header, 0)

	if tableMetaHeader.Type != segEntity.TableHeaderTypeColumn {
		logx.W(ctx, "buildText2sqlMeta|TableName:%s|header.Type:%d",
			m.TableName, tableMetaHeader.Type)
		return nil, fmt.Errorf("wrong tableMetaHeader.Type:%d", tableMetaHeader.Type)
	}
	h := &retrieval.Text2SQLMeta_Header{
		Type: retrieval.Text2SQLMeta_Header_HeaderType(tableMetaHeader.Type),
	}
	headerRows := tableMetaHeader.Rows
	logx.I(ctx, "buildText2sqlMeta|headerRows:%+v", &headerRows)
	if err := checkDuplicateHeaderName(ctx, headerRows); err != nil {
		logx.W(ctx, "buildText2sqlMeta|TableName:%s|checkDuplicateHeaderName|err:%+v",
			m.TableName, err)
		return nil, err
	}
	h.Rows = convertRow(headerRows)
	logx.I(ctx, "buildText2sqlMeta|h.Rows:%+v", &h.Rows)
	if len(h.Rows) != 1 {
		logx.E(ctx, "buildText2sqlMeta|TableName:%s|len(tableMetaHeader.Rows):%d",
			m.TableName, len(h.Rows))
		return nil, fmt.Errorf("wrong len(h.Rows)= %d", len(h.Rows))
	}
	m.Headers = append(m.Headers, h)

	return m, nil
}

// updateDocStatus 更新文档状态
func (d *DocToIndexTaskHandler) updateDocStatus(ctx context.Context, doc *docEntity.Doc, event string) error {
	doc.IsCreatingIndex = false
	doc.RemoveProcessingFlag([]uint64{docEntity.DocProcessingFlagCreatingIndex})
	docFilter := &docEntity.DocFilter{
		RobotId: doc.RobotID,
		IDs:     []uint64{doc.ID},
	}
	updateCols := []string{docEntity.DocTblColStatus, docEntity.DocTblColIsCreatingIndex, docEntity.DocTblColProcessingFlag}
	err := d.docLogic.UpdateDocStatusMachineByEvent(ctx, updateCols, docFilter, doc, event)
	if err != nil {
		return errs.ErrUpdateDocStatusFail
	}
	return nil
}
