package async

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
	"golang.org/x/exp/maps"
	"gorm.io/gorm"
)

const (
	renameSegGenIndexPrefix = "rename:seg:gen:index:"
)

// DocRenameToIndexTaskHandler 文档重命名入向量库任务
type DocRenameToIndexTaskHandler struct {
	*taskCommon

	task                task_scheduler.Task
	p                   entity.DocRenameToIndexParams
	VectorLabels        []*retrieval.VectorLabel
	Text2SQLSegmentMeta segEntity.Text2SQLSegmentMeta
	newPrefix           string
	app                 *entity.App
}

func registerDocRenameToIndexTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.DocRenameToIndexTask,
		func(t task_scheduler.Task, params entity.DocRenameToIndexParams) task_scheduler.TaskHandler {
			return &DocRenameToIndexTaskHandler{
				taskCommon:   tc,
				task:         t,
				p:            params,
				VectorLabels: make([]*retrieval.VectorLabel, 0),
			}
		},
	)
}

// Prepare 数据准备
func (d *DocRenameToIndexTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(DocRenameToIndex) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	app, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
	if err != nil {
		return kv, err
	}
	d.app = app
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
	// doc.BatchID += 1 修复文档重命名后成功后，无法生成问答的问题，code:450042, msg:分片内容为空
	doc.Status = docEntity.DocStatusCreatingIndex
	doc.IsCreatingIndex = true
	doc.AddProcessingFlag([]uint64{docEntity.DocProcessingFlagCreatingIndex})
	// TODO: 此处按理说处于审核中的状态,是否需要改为学习中待定
	if err = d.docLogic.UpdateCreatingIndexFlag(ctx, doc); err != nil {
		return kv, err
	}

	batchSize := config.GetMainConfig().OfflineConfig.SyncVectorAddBatchSize
	if batchSize <= 0 {
		batchSize = defaultSyncAddVectorBatchSize
	}
	page := 1
	pageSize := batchSize
	count := batchSize
	for count != 0 {
		ids, err := d.segLogic.GetPagedSegmentIDsByDocID(ctx, doc.ID, uint32(page), uint32(pageSize), d.p.RobotID)
		if err != nil {
			return kv, err
		}
		page++
		count = len(ids)
		if count == 0 {
			continue
		}
		idsStr := make([]string, 0, len(ids))
		for _, id := range ids {
			idsStr = append(idsStr, cast.ToString(id))
		}
		idChunksStr, err := jsonx.MarshalToString(idsStr)
		if err != nil {
			logx.E(ctx, "task(DocRenameToIndex) Prepare|jsonx.MarshalToString err:%+v", err)
			kv = make(task_scheduler.TaskKV) // 重置kv
			return kv, err
		}
		logx.D(ctx, "task(DocRenameToIndex) Prepare index:%d, seg.IDs: %+v", page, idChunksStr)
		kv[fmt.Sprintf("%s%d", renameSegGenIndexPrefix, page)] = fmt.Sprintf("%s", idChunksStr)
	}
	return kv, nil
}

// Init 初始化
func (d *DocRenameToIndexTaskHandler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(DocRenameToIndex) Init, task: %+v, params: %+v, kvs: %d", d.task, d.p, len(kv))
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
	d.newPrefix = util.FileNameNoSuffix(doc.FileNameInAudit) + ": \n"
	d.Text2SQLSegmentMeta.FileName = d.newPrefix

	logx.D(ctx, "task(DocRenameToIndex) Init success, VectorLabels: %+v, Text2SQLSegmentMeta: %+v",
		d.VectorLabels, d.Text2SQLSegmentMeta)
	return nil
}

// Process 任务处理
func (d *DocRenameToIndexTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(DocRenameToIndex) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(DocRenameToIndex) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			logx.D(ctx, "task(DocRenameToIndex) appDB.HasDeleted()|appID:%d", d.p.RobotID)
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(DocRenameToIndex) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		newCtx := util.SetMultipleMetaData(ctx, appDB.SpaceId, appDB.Uin)
		if strings.HasPrefix(key, renameSegGenIndexPrefix) {
			value := make([]string, 0)
			err := jsonx.Unmarshal([]byte(v), &value)
			if err != nil {
				logx.E(ctx, "task(DocRenameToIndex) jsonx.Unmarshal  err:%+v", err)
				return err
			}
			ids := make([]uint64, 0)
			for _, idStr := range value {
				ids = append(ids, cast.ToUint64(idStr))
			}

			docSegmentMap, orgDataList, bigDataList, err := d.getSegments(ctx, ids)
			if err != nil {
				logx.E(ctx, "task(DocRenameToIndex) getSegments err:%+v", err)
				return err
			}

			embeddingVersion := appDB.Embedding.Version
			embeddingModel, err :=
				d.kbLogic.GetKnowledgeEmbeddingModel(ctx, appDB.CorpBizId, appDB.BizId, appDB.BizId, appDB.IsShared)

			if err != nil {
				logx.E(ctx, "task(DocRenameToIndex) GetKnowledgeEmbeddingModel err:%+v", err)
				return err
			}
			logx.I(ctx, "task(DocRenameToIndex) kb "+
				" embeddingModelName:%s, app embeddingVersion:%d", embeddingModel, embeddingVersion)

			for segmentType, segments := range docSegmentMap {
				// 更新文档切片内容
				if err = d.segLogic.BatchUpdateSegmentContent(ctx, segments, d.p.RobotID); err != nil {
					return err
				}
				switch segmentType {
				case segEntity.SegmentTypeText2SQLMeta:
				case segEntity.SegmentTypeText2SQLContent:
				default:
					if err = d.segLogic.GetVectorSyncLogic().BatchDirectAddSegmentKnowledge(newCtx, appDB.PrimaryId, appDB.BizId,
						segments, embeddingVersion, embeddingModel, d.VectorLabels); err != nil {
						logx.E(ctx, "task(DocRenameToIndex) BatchDirectAddSegmentKnowledge err:%+v", err)
						return err
					}
				}
			}

			// 更新org_data
			if len(orgDataList) > 0 {
				if err = d.segLogic.BatchUpdateSegmentOrgDataContent(ctx, orgDataList); err != nil {
					logx.E(ctx, "task(DocRenameToIndex) BatchUpdateSegmentOrgDataContent err:%+v", err)
					return err
				}
				logx.D(ctx, "task(DocRenameToIndex) BatchUpdateSegmentOrgDataContent success|count:%d", len(orgDataList))
			}

			// 更新big_data
			if len(bigDataList) > 0 {
				req := retrieval.AddBigDataElasticReq{Data: bigDataList, Type: retrieval.KnowledgeType_KNOWLEDGE}
				if err := d.rpc.RetrievalDirectIndex.AddBigDataElastic(ctx, &req); err != nil {
					logx.E(ctx, "task(DocRenameToIndex) AddBigDataElastic err:%+v", err)
					return err
				}
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			logx.E(ctx, "task(DocRenameToIndex) Finish kv:%s err:%+v", key, err)
			return err
		}
		logx.D(ctx, "task(DocRenameToIndex) Finish kv:%s", key)
	}
	return nil
}

// getSegments 获取切片
func (d *DocRenameToIndexTaskHandler) getSegments(ctx context.Context, segmentIDs []uint64) (
	map[string][]*segEntity.DocSegmentExtend, []*segEntity.DocSegmentOrgData, []*retrieval.BigData, error) {
	logx.I(ctx, "task(DocRenameToIndex) getSegments|segmentIDs: %+v", segmentIDs)
	segments, err := d.segLogic.GetSegmentByIDs(ctx, segmentIDs, d.p.RobotID)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(segments) == 0 {
		return nil, nil, nil, errs.ErrSegmentNotFound
	}
	appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
	if err != nil {
		return nil, nil, nil, err
	}
	corpRsp, err := d.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, d.p.CorpID)
	if err != nil || corpRsp == nil {
		return nil, nil, nil, err
	}
	robotID, docID := uint64(0), uint64(0)

	orgPrefix := ""

	docSegmentMap := make(map[string][]*segEntity.DocSegmentExtend)
	bigDataIDs := make([]string, 0, len(segments))

	docSegOrgDataMap := map[uint64]*segEntity.DocSegmentOrgData{}
	for _, segment := range segments {
		doc, err := d.docLogic.GetDocByID(ctx, segment.DocID, segment.RobotID)
		if err != nil {
			return nil, nil, nil, err
		}
		if doc == nil {
			return nil, nil, nil, errs.ErrDocNotFound
		}
		if robotID == 0 {
			robotID = segment.RobotID
		}
		if docID == 0 {
			docID = segment.DocID
		}
		if robotID != segment.RobotID || robotID != d.p.RobotID || docID != segment.DocID || docID != d.p.DocID {
			logx.E(ctx, "task(DocRenameToIndex) getSegments|seg illegal|segment: %+v", segment)
			return nil, nil, nil, errs.ErrSegmentNotFound
		}

		if orgPrefix == "" {
			// 替换文本切片前缀
			orgPrefix = segment.Title
		} else if orgPrefix != segment.Title {
			// 若过程中存在文档切片有多种前缀则报错
			logx.E(ctx, "task(DocRenameToIndex) getSegments|orgPrefix:%s != segment.Title: %+v",
				orgPrefix, segment.Title)
			return nil, nil, nil, errs.ErrWrapf(errs.ErrDocSegmentPrefixNotMatch,
				"robotID:%v,docID:%+v,segmentID: %v", d.p.RobotID, segment.DocID, segment.ID)
		}

		if segment.OrgData != "" {
			if !strings.HasPrefix(segment.OrgData, orgPrefix) {
				logx.E(ctx, "task(DocRenameToIndex) getSegments|segment.OrgData:%s, orgPrefix: %+v",
					segment.OrgData, orgPrefix)
				return nil, nil, nil, errs.ErrWrapf(errs.ErrDocSegmentPrefixNotMatch,
					"robotID:%v,docID:%+v,segmentID: %v", d.p.RobotID, segment.DocID, segment.ID)
			}
			segment.OrgData = strings.Replace(segment.OrgData, orgPrefix, d.newPrefix, 1)
		} else {
			// 如果旧表格没有orgData，则从t_doc_segment_org_data新表中获取orgData
			if _, ok := docSegOrgDataMap[segment.OrgDataBizID]; !ok {
				// 不同的segment可能属于同一个org_data
				orgData, err := d.segLogic.GetDocOrgDataByBizID(ctx,
					[]string{segEntity.DocSegmentOrgDataTblColBusinessID,
						segEntity.DocSegmentOrgDataTblColAppBizID,
						segEntity.DocSegmentOrgDataTblColOrgData},
					corpRsp.GetCorpId(), appDB.BizId, doc.BusinessID, segment.OrgDataBizID)
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil, nil, err
				}
				if orgData != nil && orgData.OrgData != "" {
					if !strings.HasPrefix(orgData.OrgData, orgPrefix) {
						logx.E(ctx, "task(DocRenameToIndex) getSegments|orgData.OrgData:%s, orgPrefix: %+v", orgData.OrgData, orgPrefix)
						return nil, nil, nil, errs.ErrWrapf(errs.ErrDocSegmentPrefixNotMatch,
							"robotID:%v,docID:%+v,segmentID: %v", d.p.RobotID, segment.DocID, segment.ID)
					}
					orgData.OrgData = strings.Replace(orgData.OrgData, orgPrefix, d.newPrefix, 1)
					docSegOrgDataMap[orgData.BusinessID] = orgData
					logx.D(ctx, "task(DocRenameToIndex) Process GetDocOrgDataByBizID|orgData.OrgData:%s", orgData.OrgData)
				}
			}
		}

		// 重新继承文档有效期
		segment.ExpireStart = d.p.ExpireStart
		segment.ExpireEnd = d.p.ExpireEnd
		segment.Title = d.newPrefix
		switch segment.SegmentType {
		case segEntity.SegmentTypeSegment:
			if !strings.HasPrefix(segment.PageContent, orgPrefix) {
				logx.E(ctx, "task(DocRenameToIndex) getSegments|segment.PageContent:%s, orgPrefix: %+v",
					segment.PageContent, segment)
				return nil, nil, nil, errs.ErrDocSegmentPrefixNotMatch
			}
			segment.PageContent = strings.Replace(segment.PageContent, orgPrefix, d.newPrefix, 1)
		case segEntity.SegmentTypeText2SQLMeta:
			meta := segEntity.Text2SQLSegmentMeta{}
			if err := jsonx.UnmarshalFromString(segment.PageContent, &meta); err != nil {
				return nil, nil, nil, err
			}
			if meta.FileName != orgPrefix {
				logx.E(ctx, "task(DocRenameToIndex) getSegments|smeta.FileName:%s != orgPrefix: %+v",
					meta.FileName, orgPrefix)
				return nil, nil, nil, errs.ErrWrapf(errs.ErrDocSegmentPrefixNotMatch,
					"robotID:%v,docID:%+v,segmentID: %v", d.p.RobotID, segment.DocID, segment.ID)
			}
			meta.FileName = d.newPrefix
			metaStr, err := jsonx.MarshalToString(meta)
			if err != nil {
				return nil, nil, nil, errs.ErrWrapf(err, "robotID:%v,docID:%+v,segmentID: %v",
					d.p.RobotID, segment.DocID, segment.ID)
			}
			segment.PageContent = metaStr
		}
		if segment.BigDataID != "" {
			bigDataIDs = append(bigDataIDs, segment.BigDataID)
		}

		docSegments, ok := docSegmentMap[segment.SegmentType]
		if !ok {
			docSegments = make([]*segEntity.DocSegmentExtend, 0)
		}
		docSegments = append(docSegments, segment)
		docSegmentMap[segment.SegmentType] = docSegments
	}
	req := retrieval.BatchGetBigDataESByRobotBigDataIDReq{
		RobotId:    robotID,
		BigDataIds: bigDataIDs,
		Type:       retrieval.KnowledgeType_KNOWLEDGE,
	}
	rsp, err := d.rpc.RetrievalDirectIndex.BatchGetBigDataESByRobotBigDataID(ctx, &req)
	if err != nil {
		return nil, nil, nil, err
	}
	bigDataList := rsp.GetData()
	if orgPrefix != "" {
		for i, bigData := range bigDataList {
			bigDataList[i].BigString = strings.Replace(bigData.BigString, orgPrefix, d.newPrefix, 1)
		}
	}

	return docSegmentMap, maps.Values(docSegOrgDataMap), bigDataList, nil
}

// Fail 任务失败
func (d *DocRenameToIndexTaskHandler) Fail(ctx context.Context) error {
	doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc.HasDeleted() {
		return nil
	}
	doc.Status = docEntity.DocStatusCreateIndexFail
	doc.IsCreatingIndex = false
	doc.RemoveProcessingFlag([]uint64{docEntity.DocProcessingFlagCreatingIndex})
	if err = d.docLogic.UpdateCreatingIndexFlag(ctx, doc); err != nil {
		return err
	}
	err = d.failNotice(ctx, doc)
	if err != nil {
		return err
	}
	return nil
}

func (d *DocRenameToIndexTaskHandler) failNotice(ctx context.Context, doc *docEntity.Doc) error {
	logx.D(ctx, "failNotice task: %+v, doc: %+v", d.task, doc)
	operations := []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}}}
	noticeOptions := []releaseEntity.NoticeOption{
		releaseEntity.WithGlobalFlag(),
		releaseEntity.WithPageID(releaseEntity.NoticeDocPageID),
		releaseEntity.WithLevel(releaseEntity.LevelError),
		releaseEntity.WithSubject(i18n.Translate(ctx, i18nkey.KeyDocumentRenameLearningFailure)),
		releaseEntity.WithContent(i18n.Translate(ctx, i18nkey.KeyDocumentRenameLearningFailureWithName, doc.FileNameInAudit)),
	}
	notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocRename, doc.ID, doc.CorpID, doc.RobotID, doc.StaffID, noticeOptions...)
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
func (d *DocRenameToIndexTaskHandler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocRenameToIndexTaskHandler) Done(ctx context.Context) error {
	logx.D(ctx, "task(DocRenameToIndex) Done")
	doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc.HasDeleted() {
		return nil
	}
	// 默认知识库重命名学习完成不能立即修改文档名称, 需要在发布完成时修改
	if d.app != nil && d.app.IsShared {
		// 共享知识库重命名学习完成时修改文档名称
		doc.FileName = doc.FileNameInAudit
		doc.FileNameInAudit = ""
	}
	doc.IsCreatingIndex = false
	doc.RemoveProcessingFlag([]uint64{docEntity.DocProcessingFlagCreatingIndex})
	doc.Status = docEntity.DocStatusWaitRelease
	doc.NextAction = qaEntity.NextActionUpdate

	if err := d.docLogic.UpdateDocNameAndStatus(ctx, doc); err != nil {
		return err
	}

	return nil
}
