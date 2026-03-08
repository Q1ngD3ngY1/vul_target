package task

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	logicKnowConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"gorm.io/gorm"
	"strings"

	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"
)

const (
	renameSegGenIndexPrefix = "rename:seg:gen:index:"
)

// DocRenameToIndexScheduler 文档重命名入向量库任务
type DocRenameToIndexScheduler struct {
	dao                 dao.Dao
	task                task_scheduler.Task
	p                   model.DocRenameToIndexParams
	VectorLabels        []*retrieval.VectorLabel
	Text2SQLSegmentMeta model.Text2SQLSegmentMeta
	appInfo             *admin.GetAppInfoRsp

	newPrefix string
}

func initDocRenameToIndexScheduler() {
	task_scheduler.Register(
		model.DocRenameToIndexTask,
		func(t task_scheduler.Task, params model.DocRenameToIndexParams) task_scheduler.TaskHandler {
			return &DocRenameToIndexScheduler{
				dao:          dao.New(),
				task:         t,
				p:            params,
				VectorLabels: make([]*retrieval.VectorLabel, 0),
			}
		},
	)
}

// Prepare 数据准备
func (d *DocRenameToIndexScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(DocRenameToIndex) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	appBizId, err := dao.GetAppBizIDByAppID(ctx, d.p.RobotID)
	if err != nil {
		return kv, err
	}
	appInfo, err := client.GetAppInfo(ctx, appBizId, model.AppTestScenes)
	if err != nil {
		return kv, err
	}
	d.appInfo = appInfo
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
	//doc.BatchID += 1 修复文档重命名后成功后，无法生成问答的问题，code:450042, msg:分片内容为空
	doc.Status = model.DocStatusCreatingIndex
	doc.IsCreatingIndex = true
	doc.AddProcessingFlag([]uint64{model.DocProcessingFlagCreatingIndex})
	// TODO: 此处按理说处于审核中的状态,是否需要改为学习中待定
	if err = d.dao.UpdateCreatingIndexFlag(ctx, doc); err != nil {
		return kv, err
	}

	batchSize := utilConfig.GetMainConfig().OfflineConfig.SyncVectorAddBatchSize
	if batchSize <= 0 {
		batchSize = defaultSyncAddVectorBatchSize
	}
	page := 1
	pageSize := batchSize
	count := batchSize
	for count != 0 {
		ids, err := d.dao.GetPagedSegmentIDsByDocID(ctx, doc.ID, uint32(page), uint32(pageSize), d.p.RobotID)
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
		idChunksStr, err := jsoniter.MarshalToString(idsStr)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocRenameToIndex) Prepare|jsoniter.MarshalToString err:%+v", err)
			kv = make(task_scheduler.TaskKV) // 重置kv
			return kv, err
		}
		log.DebugContextf(ctx, "task(DocRenameToIndex) Prepare index:%d, seg.IDs: %+v", page, idChunksStr)
		kv[fmt.Sprintf("%s%d", renameSegGenIndexPrefix, page)] = fmt.Sprintf("%s", idChunksStr)
	}
	return kv, nil
}

// Init 初始化
func (d *DocRenameToIndexScheduler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(DocRenameToIndex) Init, task: %+v, params: %+v, kvs: %d", d.task, d.p, len(kv))
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
	d.newPrefix = util.FileNameNoSuffix(doc.FileNameInAudit) + ": \n"
	d.Text2SQLSegmentMeta.FileName = d.newPrefix

	log.DebugContextf(ctx, "task(DocRenameToIndex) Init success, VectorLabels: %+v, Text2SQLSegmentMeta: %+v",
		d.VectorLabels, d.Text2SQLSegmentMeta)
	return nil
}

// Process 任务处理
func (d *DocRenameToIndexScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(DocRenameToIndex) Process, task: %+v, params: %+v", d.task, d.p)
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
		log.DebugContextf(ctx, "task(DocRenameToIndex) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			log.DebugContextf(ctx, "task(DocRenameToIndex) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(DocRenameToIndex) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		ctx = pkg.WithSpaceID(ctx, appDB.SpaceID)
		if strings.HasPrefix(key, renameSegGenIndexPrefix) {
			value := make([]string, 0)
			err := jsoniter.Unmarshal([]byte(v), &value)
			if err != nil {
				log.ErrorContextf(ctx, "task(DocRenameToIndex) jsoniter.Unmarshal  err:%+v", err)
				return err
			}
			ids := make([]uint64, 0)
			for _, idStr := range value {
				ids = append(ids, cast.ToUint64(idStr))
			}

			docSegmentMap, bigDataList, err := d.getSegments(ctx, ids)
			if err != nil {
				log.ErrorContextf(ctx, "task(DocRenameToIndex) getSegments err:%+v", err)
				return err
			}

			embeddingConf, _, err := appDB.GetEmbeddingConf()
			if err != nil {
				log.ErrorContextf(ctx, "task(DocRenameToIndex) GetEmbeddingConf() err:%+v", err)
				return err
			}
			embeddingVersion := embeddingConf.Version

			for segmentType, segments := range docSegmentMap {
				// 更新文档切片内容
				if err = d.dao.BatchUpdateSegmentContent(ctx, segments, d.p.RobotID); err != nil {
					return err
				}
				switch segmentType {
				case model.SegmentTypeText2SQLMeta:
				case model.SegmentTypeText2SQLContent:
				default:
					if err = d.dao.BatchDirectAddSegmentKnowledge(ctx, appDB.ID,
						segments, embeddingVersion, d.VectorLabels, embeddingModelName); err != nil {
						log.ErrorContextf(ctx, "task(DocRenameToIndex) BatchDirectAddSegmentKnowledge err:%+v", err)
						return err
					}
				}
			}
			// 更新big_data
			if err := d.dao.AddBigDataElastic(ctx, bigDataList, retrieval.KnowledgeType_KNOWLEDGE); err != nil {
				log.ErrorContextf(ctx, "task(DocRenameToIndex) AddBigDataElastic err:%+v", err)
				return err
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			log.ErrorContextf(ctx, "task(DocRenameToIndex) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(DocRenameToIndex) Finish kv:%s", key)
	}
	return nil
}

// getSegments 获取切片
func (d *DocRenameToIndexScheduler) getSegments(ctx context.Context, segmentIDs []uint64) (
	docSegmentMap map[string][]*model.DocSegmentExtend, bigDataList []*retrieval.BigData, err error) {
	log.InfoContextf(ctx, "task(DocRenameToIndex) getSegments|segmentIDs: %+v", segmentIDs)
	segments, err := d.dao.GetSegmentByIDs(ctx, segmentIDs, d.p.RobotID)
	if err != nil {
		return nil, nil, err
	}
	if len(segments) == 0 {
		return nil, nil, errs.ErrSegmentNotFound
	}
	appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
	if err != nil {
		return nil, nil, err
	}
	corpReq := &admin.GetCorpReq{
		Id: d.p.CorpID,
	}
	corpRsp, err := d.dao.GetAdminApiCli().GetCorp(ctx, corpReq)
	if err != nil || corpRsp == nil {
		return nil, nil, err
	}
	robotID, docID := uint64(0), uint64(0)

	orgPrefix := ""

	docSegmentMap = make(map[string][]*model.DocSegmentExtend)
	bigDataIDs := make([]string, 0, len(segments))

	for _, segment := range segments {
		doc, err := d.dao.GetDocByID(ctx, segment.DocID, segment.RobotID)
		if err != nil {
			return nil, nil, err
		}
		if doc == nil {
			return nil, nil, errs.ErrDocNotFound
		}
		if segment.OrgData == "" { // 如果旧表格没有orgData，则从t_doc_segment_org_data新表中获取orgData
			orgData, err := dao.GetDocSegmentOrgDataDao().GetDocOrgDataByBizID(ctx,
				[]string{dao.DocSegmentOrgDataTblColOrgData}, corpRsp.GetCorpBizId(), appDB.BusinessID,
				doc.BusinessID, segment.OrgDataBizID)
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, nil, err
			}

			if orgData != nil && orgData.OrgData != "" {
				segment.OrgData = orgData.OrgData
			}
			log.DebugContextf(ctx, "task(DocRenameToIndex) Process GetDocOrgDataByBizID|segment.OrgData:%s",
				segment.OrgData)
		}
		if robotID == 0 {
			robotID = segment.RobotID
		}
		if docID == 0 {
			docID = segment.DocID
		}
		if robotID != segment.RobotID || robotID != d.p.RobotID || docID != segment.DocID || docID != d.p.DocID {
			log.ErrorContextf(ctx, "task(DocRenameToIndex) getSegments|seg illegal|segment: %+v", segment)
			return nil, nil, errs.ErrSegmentNotFound
		}

		// 重新继承文档有效期
		segment.ExpireStart = d.p.ExpireStart
		segment.ExpireEnd = d.p.ExpireEnd
		if orgPrefix == "" {
			// 替换文本切片前缀
			orgPrefix = segment.Title
		} else if orgPrefix != segment.Title {
			// 若过程中存在文档切片有多种前缀则报错
			log.ErrorContextf(ctx, "task(DocRenameToIndex) getSegments|orgPrefix:%s != segment.Title: %+v",
				orgPrefix, segment.Title)
			return nil, nil, errs.ErrWrapf(errs.ErrDocSegmentPrefixNotMatch,
				"robotID:%v,docID:%+v,segmentID: %v", d.p.RobotID, segment.DocID, segment.ID)
		}

		segment.Title = d.newPrefix
		switch segment.SegmentType {
		case model.SegmentTypeSegment:
			if !strings.HasPrefix(segment.PageContent, orgPrefix) {
				log.ErrorContextf(ctx, "task(DocRenameToIndex) getSegments|segment.PageContent:%s, orgPrefix: %+v",
					segment.PageContent, segment)
				return nil, nil, errs.ErrDocSegmentPrefixNotMatch
			}
			segment.PageContent = strings.Replace(segment.PageContent, orgPrefix, d.newPrefix, 1)
			if !strings.HasPrefix(segment.OrgData, orgPrefix) {
				log.ErrorContextf(ctx, "task(DocRenameToIndex) getSegments|segment.OrgData:%s, orgPrefix: %+v",
					segment.OrgData, orgPrefix)
				return nil, nil, errs.ErrWrapf(errs.ErrDocSegmentPrefixNotMatch,
					"robotID:%v,docID:%+v,segmentID: %v", d.p.RobotID, segment.DocID, segment.ID)
			}
			segment.OrgData = strings.Replace(segment.OrgData, orgPrefix, d.newPrefix, 1)
		case model.SegmentTypeText2SQLMeta:
			meta := model.Text2SQLSegmentMeta{}
			if err := jsoniter.UnmarshalFromString(segment.PageContent, &meta); err != nil {
				return nil, nil, err
			}
			if meta.FileName != orgPrefix {
				log.ErrorContextf(ctx, "task(DocRenameToIndex) getSegments|smeta.FileName:%s != orgPrefix: %+v",
					meta.FileName, orgPrefix)
				return nil, nil, errs.ErrWrapf(errs.ErrDocSegmentPrefixNotMatch,
					"robotID:%v,docID:%+v,segmentID: %v", d.p.RobotID, segment.DocID, segment.ID)
			}
			meta.FileName = d.newPrefix
			metaStr, err := jsoniter.MarshalToString(meta)
			if err != nil {
				return nil, nil, errs.ErrWrapf(err, "robotID:%v,docID:%+v,segmentID: %v",
					d.p.RobotID, segment.DocID, segment.ID)
			}
			segment.PageContent = metaStr
		}
		if segment.BigDataID != "" {
			bigDataIDs = append(bigDataIDs, segment.BigDataID)
		}

		docSegments, ok := docSegmentMap[segment.SegmentType]
		if !ok {
			docSegments = make([]*model.DocSegmentExtend, 0)
		}
		docSegments = append(docSegments, segment)
		docSegmentMap[segment.SegmentType] = docSegments
	}
	bigDataList, err = d.dao.BatchGetBigDataESByRobotBigDataID(ctx, robotID, bigDataIDs, retrieval.KnowledgeType_KNOWLEDGE)
	if err != nil {
		return nil, nil, err
	}
	if orgPrefix != "" {
		for i, bigData := range bigDataList {
			bigDataList[i].BigString = strings.Replace(bigData.BigString, orgPrefix, d.newPrefix, 1)
		}
	}

	return docSegmentMap, bigDataList, nil
}

// Fail 任务失败
func (d *DocRenameToIndexScheduler) Fail(ctx context.Context) error {
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc.HasDeleted() {
		return nil
	}
	doc.Status = model.DocStatusCreateIndexFail
	doc.IsCreatingIndex = false
	doc.RemoveProcessingFlag([]uint64{model.DocProcessingFlagCreatingIndex})
	if err = d.dao.UpdateCreatingIndexFlag(ctx, doc); err != nil {
		return err
	}
	err = d.failNotice(ctx, doc)
	if err != nil {
		return err
	}
	return nil
}

func (d *DocRenameToIndexScheduler) failNotice(ctx context.Context, doc *model.Doc) error {
	log.DebugContextf(ctx, "failNotice task: %+v, doc: %+v", d.task, doc)
	operations := []model.Operation{{Typ: model.OpTypeViewDetail, Params: model.OpParams{}}}
	noticeOptions := []model.NoticeOption{
		model.WithGlobalFlag(),
		model.WithPageID(model.NoticeDocPageID),
		model.WithLevel(model.LevelError),
		model.WithSubject(i18n.Translate(ctx, i18nkey.KeyDocumentRenameLearningFailure)),
		model.WithContent(i18n.Translate(ctx, i18nkey.KeyDocumentRenameLearningFailureWithName, doc.FileNameInAudit)),
	}
	notice := model.NewNotice(model.NoticeTypeDocRename, doc.ID, doc.CorpID, doc.RobotID, doc.StaffID, noticeOptions...)
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
func (d *DocRenameToIndexScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocRenameToIndexScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(DocRenameToIndex) Done")
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc.HasDeleted() {
		return nil
	}
	// 默认知识库重命名学习完成不能立即修改文档名称, 需要在发布完成时修改
	if d.appInfo != nil && d.appInfo.GetIsShareKnowledgeBase() {
		// 共享知识库重命名学习完成时修改文档名称
		doc.FileName = doc.FileNameInAudit
		doc.FileNameInAudit = ""
	}
	doc.IsCreatingIndex = false
	doc.RemoveProcessingFlag([]uint64{model.DocProcessingFlagCreatingIndex})
	doc.Status = model.DocStatusWaitRelease
	doc.NextAction = model.NextActionUpdate

	if err := d.dao.UpdateDocNameAndStatus(ctx, doc); err != nil {
		return err
	}

	return nil
}
