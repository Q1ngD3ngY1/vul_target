package async

import (
	"context"
	"fmt"
	"math"
	"strings"

	"git.woa.com/adp/kb/kb-config/internal/util"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	dbdao "git.woa.com/adp/kb/kb-config/internal/dao/database"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	qaLogic "git.woa.com/adp/kb/kb-config/internal/logic/qa"
	segLogic "git.woa.com/adp/kb/kb-config/internal/logic/segment"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

const (
	segDeletePrefix         = "seg:delete:"
	segGenQAPrefix          = "seg:gen:qa:"
	segGenIndexPrefix       = "seg:gen:index:"
	qaDeletePrefix          = "qa:delete:"
	orgDataDeletePrefix     = "orgData:delete:"
	tempOrgDataDeletePrefix = "tempOrgData:delete:"
	sheetDeletePrefix       = "sheet:delete:"

	defaultSyncAddVectorBatchSize      = 20    // 默认插入Vector分批同步数量
	defaultSyncDeletedVectorBatchSize  = 100   // 默认删除Vector分批同步数量
	defaultSyncDeletedOrgDataBatchSize = 10000 // 默认删除OrgData分批数量
)

// DocDeleteTaskHandler 文档删除任务
type DocDeleteTaskHandler struct {
	*taskCommon

	task  task_scheduler.Task
	p     entity.DocDeleteParams
	dbDao dbdao.Dao
}

func registerDocDeleteTaskHandler(tc *taskCommon, dbDao dbdao.Dao) {
	task_scheduler.Register(
		entity.DocDeleteTask,
		func(t task_scheduler.Task, params entity.DocDeleteParams) task_scheduler.TaskHandler {
			return &DocDeleteTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
				dbDao:      dbDao,
			}
		},
	)
}

// Prepare 数据准备
func (d *DocDeleteTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(DocDelete) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return kv, err
	}
	if doc == nil {
		return kv, errs.ErrDocNotFound
	}
	qas, err := getDocNotDeleteQA(ctx, doc, d.qaLogic)
	if err != nil {
		return kv, err
	}
	segs, err := getDocNotDeleteSegment(ctx, doc, d.segLogic)
	if err != nil {
		return kv, err
	}

	// 问答切片
	for _, qa := range qas {
		kv[fmt.Sprintf("%s%d", qaDeletePrefix, qa.ID)] = fmt.Sprintf("%d", qa.ID)
	}

	// 文档切片
	batchSize := config.GetMainConfig().OfflineConfig.SyncVectorDeletedBatchSize
	if batchSize <= 0 {
		batchSize = defaultSyncDeletedVectorBatchSize
	}
	for index, segChunks := range slicex.Chunk(segs, batchSize) {
		var idsStr []string
		for _, seg := range segChunks {
			idsStr = append(idsStr, cast.ToString(seg.ID))
		}
		idChunksStr, err := jsonx.MarshalToString(idsStr)
		if err != nil {
			logx.E(ctx, "task(DocDelete) Prepare|jsonx.MarshalToString err:%+v", err)
			kv = make(task_scheduler.TaskKV) // 重置kv
			return kv, err
		}
		logx.D(ctx, "task(DocDelete) Prepare index:%d, seg.IDs: %+v", index, idChunksStr)
		kv[fmt.Sprintf("%s%d", segDeletePrefix, index)] = fmt.Sprintf("%s", idChunksStr)
	}
	// 直接在后面逻辑中分批删除
	// OrgData删除
	kv[fmt.Sprintf("%s%d", orgDataDeletePrefix, doc.ID)] = fmt.Sprintf("%d", doc.ID)
	// 干预OrgData删除
	kv[fmt.Sprintf("%s%d", tempOrgDataDeletePrefix, doc.ID)] = fmt.Sprintf("%d", doc.ID)
	// 干预sheet删除
	kv[fmt.Sprintf("%s%d", sheetDeletePrefix, doc.ID)] = fmt.Sprintf("%d", doc.ID)
	return kv, nil
}

// Init 初始化
func (d *DocDeleteTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *DocDeleteTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(DocDelete) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(DocDelete) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			logx.D(ctx, "task(DocDelete) appDB.HasDeleted()|appID:%d", d.p.RobotID)
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(DocDelete) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		newCtx := util.SetMultipleMetaData(ctx, appDB.SpaceId, appDB.Uin)
		if strings.HasPrefix(key, qaDeletePrefix) {
			id := cast.ToUint64(v)
			qa, err := d.qaLogic.GetQAByID(ctx, id)
			if err != nil {
				return err
			}
			if err = d.qaLogic.DeleteDocToQA(newCtx, qa); err != nil {
				return err
			}
		}
		if strings.HasPrefix(key, segDeletePrefix) {
			value := make([]string, 0)
			err := jsonx.Unmarshal([]byte(v), &value)
			if err != nil {
				logx.E(ctx, "task(DocDelete) jsonx.Unmarshal  err:%+v", err)
				return err
			}
			ids := make([]uint64, 0)
			for _, idStr := range value {
				ids = append(ids, cast.ToUint64(idStr))
			}
			docSegments, err := d.getSegments(ctx, ids)
			if err != nil {
				logx.E(ctx, "task(DocDelete) getSegments err:%+v", err)
				return err
			}
			if err = d.segLogic.BatchDeleteSegments(newCtx, docSegments, d.p.RobotID); err != nil {
				return err
			}
			req := retrieval.DeleteBigDataElasticReq{
				RobotId:    d.p.RobotID,
				DocId:      d.p.DocID,
				Type:       retrieval.KnowledgeType_KNOWLEDGE,
				HardDelete: true,
			}
			if err = d.rpc.RetrievalDirectIndex.DeleteBigDataElastic(ctx, &req); err != nil {
				return err
			}
			// 删除知识库向量
			// 仅删除非问答、非text2sql的切片
			// 问答切片在qaDeletePrefix中删除
			// text2sql切片在Done中删除

			deleteKnowledgeSegments := make([]*segEntity.DocSegmentExtend, 0)
			for _, seg := range docSegments {
				if !seg.IsSegmentForQA() && !seg.IsText2sqlSegmentType() {
					deleteKnowledgeSegments = append(deleteKnowledgeSegments, seg)
				}
			}
			if len(deleteKnowledgeSegments) > 0 {
				embeddingVersion := appDB.Embedding.Version
				embeddingModel, err :=
					d.kbLogic.GetKnowledgeEmbeddingModel(ctx, appDB.CorpBizId, appDB.BizId, appDB.BizId, appDB.IsShared)

				if err != nil {
					logx.E(ctx, "task(DocDelete) GetKnowledgeEmbeddingModel err:%+v", err)
					return err
				}

				logx.I(ctx, "task(DocDelete) kb "+
					" embeddingModelName:%s, app embeddingVersion:%d", embeddingModel, embeddingVersion)
				if err = d.segLogic.GetVectorSyncLogic().BatchDirectDeleteSegmentKnowledge(newCtx, appDB.PrimaryId,
					deleteKnowledgeSegments, embeddingVersion, embeddingModel); err != nil {
					return err
				}
			}
			// feature_permission
			// 删除文档需要删除角色文档绑定关系,每次删除一万条
			corp, err := d.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, d.p.CorpID)
			// corp, err := d.dao.GetCorpByID(ctx, d.p.CorpPrimaryId)
			if err != nil {
				logx.E(ctx, "doc_delete getCorp err:%+v,doc_id:%v,corp_id:%v", err, d.p.DocID, d.p.CorpID)
				return err
			}
			doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
			if err != nil {
				logx.E(ctx, "doc_delete getDoc err:%+v,doc_id:%v,corp_id:%v", err, d.p.DocID, d.p.CorpID)
				return err
			}
			logx.D(ctx, "feature_permission deleteRoleDoc corpBizId:%v,appBizId:%v,docBizId:%v",
				corp.GetCorpId(), appDB.BizId, doc.BusinessID)
			err = d.userLogic.DeleteKnowledgeRoleDocList(ctx, corp.GetCorpId(), appDB.BizId,
				&entity.KnowledgeRoleDocFilter{
					DocBizIDs: []uint64{doc.BusinessID},
					BatchSize: 10000,
				})
			if err != nil { // 柔性放过
				logx.E(ctx, "feature_permission deleteRoleDoc err:%v,corp_biz_id:%v,app_biz_id:%v,doc_biz_id:%v",
					err, corp.GetCorpId(), appDB.BizId, doc.BusinessID)
			}
		}

		doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
		if err != nil {
			return err
		}
		if doc == nil {
			return errs.ErrDocNotFound
		}

		corpBizID, appBizID, _, _, err := d.segLogic.SegmentCommonIDsToBizIDs(ctx, d.p.CorpID,
			d.p.RobotID, 0, 0)
		// OrgData删除
		if strings.HasPrefix(key, orgDataDeletePrefix) {
			err := d.segLogic.BatchDeleteDocOrgDataByDocBizID(ctx, corpBizID, appBizID,
				doc.BusinessID, defaultSyncDeletedOrgDataBatchSize)
			if err != nil {
				logx.E(ctx, "BatchDeleteDocOrgDataByDocBizID err:%v,corpBizID:%v,appBizID:%v,docBizID:%v",
					err, corpBizID, appBizID, doc.BusinessID)
			}
		}
		// 干预OrgData删除
		if strings.HasPrefix(key, tempOrgDataDeletePrefix) {
			if !docEntity.IsTableTypeDocument(doc.FileType) {
				err := d.segLogic.BatchDeleteTemporaryDocOrgDataByDocBizID(ctx, corpBizID, appBizID,
					doc.BusinessID, defaultSyncDeletedOrgDataBatchSize)
				if err != nil {
					logx.E(ctx, "BatchDeleteDocOrgDataByDocBizID Temporary err:%v,corpBizID:%v,appBizID:%v,docBizID:%v",
						err, corpBizID, appBizID, doc.BusinessID)
				}
			}
		}
		// 干预sheet删除
		if strings.HasPrefix(key, sheetDeletePrefix) {
			if docEntity.IsTableTypeDocument(doc.FileType) {
				err := d.segLogic.BatchDeleteDocSegmentSheetByDocBizID(ctx, corpBizID, appBizID,
					doc.BusinessID, defaultSyncDeletedOrgDataBatchSize)
				if err != nil {
					logx.E(ctx, "BatchDeleteDocSegmentSheetByDocBizID err:%v,corpBizID:%v,appBizID:%v,docBizID:%v",
						err, corpBizID, appBizID, doc.BusinessID)
				}
				err = d.segLogic.DeleteSheetDbTableAndColumns(ctx, d.dbDao, corpBizID, appBizID, doc.BusinessID, d.p.RobotID)
				if err != nil {
					logx.E(ctx, "deleteSheetDbTableAndColumns %v, %v", doc.BusinessID, err)
					return err
				}
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			logx.E(ctx, "task(DocDelete) Finish kv:%s err:%+v", key, err)
			return err
		}
		logx.D(ctx, "task(DocDelete) Finish kv:%s", key)
	}
	return nil
}

// getSegments 获取切片
func (d *DocDeleteTaskHandler) getSegments(ctx context.Context, segmentIDs []uint64) (
	docSegments []*segEntity.DocSegmentExtend, err error) {
	logx.I(ctx, "task(DocDelete) getSegments|segmentIDs: %+v", segmentIDs)
	segments, err := d.segLogic.GetSegmentByIDs(ctx, segmentIDs, d.p.RobotID)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, errs.ErrSegmentNotFound
	}

	robotID, docID := uint64(0), uint64(0)

	docSegments = make([]*segEntity.DocSegmentExtend, 0)

	for _, segment := range segments {
		if robotID == 0 {
			robotID = segment.RobotID
		}
		if docID == 0 {
			docID = segment.DocID
		}
		if robotID != segment.RobotID || robotID != d.p.RobotID || docID != segment.DocID || docID != d.p.DocID {
			logx.E(ctx, "task(DocDelete) getSegments|seg illegal|segment: %+v", segment)
			return nil, errs.ErrSegmentNotFound
		}

		docSegments = append(docSegments, segment)
	}

	if len(docSegments) == 0 {
		logx.D(ctx, "task(DocDelete) getSegments|len(docSegments):%d", len(docSegments))
		return nil, nil
	}
	return docSegments, nil
}

// Fail 任务失败
func (d *DocDeleteTaskHandler) Fail(_ context.Context) error {
	return nil
}

// Stop 任务停止
func (d *DocDeleteTaskHandler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocDeleteTaskHandler) Done(ctx context.Context) error {
	logx.D(ctx, "task(DocDelete) Done")
	doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	req := retrieval.DeleteText2SQLReq{
		RobotId:     doc.RobotID,
		DocId:       doc.ID,
		SegmentType: segEntity.SegmentTypeText2SQLContent,
	}
	if err = d.rpc.RetrievalDirectIndex.DeleteText2SQL(ctx, &req); err != nil {
		return err
	}
	if err = d.docLogic.DeleteDocSuccess(ctx, doc); err != nil {
		return err
	}
	return nil
}

// getDocNotDeleteQA 文档未删除的QA
func getDocNotDeleteQA(ctx context.Context, doc *docEntity.Doc, qaLogic *qaLogic.Logic) ([]*qaEntity.DocQA,
	error) {
	logx.D(ctx, "getDocNotDeleteQA|doc: %+v", doc)
	pageSize := uint32(1000)
	page := uint32(1)
	qas := make([]*qaEntity.DocQA, 0)
	for {
		req := &qaEntity.QAListReq{
			CorpID:    doc.CorpID,
			RobotID:   doc.RobotID,
			DocID:     []uint64{doc.ID},
			IsDeleted: qaEntity.QAIsNotDeleted,
			Page:      page,
			PageSize:  pageSize,
		}
		list, err := qaLogic.GetQAList(ctx, req)
		if err != nil {
			return nil, err
		}
		if len(list) == 0 {
			break
		}
		qas = append(qas, list...)
		page++
	}
	return qas, nil
}

// getDocNotDeleteSegment 文档未删除的segment
func getDocNotDeleteSegment(ctx context.Context, doc *docEntity.Doc, segLogic *segLogic.Logic) ([]*segEntity.DocSegmentExtend, error) {
	logx.I(ctx, "getDocNotDeleteSegment|doc: %+v", doc)
	if doc == nil {
		logx.E(ctx, "getDocNotDeleteSegment|doc is null")
		return nil, errs.ErrDocNotFound
	}

	total, err := segLogic.GetSegmentListCount(ctx, doc.CorpID, doc.ID, doc.RobotID)
	if err != nil {
		return nil, err
	}
	logx.I(ctx, "getDocNotDeleteSegment|total: %d", total)
	segments := make([]*segEntity.DocSegmentExtend, 0)
	segmentChan := make(chan *segEntity.DocSegmentExtend, 5000)
	finish := make(chan any)
	go func() {
		defer gox.Recover()
		for segment := range segmentChan {
			segments = append(segments, segment)
		}
		finish <- nil
	}()
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 1; i <= pages; i++ {
		page := uint32(i)
		list, err := segLogic.GetSegmentList(ctx, doc.CorpID, doc.ID, page, uint32(pageSize), doc.RobotID)
		if err != nil {
			return nil, err
		}
		for _, seg := range list {
			segmentChan <- seg
		}
	}
	close(segmentChan)
	<-finish
	return segments, nil
}

// getDocNotDeleteOrgData 文档未删除的OrgData
func (d *DocDeleteTaskHandler) getDocNotDeleteOrgData(ctx context.Context, doc *docEntity.Doc, dDao dao.Dao) ([]*segEntity.DocSegmentOrgData, error) {
	if doc == nil {
		logx.E(ctx, "getDocNotDeleteOrgData|doc is null")
		return nil, errs.ErrDocNotFound
	}
	corpBizID, appBizID, _, _, err := d.segLogic.SegmentCommonIDsToBizIDs(ctx, doc.CorpID,
		doc.RobotID, 0, 0)
	if err != nil {
		logx.E(ctx, "getDocNotDeleteOrgData|SegmentCommonIDsToBizIDs|err:%v", err)
		return nil, err
	}
	deleteFlag := ptrx.Bool(false)
	filter := &segEntity.DocSegmentOrgDataFilter{
		CorpBizID:          corpBizID,
		AppBizID:           appBizID,
		DocBizID:           doc.BusinessID,
		IsDeleted:          deleteFlag,
		IsTemporaryDeleted: deleteFlag,
		RouterAppBizID:     appBizID,
	}
	total, err := d.segLogic.GetDocOrgDatumCount(ctx, filter)
	if err != nil {
		logx.E(ctx, "getDocNotDeleteOrgData|GetDocOrgDataCount|err:%v", err)
		return nil, err
	}
	// 当total为0时，提前返回空列表
	if total == 0 {
		return []*segEntity.DocSegmentOrgData{}, nil
	}
	orgDataList := make([]*segEntity.DocSegmentOrgData, 0)
	orgDataChan := make(chan *segEntity.DocSegmentOrgData, 5000)
	finish := make(chan any)
	go func() {
		defer gox.Recover()
		for orgData := range orgDataChan {
			orgDataList = append(orgDataList, orgData)
		}
		finish <- nil
	}()
	pageSize := uint32(500)
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	defer close(orgDataChan)
	for i := 1; i <= pages; i++ {
		page := uint32(i)
		offset, limit := utilx.Page(page, pageSize)
		filter := &segEntity.DocSegmentOrgDataFilter{
			CorpBizID:      corpBizID,
			AppBizID:       appBizID,
			DocBizID:       doc.BusinessID,
			Offset:         offset,
			Limit:          limit,
			RouterAppBizID: appBizID,
		}
		list, err := d.segLogic.GetDocOrgDataList(ctx,
			segEntity.DocSegmentOrgDataTblColList, filter)
		if err != nil {
			return nil, err
		}
		for _, orgData := range list {
			orgDataChan <- orgData
		}
	}
	<-finish
	return orgDataList, nil
}

// getDocNotDeleteTemporaryOrgData 文档未删除的干预中的OrgData
func (d *DocDeleteTaskHandler) getDocNotDeleteTemporaryOrgData(ctx context.Context, doc *docEntity.Doc, dao dao.Dao) (
	[]*segEntity.DocSegmentOrgDataTemporary, error) {
	if doc == nil {
		logx.E(ctx, "getDocNotDeleteTemporaryOrgData|doc is null")
		return nil, errs.ErrDocNotFound
	}
	corpBizID, appBizID, _, _, err := d.segLogic.SegmentCommonIDsToBizIDs(ctx, doc.CorpID,
		doc.RobotID, 0, 0)
	if err != nil {
		logx.E(ctx, "getDocNotDeleteTemporaryOrgData|SegmentCommonIDsToBizIDs|err:%v", err)
		return nil, err
	}
	filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
		CorpBizID: corpBizID,
		AppBizID:  appBizID,
		DocBizID:  doc.BusinessID,
		IsDeleted: ptrx.Bool(false),
	}
	total, err := d.segLogic.GetDocTemporaryOrgDataCount(ctx, filter)
	if err != nil {
		logx.E(ctx, "getDocNotDeleteTemporaryOrgData|GetDocOrgDataCountByDocBizID|err:%v", err)
		return nil, err
	}
	// 当total为0时，提前返回空列表
	if total == 0 {
		return []*segEntity.DocSegmentOrgDataTemporary{}, nil
	}

	orgDataList := make([]*segEntity.DocSegmentOrgDataTemporary, 0)
	orgDataChan := make(chan *segEntity.DocSegmentOrgDataTemporary, 5000)
	finish := make(chan any)
	go func() {
		defer gox.Recover()
		for orgData := range orgDataChan {
			orgDataList = append(orgDataList, orgData)
		}
		finish <- nil
	}()
	pageSize := uint32(500)
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	defer close(orgDataChan)
	for i := 1; i <= pages; i++ {
		page := uint32(i)
		offset, limit := utilx.Page(page, pageSize)
		filter := &segEntity.DocSegmentOrgDataTemporaryFilter{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
			DocBizID:  doc.BusinessID,
			Offset:    offset,
			Limit:     limit,
		}
		list, err := d.segLogic.GetDocTemporaryOrgDataByDocBizID(ctx,
			segEntity.DocSegmentOrgDataTemporaryTblColList, filter)
		if err != nil {
			return nil, err
		}
		for _, orgData := range list {
			orgDataChan <- orgData
		}
	}
	<-finish
	return orgDataList, nil
}

// getDocNotDeleteTemporarySheet 文档未删除的干预中的sheet
func (d *DocDeleteTaskHandler) getDocNotDeleteTemporarySheet(ctx context.Context, doc *docEntity.Doc, dDao dao.Dao) (
	[]*segEntity.DocSegmentSheetTemporary, error) {
	if doc == nil {
		logx.E(ctx, "getDocNotDeleteTemporarySheet|doc is null")
		return nil, errs.ErrDocNotFound
	}
	corpBizID, appBizID, _, _, err := d.segLogic.SegmentCommonIDsToBizIDs(ctx, doc.CorpID,
		doc.RobotID, 0, 0)
	if err != nil {
		logx.E(ctx, "getDocNotDeleteTemporarySheet|SegmentCommonIDsToBizIDs|err:%v", err)
		return nil, err
	}
	filter := &segEntity.DocSegmentSheetTemporaryFilter{
		CorpBizID: corpBizID,
		AppBizID:  appBizID,
		DocBizID:  doc.BusinessID,
		IsDeleted: ptrx.Bool(false),
	}
	total, err := d.segLogic.GetDocSegmentSheetTemporaryCount(ctx, filter)
	if err != nil {
		logx.E(ctx, "getDocNotDeleteTemporarySheet|GetDocOrgDataCountByDocBizID|err:%v", err)
		return nil, err
	}
	// 当total为0时，提前返回空列表
	if total == 0 {
		return []*segEntity.DocSegmentSheetTemporary{}, nil
	}
	sheetList := make([]*segEntity.DocSegmentSheetTemporary, 0)
	sheetChan := make(chan *segEntity.DocSegmentSheetTemporary, 5000)
	finish := make(chan any)
	go func() {
		defer gox.Recover()
		for orgData := range sheetChan {
			sheetList = append(sheetList, orgData)
		}
		finish <- nil
	}()
	pageSize := uint32(500)
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	defer close(sheetChan)
	for i := 1; i <= pages; i++ {
		page := uint32(i)
		offset, limit := utilx.Page(page, pageSize)
		filter := &segEntity.DocSegmentSheetTemporaryFilter{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
			DocBizID:  doc.BusinessID,
			Offset:    offset,
			Limit:     limit,
		}
		list, err := d.segLogic.GetSegmentSheetTemporaryList(ctx,
			segEntity.DocSegmentSheetTemporaryTblColList, filter)
		if err != nil {
			return nil, err
		}
		for _, sheet := range list {
			sheetChan <- sheet
		}
	}
	<-finish
	return sheetList, nil
}
