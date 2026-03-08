package task

import (
	"context"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	logicKnowConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/knowledge_config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"math"
	"strings"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/common"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"git.woa.com/dialogue-platform/common/v3/errors"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	jsoniter "github.com/json-iterator/go"
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

// DocDeleteScheduler 文档删除任务
type DocDeleteScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    model.DocDeleteParams
}

func initDocDeleteScheduler() {
	task_scheduler.Register(
		model.DocDeleteTask,
		func(t task_scheduler.Task, params model.DocDeleteParams) task_scheduler.TaskHandler {
			return &DocDeleteScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *DocDeleteScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(DocDelete) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return kv, err
	}
	if doc == nil {
		return kv, errs.ErrDocNotFound
	}
	qas, err := getDocNotDeleteQA(ctx, doc, d.dao)
	if err != nil {
		return kv, err
	}
	segs, err := getDocNotDeleteSegment(ctx, doc, d.dao)
	if err != nil {
		return kv, err
	}

	// 问答切片
	for _, qa := range qas {
		kv[fmt.Sprintf("%s%d", qaDeletePrefix, qa.ID)] = fmt.Sprintf("%d", qa.ID)
	}

	// 文档切片
	batchSize := utilConfig.GetMainConfig().OfflineConfig.SyncVectorDeletedBatchSize
	if batchSize <= 0 {
		batchSize = defaultSyncDeletedVectorBatchSize
	}
	for index, segChunks := range slicex.Chunk(segs, batchSize) {
		var idsStr []string
		for _, seg := range segChunks {
			idsStr = append(idsStr, cast.ToString(seg.ID))
		}
		idChunksStr, err := jsoniter.MarshalToString(idsStr)
		if err != nil {
			log.ErrorContextf(ctx, "task(DocDelete) Prepare|jsoniter.MarshalToString err:%+v", err)
			kv = make(task_scheduler.TaskKV) // 重置kv
			return kv, err
		}
		log.DebugContextf(ctx, "task(DocDelete) Prepare index:%d, seg.IDs: %+v", index, idChunksStr)
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
func (d *DocDeleteScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *DocDeleteScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(DocDelete) Process, task: %+v, params: %+v", d.task, d.p)
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
		log.DebugContextf(ctx, "task(DocDelete) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			log.DebugContextf(ctx, "task(DocDelete) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(DocDelete) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		ctx = pkg.WithSpaceID(ctx, appDB.SpaceID)
		if strings.HasPrefix(key, qaDeletePrefix) {
			id := cast.ToUint64(v)
			qa, err := d.dao.GetQAByID(ctx, id)
			if err != nil {
				return err
			}
			if err = d.dao.DeleteDocToQA(ctx, qa); err != nil {
				return err
			}
		}
		if strings.HasPrefix(key, segDeletePrefix) {
			value := make([]string, 0)
			err := jsoniter.Unmarshal([]byte(v), &value)
			if err != nil {
				log.ErrorContextf(ctx, "task(DocDelete) jsoniter.Unmarshal  err:%+v", err)
				return err
			}
			ids := make([]uint64, 0)
			for _, idStr := range value {
				ids = append(ids, cast.ToUint64(idStr))
			}
			docSegments, err := d.getSegments(ctx, ids)
			if err != nil {
				log.ErrorContextf(ctx, "task(DocDelete) getSegments err:%+v", err)
				return err
			}
			if err = d.dao.BatchDeleteSegments(ctx, docSegments, d.p.RobotID); err != nil {
				return err
			}
			if err = d.dao.DeleteBigDataElastic(ctx, d.p.RobotID, d.p.DocID,
				bot_retrieval_server.KnowledgeType_KNOWLEDGE, true); err != nil {
				return err
			}

			deleteKnowledgeSegments := make([]*model.DocSegmentExtend, 0)
			for _, seg := range docSegments {
				if !seg.IsSegmentForQA() && !seg.IsText2sqlSegmentType() {
					deleteKnowledgeSegments = append(deleteKnowledgeSegments, seg)
				}
			}
			if len(deleteKnowledgeSegments) > 0 {
				embeddingConf, _, err := appDB.GetEmbeddingConf()
				if err != nil {
					log.ErrorContextf(ctx, "task(DocDelete) GetEmbeddingConf() err:%+v", err)
					return err
				}
				embeddingVersion := embeddingConf.Version
				if err = d.dao.BatchDirectDeleteSegmentKnowledge(ctx, appDB.ID,
					deleteKnowledgeSegments, embeddingVersion, embeddingModelName); err != nil {
					return err
				}
			}
			//feature_permission
			//删除文档需要删除角色文档绑定关系,每次删除一万条
			corp, err := d.dao.GetCorpByID(ctx, d.p.CorpID)
			if err != nil {
				log.ErrorContextf(ctx, "doc_delete getCorp err:%+v,doc_id:%v,corp_id:%v", err, d.p.DocID, d.p.CorpID)
				return err
			}
			doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
			if err != nil {
				log.ErrorContextf(ctx, "doc_delete getDoc err:%+v,doc_id:%v,corp_id:%v", err, d.p.DocID, d.p.CorpID)
				return err
			}
			log.DebugContextf(ctx, "feature_permission deleteRoleDoc corpBizId:%v,appBizId:%v,docBizId:%v",
				corp.BusinessID, appDB.BusinessID, doc.BusinessID)
			err = dao.GetRoleDao(nil).BatchDeleteRoleDoc(ctx, corp.BusinessID, appDB.BusinessID, doc.BusinessID)
			if err != nil { //柔性放过
				log.ErrorContextf(ctx, "feature_permission deleteRoleDoc err:%v,corp_biz_id:%v,app_biz_id:%v,doc_biz_id:%v",
					err, corp.BusinessID, appDB.BusinessID, doc.BusinessID)
			}
		}

		doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
		if err != nil {
			return err
		}
		if doc == nil {
			return errs.ErrDocNotFound
		}

		corpBizID, appBizID, _, _, err := d.dao.SegmentCommonIDsToBizIDs(ctx, d.p.CorpID,
			d.p.RobotID, 0, 0)
		// OrgData删除
		if strings.HasPrefix(key, orgDataDeletePrefix) {
			err := dao.GetDocSegmentOrgDataDao().BatchDeleteDocOrgDataByDocBizID(ctx, nil, corpBizID, appBizID,
				doc.BusinessID, defaultSyncDeletedOrgDataBatchSize)
			if err != nil {
				log.ErrorContextf(ctx, "BatchDeleteDocOrgDataByDocBizID err:%v,corpBizID:%v,appBizID:%v,docBizID:%v",
					err, corpBizID, appBizID, doc.BusinessID)
			}
		}
		// 干预OrgData删除
		if strings.HasPrefix(key, tempOrgDataDeletePrefix) {
			if !model.IsTableTypeDocument(doc.FileType) {
				err := dao.GetDocSegmentOrgDataTemporaryDao().BatchDeleteDocOrgDataByDocBizID(ctx, nil, corpBizID, appBizID,
					doc.BusinessID, defaultSyncDeletedOrgDataBatchSize)
				if err != nil {
					log.ErrorContextf(ctx, "BatchDeleteDocOrgDataByDocBizID Temporary err:%v,corpBizID:%v,appBizID:%v,docBizID:%v",
						err, corpBizID, appBizID, doc.BusinessID)
				}
			}
		}
		// 干预sheet删除
		if strings.HasPrefix(key, sheetDeletePrefix) {
			if model.IsTableTypeDocument(doc.FileType) {
				err := dao.GetDocSegmentSheetTemporaryDao().BatchDeleteDocSegmentSheetByDocBizID(ctx, nil, corpBizID, appBizID,
					doc.BusinessID, defaultSyncDeletedOrgDataBatchSize)
				if err != nil {
					log.ErrorContextf(ctx, "BatchDeleteDocSegmentSheetByDocBizID err:%v,corpBizID:%v,appBizID:%v,docBizID:%v",
						err, corpBizID, appBizID, doc.BusinessID)
				}
				err = dao.DeleteSheetDbTableAndColumns(ctx, corpBizID, appBizID, doc.BusinessID, d.p.RobotID)
				if err != nil {
					log.ErrorContextf(ctx, "deleteSheetDbTableAndColumns %v, %v", doc.BusinessID, err)
					return err
				}
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			log.ErrorContextf(ctx, "task(DocDelete) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(DocDelete) Finish kv:%s", key)
	}
	return nil
}

func (d *DocDeleteScheduler) deleteSheetDbTableAndColumns(ctx context.Context, corpBizID, appBizID, docBizID uint64) error {
	metaMappings, err := dao.GetDocMetaDataDao().GetDocMetaDataByDocId(ctx, docBizID, d.p.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocMetaDataByDocId err: %v", err)
		return err
	}

	for _, mapping := range metaMappings {
		isExist, err := dao.GetDBTableDao().Text2sqlExistsByDbSourceBizID(ctx, corpBizID, appBizID, mapping.BusinessID)
		if err != nil {
			return err
		}
		if !isExist {
			continue
		}

		dbTable, err := dao.GetDBTableDao().Text2sqlGetByDbSourceBizID(ctx, corpBizID, appBizID, mapping.BusinessID)
		if err != nil {
			return err
		}
		log.InfoContextf(ctx, "delete db table %v %v for doc", dbTable.DBTableBizID, dbTable.AliasName)
		err = dao.GetDBTableDao().SoftDeleteByBizID(ctx, corpBizID, appBizID, dbTable.DBTableBizID)
		if err != nil {
			return err
		}
		err = dao.GetDBTableColumnDao().SoftDeleteByTableBizID(ctx, corpBizID, appBizID, dbTable.DBTableBizID)
		if err != nil {
			return err
		}
	}
	return nil
}

// getSegments 获取切片
func (d *DocDeleteScheduler) getSegments(ctx context.Context, segmentIDs []uint64) (
	docSegments []*model.DocSegmentExtend, err error) {
	log.InfoContextf(ctx, "task(DocDelete) getSegments|segmentIDs: %+v", segmentIDs)
	segments, err := d.dao.GetSegmentByIDs(ctx, segmentIDs, d.p.RobotID)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, errs.ErrSegmentNotFound
	}

	robotID, docID := uint64(0), uint64(0)

	docSegments = make([]*model.DocSegmentExtend, 0)

	for _, segment := range segments {
		if robotID == 0 {
			robotID = segment.RobotID
		}
		if docID == 0 {
			docID = segment.DocID
		}
		if robotID != segment.RobotID || robotID != d.p.RobotID || docID != segment.DocID || docID != d.p.DocID {
			log.ErrorContextf(ctx, "task(DocDelete) getSegments|seg illegal|segment: %+v", segment)
			return nil, errs.ErrSegmentNotFound
		}

		docSegments = append(docSegments, segment)
	}

	if len(docSegments) == 0 {
		log.DebugContextf(ctx, "task(DocDelete) getSegments|len(docSegments):%d", len(docSegments))
		return nil, nil
	}
	return docSegments, nil
}

// Fail 任务失败
func (d *DocDeleteScheduler) Fail(_ context.Context) error {
	return nil
}

// Stop 任务停止
func (d *DocDeleteScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocDeleteScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(DocDelete) Done")
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	if err = d.dao.DeleteText2SQL(ctx, doc.RobotID, doc.ID); err != nil {
		return err
	}
	if err = d.dao.DeleteDocSuccess(ctx, doc); err != nil {
		return err
	}
	return nil
}

// getDocNotDeleteQA 文档未删除的QA
func getDocNotDeleteQA(ctx context.Context, doc *model.Doc, dao dao.Dao) ([]*model.DocQA,
	error) {
	pageSize := uint32(1000)
	page := uint32(1)
	qas := make([]*model.DocQA, 0)
	for {
		req := &model.QAListReq{
			CorpID:    doc.CorpID,
			RobotID:   doc.RobotID,
			DocID:     []uint64{doc.ID},
			IsDeleted: model.QAIsNotDeleted,
			Page:      page,
			PageSize:  pageSize,
		}
		list, err := dao.GetQAList(ctx, req)
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
func getDocNotDeleteSegment(ctx context.Context, doc *model.Doc, dao dao.Dao) ([]*model.DocSegmentExtend, error) {
	total, err := dao.GetSegmentListCount(ctx, doc.CorpID, doc.ID, doc.RobotID)
	if err != nil {
		return nil, err
	}
	segments := make([]*model.DocSegmentExtend, 0)
	segmentChan := make(chan *model.DocSegmentExtend, 5000)
	finish := make(chan any)
	go func() {
		defer errors.PanicHandler()
		for segment := range segmentChan {
			segments = append(segments, segment)
		}
		finish <- nil
	}()
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 1; i <= pages; i++ {
		page := uint32(i)
		list, err := dao.GetSegmentList(ctx, doc.CorpID, doc.ID, page, uint32(pageSize), doc.RobotID)
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
func getDocNotDeleteOrgData(ctx context.Context, doc *model.Doc, d dao.Dao) ([]*model.DocSegmentOrgData, error) {
	if doc == nil {
		log.ErrorContextf(ctx, "getDocNotDeleteOrgData|doc is null")
		return nil, errs.ErrDocNotFound
	}
	corpBizID, appBizID, _, _, err := d.SegmentCommonIDsToBizIDs(ctx, doc.CorpID,
		doc.RobotID, 0, 0)
	if err != nil {
		log.ErrorContextf(ctx, "getDocNotDeleteOrgData|SegmentCommonIDsToBizIDs|err:%v", err)
		return nil, err
	}
	deleteFlag := dao.IsNotDeleted
	filter := &dao.DocSegmentOrgDataFilter{
		CorpBizID:          corpBizID,
		AppBizID:           appBizID,
		DocBizID:           doc.BusinessID,
		IsDeleted:          &deleteFlag,
		IsTemporaryDeleted: &deleteFlag,
		RouterAppBizID:     appBizID,
	}
	total, err := dao.GetDocSegmentOrgDataDao().GetDocOrgDataCount(ctx, filter)
	if err != nil {
		log.ErrorContextf(ctx, "getDocNotDeleteOrgData|GetDocOrgDataCount|err:%v", err)
		return nil, err
	}
	// 当total为0时，提前返回空列表
	if total == 0 {
		return []*model.DocSegmentOrgData{}, nil
	}
	orgDataList := make([]*model.DocSegmentOrgData, 0)
	orgDataChan := make(chan *model.DocSegmentOrgData, 5000)
	finish := make(chan any)
	go func() {
		defer errors.PanicHandler()
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
		offset := common.GetOffsetByPage(page, pageSize)
		limit := pageSize
		filter := &dao.DocSegmentOrgDataFilter{
			CorpBizID:      corpBizID,
			AppBizID:       appBizID,
			DocBizID:       doc.BusinessID,
			Offset:         offset,
			Limit:          limit,
			RouterAppBizID: appBizID,
		}
		list, err := dao.GetDocSegmentOrgDataDao().GetDocOrgDataList(ctx,
			dao.DocSegmentOrgDataTblColList, filter)
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
func getDocNotDeleteTemporaryOrgData(ctx context.Context, doc *model.Doc, d dao.Dao) ([]*model.DocSegmentOrgDataTemporary, error) {
	if doc == nil {
		log.ErrorContextf(ctx, "getDocNotDeleteTemporaryOrgData|doc is null")
		return nil, errs.ErrDocNotFound
	}
	corpBizID, appBizID, _, _, err := d.SegmentCommonIDsToBizIDs(ctx, doc.CorpID,
		doc.RobotID, 0, 0)
	if err != nil {
		log.ErrorContextf(ctx, "getDocNotDeleteTemporaryOrgData|SegmentCommonIDsToBizIDs|err:%v", err)
		return nil, err
	}
	deletedFlag := dao.IsNotDeleted
	filter := &dao.DocSegmentOrgDataTemporaryFilter{
		CorpBizID: corpBizID,
		AppBizID:  appBizID,
		DocBizID:  doc.BusinessID,
		IsDeleted: &deletedFlag,
	}
	total, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataCount(ctx, filter)
	if err != nil {
		log.ErrorContextf(ctx, "getDocNotDeleteTemporaryOrgData|GetDocOrgDataCountByDocBizID|err:%v", err)
		return nil, err
	}
	// 当total为0时，提前返回空列表
	if total == 0 {
		return []*model.DocSegmentOrgDataTemporary{}, nil
	}

	orgDataList := make([]*model.DocSegmentOrgDataTemporary, 0)
	orgDataChan := make(chan *model.DocSegmentOrgDataTemporary, 5000)
	finish := make(chan any)
	go func() {
		defer errors.PanicHandler()
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
		offset := common.GetOffsetByPage(page, pageSize)
		limit := pageSize
		filter := &dao.DocSegmentOrgDataTemporaryFilter{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
			DocBizID:  doc.BusinessID,
			Offset:    offset,
			Limit:     limit,
		}
		list, err := dao.GetDocSegmentOrgDataTemporaryDao().GetDocOrgDataByDocBizID(ctx,
			dao.DocSegmentOrgDataTemporaryTblColList, filter)
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
func getDocNotDeleteTemporarySheet(ctx context.Context, doc *model.Doc, d dao.Dao) ([]*model.DocSegmentSheetTemporary, error) {
	if doc == nil {
		log.ErrorContextf(ctx, "getDocNotDeleteTemporarySheet|doc is null")
		return nil, errs.ErrDocNotFound
	}
	corpBizID, appBizID, _, _, err := d.SegmentCommonIDsToBizIDs(ctx, doc.CorpID,
		doc.RobotID, 0, 0)
	if err != nil {
		log.ErrorContextf(ctx, "getDocNotDeleteTemporarySheet|SegmentCommonIDsToBizIDs|err:%v", err)
		return nil, err
	}
	deletedFlag := dao.IsNotDeleted
	filter := &dao.DocSegmentSheetTemporaryFilter{
		CorpBizID: corpBizID,
		AppBizID:  appBizID,
		DocBizID:  doc.BusinessID,
		IsDeleted: &deletedFlag,
	}
	total, err := dao.GetDocSegmentSheetTemporaryDao().GetDocSheetCount(ctx, filter)
	if err != nil {
		log.ErrorContextf(ctx, "getDocNotDeleteTemporarySheet|GetDocOrgDataCountByDocBizID|err:%v", err)
		return nil, err
	}
	// 当total为0时，提前返回空列表
	if total == 0 {
		return []*model.DocSegmentSheetTemporary{}, nil
	}
	sheetList := make([]*model.DocSegmentSheetTemporary, 0)
	sheetChan := make(chan *model.DocSegmentSheetTemporary, 5000)
	finish := make(chan any)
	go func() {
		defer errors.PanicHandler()
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
		offset := common.GetOffsetByPage(page, pageSize)
		limit := pageSize
		filter := &dao.DocSegmentSheetTemporaryFilter{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
			DocBizID:  doc.BusinessID,
			Offset:    offset,
			Limit:     limit,
		}
		list, err := dao.GetDocSegmentSheetTemporaryDao().GetSheetList(ctx,
			dao.DocSegmentSheetTemporaryTblColList, filter)
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
