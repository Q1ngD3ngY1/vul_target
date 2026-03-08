package async

import (
	"context"
	"fmt"
	"strings"
	"time"

	"git.woa.com/adp/kb/kb-config/internal/util"
	"github.com/spf13/cast"
	"golang.org/x/sync/errgroup"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao/vector"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	dbentity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
)

// BatchUpdateVectorTaskHandler 批量更新文档/问答向量
type BatchUpdateVectorTaskHandler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity.BatchUpdateVector
}

const (
	defaultUpdateSize = 100
	defaultGLimit     = 10
	docVectorPrefix   = "doc:Vector:"
	qaVectorPrefix    = "qa:Vector:"
	dbVectorPrefix    = "db:Vector:"
)

func registerBatchUpdateVectorTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.BatchUpdateVectorTask,
		func(t task_scheduler.Task, params entity.BatchUpdateVector) task_scheduler.TaskHandler {
			return &BatchUpdateVectorTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

func (d *BatchUpdateVectorTaskHandler) getAppEmbeddingInfo(ctx context.Context, appDB *entity.App) (string, uint64) {
	var (
		embeddingName    string
		embeddingVersion uint64
		err              error
	)

	embeddingVersion = appDB.Embedding.Version

	embeddingName, err =
		d.kbLogic.GetKnowledgeEmbeddingModel(ctx, appDB.CorpBizId, appDB.BizId, appDB.BizId, appDB.IsShared)

	if err != nil {
		if err != nil {
			logx.W(ctx, "task(BatchUpdateVector) GetShareKnowledgeBaseConfig err:%+v", err)
		}
	}
	if embeddingName != "" {
		embeddingVersion = entity.GetEmbeddingVersion(embeddingName)
	}
	logx.I(ctx, "task(BatchUpdateVector) getAppEmbeddingInfo embeddingName:%s,embeddingVersion:%v",
		embeddingName, embeddingVersion)
	return embeddingName, embeddingVersion

}

// Prepare .
func (d *BatchUpdateVectorTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	docBatchSize := config.GetMainConfig().Permissions.UpdateDocVectorSize
	if docBatchSize <= 0 {
		docBatchSize = defaultUpdateSize
	}
	qaBatchSize := config.GetMainConfig().Permissions.UpdateQaVectorSize
	if qaBatchSize <= 0 {
		qaBatchSize = defaultUpdateSize
	}
	dbBatchSize := config.GetMainConfig().Permissions.UpdateDbVectorSize
	if dbBatchSize <= 0 {
		dbBatchSize = defaultUpdateSize
	}
	kv := make(task_scheduler.TaskKV)
	docIDs, qaIDs, dbTableIDs := make([]string, 0), make([]string, 0), make([]string, 0)
	for knowID, knowData := range d.p.KnowIDs { // 按知识库维度传参
		for _, docID := range knowData.DocIDs {
			docIDs = append(docIDs, cast.ToString(knowID)+"_"+cast.ToString(docID))
		}
		for _, qaID := range knowData.QaIDs {
			qaIDs = append(qaIDs, cast.ToString(knowID)+"_"+cast.ToString(qaID))
		}
		for _, dbTableBizID := range knowData.DbTableBizIDs {
			dbTableIDs = append(dbTableIDs, cast.ToString(knowID)+"_"+cast.ToString(dbTableBizID))
		}
	}
	if len(docIDs) > 0 {
		for index, idChunks := range slicex.Chunk(docIDs, docBatchSize) {
			var ids []string
			for _, v := range idChunks {
				ids = append(ids, cast.ToString(v))
			}
			idChunksStr, err := jsonx.MarshalToString(ids)
			if err != nil {
				logx.E(ctx, "task(BatchUpdateVector) Prepare|jsonx.MarshalToString err:%+v", err)
				kv = make(task_scheduler.TaskKV) // 重置kv
				return kv, err
			}
			kv[fmt.Sprintf("%s%d", docVectorPrefix, index)] = idChunksStr
		}
	}
	if len(qaIDs) > 0 {
		for index, idChunks := range slicex.Chunk(qaIDs, qaBatchSize) {
			var ids []string
			for _, v := range idChunks {
				ids = append(ids, cast.ToString(v))
			}
			idChunksStr, err := jsonx.MarshalToString(ids)
			if err != nil {
				logx.E(ctx, "task(BatchUpdateVector) Prepare|jsonx.MarshalToString err:%+v", err)
				kv = make(task_scheduler.TaskKV) // 重置kv
				return kv, err
			}
			kv[fmt.Sprintf("%s%d", qaVectorPrefix, index)] = idChunksStr
		}
	}
	if len(dbTableIDs) > 0 {
		for index, idChunks := range slicex.Chunk(dbTableIDs, dbBatchSize) {
			var ids []string
			for _, v := range idChunks {
				ids = append(ids, cast.ToString(v))
			}
			idChunksStr, err := jsonx.MarshalToString(ids)
			if err != nil {
				logx.E(ctx, "task(BatchUpdateVector) Prepare|jsonx.MarshalToString err:%+v", err)
				kv = make(task_scheduler.TaskKV) // 重置kv
				return kv, err
			}
			kv[fmt.Sprintf("%s%d", dbVectorPrefix, index)] = idChunksStr
		}
	}
	return kv, nil
}

// Init .
func (d *BatchUpdateVectorTaskHandler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process .
func (d *BatchUpdateVectorTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(BatchUpdateVector) Process, task: %+v, params: %+v", d.task, d.p)
	docGLimit := config.GetMainConfig().Permissions.UpdateDocVectorLimit
	if docGLimit <= 0 {
		docGLimit = defaultGLimit
	}
	qaGLimit := config.GetMainConfig().Permissions.UpdateQaVectorLimit
	if qaGLimit <= 0 {
		qaGLimit = defaultGLimit
	}
	dbGLimit := config.GetMainConfig().Permissions.UpdateDbVectorLimit
	if dbGLimit <= 0 {
		dbGLimit = defaultGLimit
	}
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(BatchUpdateVector) Start k:%s, v:%s", k, v)
		if strings.HasPrefix(k, docVectorPrefix) { // 文档标签更新任务
			value := make([]string, 0)
			err := jsonx.Unmarshal([]byte(v), &value)
			if err != nil {
				logx.E(ctx, "task(BatchUpdateVector) jsonx.Unmarshal  err:%+v", err)
				return err
			}
			g, gCtx := errgroup.WithContext(ctx)
			g.SetLimit(docGLimit) // 并发10个
			for _, v := range value {
				g.Go(func() error {
					logx.I(gCtx, "task(BatchUpdateVector) processDoc docId:%v", v)
					return d.processDoc(gCtx, v)
				})
			}
			if err := g.Wait(); err != nil {
				logx.E(ctx, "task(BatchUpdateVector) processDoc err:%v,value:%v", err, value)
				return err
			}
		} else if strings.HasPrefix(k, qaVectorPrefix) { // 问答标签更新任务
			value := make([]string, 0)
			err := jsonx.Unmarshal([]byte(v), &value)
			if err != nil {
				logx.E(ctx, "task(BatchUpdateVector) jsonx.Unmarshal  err:%+v", err)
				return err
			}
			g, gCtx := errgroup.WithContext(ctx)
			g.SetLimit(qaGLimit) // 并发10个
			for _, v := range value {
				g.Go(func() error {
					logx.I(gCtx, "task(BatchUpdateVector) processQa qaId:%v", v)
					return d.processQa(gCtx, v)
				})
			}
			if err := g.Wait(); err != nil {
				logx.E(ctx, "task(BatchUpdateVector) processQa err:%v,value:%v", err, value)
				return err
			}
		} else if strings.HasPrefix(k, dbVectorPrefix) { // 数据库标签更新任务
			value := make([]string, 0)
			err := jsonx.Unmarshal([]byte(v), &value)
			if err != nil {
				logx.E(ctx, "task(BatchUpdateVector) jsonx.Unmarshal  err:%+v", err)
				return err
			}
			g, gCtx := errgroup.WithContext(ctx)
			g.SetLimit(dbGLimit) // 并发10个
			for _, v := range value {
				g.Go(func() error {
					logx.I(gCtx, "task(BatchUpdateVector) processDB dbBizId:%v", v)
					return d.processDB(gCtx, v)
				})
			}
			if err := g.Wait(); err != nil {
				logx.E(ctx, "task(BatchUpdateVector) processDB err:%v,value:%v", err, value)
				return err
			}
		}
		if err := progress.Finish(ctx, k); err != nil {
			logx.E(ctx, "task(BatchUpdateVector) Finish key:%s,value:%v,err:%+v", k, v, err)
			return err
		}
		logx.D(ctx, "task(BatchUpdateVector) Finish key:%s", k)
	}
	logx.D(ctx, "task(BatchUpdateVector) task:%+v,remain:%v", d.task, progress.Remain())
	return nil
}

func (d *BatchUpdateVectorTaskHandler) processDoc(ctx context.Context, AppBizIDAndDocID string) error {
	arr := strings.Split(AppBizIDAndDocID, "_")
	if len(arr) != 2 {
		logx.E(ctx, "task(BatchUpdateVector) processDoc AppBizIDAndDocID:%v err", AppBizIDAndDocID)
		return nil
	}
	appBizID, docID := cast.ToUint64(arr[0]), cast.ToUint64(arr[1])
	// 1.先取文档信息
	doc, err := d.docLogic.GetDocByID(ctx, docID, appBizID)
	if err != nil {
		logx.E(ctx, "task(BatchUpdateVector) processDoc GetDocByID err:%v,docID:%v", err, docID)
		return err
	}
	if doc == nil || doc.IsDeleted || doc.IsExpire() || doc.IsCharSizeExceeded() ||
		doc.Status == docEntity.DocStatusCreateIndexFail {
		logx.I(ctx, "task(BatchUpdateVector) processDoc skip doc:%+v", doc)
		return nil
	}
	if doc.Status != docEntity.DocStatusWaitRelease && doc.Status != docEntity.DocStatusReleaseSuccess {
		logx.I(ctx, "task(BatchUpdateVector) processDoc status no need process, skip doc:%+v", doc)
		return nil
	}
	// 2.获取应用信息 得到embedding 版本
	appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, doc.RobotID)
	if err != nil {
		logx.E(ctx, "task(BatchUpdateVector) processDoc DescribeAppByPrimaryIdWithoutNotFoundError err:%v,docID:%v", docID)
		return err
	}
	if appDB.HasDeleted() {
		logx.I(ctx, "task(BatchUpdateVector) processDoc skip docID:%v", docID)
		return nil
	}
	newCtx := util.SetMultipleMetaData(ctx, appDB.SpaceId, appDB.Uin)
	embeddingModel, embeddingVersion := d.getAppEmbeddingInfo(ctx, appDB)
	// 3.获取doc需要分类和角色向量
	hasRole := false
	vectorLabels, err := getDocCateAndRoleLabels(ctx, appDB.BizId, doc, d.userLogic, d.cateLogic)
	if err != nil {
		logx.E(ctx, "task(BatchUpdateVector) processDoc getDocVectorLabels err:%v,docID:%v", err, docID)
		return err
	}
	for _, v := range vectorLabels {
		if v.Name == config.GetMainConfig().Permissions.RoleRetrievalKey {
			hasRole = true
		}
		logx.D(ctx, "task(BatchUpdateVector) processDoc docID:%v,vectorLabel:%+v", docID, v)
	}
	if !hasRole { // 如果角色为空，赋值为空
		vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
			Name:  config.GetMainConfig().Permissions.RoleRetrievalKey,
			Value: "",
		})
	}
	// 4.获取评测端 文档切片
	segIDs, startID, limit := make([]uint64, 0), uint64(0), uint64(config.GetMainConfig().Permissions.GetSegmentLimit)
	for {
		segs, lastID, err := d.segLogic.GetSegmentByDocID(ctx, appDB.PrimaryId, doc.ID, startID, limit,
			[]string{segEntity.DocSegmentTblColID, segEntity.DocSegmentTblColType,
				segEntity.DocSegmentTblColSegmentType, segEntity.DocSegmentTblColPageContent})
		if err != nil {
			logx.E(ctx, "FullUpdateLabel processTestDoc getDocNotDeleteSegment err:%v,docID:%v", err, doc.ID)
			return err
		}
		if len(segs) == 0 {
			break
		}
		startID = lastID
		// 5.写评测端 es和向量
		segmentMap, text2sqlMap := make([]uint64, 0), make(map[uint32][]uint64, 0)
		for _, segment := range segs {
			segIDs = append(segIDs, segment.ID)
			if segment.IsSegmentForQA() || segment.SegmentType == segEntity.SegmentTypeText2SQLMeta {
				continue
			}
			if segment.SegmentType == segEntity.SegmentTypeText2SQLContent { // 是表格需要获取列数
				cells, err := getColumnsCount(ctx, segment.PageContent)
				if err != nil {
					logx.E(ctx,
						"task(BatchUpdateVector) processDoc getColumnsCount err:%v,segmentID:%vcontent:%v",
						err, segment.ID, segment.PageContent)
					continue
				}
				text2sqlMap[cells] = append(text2sqlMap[cells], segment.ID)
			} else {
				segmentMap = append(segmentMap, segment.ID)
			}
		}
		if err = batchUpdateSegVector(newCtx, d.rpc, appDB.PrimaryId, appDB.BizId, embeddingVersion, embeddingModel, retrieval.EnvType_Test,
			vectorLabels, segmentMap, text2sqlMap); err != nil {
			logx.E(ctx,
				"task(BatchUpdateVector) processDoc batchUpdateSegVector err:%v,segmentMap:%v,text2sqlMap:%v",
				err, segmentMap, text2sqlMap)
			return err
		}
	}
	if appDB.IsShared { // 共享知识库不需要双写
		return nil
	}
	// 6.如果是分类发起的，只需要写评测端，更新文档和文档切片状态为待发布
	if d.p.Type == entity.UpdateVectorByCate {
		err = d.releaseLogic.UpdateDocWaitRelease(ctx, appDB.PrimaryId, doc.ID, segIDs)
		if err != nil {
			logx.E(ctx, "task(BatchUpdateVector) processDoc BatchUpdateSegment err:%v,docID:%v,segIDs:%v",
				err, docID, segIDs)
			return err
		}
		return nil
	}
	// 7.其他情况 双写发布端es和向量
	// 获取发布端文档切片
	if appDB.QaVersion == 0 { // 默认知识库未发布不需要双写
		return nil
	}
	startID = 0
	for {
		prodSegs, lastID, err := d.qaLogic.GetVectorSyncLogic().GetSegmentNodeByDocID(ctx, appDB.PrimaryId, docID, startID,
			limit,
			[]string{vector.NodeTblColId, vector.NodeTblColRelatedId, vector.NodeTblColPageContent,
				vector.NodeTblColSegmentType})
		if err != nil {
			logx.E(ctx, "FullUpdateLabel processProdDoc GetNodeIdsList err:%v,docID:%v", err, docID)
			return err
		}
		if len(prodSegs) == 0 {
			break
		}
		startID = lastID
		segmentMap, text2sqlMap := make([]uint64, 0), make(map[uint32][]uint64, 0)
		for _, segment := range prodSegs {
			if segment.SegmentType == segEntity.SegmentTypeText2SQLMeta {
				continue
			}
			if segment.SegmentType == segEntity.SegmentTypeText2SQLContent { // 是表格需要获取列数
				cells, err := getColumnsCount(ctx, segment.PageContent)
				if err != nil {
					logx.E(ctx,
						"task(BatchUpdateVector) processDoc getColumnsCount err:%v,segmentID:%vcontent:%v",
						err, segment.ID, segment.PageContent)
					continue
				}
				text2sqlMap[cells] = append(text2sqlMap[cells], segment.RelatedID)
			} else {
				segmentMap = append(segmentMap, segment.RelatedID)
			}
		}
		if err = batchUpdateSegVector(ctx, d.rpc, appDB.PrimaryId, appDB.BizId, embeddingVersion, embeddingModel, retrieval.EnvType_Prod,
			vectorLabels, segmentMap, text2sqlMap); err != nil {
			logx.E(ctx,
				"task(BatchUpdateVector) processDoc batchUpdateSegVector err:%v,segmentMap:%v,text2sqlMap:%v",
				err, segmentMap, text2sqlMap)
			return err
		}
	}
	return nil
}

// batchUpdateSegVector 批量写标签
func batchUpdateSegVector(ctx context.Context, r *rpc.RPC, appID, appBizID, embeddingVersion uint64, embeddingModel string,
	envType retrieval.EnvType,
	vectorLabels []*retrieval.VectorLabel, segmentMap []uint64, text2sqlMap map[uint32][]uint64) error {
	sleepSwitch, sleepMillisecond := config.GetMainConfig().Permissions.UpdateVectorSleepSwitch,
		config.GetMainConfig().Permissions.UpdateVectorSleepMillisecond
	if len(segmentMap) > 0 {
		for _, ids := range slicex.Chunk(segmentMap, defaultUpdateSize) { // 普通切片,每批100个
			req := &retrieval.UpdateLabelReq{
				RobotId:            appID,
				AppBizId:           appBizID,
				EnvType:            envType,
				IndexId:            entity.SegmentReviewVersionID,
				Ids:                ids, // 切片的主键id
				DocType:            entity.DocTypeSegment,
				EmbeddingVersion:   embeddingVersion,
				EmbeddingModelName: embeddingModel,
				Labels:             vectorLabels,
				SegmentType:        segEntity.SegmentTypeSegment,
			}
			_, err := r.UpdateVectorLabel(ctx, req)
			if err != nil {
				logx.E(ctx, "task(BatchUpdateVector) processDoc segment UpdateVectorLabel err:%v,req:%+v",
					err, req)
				return err
			}
			if sleepSwitch {
				time.Sleep(time.Duration(sleepMillisecond) * time.Millisecond)
			}
		}
	}
	for cell, segList := range text2sqlMap { // 表格切片,按列数分组,每批100个
		if len(segList) == 0 {
			continue
		}
		for _, ids := range slicex.Chunk(segList, defaultUpdateSize) {
			req := &retrieval.UpdateLabelReq{
				RobotId:            appID,
				AppBizId:           appBizID,
				EnvType:            envType,
				IndexId:            entity.SegmentReviewVersionID,
				Ids:                ids, // 切片的主键id
				DocType:            entity.DocTypeSegment,
				EmbeddingVersion:   embeddingVersion,
				EmbeddingModelName: embeddingModel,
				Labels:             vectorLabels,
				SegmentType:        segEntity.SegmentTypeText2SQLContent,
				ColumnsCount:       cell,
			}
			_, err := r.UpdateVectorLabel(ctx, req)
			if err != nil {
				logx.E(ctx, "task(BatchUpdateVector) processDoc text2sql UpdateVectorLabel err:%v,req:%+v",
					err, req)
				return err
			}
			if sleepSwitch {
				time.Sleep(time.Duration(sleepMillisecond) * time.Millisecond)
			}
		}
	}
	return nil
}

// getColumnsCount 解析文档切片内容，获取表格列数
func getColumnsCount(ctx context.Context, pageContent string) (uint32, error) {
	content := segEntity.Text2SQLSegmentContent{}
	err := jsonx.Unmarshal([]byte(pageContent), &content)
	if err != nil {
		logx.E(ctx, "getColumnsCount|Unmarshal|pageContent:%+v|err:%+v", pageContent, err)
		return 0, err
	}
	return uint32(len(content.Cells)), nil
}

func (d *BatchUpdateVectorTaskHandler) processQa(ctx context.Context, AppBizIDAndQaID string) error {
	arr := strings.Split(AppBizIDAndQaID, "_")
	if len(arr) != 2 {
		logx.E(ctx, "task(BatchUpdateVector) processQa AppBizIDAndQaID:%v err", AppBizIDAndQaID)
		return nil
	}
	qaID := cast.ToUint64(arr[1])
	// 1.先取问答信息
	qa, err := d.qaLogic.GetQAByID(ctx, qaID)
	if err != nil {
		logx.E(ctx, "task(BatchUpdateVector) processQa GetQAByID err:%v,qaID:%v", err, qaID)
		return err
	}
	if qa == nil || qa.IsDeleted == qaEntity.QAIsDeleted || !qa.IsAccepted() || qa.IsExpire() || qa.IsCharExceeded() {
		logx.I(ctx, "task(BatchUpdateVector) processQa skip qaID:%v", qaID)
		return nil
	}
	if qa.Status() != qaEntity.QAReleaseStatusInit && qa.ReleaseStatus != qaEntity.QAReleaseStatusSuccess {
		logx.I(ctx, "task(BatchUpdateVector) processQa status no need process, skip qaID:%v", qaID)
		return nil
	}
	// 2.获取应用信息 得到embedding 版本
	appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, qa.RobotID)
	if err != nil {
		logx.E(ctx, "task(BatchUpdateVector) processQa DescribeAppByPrimaryIdWithoutNotFoundError err:%v,qaID:%v", qaID)
		return err
	}
	if appDB.HasDeleted() {
		logx.I(ctx, "task(BatchUpdateVector) processQa skip qaID:%v", qaID)
		return nil
	}
	embeddingName, embeddingVersion := d.getAppEmbeddingInfo(ctx, appDB)

	newCtx := util.SetMultipleMetaData(ctx, appDB.SpaceId, appDB.Uin)
	// 3.获取qa分类和角色标签
	vectorLabels, err := d.qaLogic.GetVectorSyncLogic().GetQACateAndRoleLabels(newCtx, appDB.BizId, qa)
	if err != nil {
		logx.E(ctx, "task(BatchUpdateVector) processQa GetQACateAndRoleLabels err:%v,qaID:%v", err, qaID)
		return err
	}
	hasRole := false
	for _, v := range vectorLabels {
		if v.Name == config.GetMainConfig().Permissions.RoleRetrievalKey {
			hasRole = true
		}
		logx.D(ctx, "task(BatchUpdateVector) processQa qaID:%v,vectorLabel:%+v", qaID, v)
	}
	if !hasRole { // 如果角色为空，赋值为空
		vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
			Name:  config.GetMainConfig().Permissions.RoleRetrievalKey,
			Value: "",
		})
	}
	// 4.获取qa的相似问
	similarQuestions, err := d.qaLogic.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		logx.E(ctx, "task(BatchUpdateVector) processQa GetSimilarQuestionsByQA err:%v,qaID:%v", err, qaID)
		return err
	}
	// 5.写评测端es和向量
	// 标准问需要双写两个向量库
	req := &retrieval.UpdateLabelReq{
		RobotId:            qa.RobotID,
		AppBizId:           appDB.BizId,
		EnvType:            retrieval.EnvType_Test,
		IndexId:            entity.ReviewVersionID,
		Ids:                []uint64{qa.ID},
		DocType:            entity.DocTypeQA,
		QaType:             entity.QATypeStandard,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingName,
		Labels:             vectorLabels,
	}
	_, err = d.rpc.RetrievalDirectIndex.UpdateVectorLabel(newCtx, req)
	if err != nil {
		logx.E(ctx, "task(BatchUpdateVector) processQa UpdateVectorLabel err:%v,req:%+v", err, req)
		return err
	}
	req = &retrieval.UpdateLabelReq{
		RobotId:            qa.RobotID,
		AppBizId:           appDB.BizId,
		EnvType:            retrieval.EnvType_Test,
		IndexId:            entity.SimilarVersionID,
		Ids:                []uint64{qa.ID},
		DocType:            entity.DocTypeQA,
		QaType:             entity.QATypeStandard,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingName,
		Labels:             vectorLabels,
	}
	_, err = d.rpc.RetrievalDirectIndex.UpdateVectorLabel(newCtx, req)
	if err != nil {
		logx.E(newCtx, "task(BatchUpdateVector) processQa UpdateVectorLabel err:%v,req:%+v", err, req)
		return err
	}
	sleepSwitch, sleepMillisecond := config.GetMainConfig().Permissions.UpdateVectorSleepSwitch,
		config.GetMainConfig().Permissions.UpdateVectorSleepMillisecond
	simBizIDs := make([]uint64, 0, len(similarQuestions))
	if len(similarQuestions) > 0 {
		for _, sims := range slicex.Chunk(similarQuestions, defaultUpdateSize) {
			tmp := make([]uint64, 0, defaultUpdateSize)
			for _, v := range sims {
				tmp = append(tmp, v.SimilarID)
				simBizIDs = append(simBizIDs, v.SimilarID)
			}
			req := &retrieval.UpdateLabelReq{
				RobotId:            qa.RobotID,
				AppBizId:           appDB.BizId,
				EnvType:            retrieval.EnvType_Test,
				IndexId:            entity.ReviewVersionID,
				Ids:                tmp, // 相似问业务id
				DocType:            entity.DocTypeQA,
				QaType:             entity.QATypeSimilar,
				EmbeddingVersion:   embeddingVersion,
				EmbeddingModelName: embeddingName,
				Labels:             vectorLabels,
			}
			_, err = d.rpc.RetrievalDirectIndex.UpdateVectorLabel(newCtx, req)
			if err != nil {
				logx.E(newCtx, "task(BatchUpdateVector) processQa UpdateVectorLabel err:%v,req:%+v", err, req)
				return err
			}
			if sleepSwitch {
				time.Sleep(time.Duration(sleepMillisecond) * time.Millisecond)
			}
		}
	}
	if appDB.IsShared { // 共享知识库不需要双写
		return nil
	}
	// 6.如果是分类发起的，只需要写评测端，更新标准问和相似问状态为待发布
	if d.p.Type == entity.UpdateVectorByCate {
		err = d.releaseLogic.UpdateQAWaitRelease(ctx, appDB.PrimaryId, qaID, simBizIDs)
		if err != nil {
			logx.E(ctx, "task(BatchUpdateVector) processQa UpdateQAWaitRelease err:%v,qaID:%v,simBizIDs:%v",
				err, qaID, simBizIDs)
			return err
		}
		return nil
	}
	// 7.其他情况 双写发布端es和向量
	// 获取发布端qa的相似问
	if appDB.QaVersion == 0 { // 默认知识库未发布不需要双写
		return nil
	}
	prodSims, err := d.qaLogic.GetVectorSyncLogic().GetNodeIdsList(ctx, appDB.PrimaryId,
		[]string{vector.NodeTblColId, vector.NodeTblColRelatedId},
		&entity.RetrievalNodeFilter{
			APPID:    appDB.PrimaryId,
			DocType:  entity.DocTypeQA,
			ParentID: qaID,
		})
	if err != nil {
		logx.E(ctx, "task(BatchUpdateVector) processQa GetNodeIdsList err:%v,qaID:%v", err, qaID)
		return err
	}
	req = &retrieval.UpdateLabelReq{
		RobotId:            qa.RobotID,
		AppBizId:           appDB.BizId,
		EnvType:            retrieval.EnvType_Prod,
		IndexId:            entity.ReviewVersionID,
		Ids:                []uint64{qa.ID},
		DocType:            entity.DocTypeQA,
		QaType:             entity.QATypeStandard,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingName,
		Labels:             vectorLabels,
	}
	_, err = d.rpc.RetrievalDirectIndex.UpdateVectorLabel(newCtx, req)
	if err != nil {
		logx.E(ctx, "task(BatchUpdateVector) processQa UpdateVectorLabel err:%v,req:%+v", err, req)
		return err
	}
	req = &retrieval.UpdateLabelReq{
		RobotId:            qa.RobotID,
		AppBizId:           appDB.BizId,
		EnvType:            retrieval.EnvType_Prod,
		IndexId:            entity.SimilarVersionID,
		Ids:                []uint64{qa.ID},
		DocType:            entity.DocTypeQA,
		QaType:             entity.QATypeStandard,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingName,
		Labels:             vectorLabels,
	}
	_, err = d.rpc.RetrievalDirectIndex.UpdateVectorLabel(newCtx, req)
	if err != nil {
		logx.E(ctx, "task(BatchUpdateVector) processQa UpdateVectorLabel err:%v,req:%+v", err, req)
		return err
	}
	if len(prodSims) > 0 {
		for _, sims := range slicex.Chunk(prodSims, defaultUpdateSize) {
			tmp := make([]uint64, 0, defaultUpdateSize)
			for _, v := range sims {
				tmp = append(tmp, v.RelatedID)
			}
			req := &retrieval.UpdateLabelReq{
				RobotId:            qa.RobotID,
				AppBizId:           appDB.BizId,
				EnvType:            retrieval.EnvType_Prod,
				IndexId:            entity.ReviewVersionID,
				Ids:                tmp, // 相似问业务id
				DocType:            entity.DocTypeQA,
				QaType:             entity.QATypeSimilar,
				EmbeddingVersion:   embeddingVersion,
				EmbeddingModelName: embeddingName,
				Labels:             vectorLabels,
			}
			_, err = d.rpc.RetrievalDirectIndex.UpdateVectorLabel(newCtx, req)
			if err != nil {
				logx.E(newCtx, "task(BatchUpdateVector) processQa UpdateVectorLabel err:%v,req:%+v", err, req)
				return err
			}
			if sleepSwitch {
				time.Sleep(time.Duration(sleepMillisecond) * time.Millisecond)
			}
		}
	}
	return nil
}

func (d *BatchUpdateVectorTaskHandler) processDB(ctx context.Context, AppBizIDAndDbTableBizID string) error {
	arr := strings.Split(AppBizIDAndDbTableBizID, "_")
	if len(arr) != 2 {
		logx.E(ctx, "task(BatchUpdateVector) processQa AppBizIDAndDbTableBizID:%v err",
			AppBizIDAndDbTableBizID)
		return nil
	}
	appBizID, dbTableBizID := cast.ToUint64(arr[0]), cast.ToUint64(arr[1])
	// 1.取db信息
	tableFilter := dbentity.TableFilter{
		CorpBizID:    d.p.CorpBizID,
		AppBizID:     appBizID,
		DBTableBizID: dbTableBizID,
	}
	dbTable, err := d.dbLogic.DescribeTable(ctx, &tableFilter)
	// dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, d.p.CorpBizID, appBizID, dbTableBizID)
	if err != nil {
		logx.E(ctx,
			"task(BatchUpdateVector) processDB getDBList err:%v,corpBizID:%v,appBizID:%v,dbTableBizID:%+v",
			err, d.p.CorpBizID, appBizID, dbTableBizID)
		return err
	}
	if dbTable.IsDeleted {
		logx.I(ctx, "task(BatchUpdateVector) processDB skip dbTableBizID:%v", dbTableBizID)
		return nil
	}
	if dbTable.ReleaseStatus != releaseEntity.ReleaseStatusInit && dbTable.ReleaseStatus != releaseEntity.ReleaseStatusSuccess {
		logx.I(ctx, "task(BatchUpdateVector) processDB status no need process, skip dbTableBizID:%v", dbTableBizID)
		return nil
	}

	// 2.更新text2sql 检索入库的信息
	dbFilter := dbentity.DatabaseFilter{
		CorpBizID:     d.p.CorpBizID,
		AppBizID:      appBizID,
		DBSourceBizID: dbTable.DBSourceBizID,
	}
	dbSource, err := d.dbLogic.DescribeDatabase(ctx, &dbFilter)
	// dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, dbTable.CorpBizID, dbTable.AppBizID, dbTable.DBSourceBizID)
	if err != nil {
		logx.E(ctx,
			"task(BatchUpdateVector) processDB getDBSource err:%v,corpBizID:%v,appBizID:%v,dbBizID:%+v",
			err, dbTable.CorpBizID, dbTable.AppBizID, dbTable.DBSourceBizID)
		return err
	}
	app, err := d.rpc.AppAdmin.DescribeAppById(ctx, dbTable.AppBizID)
	if err != nil {
		logx.E(ctx, "task(BatchUpdateVector) processDB DescribeAppById err:%v,appBizID:%v", err,
			dbTable.AppBizID)
		return err
	}
	newCtx := util.SetMultipleMetaData(ctx, app.SpaceId, app.Uin)

	// 3.取数据表关联的标签
	err = d.dbLogic.AddDbTableData2ES1(newCtx, dbSource, app.PrimaryId, dbTableBizID, retrieval.EnvType_Test)
	if err != nil {
		return err
	}
	err = d.dbLogic.AddDbTableData2ES1(newCtx, dbSource, app.PrimaryId, dbTableBizID, retrieval.EnvType_Prod)
	if err != nil {
		return err
	}
	return nil
}

// Done .
func (d *BatchUpdateVectorTaskHandler) Done(ctx context.Context) error {
	logx.I(ctx, "task(BatchUpdateVector) finish")
	return nil
}

// Fail .
func (d *BatchUpdateVectorTaskHandler) Fail(ctx context.Context) error {
	logx.I(ctx, "task(BatchUpdateVector) fail")
	return nil
}

// Stop .
func (d *BatchUpdateVectorTaskHandler) Stop(ctx context.Context) error {
	return nil
}
