package task

import (
	"context"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/db_source"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/vector"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"
	"golang.org/x/sync/errgroup"
)

// BatchUpdateVectorScheduler 批量更新文档/问答向量
type BatchUpdateVectorScheduler struct {
	dao    dao.Dao
	vector *vector.SyncVector
	task   task_scheduler.Task
	p      model.BatchUpdateVector
}

const (
	defaultUpdateSize = 100
	defaultGLimit     = 10
	docVectorPrefix   = "doc:Vector:"
	qaVectorPrefix    = "qa:Vector:"
	dbVectorPrefix    = "db:Vector:"

	DocType     = 1
	QaType      = 2
	DbTableType = 3
)

func initBatchUpdateVectorScheduler() {
	task_scheduler.Register(
		model.BatchUpdateVectorTask,
		func(t task_scheduler.Task, params model.BatchUpdateVector) task_scheduler.TaskHandler {
			d := dao.New()
			return &BatchUpdateVectorScheduler{
				dao:    d,
				vector: vector.NewVectorSync(d.GetDB(), d.GetTdsqlGormDB()),
				task:   t,
				p:      params,
			}
		},
	)
}

// Prepare .
func (b *BatchUpdateVectorScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, b.p.Language)
	docBatchSize := utilConfig.GetMainConfig().Permissions.UpdateDocVectorSize
	if docBatchSize <= 0 {
		docBatchSize = defaultUpdateSize
	}
	qaBatchSize := utilConfig.GetMainConfig().Permissions.UpdateQaVectorSize
	if qaBatchSize <= 0 {
		qaBatchSize = defaultUpdateSize
	}
	dbBatchSize := utilConfig.GetMainConfig().Permissions.UpdateDbVectorSize
	if dbBatchSize <= 0 {
		dbBatchSize = defaultUpdateSize
	}
	kv := make(task_scheduler.TaskKV)
	docIDs, qaIDs, dbTableIDs := make([]string, 0), make([]string, 0), make([]string, 0)
	for knowID, knowData := range b.p.KnowIDs { //按知识库维度传参
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
			idChunksStr, err := jsoniter.MarshalToString(ids)
			if err != nil {
				log.ErrorContextf(ctx, "task(BatchUpdateVector) Prepare|jsoniter.MarshalToString err:%+v", err)
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
			idChunksStr, err := jsoniter.MarshalToString(ids)
			if err != nil {
				log.ErrorContextf(ctx, "task(BatchUpdateVector) Prepare|jsoniter.MarshalToString err:%+v", err)
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
			idChunksStr, err := jsoniter.MarshalToString(ids)
			if err != nil {
				log.ErrorContextf(ctx, "task(BatchUpdateVector) Prepare|jsoniter.MarshalToString err:%+v", err)
				kv = make(task_scheduler.TaskKV) // 重置kv
				return kv, err
			}
			kv[fmt.Sprintf("%s%d", dbVectorPrefix, index)] = idChunksStr
		}
	}
	return kv, nil
}

// Init .
func (b *BatchUpdateVectorScheduler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, b.p.Language)
	return nil
}

// Process .
func (b *BatchUpdateVectorScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(BatchUpdateVector) Process, task: %+v, params: %+v", b.task, b.p)
	docGLimit := utilConfig.GetMainConfig().Permissions.UpdateDocVectorLimit
	if docGLimit <= 0 {
		docGLimit = defaultGLimit
	}
	qaGLimit := utilConfig.GetMainConfig().Permissions.UpdateQaVectorLimit
	if qaGLimit <= 0 {
		qaGLimit = defaultGLimit
	}
	dbGLimit := utilConfig.GetMainConfig().Permissions.UpdateDbVectorLimit
	if dbGLimit <= 0 {
		dbGLimit = defaultGLimit
	}
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(BatchUpdateVector) Start k:%s, v:%s", k, v)
		if strings.HasPrefix(k, docVectorPrefix) { //文档标签更新任务
			value := make([]string, 0)
			err := jsoniter.Unmarshal([]byte(v), &value)
			if err != nil {
				log.ErrorContextf(ctx, "task(BatchUpdateVector) jsoniter.Unmarshal  err:%+v", err)
				return err
			}
			g, gCtx := errgroup.WithContext(ctx)
			g.SetLimit(docGLimit) //并发10个
			for _, v := range value {
				g.Go(func() error {
					log.InfoContextf(gCtx, "task(BatchUpdateVector) processDoc docId:%v", v)
					return b.processDoc(gCtx, v)
				})
			}
			if err := g.Wait(); err != nil {
				log.ErrorContextf(ctx, "task(BatchUpdateVector) processDoc err:%v,value:%v", err, value)
				return err
			}
		} else if strings.HasPrefix(k, qaVectorPrefix) { //问答标签更新任务
			value := make([]string, 0)
			err := jsoniter.Unmarshal([]byte(v), &value)
			if err != nil {
				log.ErrorContextf(ctx, "task(BatchUpdateVector) jsoniter.Unmarshal  err:%+v", err)
				return err
			}
			g, gCtx := errgroup.WithContext(ctx)
			g.SetLimit(qaGLimit) //并发10个
			for _, v := range value {
				g.Go(func() error {
					log.InfoContextf(gCtx, "task(BatchUpdateVector) processQa qaId:%v", v)
					return b.processQa(gCtx, v)
				})
			}
			if err := g.Wait(); err != nil {
				log.ErrorContextf(ctx, "task(BatchUpdateVector) processQa err:%v,value:%v", err, value)
				return err
			}
		} else if strings.HasPrefix(k, dbVectorPrefix) { //数据库标签更新任务
			value := make([]string, 0)
			err := jsoniter.Unmarshal([]byte(v), &value)
			if err != nil {
				log.ErrorContextf(ctx, "task(BatchUpdateVector) jsoniter.Unmarshal  err:%+v", err)
				return err
			}
			g, gCtx := errgroup.WithContext(ctx)
			g.SetLimit(dbGLimit) //并发10个
			for _, v := range value {
				g.Go(func() error {
					log.InfoContextf(gCtx, "task(BatchUpdateVector) processDB dbBizId:%v", v)
					return b.processDB(gCtx, v)
				})
			}
			if err := g.Wait(); err != nil {
				log.ErrorContextf(ctx, "task(BatchUpdateVector) processDB err:%v,value:%v", err, value)
				return err
			}
		}
		if err := progress.Finish(ctx, k); err != nil {
			log.ErrorContextf(ctx, "task(BatchUpdateVector) Finish key:%s,value:%v,err:%+v", k, v, err)
			return err
		}
		log.DebugContextf(ctx, "task(BatchUpdateVector) Finish key:%s", k)
	}
	log.DebugContextf(ctx, "task(BatchUpdateVector) task:%+v,remain:%v", b.task, progress.Remain())
	return nil
}

func (b *BatchUpdateVectorScheduler) processDoc(ctx context.Context, AppBizIDAndDocID string) error {
	arr := strings.Split(AppBizIDAndDocID, "_")
	if len(arr) != 2 {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processDoc AppBizIDAndDocID:%v err", AppBizIDAndDocID)
		return nil
	}
	appBizID, docID := cast.ToUint64(arr[0]), cast.ToUint64(arr[1])
	//1.先取文档信息
	doc, err := b.dao.GetDocByID(ctx, docID, appBizID)
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processDoc GetDocByID err:%v,docID:%v", err, docID)
		return err
	}
	if doc == nil || doc.IsDeleted == model.DocIsDeleted || doc.IsExpire() || doc.IsCharSizeExceeded() ||
		doc.Status == model.DocStatusCreateIndexFail {
		log.InfoContextf(ctx, "task(BatchUpdateVector) processDoc skip doc:%+v", doc)
		return nil
	}
	if doc.Status != model.DocStatusWaitRelease && doc.Status != model.DocStatusReleaseSuccess {
		log.InfoContextf(ctx, "task(BatchUpdateVector) processDoc status no need process, skip doc:%+v", doc)
		return nil
	}
	//2.获取应用信息 得到embedding 版本
	appDB, err := b.dao.GetAppByID(ctx, doc.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processDoc GetAppByID err:%v,docID:%v", docID)
		return err
	}
	if appDB.HasDeleted() {
		log.InfoContextf(ctx, "task(BatchUpdateVector) processDoc skip docID:%v", docID)
		return nil
	}
	ctx = pkg.WithSpaceID(ctx, appDB.SpaceID)
	embeddingConf, _, err := appDB.GetEmbeddingConf()
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processDoc GetEmbeddingConf err:%v,appId:%v", err, appDB.ID)
		return err
	}
	embeddingVersion := embeddingConf.Version
	//3.获取doc需要分类和角色向量
	hasRole := false
	vectorLabels, err := getDocCateAndRoleLabels(ctx, appDB.BusinessID, doc, b.dao)
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processDoc getDocVectorLabels err:%v,docID:%v", err, docID)
		return err
	}
	for _, v := range vectorLabels {
		if v.Name == utilConfig.GetMainConfig().Permissions.RoleRetrievalKey {
			hasRole = true
		}
		log.DebugContextf(ctx, "task(BatchUpdateVector) processDoc docID:%v,vectorLabel:%+v", docID, v)
	}
	if !hasRole { //如果角色为空，赋值为空
		vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
			Name:  utilConfig.GetMainConfig().Permissions.RoleRetrievalKey,
			Value: "",
		})
	}
	//4.获取评测端 文档切片
	segIDs, startID, limit := make([]uint64, 0), uint64(0), uint64(utilConfig.GetMainConfig().Permissions.GetSegmentLimit)
	for {
		segs, lastID, err := dao.GetDocSegmentDao().GetSegmentByDocID(ctx, appDB.ID, doc.ID, startID, limit,
			[]string{dao.DocSegmentTblColID, dao.DocSegmentTblColType, dao.DocSegmentTblColSegmentType, dao.DocSegmentTblColPageContent})
		if err != nil {
			log.ErrorContextf(ctx, "FullUpdateLabel processTestDoc getDocNotDeleteSegment err:%v,docID:%v", err, doc.ID)
			return err
		}
		if len(segs) == 0 {
			break
		}
		startID = lastID
		//5.写评测端 es和向量
		segmentMap, text2sqlMap := make([]uint64, 0), make(map[uint32][]uint64, 0)
		for _, segment := range segs {
			segIDs = append(segIDs, segment.ID)
			if segment.IsSegmentForQA() || segment.SegmentType == model.SegmentTypeText2SQLMeta {
				continue
			}
			if segment.SegmentType == model.SegmentTypeText2SQLContent { //是表格需要获取列数
				cells, err := getColumnsCount(ctx, segment.PageContent)
				if err != nil {
					log.ErrorContextf(ctx, "task(BatchUpdateVector) processDoc getColumnsCount err:%v,segmentID:%vcontent:%v",
						err, segment.ID, segment.PageContent)
					continue
				}
				text2sqlMap[cells] = append(text2sqlMap[cells], segment.ID)
			} else {
				segmentMap = append(segmentMap, segment.ID)
			}
		}
		if err = batchUpdateSegVector(ctx, appDB.ID, appDB.BusinessID, embeddingVersion, retrieval.EnvType_Test,
			vectorLabels, segmentMap, text2sqlMap); err != nil {
			log.ErrorContextf(ctx, "task(BatchUpdateVector) processDoc batchUpdateSegVector err:%v,segmentMap:%v,text2sqlMap:%v",
				err, segmentMap, text2sqlMap)
			return err
		}
	}
	if appDB.IsShared { //共享知识库不需要双写
		return nil
	}
	//6.如果是分类发起的，只需要写评测端，更新文档和文档切片状态为待发布
	if b.p.Type == model.UpdateVectorByCate {
		err = dao.GetDocDao().UpdateDocWaitRelease(ctx, appDB.ID, doc.ID, segIDs)
		if err != nil {
			log.ErrorContextf(ctx, "task(BatchUpdateVector) processDoc BatchUpdateSegment err:%v,docID:%v,segIDs:%v",
				err, docID, segIDs)
			return err
		}
		return nil
	}
	//7.其他情况 双写发布端es和向量
	//获取发布端文档切片
	if appDB.QAVersion == 0 { //默认知识库未发布不需要双写
		return nil
	}
	startID = 0
	for {
		prodSegs, lastID, err := dao.GetRetrievalNodeDao().GetSegmentNodeByDocID(ctx, appDB.ID, docID, startID, limit,
			[]string{dao.NodeTblColId, dao.NodeTblColRelatedId, dao.NodeTblColPageContent, dao.NodeTblColSegmentType})
		if err != nil {
			log.ErrorContextf(ctx, "FullUpdateLabel processProdDoc GetNodeIdsList err:%v,docID:%v", err, docID)
			return err
		}
		if len(prodSegs) == 0 {
			break
		}
		startID = lastID
		segmentMap, text2sqlMap := make([]uint64, 0), make(map[uint32][]uint64, 0)
		for _, segment := range prodSegs {
			if segment.SegmentType == model.SegmentTypeText2SQLMeta {
				continue
			}
			if segment.SegmentType == model.SegmentTypeText2SQLContent { //是表格需要获取列数
				cells, err := getColumnsCount(ctx, segment.PageContent)
				if err != nil {
					log.ErrorContextf(ctx, "task(BatchUpdateVector) processDoc getColumnsCount err:%v,segmentID:%vcontent:%v",
						err, segment.ID, segment.PageContent)
					continue
				}
				text2sqlMap[cells] = append(text2sqlMap[cells], segment.RelatedID)
			} else {
				segmentMap = append(segmentMap, segment.RelatedID)
			}
		}
		if err = batchUpdateSegVector(ctx, appDB.ID, appDB.BusinessID, embeddingVersion, retrieval.EnvType_Prod,
			vectorLabels, segmentMap, text2sqlMap); err != nil {
			log.ErrorContextf(ctx, "task(BatchUpdateVector) processDoc batchUpdateSegVector err:%v,segmentMap:%v,text2sqlMap:%v",
				err, segmentMap, text2sqlMap)
			return err
		}
	}
	return nil
}

// batchUpdateSegVector 批量写标签
func batchUpdateSegVector(ctx context.Context, appID, appBizID, embeddingVersion uint64, envType retrieval.EnvType,
	vectorLabels []*retrieval.VectorLabel, segmentMap []uint64, text2sqlMap map[uint32][]uint64) error {
	sleepSwitch, sleepMillisecond := utilConfig.GetMainConfig().Permissions.UpdateVectorSleepSwitch,
		utilConfig.GetMainConfig().Permissions.UpdateVectorSleepMillisecond
	if len(segmentMap) > 0 {
		for _, ids := range slicex.Chunk(segmentMap, defaultUpdateSize) { //普通切片,每批100个
			req := &retrieval.UpdateLabelReq{
				RobotId:          appID,
				AppBizId:         appBizID,
				EnvType:          envType,
				IndexId:          model.SegmentReviewVersionID,
				Ids:              ids, //切片的主键id
				DocType:          model.DocTypeSegment,
				EmbeddingVersion: embeddingVersion,
				Labels:           vectorLabels,
				SegmentType:      model.SegmentTypeSegment,
			}
			_, err := client.UpdateVectorLabel(ctx, req)
			if err != nil {
				log.ErrorContextf(ctx, "task(BatchUpdateVector) processDoc segment UpdateVectorLabel err:%v,req:%+v", err, req)
				return err
			}
			if sleepSwitch {
				time.Sleep(time.Duration(sleepMillisecond) * time.Millisecond)
			}
		}
	}
	for cell, segList := range text2sqlMap { //表格切片,按列数分组,每批100个
		if len(segList) == 0 {
			continue
		}
		for _, ids := range slicex.Chunk(segList, defaultUpdateSize) {
			req := &retrieval.UpdateLabelReq{
				RobotId:          appID,
				AppBizId:         appBizID,
				EnvType:          envType,
				IndexId:          model.SegmentReviewVersionID,
				Ids:              ids, //切片的主键id
				DocType:          model.DocTypeSegment,
				EmbeddingVersion: embeddingVersion,
				Labels:           vectorLabels,
				SegmentType:      model.SegmentTypeText2SQLContent,
				ColumnsCount:     cell,
			}
			_, err := client.UpdateVectorLabel(ctx, req)
			if err != nil {
				log.ErrorContextf(ctx, "task(BatchUpdateVector) processDoc text2sql UpdateVectorLabel err:%v,req:%+v", err, req)
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
	content := model.Text2SQLSegmentContent{}
	err := jsoniter.Unmarshal([]byte(pageContent), &content)
	if err != nil {
		log.ErrorContextf(ctx, "getColumnsCount|Unmarshal|pageContent:%+v|err:%+v", pageContent, err)
		return 0, err
	}
	return uint32(len(content.Cells)), nil
}

func (b *BatchUpdateVectorScheduler) processQa(ctx context.Context, AppBizIDAndQaID string) error {
	arr := strings.Split(AppBizIDAndQaID, "_")
	if len(arr) != 2 {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processQa AppBizIDAndQaID:%v err", AppBizIDAndQaID)
		return nil
	}
	qaID := cast.ToUint64(arr[1])
	//1.先取问答信息
	qa, err := b.dao.GetQAByID(ctx, qaID)
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processQa GetQAByID err:%v,qaID:%v", err, qaID)
		return err
	}
	if qa == nil || qa.IsDeleted == model.QAIsDeleted || !qa.IsAccepted() || qa.IsExpire() || qa.IsCharExceeded() {
		log.InfoContextf(ctx, "task(BatchUpdateVector) processQa skip qaID:%v", qaID)
		return nil
	}
	if qa.Status() != model.QAReleaseStatusInit && qa.ReleaseStatus != model.QAReleaseStatusSuccess {
		log.InfoContextf(ctx, "task(BatchUpdateVector) processQa status no need process, skip qaID:%v", qaID)
		return nil
	}
	//2.获取应用信息 得到embedding 版本
	appDB, err := b.dao.GetAppByID(ctx, qa.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processQa GetAppByID err:%v,qaID:%v", qaID)
		return err
	}
	if appDB.HasDeleted() {
		log.InfoContextf(ctx, "task(BatchUpdateVector) processQa skip qaID:%v", qaID)
		return nil
	}
	ctx = pkg.WithSpaceID(ctx, appDB.SpaceID)
	embeddingConf, _, err := appDB.GetEmbeddingConf()
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processQa GetEmbeddingConf err:%v,appId:%v,qaID:%v", err, appDB.ID, qaID)
		return err
	}
	embeddingVersion := embeddingConf.Version
	//3.获取qa分类和角色标签
	vectorLabels, err := b.vector.GetQACateAndRoleLabels(ctx, appDB.BusinessID, qa)
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processQa GetQACateAndRoleLabels err:%v,qaID:%v", err, qaID)
		return err
	}
	hasRole := false
	for _, v := range vectorLabels {
		if v.Name == utilConfig.GetMainConfig().Permissions.RoleRetrievalKey {
			hasRole = true
		}
		log.DebugContextf(ctx, "task(BatchUpdateVector) processQa qaID:%v,vectorLabel:%+v", qaID, v)
	}
	if !hasRole { //如果角色为空，赋值为空
		vectorLabels = append(vectorLabels, &retrieval.VectorLabel{
			Name:  utilConfig.GetMainConfig().Permissions.RoleRetrievalKey,
			Value: "",
		})
	}
	//4.获取qa的相似问
	similarQuestions, err := b.dao.GetSimilarQuestionsByQA(ctx, qa)
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processQa GetSimilarQuestionsByQA err:%v,qaID:%v", err, qaID)
		return err
	}
	//5.写评测端es和向量
	//标准问需要双写两个向量库
	req := &retrieval.UpdateLabelReq{
		RobotId:          qa.RobotID,
		AppBizId:         appDB.BusinessID,
		EnvType:          retrieval.EnvType_Test,
		IndexId:          model.ReviewVersionID,
		Ids:              []uint64{qa.ID},
		DocType:          model.DocTypeQA,
		QaType:           model.QATypeStandard,
		EmbeddingVersion: embeddingVersion,
		Labels:           vectorLabels,
	}
	_, err = client.UpdateVectorLabel(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processQa UpdateVectorLabel err:%v,req:%+v", err, req)
		return err
	}
	req = &retrieval.UpdateLabelReq{
		RobotId:          qa.RobotID,
		AppBizId:         appDB.BusinessID,
		EnvType:          retrieval.EnvType_Test,
		IndexId:          model.SimilarVersionID,
		Ids:              []uint64{qa.ID},
		DocType:          model.DocTypeQA,
		QaType:           model.QATypeStandard,
		EmbeddingVersion: embeddingVersion,
		Labels:           vectorLabels,
	}
	_, err = client.UpdateVectorLabel(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processQa UpdateVectorLabel err:%v,req:%+v", err, req)
		return err
	}
	sleepSwitch, sleepMillisecond := utilConfig.GetMainConfig().Permissions.UpdateVectorSleepSwitch,
		utilConfig.GetMainConfig().Permissions.UpdateVectorSleepMillisecond
	simBizIDs := make([]uint64, 0, len(similarQuestions))
	if len(similarQuestions) > 0 {
		for _, sims := range slicex.Chunk(similarQuestions, defaultUpdateSize) {
			tmp := make([]uint64, 0, defaultUpdateSize)
			for _, v := range sims {
				tmp = append(tmp, v.SimilarID)
				simBizIDs = append(simBizIDs, v.SimilarID)
			}
			req := &retrieval.UpdateLabelReq{
				RobotId:          qa.RobotID,
				AppBizId:         appDB.BusinessID,
				EnvType:          retrieval.EnvType_Test,
				IndexId:          model.ReviewVersionID,
				Ids:              tmp, //相似问业务id
				DocType:          model.DocTypeQA,
				QaType:           model.QATypeSimilar,
				EmbeddingVersion: embeddingVersion,
				Labels:           vectorLabels,
			}
			_, err = client.UpdateVectorLabel(ctx, req)
			if err != nil {
				log.ErrorContextf(ctx, "task(BatchUpdateVector) processQa UpdateVectorLabel err:%v,req:%+v", err, req)
				return err
			}
			if sleepSwitch {
				time.Sleep(time.Duration(sleepMillisecond) * time.Millisecond)
			}
		}
	}
	if appDB.IsShared { //共享知识库不需要双写
		return nil
	}
	//6.如果是分类发起的，只需要写评测端，更新标准问和相似问状态为待发布
	if b.p.Type == model.UpdateVectorByCate {
		err = dao.GetDocQaDao().UpdateQAWaitRelease(ctx, appDB.ID, qaID, simBizIDs)
		if err != nil {
			log.ErrorContextf(ctx, "task(BatchUpdateVector) processQa UpdateQAWaitRelease err:%v,qaID:%v,simBizIDs:%v",
				err, qaID, simBizIDs)
			return err
		}
		return nil
	}
	//7.其他情况 双写发布端es和向量
	//获取发布端qa的相似问
	if appDB.QAVersion == 0 { //默认知识库未发布不需要双写
		return nil
	}
	prodSims, err := dao.GetRetrievalNodeDao().GetNodeIdsList(ctx, appDB.ID,
		[]string{dao.NodeTblColId, dao.NodeTblColRelatedId},
		&dao.RetrievalNodeFilter{
			APPID:    appDB.ID,
			DocType:  model.DocTypeQA,
			ParentID: qaID,
		})
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processQa GetNodeIdsList err:%v,qaID:%v", err, qaID)
		return err
	}
	req = &retrieval.UpdateLabelReq{
		RobotId:          qa.RobotID,
		AppBizId:         appDB.BusinessID,
		EnvType:          retrieval.EnvType_Prod,
		IndexId:          model.ReviewVersionID,
		Ids:              []uint64{qa.ID},
		DocType:          model.DocTypeQA,
		QaType:           model.QATypeStandard,
		EmbeddingVersion: embeddingVersion,
		Labels:           vectorLabels,
	}
	_, err = client.UpdateVectorLabel(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processQa UpdateVectorLabel err:%v,req:%+v", err, req)
		return err
	}
	req = &retrieval.UpdateLabelReq{
		RobotId:          qa.RobotID,
		AppBizId:         appDB.BusinessID,
		EnvType:          retrieval.EnvType_Prod,
		IndexId:          model.SimilarVersionID,
		Ids:              []uint64{qa.ID},
		DocType:          model.DocTypeQA,
		QaType:           model.QATypeStandard,
		EmbeddingVersion: embeddingVersion,
		Labels:           vectorLabels,
	}
	_, err = client.UpdateVectorLabel(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processQa UpdateVectorLabel err:%v,req:%+v", err, req)
		return err
	}
	if len(prodSims) > 0 {
		for _, sims := range slicex.Chunk(prodSims, defaultUpdateSize) {
			tmp := make([]uint64, 0, defaultUpdateSize)
			for _, v := range sims {
				tmp = append(tmp, v.RelatedID)
			}
			req := &retrieval.UpdateLabelReq{
				RobotId:          qa.RobotID,
				AppBizId:         appDB.BusinessID,
				EnvType:          retrieval.EnvType_Prod,
				IndexId:          model.ReviewVersionID,
				Ids:              tmp, //相似问业务id
				DocType:          model.DocTypeQA,
				QaType:           model.QATypeSimilar,
				EmbeddingVersion: embeddingVersion,
				Labels:           vectorLabels,
			}
			_, err = client.UpdateVectorLabel(ctx, req)
			if err != nil {
				log.ErrorContextf(ctx, "task(BatchUpdateVector) processQa UpdateVectorLabel err:%v,req:%+v", err, req)
				return err
			}
			if sleepSwitch {
				time.Sleep(time.Duration(sleepMillisecond) * time.Millisecond)
			}
		}
	}
	return nil
}

func (b *BatchUpdateVectorScheduler) processDB(ctx context.Context, AppBizIDAndDbTableBizID string) error {
	arr := strings.Split(AppBizIDAndDbTableBizID, "_")
	if len(arr) != 2 {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processQa AppBizIDAndDbTableBizID:%v err", AppBizIDAndDbTableBizID)
		return nil
	}
	appBizID, dbTableBizID := cast.ToUint64(arr[0]), cast.ToUint64(arr[1])
	//1.取db信息
	dbTable, err := dao.GetDBTableDao().GetByBizID(ctx, b.p.CorpBizID, appBizID, dbTableBizID)
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processDB getDBList err:%v,corpBizID:%v,appBizID:%v,dbTableBizID:%+v",
			err, b.p.CorpBizID, appBizID, dbTableBizID)
		return err
	}
	if dbTable.IsDeleted == true {
		log.InfoContextf(ctx, "task(BatchUpdateVector) processDB skip dbTableBizID:%v", dbTableBizID)
		return nil
	}

	if dbTable.ReleaseStatus != model.ReleaseStatusUnreleased && dbTable.ReleaseStatus != model.ReleaseStatusReleased {
		log.InfoContextf(ctx, "task(BatchUpdateVector) processDB status no need process, skip dbTableBizID:%v", dbTableBizID)
		return nil
	}

	// 2.更新text2sql 检索入库的信息
	dbSource, err := dao.GetDBSourceDao().GetByBizID(ctx, dbTable.CorpBizID, dbTable.AppBizID, dbTable.DBSourceBizID)
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processDB getDBSource err:%v,corpBizID:%v,appBizID:%v,dbBizID:%+v",
			err, dbTable.CorpBizID, dbTable.AppBizID, dbTable.DBSourceBizID)
		return err
	}
	app, err := client.GetAppInfo(ctx, dbTable.AppBizID, 1)
	if err != nil {
		log.ErrorContextf(ctx, "task(BatchUpdateVector) processDB getApp err:%v,appBizID:%v", err, dbTable.AppBizID)
		return err
	}
	ctx = pkg.WithSpaceID(ctx, app.GetSpaceId())

	//3.取数据表关联的标签
	err = db_source.AddDbTableData2ES1(ctx, dbSource, app.Id, dbTableBizID, retrieval.EnvType_Test)
	if err != nil {
		return err
	}
	err = db_source.AddDbTableData2ES1(ctx, dbSource, app.Id, dbTableBizID, retrieval.EnvType_Prod)
	if err != nil {
		return err
	}
	return nil
}

// Done .
func (b *BatchUpdateVectorScheduler) Done(ctx context.Context) error {
	log.InfoContextf(ctx, "task(BatchUpdateVector) finish")
	return nil
}

// Fail .
func (b *BatchUpdateVectorScheduler) Fail(ctx context.Context) error {
	log.InfoContextf(ctx, "task(BatchUpdateVector) fail")
	return nil
}

// Stop .
func (b *BatchUpdateVectorScheduler) Stop(ctx context.Context) error {
	return nil
}
