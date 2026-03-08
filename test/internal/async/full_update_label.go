package async

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao/vector"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/logic/app"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
	"golang.org/x/sync/errgroup"
)

var syncLabelCache app.UpgradeCache

// FullUpdateLabelTaskHandler 全量刷数据标签
type FullUpdateLabelTaskHandler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity.FullUpdateLabel
}

func registerFullUpdateLabelTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.FullUpdateLabelTask,
		func(t task_scheduler.Task, params entity.FullUpdateLabel) task_scheduler.TaskHandler {
			return &FullUpdateLabelTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
	syncLabelCache.UpgradeType = app.SyncVectorLabel
	syncLabelCache.ExpiredTimeS = 30 * 24 * 3600
	syncLabelCache.Rdb = tc.adminRdb
}

// Prepare .
func (d *FullUpdateLabelTaskHandler) Prepare(ctx context.Context) (kv task_scheduler.TaskKV, err error) {
	logx.I(ctx, "FullUpdateLabel Prepare, req:%+v", d.p)
	// 按应用维度刷数据
	kv = make(task_scheduler.TaskKV)
	if len(d.p.AppIDs) == 0 {
		// appIDs, err := dao.GetRobotDao().GetAllValidAppIDs(ctx, 0)
		apps, _, err := d.rpc.AppAdmin.ListAllAppBaseInfo(ctx, nil)
		if err != nil {
			logx.E(ctx, "FullUpdateLabel ListAllAppBaseInfo err:%v,req:%+v", err, d.p)
			return nil, err
		}
		appIDs := slicex.Pluck(apps, func(v *entity.AppBaseInfo) uint64 { return v.PrimaryId })
		pendingIDs, err := syncLabelCache.GetNotUpgradedApps(ctx, appIDs)
		if err != nil {
			logx.E(ctx, "FullUpdateLabel GetNotUpgradedApps err:%v", err)
			return nil, err
		}
		for _, id := range pendingIDs {
			if d.p.MaxID != 0 && id >= d.p.MaxID {
				continue
			}
			kv[cast.ToString(id)] = ""
		}
		return kv, nil

	}
	appListReq := appconfig.ListAppBaseInfoReq{
		AppPrimaryIds: d.p.AppIDs,
		PageNumber:    1,
		PageSize:      uint32(len(d.p.AppIDs)),
	}
	apps, _, err := d.rpc.AppAdmin.ListAppBaseInfo(ctx, &appListReq)
	// apps, err := dao.GetRobotDao().GetAppList(ctx, []string{dao.RobotTblColId}, &dao.RobotFilter{
	// 	IDs:            d.p.AppIDs,
	// 	Limit:          uint32(len(d.p.AppIDs)),
	// 	OrderColumn:    []string{dao.RobotTblColId},
	// 	OrderDirection: []string{dao.SqlOrderByAsc},
	// 	IsDeleted:      ptrx.Int(util.IsNotDeleted),
	// })
	if err != nil {
		logx.W(ctx, "FullUpdateLabel getAppList err:%v,req:%+v", err, d.p)
		return nil, err
	}
	if len(apps) == 0 {
		logx.I(ctx, "FullUpdateLabel get apps empty,req:%+v", d.p)
		return nil, nil
	}
	for _, app := range apps {
		kv[cast.ToString(app.PrimaryId)] = ""
	}
	return kv, nil
}

// Init .
func (d *FullUpdateLabelTaskHandler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	return nil
}

// Process .
func (d *FullUpdateLabelTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	for k := range progress.TaskKV(ctx) {
		if !config.GetMainConfig().Permissions.UpdateVectorSwitch {
			return errx.New(1000, "停止任务")
		}
		startTime := time.Now()
		logx.I(ctx, "FullUpdateLabel Process,k:%v", k)
		robotID := cast.ToUint64(k)
		// 1.获取应用信息 得到embedding 版本
		appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, robotID)
		if err != nil {
			logx.E(ctx, "FullUpdateLabel DescribeAppByPrimaryIdWithoutNotFoundError err:%v,robotID:%v", err, robotID)
			continue
		}
		if appDB.HasDeleted() { // 已经删除,直接跳过
			err := syncLabelCache.SetAppFinish(ctx, robotID)
			if err != nil {
				logx.E(ctx, "FullUpdateLabel redis hset err:%v,k:%v", err, k)
			}
			if err := progress.Finish(ctx, k); err != nil {
				logx.E(ctx, "FullUpdateLabel Finish key:%s,err:%+v", k, err)
				return err
			}
			logx.I(ctx, "FullUpdateLabel Finish key:%s,cost:%v", k, time.Since(startTime))
			return nil
		}

		embeddingVersion := appDB.Embedding.Version
		embeddingName := ""
		embeddingName, err =
			d.kbLogic.GetKnowledgeEmbeddingModel(ctx, appDB.CorpBizId, appDB.BizId, appDB.BizId, appDB.IsShared)

		if err != nil {
			if err != nil {
				logx.W(ctx, "task(FullUpdateDBLabel) GetShareKnowledgeBaseConfig err:%+v", err)
			}
		}
		if embeddingName != "" {
			embeddingVersion = entity.GetEmbeddingVersion(embeddingName)
		}

		logx.I(ctx, "task(FullUpdateLabelTaskHandler) "+
			" embeddingModelName:%s, using embeddingVersion:%d", embeddingName, embeddingVersion)

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(3)
		g.Go(func() error { // 问答
			err1 := d.processQa(gCtx, appDB, embeddingVersion, embeddingName)
			if err1 != nil {
				logx.E(gCtx, "FullUpdateLabel processQa err:%v,robotID:%v", err1, robotID)
				return err1
			}
			logx.I(gCtx, "FullUpdateLabel processQa key:%s,cost:%v", k, time.Since(startTime))
			return nil
		})
		g.Go(func() error { // 评测端文档
			docList, err2 := d.docLogic.GetDocList(gCtx, docEntity.DocTblColList, &docEntity.DocFilter{
				RobotId:   robotID,
				IsDeleted: ptrx.Bool(false),
			})
			if err2 != nil {
				logx.E(gCtx, "FullUpdateLabel GetDocList err:%v,robotID:%v", err2, robotID)
				return err2
			}
			for _, doc := range docList {
				err2 = d.processTestDoc(gCtx, appDB, embeddingVersion, embeddingName, doc)
				if err2 != nil {
					logx.E(gCtx, "FullUpdateLabel processTestDoc err:%v,robotID:%v,docID:%+v", err2, robotID, doc.ID)
					return err2
				}
			}
			logx.I(gCtx, "FullUpdateLabel processTestDoc key:%s,cost:%v", k, time.Since(startTime))
			return nil
		})
		g.Go(func() error { // 发布端文档
			nodeList, err3 := d.qaLogic.GetVectorSyncLogic().GetDocNodeList(gCtx, robotID)
			if err3 != nil {
				logx.E(gCtx, "FullUpdateLabel GetNodeIdsList err:%v,robotID:%v", err3, robotID)
				return err3
			}
			for _, node := range nodeList {
				err3 = d.processProdDoc(gCtx, appDB, embeddingVersion, embeddingName, node.DocID)
				if err3 != nil {
					logx.E(gCtx, "FullUpdateLabel processProdDoc err:%v,robotID:%v,docID:%+v", err3, robotID, node.DocID)
					return err3
				}
			}
			logx.I(gCtx, "FullUpdateLabel processProdDoc key:%s,cost:%v", k, time.Since(startTime))
			return nil
		})
		if err := g.Wait(); err != nil { // 柔性放过
			logx.E(ctx, "FullUpdateLabel Process err:%v,robotID:%v", err, robotID)
		} else {
			err := syncLabelCache.SetAppFinish(ctx, robotID)
			if err != nil {
				logx.E(ctx, "FullUpdateLabel redis hset err:%v,k:%v", err, k)
			}
		}
		if err := progress.Finish(ctx, k); err != nil {
			logx.E(ctx, "FullUpdateLabel Finish key:%s,err:%+v", k, err)
			return err
		}
		logx.I(ctx, "FullUpdateLabel Finish key:%s,cost:%v", k, time.Since(startTime))
	}
	return nil
}

func (d *FullUpdateLabelTaskHandler) processQa(ctx context.Context, appDB *entity.App, embeddingVersion uint64, embeddingModel string) error {
	robotID := appDB.PrimaryId
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(3)
	var err1, err2, err3 error
	qaList, nodeList, cateList := make([]*qaEntity.DocQA, 0), make([]*entity.RetrievalNodeInfo, 0),
		make([]*cateEntity.CateInfo, 0)
	g.Go(func() error { // 获取评测端所有问答
		qaList, err1 = d.qaLogic.GetAllDocQas(gCtx, qaEntity.DocQaTblColList, &qaEntity.DocQaFilter{
			RobotId:   robotID,
			IsDeleted: ptrx.Uint32(qaEntity.QAIsNotDeleted),
		})
		if err1 != nil {
			logx.E(gCtx, "FullUpdateLabel processQa GetAllDocQas err:%v,robotID:%v", err1, robotID)
			return err1
		}
		return nil
	})
	g.Go(func() error { // 获取发布端所有节点信息
		nodeList, err2 = d.qaLogic.GetVectorSyncLogic().GetNodeIdsList(gCtx, robotID,
			[]string{vector.NodeTblColId, vector.NodeTblColRelatedId, vector.NodeTblColParentId},
			&entity.RetrievalNodeFilter{APPID: robotID, DocType: entity.DocTypeQA})
		if err2 != nil {
			logx.E(gCtx, "FullUpdateLabel processQa GetNodeIdsList err:%v,robotID:%v", err2, robotID)
			return err2
		}
		return nil
	})
	g.Go(func() error { // 获取应用所有未删除分类
		cateList, err3 = d.cateLogic.DescribeCateList(gCtx, cateEntity.QACate, appDB.CorpPrimaryId, robotID)
		if err3 != nil {
			logx.E(gCtx, "FullUpdateLabel processQa DescribeCateList err:%v,robotID:%v", err3, robotID)
			return err3
		}
		return nil
	})
	if err := g.Wait(); err != nil { // 柔性放过
		logx.E(ctx, "FullUpdateLabel Process err:%v,robotID:%v", err, robotID)
		return err
	}
	// 初始化映射关系
	cateByID := make(map[uint64]uint64, len(cateList))
	testQaByCate := make(map[uint64][]uint64, len(cateList))
	testCateByQaID := make(map[uint64]uint64, len(qaList))
	prodQaByCate := make(map[uint64][]uint64, len(cateList))
	prodSimByQa := make(map[uint64][]uint64, len(qaList))
	// 构建分类映射
	for _, v := range cateList {
		cateByID[v.ID] = v.BusinessID
	}
	// 构建评测端问答映射
	for _, v := range qaList {
		cateBizID := cateByID[v.CategoryID] // 如果是全部分类，赋值为0
		testCateByQaID[v.ID] = cateBizID
		if !v.IsAccepted() || v.IsExpire() || v.IsCharExceeded() {
			continue
		}
		testQaByCate[cateBizID] = append(testQaByCate[cateBizID], v.ID)
	}
	// 处理发布端节点数据
	deleteQaList := make([]uint64, 0, len(nodeList))
	for _, v := range nodeList {
		if v.RelatedID < 100000000000 { // 标准问
			if cateBizID, ok := testCateByQaID[v.RelatedID]; ok {
				prodQaByCate[cateBizID] = append(prodQaByCate[cateBizID], v.RelatedID)
			} else { // 发布端还有，评测端已删除的问答，需要再次获取
				deleteQaList = append(deleteQaList, v.RelatedID)
			}
		} else { // 相似问
			prodSimByQa[v.ParentID] = append(prodSimByQa[v.ParentID], v.RelatedID)
		}
	}
	// 处理已删除未发布的问答
	if len(deleteQaList) > 0 {
		for _, ids := range slicex.Chunk(deleteQaList, 200) {
			tmp, err := d.qaLogic.GetQADetails(ctx, appDB.CorpPrimaryId, robotID, ids)
			if err != nil { // 柔性放过
				logx.E(ctx, "FullUpdateLabel processQa get deleteQaList err:%v,robotID:%v,ids:%v", err, robotID, ids)
				continue
			}
			for qaID, qa := range tmp {
				if cateBizID, ok := cateByID[qa.CategoryID]; ok { // 发布端问答的分类已经被删除了，直接过滤，不需要更新标签
					prodQaByCate[cateBizID] = append(prodQaByCate[cateBizID], qaID)
				}
			}
		}
	}
	// 4.双写两端的es和向量
	sleepSwitch, sleepMillisecond := config.GetMainConfig().Permissions.UpdateVectorSleepSwitch,
		config.GetMainConfig().Permissions.UpdateVectorSleepMillisecond
	updateFunc := func(newCtx context.Context, envType retrieval.EnvType, indexID uint64, qaType uint32, qaIDs []uint64, labels []*retrieval.VectorLabel) error {
		req := &retrieval.UpdateLabelReq{
			RobotId:            robotID,
			AppBizId:           appDB.BizId,
			EnvType:            envType,
			IndexId:            indexID,
			Ids:                qaIDs,
			DocType:            entity.DocTypeQA,
			QaType:             qaType,
			EmbeddingModelName: embeddingModel,
			EmbeddingVersion:   embeddingVersion,

			Labels: labels,
		}
		if _, err := d.rpc.RetrievalDirectIndex.UpdateVectorLabel(newCtx, req); err != nil {
			logx.E(newCtx, "FullUpdateLabel processQa update %s err:%v,req:%+v", envType, err, req)
			return err
		}
		if sleepSwitch {
			time.Sleep(time.Duration(sleepMillisecond) * time.Millisecond)
		}
		return nil
	}
	g, gCtx = errgroup.WithContext(ctx)
	g.SetLimit(2)
	g.Go(func() error { // 写评测端es和向量
		for casteBizID, testQa := range testQaByCate {
			labels := []*retrieval.VectorLabel{
				{
					Name:  config.GetMainConfig().Permissions.CateRetrievalKey,
					Value: cast.ToString(casteBizID),
				},
			}
			for _, qaIDs := range slicex.Chunk(testQa, defaultUpdateSize) {
				// 双写评测端标准问
				if err := updateFunc(gCtx, retrieval.EnvType_Test, entity.ReviewVersionID, entity.QATypeStandard, qaIDs, labels); err != nil {
					return err
				}
				if err := updateFunc(gCtx, retrieval.EnvType_Test, entity.SimilarVersionID, entity.QATypeStandard, qaIDs, labels); err != nil {
					return err
				}
				// 获取100条问答的相似问
				simMap, err := d.qaLogic.GetSimilarQuestionsSimpleByQAIDs(gCtx, appDB.CorpPrimaryId, robotID, qaIDs)
				if err != nil {
					logx.E(gCtx, "FullUpdateLabel processQa Get sim err:%v,robotID:%v,qaIDS:%v", err, robotID, qaIDs)
					return err
				}
				simBizIDs := make([]uint64, 0)
				for _, sims := range simMap {
					for _, v := range sims {
						simBizIDs = append(simBizIDs, v.SimilarID)
					}
				}
				for _, simIDs := range slicex.Chunk(simBizIDs, defaultUpdateSize) {
					if err := updateFunc(gCtx, retrieval.EnvType_Test, entity.ReviewVersionID, entity.QATypeSimilar, simIDs, labels); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
	g.Go(func() error { // 写发布端es和向量
		for casteBizID, prodQa := range prodQaByCate {
			labels := []*retrieval.VectorLabel{
				{
					Name:  config.GetMainConfig().Permissions.CateRetrievalKey,
					Value: cast.ToString(casteBizID),
				},
			}
			for _, prodIDs := range slicex.Chunk(prodQa, defaultUpdateSize) {
				// 双写发布端标准问
				if err := updateFunc(gCtx, retrieval.EnvType_Prod, entity.ReviewVersionID, entity.QATypeStandard, prodIDs, labels); err != nil {
					return err
				}
				if err := updateFunc(gCtx, retrieval.EnvType_Prod, entity.SimilarVersionID, entity.QATypeStandard, prodIDs, labels); err != nil {
					return err
				}
				// 取100条发布端标准问的相似问
				sims := make([]uint64, 0)
				for _, prodID := range prodIDs {
					sims = append(sims, prodSimByQa[prodID]...)
				}
				for _, simIDs := range slicex.Chunk(sims, defaultUpdateSize) {
					if err := updateFunc(gCtx, retrieval.EnvType_Prod, entity.ReviewVersionID, entity.QATypeSimilar, simIDs, labels); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		logx.E(ctx, "FullUpdateLabel Process UpdateVectorLabel err:%v,robotID:%v", err, robotID)
		return err
	}
	return nil
}

func (d *FullUpdateLabelTaskHandler) processTestDoc(ctx context.Context, appDB *entity.App, embeddingVersion uint64, embeddingModel string,
	doc *docEntity.Doc) (err error) {
	if doc == nil || doc.IsDeleted || doc.IsExpire() || doc.IsCharSizeExceeded() ||
		doc.Status == docEntity.DocStatusCreateIndexFail {
		return nil
	}
	// 1.获取doc分类
	cateInfo := &cateEntity.CateInfo{}
	if doc.CategoryID > 0 {
		cateInfo, err = d.cateLogic.DescribeCateByID(ctx, cateEntity.DocCate, uint64(doc.CategoryID), doc.CorpID, doc.RobotID)
		if err != nil {
			logx.E(ctx, "FullUpdateLabel processTestDoc getCateInfo err:%v,docID:%+v", err, doc.ID)
			return err
		}
	}
	labels := make([]*retrieval.VectorLabel, 0)
	labels = append(labels, &retrieval.VectorLabel{
		Name:  config.GetMainConfig().Permissions.CateRetrievalKey, // 分类向量统一key
		Value: cast.ToString(cateInfo.BusinessID),
	})
	// 2.批量获取评测端文档5000个切片
	startID, limit := uint64(0), uint64(config.GetMainConfig().Permissions.GetSegmentLimit)
	for {
		segs, lastID, err := d.segLogic.GetSegmentByDocID(ctx, appDB.PrimaryId, doc.ID, startID, limit,
			[]string{segEntity.DocSegmentTblColID, segEntity.DocSegmentTblColType, segEntity.DocSegmentTblColSegmentType, segEntity.DocSegmentTblColPageContent})
		if err != nil {
			logx.E(ctx, "FullUpdateLabel processTestDoc getDocNotDeleteSegment err:%v,docID:%v", err, doc.ID)
			return err
		}
		if len(segs) == 0 {
			break
		}
		startID = lastID
		logx.D(ctx, "FullUpdateLabel processTestDoc startID:%v,segs:%v", startID, len(segs))
		// 3.写评测端 es和向量
		segmentMap, text2sqlMap := make([]uint64, 0), make(map[uint32][]uint64, 0)
		for _, segment := range segs {
			if segment.IsSegmentForQA() || segment.SegmentType == segEntity.SegmentTypeText2SQLMeta {
				continue
			}
			if segment.SegmentType == segEntity.SegmentTypeText2SQLContent { // 是表格需要获取列数
				cells, err := getColumnsCount(ctx, segment.PageContent)
				if err != nil {
					logx.E(ctx, "FullUpdateLabel processTestDoc getColumnsCount err:%v,segmentID:%vcontent:%v",
						err, segment.ID, segment.PageContent)
					continue
				}
				text2sqlMap[cells] = append(text2sqlMap[cells], segment.ID)
			} else {
				segmentMap = append(segmentMap, segment.ID)
			}
		}
		if err = batchUpdateSegVector(ctx, d.rpc, appDB.PrimaryId, appDB.BizId, embeddingVersion, embeddingModel, retrieval.EnvType_Test,
			labels, segmentMap, text2sqlMap); err != nil {
			logx.E(ctx, "FullUpdateLabel processTestDoc batchUpdateSegVector err:%v,segmentMap:%v,text2sqlMap:%v",
				err, segmentMap, text2sqlMap)
			return err
		}
	}
	return nil
}

func (d *FullUpdateLabelTaskHandler) processProdDoc(ctx context.Context, appDB *entity.App, embeddingVersion uint64, embeddingModel string, docID uint64) error {
	// 1.获取文档信息 注意文档可能已经删除
	doc, err := d.docLogic.GetDocByID(ctx, docID, appDB.PrimaryId)
	if err != nil || doc == nil {
		logx.E(ctx, "FullUpdateLabel processProdDoc GetDocByID err:%v,docID:%v", err, docID)
		return err
	}
	// 2.获取doc分类 注意文档分类可能已经删除
	cateInfo := &cateEntity.CateInfo{}
	if doc.CategoryID > 0 {
		cateInfo, err = d.cateLogic.DescribeCateByID(ctx, cateEntity.DocCate, uint64(doc.CategoryID), doc.CorpID, doc.RobotID)
		if err != nil {
			logx.E(ctx, "FullUpdateLabel processProdDoc getCateInfo err:%v,docID:%+v", err, doc.ID)
			return err
		}
	}
	labels := make([]*retrieval.VectorLabel, 0)
	labels = append(labels, &retrieval.VectorLabel{
		Name:  config.GetMainConfig().Permissions.CateRetrievalKey, // 分类向量统一key
		Value: cast.ToString(cateInfo.BusinessID),
	})
	// 3.批量获取发布端文档5000个切片
	startID, limit := uint64(0), uint64(config.GetMainConfig().Permissions.GetSegmentLimit)
	for {
		prodSegs, lastID, err := d.qaLogic.GetVectorSyncLogic().GetSegmentNodeByDocID(ctx, appDB.PrimaryId, docID, startID, limit,
			[]string{vector.NodeTblColId, vector.NodeTblColRelatedId, vector.NodeTblColPageContent, vector.NodeTblColSegmentType})
		if err != nil {
			logx.E(ctx, "FullUpdateLabel processProdDoc GetNodeIdsList err:%v,docID:%v", err, docID)
			return err
		}
		if len(prodSegs) == 0 {
			break
		}
		startID = lastID
		// 4.写发布端es和向量
		segmentMap, text2sqlMap := make([]uint64, 0), make(map[uint32][]uint64, 0)
		for _, segment := range prodSegs {
			if segment.SegmentType == segEntity.SegmentTypeText2SQLMeta {
				continue
			}
			if segment.SegmentType == segEntity.SegmentTypeText2SQLContent { // 是表格需要获取列数
				cells, err := getColumnsCount(ctx, segment.PageContent)
				if err != nil {
					logx.E(ctx, "FullUpdateLabel processProdDoc getColumnsCount err:%v,segmentID:%vcontent:%v", err, segment.ID, segment.PageContent)
					continue
				}
				text2sqlMap[cells] = append(text2sqlMap[cells], segment.RelatedID)
			} else {
				segmentMap = append(segmentMap, segment.RelatedID)
			}
		}
		if err = batchUpdateSegVector(ctx, d.rpc, appDB.PrimaryId, appDB.BizId, embeddingVersion, embeddingModel, retrieval.EnvType_Prod,
			labels, segmentMap, text2sqlMap); err != nil {
			logx.E(ctx, "FullUpdateLabel processProdDoc batchUpdateSegVector err:%v,segmentMap:%v,text2sqlMap:%v",
				err, segmentMap, text2sqlMap)
			return err
		}
	}
	return nil
}

// Done .
func (d *FullUpdateLabelTaskHandler) Done(ctx context.Context) error {
	logx.I(ctx, "task FullUpdateLabel finish")
	return nil
}

// Fail .
func (d *FullUpdateLabelTaskHandler) Fail(ctx context.Context) error {
	logx.I(ctx, "task FullUpdateLabel fail")
	return nil
}

// Stop .
func (d *FullUpdateLabelTaskHandler) Stop(ctx context.Context) error {
	return nil
}
