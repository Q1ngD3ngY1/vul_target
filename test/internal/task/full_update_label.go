package task

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/errs"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/vector"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"github.com/spf13/cast"
	"golang.org/x/sync/errgroup"
)

var syncLabelCache app.UpgradeCache

// FullUpdateLabelScheduler 全量刷数据标签
type FullUpdateLabelScheduler struct {
	dao    dao.Dao
	vector *vector.SyncVector
	task   task_scheduler.Task
	p      model.FullUpdateLabel
}

const (
	updateAppIDs = "lke_update_app_ids"
)

func initFullUpdateLabelScheduler() {
	task_scheduler.Register(
		model.FullUpdateLabelTask,
		func(t task_scheduler.Task, params model.FullUpdateLabel) task_scheduler.TaskHandler {
			d := dao.New()
			return &FullUpdateLabelScheduler{
				dao:    d,
				vector: vector.NewVectorSync(d.GetDB(), d.GetTdsqlGormDB()),
				task:   t,
				p:      params,
			}
		},
	)
	syncLabelCache.UpgradeType = app.SyncVectorLabel
	syncLabelCache.ExpiredTimeS = 30 * 24 * 3600
}

// Prepare .
func (f *FullUpdateLabelScheduler) Prepare(ctx context.Context) (kv task_scheduler.TaskKV, err error) {
	log.InfoContextf(ctx, "FullUpdateLabel Prepare, req:%+v", f.p)
	//按应用维度刷数据
	kv = make(task_scheduler.TaskKV)
	if len(f.p.AppIDs) == 0 {
		appIDs, err := dao.GetRobotDao().GetAllValidAppIDs(ctx, 0)
		if err != nil {
			log.WarnContextf(ctx, "FullUpdateLabel GetAllValidAppIDs err:%v,req:%+v", err, f.p)
			return nil, err
		}
		pendingIDs, err := syncLabelCache.GetNotUpgradedApps(ctx, appIDs)
		if err != nil {
			log.ErrorContextf(ctx, "FullUpdateLabel GetNotUpgradedApps err:%v", err)
			return nil, err
		}
		for _, id := range pendingIDs {
			if f.p.MaxID != 0 && id >= f.p.MaxID {
				continue
			}
			kv[cast.ToString(id)] = ""
		}
		return kv, nil

	}
	apps, err := dao.GetRobotDao().GetAppList(ctx, []string{dao.RobotTblColId}, &dao.RobotFilter{
		IDs:            f.p.AppIDs,
		Limit:          uint32(len(f.p.AppIDs)),
		OrderColumn:    []string{dao.RobotTblColId},
		OrderDirection: []string{dao.SqlOrderByAsc},
		IsDeleted:      pkg.GetIntPtr(dao.IsNotDeleted),
	})
	if err != nil {
		log.WarnContextf(ctx, "FullUpdateLabel getAppList err:%v,req:%+v", err, f.p)
		return nil, err
	}
	if len(apps) == 0 {
		log.InfoContextf(ctx, "FullUpdateLabel get apps empty,req:%+v", f.p)
		return nil, nil
	}
	for _, app := range apps {
		kv[cast.ToString(app.ID)] = ""
	}
	return kv, nil
}

// Init .
func (f *FullUpdateLabelScheduler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	return nil
}

// Process .
func (f *FullUpdateLabelScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	for k := range progress.TaskKV(ctx) {
		if !utilConfig.GetMainConfig().Permissions.UpdateVectorSwitch {
			return errs.New(1000, "停止任务")
		}
		startTime := time.Now()
		log.InfoContextf(ctx, "FullUpdateLabel Process,k:%v", k)
		robotID := cast.ToUint64(k)
		//1.获取应用信息 得到embedding 版本
		appDB, err := f.dao.GetAppByID(ctx, robotID)
		if err != nil {
			log.ErrorContextf(ctx, "FullUpdateLabel GetAppByID err:%v,robotID:%v", err, robotID)
			continue
		}
		if appDB == nil || appDB.HasDeleted() { //已经删除,直接跳过
			err := syncLabelCache.SetAppFinish(ctx, robotID)
			if err != nil {
				log.ErrorContextf(ctx, "FullUpdateLabel redis hset err:%v,k:%v", err, k)
			}
			if err := progress.Finish(ctx, k); err != nil {
				log.ErrorContextf(ctx, "FullUpdateLabel Finish key:%s,err:%+v", k, err)
				return err
			}
			log.InfoContextf(ctx, "FullUpdateLabel Finish key:%s,cost:%v", k, time.Since(startTime))
			return nil
		}
		embeddingConf, _, err := appDB.GetEmbeddingConf()
		if err != nil {
			log.ErrorContextf(ctx, "FullUpdateLabel GetEmbeddingConf err:%v,robotID:%v", err, robotID)
			continue
		}
		embeddingVersion := embeddingConf.Version
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(3)
		g.Go(func() error { //问答
			err1 := f.processQa(gCtx, appDB, embeddingVersion)
			if err1 != nil {
				log.ErrorContextf(gCtx, "FullUpdateLabel processQa err:%v,robotID:%v", err1, robotID)
				return err1
			}
			log.InfoContextf(gCtx, "FullUpdateLabel processQa key:%s,cost:%v", k, time.Since(startTime))
			return nil
		})
		g.Go(func() error { //评测端文档
			docList, err2 := dao.GetDocDao().GetDocList(gCtx, dao.DocTblColList, &dao.DocFilter{
				RobotId:   robotID,
				IsDeleted: pkg.GetIntPtr(model.DocIsNotDeleted),
			})
			if err2 != nil {
				log.ErrorContextf(gCtx, "FullUpdateLabel GetDocList err:%v,robotID:%v", err2, robotID)
				return err2
			}
			for _, doc := range docList {
				err2 = f.processTestDoc(gCtx, appDB, embeddingVersion, doc)
				if err2 != nil {
					log.ErrorContextf(gCtx, "FullUpdateLabel processTestDoc err:%v,robotID:%v,docID:%+v", err2, robotID, doc.ID)
					return err2
				}
			}
			log.InfoContextf(gCtx, "FullUpdateLabel processTestDoc key:%s,cost:%v", k, time.Since(startTime))
			return nil
		})
		g.Go(func() error { //发布端文档
			nodeList, err3 := dao.GetRetrievalNodeDao().GetDocNodeList(gCtx, robotID)
			if err3 != nil {
				log.ErrorContextf(gCtx, "FullUpdateLabel GetNodeIdsList err:%v,robotID:%v", err3, robotID)
				return err3
			}
			for _, node := range nodeList {
				err3 = f.processProdDoc(gCtx, appDB, embeddingVersion, node.DocID)
				if err3 != nil {
					log.ErrorContextf(gCtx, "FullUpdateLabel processProdDoc err:%v,robotID:%v,docID:%+v", err3, robotID, node.DocID)
					return err3
				}
			}
			log.InfoContextf(gCtx, "FullUpdateLabel processProdDoc key:%s,cost:%v", k, time.Since(startTime))
			return nil
		})
		if err := g.Wait(); err != nil { //柔性放过
			log.ErrorContextf(ctx, "FullUpdateLabel Process err:%v,robotID:%v", err, robotID)
		} else {
			err := syncLabelCache.SetAppFinish(ctx, robotID)
			if err != nil {
				log.ErrorContextf(ctx, "FullUpdateLabel redis hset err:%v,k:%v", err, k)
			}
		}
		if err := progress.Finish(ctx, k); err != nil {
			log.ErrorContextf(ctx, "FullUpdateLabel Finish key:%s,err:%+v", k, err)
			return err
		}
		log.InfoContextf(ctx, "FullUpdateLabel Finish key:%s,cost:%v", k, time.Since(startTime))
	}
	return nil
}

func (f *FullUpdateLabelScheduler) processQa(ctx context.Context, appDB *model.AppDB, embeddingVersion uint64) error {
	robotID := appDB.ID
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(3)
	var err1, err2, err3 error
	qaList, nodeList, cateList := make([]*model.DocQA, 0), make([]*model.RetrievalNodeInfo, 0), make([]*model.CateInfo, 0)
	g.Go(func() error { //获取评测端所有问答
		qaList, err1 = dao.GetDocQaDao().GetAllDocQas(gCtx, dao.DocQaTblColList, &dao.DocQaFilter{
			RobotId:   robotID,
			IsDeleted: pkg.GetIntPtr(model.QAIsNotDeleted),
		})
		if err1 != nil {
			log.ErrorContextf(gCtx, "FullUpdateLabel processQa GetAllDocQas err:%v,robotID:%v", err1, robotID)
			return err1
		}
		return nil
	})
	g.Go(func() error { //获取发布端所有节点信息
		nodeList, err2 = dao.GetRetrievalNodeDao().GetNodeIdsList(gCtx, robotID,
			[]string{dao.NodeTblColId, dao.NodeTblColRelatedId, dao.NodeTblColParentId},
			&dao.RetrievalNodeFilter{APPID: robotID, DocType: model.DocTypeQA})
		if err2 != nil {
			log.ErrorContextf(gCtx, "FullUpdateLabel processQa GetNodeIdsList err:%v,robotID:%v", err2, robotID)
			return err2
		}
		return nil
	})
	g.Go(func() error { //获取应用所有未删除分类
		cateList, err3 = f.dao.GetCateList(gCtx, model.QACate, appDB.CorpID, robotID)
		if err3 != nil {
			log.ErrorContextf(gCtx, "FullUpdateLabel processQa GetCateList err:%v,robotID:%v", err3, robotID)
			return err3
		}
		return nil
	})
	if err := g.Wait(); err != nil { //柔性放过
		log.ErrorContextf(ctx, "FullUpdateLabel Process err:%v,robotID:%v", err, robotID)
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
		cateBizID := cateByID[v.CategoryID] //如果是全部分类，赋值为0
		testCateByQaID[v.ID] = cateBizID
		if !v.IsAccepted() || v.IsExpire() || v.IsCharExceeded() {
			continue
		}
		testQaByCate[cateBizID] = append(testQaByCate[cateBizID], v.ID)
	}
	// 处理发布端节点数据
	deleteQaList := make([]uint64, 0, len(nodeList))
	for _, v := range nodeList {
		if v.RelatedID < 100000000000 { //标准问
			if cateBizID, ok := testCateByQaID[v.RelatedID]; ok {
				prodQaByCate[cateBizID] = append(prodQaByCate[cateBizID], v.RelatedID)
			} else { //发布端还有，评测端已删除的问答，需要再次获取
				deleteQaList = append(deleteQaList, v.RelatedID)
			}
		} else { //相似问
			prodSimByQa[v.ParentID] = append(prodSimByQa[v.ParentID], v.RelatedID)
		}
	}
	// 处理已删除未发布的问答
	if len(deleteQaList) > 0 {
		for _, ids := range slicex.Chunk(deleteQaList, 200) {
			tmp, err := f.dao.GetQADetails(ctx, appDB.CorpID, robotID, ids)
			if err != nil { //柔性放过
				log.ErrorContextf(ctx, "FullUpdateLabel processQa get deleteQaList err:%v,robotID:%v,ids:%v", err, robotID, ids)
				continue
			}
			for qaID, qa := range tmp {
				if cateBizID, ok := cateByID[qa.CategoryID]; ok { //发布端问答的分类已经被删除了，直接过滤，不需要更新标签
					prodQaByCate[cateBizID] = append(prodQaByCate[cateBizID], qaID)
				}
			}
		}
	}
	//4.双写两端的es和向量
	sleepSwitch, sleepMillisecond := utilConfig.GetMainConfig().Permissions.UpdateVectorSleepSwitch,
		utilConfig.GetMainConfig().Permissions.UpdateVectorSleepMillisecond
	updateFunc := func(newCtx context.Context, envType retrieval.EnvType, indexID uint64, qaType uint32, qaIDs []uint64, labels []*retrieval.VectorLabel) error {
		req := &retrieval.UpdateLabelReq{
			RobotId:          robotID,
			AppBizId:         appDB.BusinessID,
			EnvType:          envType,
			IndexId:          indexID,
			Ids:              qaIDs,
			DocType:          model.DocTypeQA,
			QaType:           qaType,
			EmbeddingVersion: embeddingVersion,
			Labels:           labels,
		}
		if _, err := client.UpdateVectorLabel(newCtx, req); err != nil {
			log.ErrorContextf(newCtx, "FullUpdateLabel processQa update %s err:%v,req:%+v", envType, err, req)
			return err
		}
		if sleepSwitch {
			time.Sleep(time.Duration(sleepMillisecond) * time.Millisecond)
		}
		return nil
	}
	g, gCtx = errgroup.WithContext(ctx)
	g.SetLimit(2)
	g.Go(func() error { //写评测端es和向量
		for casteBizID, testQa := range testQaByCate {
			labels := []*retrieval.VectorLabel{
				{
					Name:  utilConfig.GetMainConfig().Permissions.CateRetrievalKey,
					Value: cast.ToString(casteBizID),
				},
			}
			for _, qaIDs := range slicex.Chunk(testQa, defaultUpdateSize) {
				// 双写评测端标准问
				if err := updateFunc(gCtx, retrieval.EnvType_Test, model.ReviewVersionID, model.QATypeStandard, qaIDs, labels); err != nil {
					return err
				}
				if err := updateFunc(gCtx, retrieval.EnvType_Test, model.SimilarVersionID, model.QATypeStandard, qaIDs, labels); err != nil {
					return err
				}
				//获取100条问答的相似问
				simMap, err := f.dao.GetSimilarQuestionsSimpleByQAIDs(gCtx, appDB.CorpID, robotID, qaIDs)
				if err != nil {
					log.ErrorContextf(gCtx, "FullUpdateLabel processQa Get sim err:%v,robotID:%v,qaIDS:%v", err, robotID, qaIDs)
					return err
				}
				simBizIDs := make([]uint64, 0)
				for _, sims := range simMap {
					for _, v := range sims {
						simBizIDs = append(simBizIDs, v.SimilarID)
					}
				}
				for _, simIDs := range slicex.Chunk(simBizIDs, defaultUpdateSize) {
					if err := updateFunc(gCtx, retrieval.EnvType_Test, model.ReviewVersionID, model.QATypeSimilar, simIDs, labels); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
	g.Go(func() error { //写发布端es和向量
		for casteBizID, prodQa := range prodQaByCate {
			labels := []*retrieval.VectorLabel{
				{
					Name:  utilConfig.GetMainConfig().Permissions.CateRetrievalKey,
					Value: cast.ToString(casteBizID),
				},
			}
			for _, prodIDs := range slicex.Chunk(prodQa, defaultUpdateSize) {
				// 双写发布端标准问
				if err := updateFunc(gCtx, retrieval.EnvType_Prod, model.ReviewVersionID, model.QATypeStandard, prodIDs, labels); err != nil {
					return err
				}
				if err := updateFunc(gCtx, retrieval.EnvType_Prod, model.SimilarVersionID, model.QATypeStandard, prodIDs, labels); err != nil {
					return err
				}
				// 取100条发布端标准问的相似问
				sims := make([]uint64, 0)
				for _, prodID := range prodIDs {
					sims = append(sims, prodSimByQa[prodID]...)
				}
				for _, simIDs := range slicex.Chunk(sims, defaultUpdateSize) {
					if err := updateFunc(gCtx, retrieval.EnvType_Prod, model.ReviewVersionID, model.QATypeSimilar, simIDs, labels); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		log.ErrorContextf(ctx, "FullUpdateLabel Process UpdateVectorLabel err:%v,robotID:%v", err, robotID)
		return err
	}
	return nil
}

func (f *FullUpdateLabelScheduler) processTestDoc(ctx context.Context, appDB *model.AppDB, embeddingVersion uint64,
	doc *model.Doc) (err error) {
	if doc == nil || doc.IsDeleted == model.DocIsDeleted || doc.IsExpire() || doc.IsCharSizeExceeded() ||
		doc.Status == model.DocStatusCreateIndexFail {
		return nil
	}
	//1.获取doc分类
	cateInfo := &model.CateInfo{}
	if doc.CategoryID > 0 {
		cateInfo, err = f.dao.GetCateByID(ctx, model.DocCate, uint64(doc.CategoryID), doc.CorpID, doc.RobotID)
		if err != nil {
			log.ErrorContextf(ctx, "FullUpdateLabel processTestDoc getCateInfo err:%v,docID:%+v", err, doc.ID)
			return err
		}
	}
	labels := make([]*retrieval.VectorLabel, 0)
	labels = append(labels, &retrieval.VectorLabel{
		Name:  utilConfig.GetMainConfig().Permissions.CateRetrievalKey, //分类向量统一key
		Value: cast.ToString(cateInfo.BusinessID),
	})
	//2.批量获取评测端文档5000个切片
	startID, limit := uint64(0), uint64(utilConfig.GetMainConfig().Permissions.GetSegmentLimit)
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
		log.DebugContextf(ctx, "FullUpdateLabel processTestDoc startID:%v,segs:%v", startID, len(segs))
		//3.写评测端 es和向量
		segmentMap, text2sqlMap := make([]uint64, 0), make(map[uint32][]uint64, 0)
		for _, segment := range segs {
			if segment.IsSegmentForQA() || segment.SegmentType == model.SegmentTypeText2SQLMeta {
				continue
			}
			if segment.SegmentType == model.SegmentTypeText2SQLContent { //是表格需要获取列数
				cells, err := getColumnsCount(ctx, segment.PageContent)
				if err != nil {
					log.ErrorContextf(ctx, "FullUpdateLabel processTestDoc getColumnsCount err:%v,segmentID:%vcontent:%v",
						err, segment.ID, segment.PageContent)
					continue
				}
				text2sqlMap[cells] = append(text2sqlMap[cells], segment.ID)
			} else {
				segmentMap = append(segmentMap, segment.ID)
			}
		}
		if err = batchUpdateSegVector(ctx, appDB.ID, appDB.BusinessID, embeddingVersion, retrieval.EnvType_Test,
			labels, segmentMap, text2sqlMap); err != nil {
			log.ErrorContextf(ctx, "FullUpdateLabel processTestDoc batchUpdateSegVector err:%v,segmentMap:%v,text2sqlMap:%v",
				err, segmentMap, text2sqlMap)
			return err
		}
	}
	return nil
}

func (f *FullUpdateLabelScheduler) processProdDoc(ctx context.Context, appDB *model.AppDB, embeddingVersion uint64, docID uint64) error {
	//1.获取文档信息 注意文档可能已经删除
	doc, err := f.dao.GetDocByID(ctx, docID, appDB.ID)
	if err != nil || doc == nil {
		log.ErrorContextf(ctx, "FullUpdateLabel processProdDoc GetDocByID err:%v,docID:%v", err, docID)
		return err
	}
	//2.获取doc分类 注意文档分类可能已经删除
	cateInfo := &model.CateInfo{}
	if doc.CategoryID > 0 {
		cateInfo, err = f.dao.GetCateByID(ctx, model.DocCate, uint64(doc.CategoryID), doc.CorpID, doc.RobotID)
		if err != nil {
			log.ErrorContextf(ctx, "FullUpdateLabel processProdDoc getCateInfo err:%v,docID:%+v", err, doc.ID)
			return err
		}
	}
	labels := make([]*retrieval.VectorLabel, 0)
	labels = append(labels, &retrieval.VectorLabel{
		Name:  utilConfig.GetMainConfig().Permissions.CateRetrievalKey, //分类向量统一key
		Value: cast.ToString(cateInfo.BusinessID),
	})
	//3.批量获取发布端文档5000个切片
	startID, limit := uint64(0), uint64(utilConfig.GetMainConfig().Permissions.GetSegmentLimit)
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
		//4.写发布端es和向量
		segmentMap, text2sqlMap := make([]uint64, 0), make(map[uint32][]uint64, 0)
		for _, segment := range prodSegs {
			if segment.SegmentType == model.SegmentTypeText2SQLMeta {
				continue
			}
			if segment.SegmentType == model.SegmentTypeText2SQLContent { //是表格需要获取列数
				cells, err := getColumnsCount(ctx, segment.PageContent)
				if err != nil {
					log.ErrorContextf(ctx, "FullUpdateLabel processProdDoc getColumnsCount err:%v,segmentID:%vcontent:%v",
						err, segment.ID, segment.PageContent)
					continue
				}
				text2sqlMap[cells] = append(text2sqlMap[cells], segment.RelatedID)
			} else {
				segmentMap = append(segmentMap, segment.RelatedID)
			}
		}
		if err = batchUpdateSegVector(ctx, appDB.ID, appDB.BusinessID, embeddingVersion, retrieval.EnvType_Prod,
			labels, segmentMap, text2sqlMap); err != nil {
			log.ErrorContextf(ctx, "FullUpdateLabel processProdDoc batchUpdateSegVector err:%v,segmentMap:%v,text2sqlMap:%v",
				err, segmentMap, text2sqlMap)
			return err
		}
	}
	return nil
}

// Done .
func (b *FullUpdateLabelScheduler) Done(ctx context.Context) error {
	log.InfoContextf(ctx, "task FullUpdateLabel finish")
	return nil
}

// Fail .
func (b *FullUpdateLabelScheduler) Fail(ctx context.Context) error {
	log.InfoContextf(ctx, "task FullUpdateLabel fail")
	return nil
}

// Stop .
func (b *FullUpdateLabelScheduler) Stop(ctx context.Context) error {
	return nil
}
