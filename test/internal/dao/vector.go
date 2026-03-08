package dao

import (
	"context"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.code.oa.com/trpc-go/trpc-go/metrics"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/go-comm/clues"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	"github.com/jmoiron/sqlx"
)

// IndexRebuild 索引重建
func (d *dao) IndexRebuild(ctx context.Context, appID, versionID uint64) error {
	botBizID, err := d.GetBotBizIDByID(ctx, appID)
	if err != nil {
		log.ErrorContextf(ctx, "索引重建失败 GetBotBizIDByID:%+v err:%+v", appID, err)
		return err
	}
	req := &pb.IndexRebuildReq{
		RobotId:   appID,
		VersionId: versionID,
		BotBizId:  botBizID,
	}
	if _, err := d.retrievalCli.IndexRebuild(ctx, req); err != nil {
		log.ErrorContextf(ctx, "索引重建失败 req:%+v err:%+v", req, err)
		return err
	}
	return nil
}

// AddQAVector 新增问答向量
func (d *dao) AddQAVector(ctx context.Context, qa *model.DocQA) error {
	var syncID uint64
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		id, err := d.addQASync(ctx, tx, qa)
		if err != nil {
			return err
		}
		syncID = id
		return nil
	})
	if err != nil {
		return err
	}
	d.vector.Push(ctx, syncID)
	return nil
}

// AddSimilarQuestionSyncBatch 新增相似问同步流水(批量)
func (d *dao) AddSimilarQuestionSyncBatch(ctx context.Context, sqs []*model.SimilarQuestion) error {
	var syncSimilarQuestionsIDs []uint64
	var err error
	if err = d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		syncSimilarQuestionsIDs, err = d.addSimilarQuestionSyncBatch(ctx, tx, sqs)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	log.InfoContextf(ctx, "AddSimilarQuestionSyncBatch, syncSimilarQuestionsIDs:%v", syncSimilarQuestionsIDs)
	d.vector.BatchPush(ctx, syncSimilarQuestionsIDs)
	return nil
}

// addSimilarQuestionSyncBatch 新增相似问同步流水(批量)
func (d *dao) addSimilarQuestionSyncBatch(ctx context.Context, tx *sqlx.Tx, sqs []*model.SimilarQuestion) ([]uint64,
	error) {
	if len(sqs) == 0 {
		return make([]uint64, 0), nil
	}
	now := time.Now()
	sqsSync := make([]*model.VectorSync, 0, len(sqs))
	for _, sim := range sqs {
		sqsSync = append(sqsSync, &model.VectorSync{
			Type:        model.VectorTypeQA,
			RelateID:    sim.RelatedQAID,
			Status:      model.StatusSyncInit,
			Request:     "",
			TryTimes:    0,
			MaxTryTimes: model.MaxTryTimes,
			UpdateTime:  now,
			CreateTime:  now,
			ExtendedId:  sim.SimilarID,
			WriteSyncId: d.GenerateSeqID(),
		})
	}
	ids, err := d.vector.AddSyncBatch(ctx, tx, sqsSync)
	if err != nil {
		log.ErrorContextf(ctx, "批量新建相似问同步任务失败(%v) err:%+v", err)
		return nil, err
	}
	return ids, nil
}

// addSimilarQuestionSync 新增相似问同步流水, 建议使用批量接口 addSimilarQuestionSyncBatch
func (d *dao) addSimilarQuestionSync(ctx context.Context, tx *sqlx.Tx, sim *model.SimilarQuestion) (uint64, error) {
	now := time.Now()
	syncID, err := d.vector.AddSync(ctx, tx, &model.VectorSync{
		Type:        model.VectorTypeQA,
		RelateID:    sim.RelatedQAID,
		Status:      model.StatusSyncInit,
		Request:     "",
		TryTimes:    0,
		MaxTryTimes: model.MaxTryTimes,
		UpdateTime:  now,
		CreateTime:  now,
		ExtendedId:  sim.SimilarID,
		WriteSyncId: d.GenerateSeqID(),
	})
	if err != nil {
		return 0, err
	}
	return syncID, nil
}

// addQASync 新增问答同步流水 (相似问场景调addSimilarQuestionSyncBatch接口)
func (d *dao) addQASync(ctx context.Context, tx *sqlx.Tx, qa *model.DocQA) (uint64, error) {
	now := time.Now()
	syncID, err := d.vector.AddSync(ctx, tx, &model.VectorSync{
		Type:        model.VectorTypeQA,
		RelateID:    qa.ID,
		Status:      model.StatusSyncInit,
		Request:     "",
		TryTimes:    0,
		MaxTryTimes: model.MaxTryTimes,
		UpdateTime:  now,
		CreateTime:  now,
		ExtendedId:  0,
		WriteSyncId: d.GenerateSeqID(),
	})
	if err != nil {
		return 0, err
	}
	return syncID, nil
}

// addRejectedQuestionSync 新增拒答问题同步流水
func (d *dao) addRejectedQuestionSync(ctx context.Context, tx *sqlx.Tx, rejectedQuestion *model.RejectedQuestion) (
	uint64, error) {
	now := time.Now()
	syncID, err := d.vector.AddSync(ctx, tx, &model.VectorSync{
		Type:        model.VectorTypeRejectedQuestion,
		RelateID:    rejectedQuestion.ID,
		Status:      model.StatusSyncInit,
		Request:     "",
		UpdateTime:  now,
		CreateTime:  now,
		ExtendedId:  0,
		WriteSyncId: d.GenerateSeqID(),
	})
	if err != nil {
		return 0, err
	}
	return syncID, nil
}

// DeleteQAVector 删除问答向量
func (d *dao) DeleteQAVector(ctx context.Context, qa *model.DocQA) error {
	var syncID uint64
	err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
		// TODO(sim): 要添加相似问sync, 但DeleteQAVector没地方调用, 暂不实现
		id, err := d.addQASync(ctx, tx, qa)
		if err != nil {
			return err
		}
		syncID = id
		return nil
	})
	if err != nil {
		return err
	}
	d.vector.Push(ctx, syncID)
	return nil
}

// Search 向量搜索
func (d *dao) Search(ctx context.Context, req *pb.SearchReq) (*pb.SearchRsp, error) {
	t0 := time.Now()
	botBizID, err := d.GetBotBizIDByID(ctx, req.RobotId)
	if err != nil {
		log.ErrorContextf(ctx, "向量搜索 GetBotBizIDByID:%+v err:%+v", req.RobotId, err)
		return nil, err
	}
	req.BotBizId = botBizID
	rsp, err := d.retrievalCli.Search(ctx, req)
	clues.AddTrack4RPC(ctx, "retrievalCli.Search", req, rsp, err, t0)
	log.DebugContextf(ctx, "向量搜索 req:%+v rsp:%+v err:%+v", req, rsp, err)
	if err != nil {
		log.ErrorContextf(ctx, "向量搜索失败 req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// Publish 通知发布
func (d *dao) Publish(ctx context.Context, robotID, versionID uint64, versionName string) (*pb.PublishRsp, error) {
	appDB, err := d.GetAppByID(ctx, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "通知向量发布 获取机器人失败 req:%+v err:%v", robotID, err)
		return nil, err
	}
	if appDB == nil {
		err = errs.ErrRobotNotFound
		log.ErrorContextf(ctx, "通知向量发布 获取机器人失败 req:%+v err:%v", robotID, err)
		return nil, err
	}
	embeddingConf, _, err := appDB.GetEmbeddingConf()
	if err != nil {
		log.ErrorContextf(ctx, "通知向量发布 GetEmbeddingConf失败 req:%+v err:%v", robotID, err)
		return nil, err
	}
	botBizID, err := d.GetBotBizIDByID(ctx, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "通知向量发布 GetBotBizIDByID:%+v err:%+v", robotID, err)
		return nil, err
	}
	req := &pb.PublishReq{
		RobotId:          robotID,
		VersionId:        versionID,
		VersionName:      versionName,
		EmbeddingVersion: embeddingConf.Version,
		BotBizId:         botBizID,
	}
	rsp, err := d.retrievalCli.Publish(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "通知向量发布失败 req:%+v err:%+v", req, err)
		return nil, err
	}
	return rsp, nil
}

// RetryPausedRelease 重试暂停的发布
func (d *dao) RetryPausedRelease(ctx context.Context, release *model.Release, req *pb.ContinueTerminatedTaskReq) error {
	if _, err := d.retrievalCli.ContinueTerminatedTask(ctx, req); err != nil {
		log.ErrorContextf(ctx, "重试暂停的发布失败 req:%+v err:%+v", req, err)
		return err
	}
	return nil
}

// BatchGetBigDataESByRobotBigDataID 获取big_data
func (d *dao) BatchGetBigDataESByRobotBigDataID(ctx context.Context, robotID uint64, bitDataIDs []string,
	knowledgeType pb.KnowledgeType) ([]*pb.BigData, error) {
	log.InfoContextf(ctx, "BatchGetBigDataESByRobotBigDataID|robotID:%s, bigDataIDs:%v", robotID, len(bitDataIDs))
	req := &pb.BatchGetBigDataESByRobotBigDataIDReq{
		RobotId:    robotID,
		BigDataIds: bitDataIDs,
		Type:       knowledgeType,
	}
	rsp, err := d.directIndexCli.BatchGetBigDataESByRobotBigDataID(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "BatchGetBigDataESByRobotBigDataID|req:%+v|err:%+v", req, err)
		metrics.IncrCounter("BatchGetBigDataESByRobotBigDataID.err", 1)
		return nil, err
	}
	metrics.IncrCounter("BatchGetBigDataESByRobotBigDataID.success", 1)
	log.InfoContextf(ctx, "BatchGetBigDataESByRobotBigDataID|bigData.len=:%d|ok", len(rsp.Data))
	return rsp.Data, nil
}

// AddBigDataElastic 新建或更新BigData数据到ES
func (d *dao) AddBigDataElastic(ctx context.Context, bigData []*pb.BigData, knowledgeType pb.KnowledgeType) error {
	log.InfoContextf(ctx, "AddBigDataElastic|bigData.len:%d", len(bigData))
	if len(bigData) == 0 {
		return nil
	}
	req := &pb.AddBigDataElasticReq{Data: bigData, Type: knowledgeType}
	if _, err := d.directIndexCli.AddBigDataElastic(ctx, req); err != nil {
		log.ErrorContextf(ctx, "AddBigDataElastic|req:%+v|err:%+v", req, err)
		metrics.IncrCounter("AddBigDataElastic.err", 1)
		return err
	}
	metrics.IncrCounter("AddBigDataElastic.success", 1)
	log.InfoContextf(ctx, "AddBigDataElastic|bigData.len=:%d|ok", len(bigData))
	return nil
}

// DeleteBigDataElastic 从ES里删除BigData
func (d *dao) DeleteBigDataElastic(ctx context.Context, robotID, docID uint64, knowledgeType pb.KnowledgeType,
	hardDelete bool) error {
	req := &pb.DeleteBigDataElasticReq{
		RobotId:    robotID,
		DocId:      docID,
		Type:       knowledgeType,
		HardDelete: hardDelete,
	}
	if _, err := d.directIndexCli.DeleteBigDataElastic(ctx, req); err != nil {
		log.ErrorContextf(ctx, "DeleteBigDataElastic|req:%+v|err:%+v", req, err)
		return err
	}
	log.InfoContextf(ctx, "DeleteBigDataElastic|robotId:%d|docId:%d|hardDelete:%v|ok", robotID, docID, hardDelete)
	return nil
}

// DirectAddSegmentKnowledge 新增分片知识
func (d *dao) DirectAddSegmentKnowledge(ctx context.Context, seg *model.DocSegmentExtend, embeddingVersion uint64,
	vectorLabels []*pb.VectorLabel, embeddingModelName string) error {
	botBizID, err := d.GetBotBizIDByID(ctx, seg.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "新增分片知识 GetBotBizIDByID:%+v err:%+v", seg.RobotID, err)
		return err
	}
	req := &pb.AddKnowledgeReq{
		RobotId:            seg.RobotID,
		IndexId:            model.SegmentReviewVersionID,
		Id:                 seg.ID,
		DocId:              seg.DocID,
		DocType:            model.DocTypeSegment,
		SegmentType:        seg.SegmentType,
		PageContent:        seg.PageContent, // 检索使用 PageContent
		EmbeddingVersion:   embeddingVersion,
		Labels:             vectorLabels,
		ExpireTime:         seg.GetExpireTime(),
		BotBizId:           botBizID,
		EmbeddingModelName: embeddingModelName,
	}
	log.InfoContextf(ctx, "DirectAddSegmentKnowledge|req:%+v", req)
	rsp, err := d.directIndexCli.AddKnowledge(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "DirectAddSegmentKnowledge|err:%v", err)
		return err
	}
	log.InfoContextf(ctx, "DirectAddSegmentKnowledge|rsp:%+v", rsp)
	return nil
}

// BatchDirectAddSegmentKnowledge 批量新增分片知识
func (d *dao) BatchDirectAddSegmentKnowledge(ctx context.Context, robotID uint64, segments []*model.DocSegmentExtend,
	embeddingVersion uint64, vectorLabels []*pb.VectorLabel, embeddingModelName string) error {
	if len(segments) == 0 {
		log.ErrorContextf(ctx, "批量新增分片知识 len(segments):%d|ignore", len(segments))
		return nil
	}
	botBizID, err := d.GetBotBizIDByID(ctx, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "批量新增分片知识 GetBotBizIDByID:%+v err:%+v", robotID, err)
		return err
	}
	knowledge := make([]*pb.KnowledgeData, 0)
	//sheetSyncMap := &sync.Map{}
	for _, seg := range segments {
		// todo 对于表格类型文档，如果sheet停用，则不入库(方案存在问题，一个seg可能对应多个sheet)
		//if interveneOriginDocBizID != 0 && (seg.FileType == model.FileTypeXlsx || seg.FileType == model.FileTypeXls ||
		//	seg.FileType == model.FileTypeCsv) {
		//	corpBizID, appBizID, _, _, err := d.SegmentCommonIDsToBizIDs(ctx, seg.CorpID,
		//		seg.RobotID, 0, seg.DocID)
		//	if err != nil {
		//		log.ErrorContextf(ctx, "SegmentCommonIDsToBizIDs|interveneOriginDocBizID:%d|SheetData:%s|err:%+v",
		//			interveneOriginDocBizID, seg.SheetData, err)
		//		return err
		//	}
		//	sheet, err := d.GetSheetFromDocSegment(ctx, seg, corpBizID, appBizID, interveneOriginDocBizID, sheetSyncMap)
		//	if err != nil {
		//		log.ErrorContextf(ctx, "GetSheetFromDocSegment|interveneOriginDocBizID:%d|SheetData:%s|err:%+v",
		//			interveneOriginDocBizID, seg.SheetData, err)
		//		return err
		//	}
		//	if sheet != nil && sheet.IsDisabled == model.SegmentIsDisabled {
		//		log.DebugContextf(ctx, "BatchAddKnowledge skip|SheetName:%s|DocID:%d", sheet.SheetName, seg.DocID)
		//		continue
		//	}
		//}
		knowledge = append(knowledge, &pb.KnowledgeData{
			Id:          seg.ID,
			SegmentType: seg.SegmentType,
			DocId:       seg.DocID,
			PageContent: seg.PageContent, // 检索使用 PageContent
			Labels:      vectorLabels,
			ExpireTime:  seg.GetExpireTime(),
		})
	}
	req := &pb.BatchAddKnowledgeReq{
		RobotId:            robotID,
		IndexId:            model.SegmentReviewVersionID,
		DocType:            model.DocTypeSegment,
		EmbeddingVersion:   embeddingVersion,
		Knowledge:          knowledge,
		BotBizId:           botBizID,
		EmbeddingModelName: embeddingModelName,
	}
	log.InfoContextf(ctx, "BatchDirectAddSegmentKnowledge|req:%+v", req)
	rsp, err := d.directIndexCli.BatchAddKnowledge(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "BatchDirectAddSegmentKnowledge|err:%v", err)
		return err
	}
	log.InfoContextf(ctx, "BatchDirectAddSegmentKnowledge|rsp:%+v", rsp)
	return nil
}

// BatchDirectDeleteSegmentKnowledge 批量删除分片知识
func (d *dao) BatchDirectDeleteSegmentKnowledge(ctx context.Context, robotID uint64,
	segments []*model.DocSegmentExtend, embeddingVersion uint64, embeddingModelName string) error {
	if len(segments) == 0 {
		log.ErrorContextf(ctx, "批量删除分片知识 len(segments):%d|ignore", len(segments))
		return nil
	}
	botBizID, err := d.GetBotBizIDByID(ctx, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "BatchDirectDeleteSegmentKnowledge GetBotBizIDByID:%+v err:%+v", robotID, err)
		return err
	}
	knowledge := make([]*pb.KnowledgeIDType, 0)
	for _, seg := range segments {
		knowledge = append(knowledge, &pb.KnowledgeIDType{
			Id:          seg.ID,
			SegmentType: seg.SegmentType,
		})
	}
	req := &pb.BatchDeleteKnowledgeReq{
		RobotId:            robotID,
		IndexId:            model.SegmentReviewVersionID,
		Data:               knowledge,
		DocType:            model.DocTypeSegment,
		EmbeddingVersion:   embeddingVersion,
		BotBizId:           botBizID,
		EmbeddingModelName: embeddingModelName,
	}
	log.InfoContextf(ctx, "BatchDirectDeleteSegmentKnowledge|req:%+v", req)
	rsp, err := d.directIndexCli.BatchDeleteKnowledge(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "BatchDirectDeleteSegmentKnowledge|err:%v", err)
		return err
	}
	log.InfoContextf(ctx, "BatchDirectDeleteSegmentKnowledge|rsp:%+v", rsp)
	return nil
}

func (d *dao) ProdEmbeddingUpgrade(ctx context.Context, robotID uint64, embeddingVersionID uint64) error {
	botBizID, err := d.GetBotBizIDByID(ctx, robotID)
	if err != nil {
		log.ErrorContextf(ctx, "UpgradeEmbedding GetBotBizIDByID:%+v err:%+v", robotID, err)
		return err
	}
	req := &pb.UpgradeEmbeddingReq{
		RobotId:            robotID,
		EmbeddingVersionId: embeddingVersionID,
		BotBizId:           botBizID,
	}
	if _, err := d.retrievalCli.UpgradeEmbedding(ctx, req); err != nil {
		log.ErrorContextf(ctx, "UpgradeEmbedding fail req:%+v err:%+v", req, err)
		return err
	}
	return nil
}

// AddText2SQL 增加或修改text2sql
func (d *dao) AddText2SQL(ctx context.Context, robotID, docID uint64, expireTime int64, meta *pb.Text2SQLMeta,
	rows []*pb.Text2SQLRowData,
	vectorLabels []*pb.VectorLabel, fileName string, corpId uint64, disableEs bool) error {
	req := &pb.AddText2SQLReq{
		RobotId:    robotID,
		DocId:      docID,
		Meta:       meta,
		Rows:       rows,
		Labels:     vectorLabels,
		ExpireTime: expireTime,
		FileName:   fileName,
		CorpId:     corpId,
		DisableEs:  disableEs,
	}
	log.InfoContextf(ctx, "AddText2SQL|req:%+v", req)
	rsp, err := d.directIndexCli.AddText2SQL(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "AddText2SQL|err:%v", err)
		return err
	}
	log.InfoContextf(ctx, "AddText2SQL|rsp:%+v", rsp)
	return nil
}

// DeleteText2SQL 删除text2sql
func (d *dao) DeleteText2SQL(ctx context.Context, robotID, docID uint64) error {
	req := &pb.DeleteText2SQLReq{
		RobotId:     robotID,
		DocId:       docID,
		SegmentType: model.SegmentTypeText2SQLContent,
	}
	log.InfoContextf(ctx, "DeleteText2SQL|req:%+v", req)
	rsp, err := d.directIndexCli.DeleteText2SQL(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteText2SQL|err:%v", err)
		return err
	}
	log.InfoContextf(ctx, "DeleteText2SQL|rsp:%+v", rsp)
	return nil
}

// BatchDeleteAllKnowledgeProd 批量删除发布库的所有知识（包括QA/文档/混合检索/text2sql等）
func (d *dao) BatchDeleteAllKnowledgeProd(
	ctx context.Context, req *pb.BatchDeleteAllKnowledgeProdReq,
) (*pb.BatchDeleteAllKnowledgeProdRsp, error) {
	return d.retrievalCli.BatchDeleteAllKnowledgeProd(ctx, req)
}
