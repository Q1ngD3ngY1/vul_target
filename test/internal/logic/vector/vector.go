package vector

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	vecEntity "git.woa.com/adp/kb/kb-config/internal/entity/vector"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	pb "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

// AddSyncBatch add sync 批量接口
func (l *VectorSyncLogic) AddSyncBatch(ctx context.Context, row []*vecEntity.VectorSync) ([]uint64, error) {
	if len(row) == 0 {
		return nil, nil
	}

	writeSyncIds := make([]uint64, 0, len(row))
	for _, r := range row {
		writeSyncIds = append(writeSyncIds, r.WriteSyncId)
	}

	// 插入任务
	err := l.vecDao.BatchCreateVectorSync(ctx, row)
	if err != nil {
		logx.E(ctx, "Failed to BatchCreateVectorSync err:%+v", err)
		return nil, err
	}

	ids := make([]uint64, 0)
	vecSyncs, err := l.vecDao.ListVectorSynsByWriteSynId(ctx, writeSyncIds)
	if err != nil {
		logx.E(ctx, "Failed to ListVectorSynsByWriteSynId err:%+v", err)
		return nil, err
	}

	for _, v := range vecSyncs {
		ids = append(ids, v.ID)
	}

	return ids, nil

}

// addSimilarQuestionSyncBatch 新增相似问同步流水(批量)
func (l *VectorSyncLogic) AddSimilarQuestionSyncBatch(ctx context.Context, sqs []*qaEntity.SimilarQuestion) ([]uint64,
	error) {
	if len(sqs) == 0 {
		return make([]uint64, 0), nil
	}
	now := time.Now()
	sqsSync := make([]*vecEntity.VectorSync, 0, len(sqs))
	for _, sim := range sqs {
		sqsSync = append(sqsSync, &vecEntity.VectorSync{
			Type:        vecEntity.VectorTypeQA,
			RelateID:    sim.RelatedQAID,
			Status:      vecEntity.StatusSyncInit,
			Request:     "",
			TryTimes:    0,
			MaxTryTimes: vecEntity.MaxTryTimes,
			UpdateTime:  now,
			CreateTime:  now,
			ExtendedId:  sim.SimilarID,
			WriteSyncId: idgen.GetId(),
		})
	}
	// TODO: refactor
	ids := make([]uint64, 0, len(sqsSync))
	ids, err := l.AddSyncBatch(ctx, sqsSync)
	if err != nil {
		logx.E(ctx, "Failed to AddSimilarQuestionSyncBatch error, err:%+v", err)
		return nil, err
	}
	return ids, nil
}

// addSimilarQuestionSync 新增相似问同步流水, 建议使用批量接口 addSimilarQuestionSyncBatch
func (l *VectorSyncLogic) AddSimilarQuestionSync(ctx context.Context, sim *qaEntity.SimilarQuestion) (uint64, error) {
	now := time.Now()
	syncID, err := l.vecDao.CreateVectorSync(ctx, &vecEntity.VectorSync{
		Type:        vecEntity.VectorTypeQA,
		RelateID:    sim.RelatedQAID,
		Status:      vecEntity.StatusSyncInit,
		Request:     "",
		TryTimes:    0,
		MaxTryTimes: vecEntity.MaxTryTimes,
		UpdateTime:  now,
		CreateTime:  now,
		ExtendedId:  sim.SimilarID,
		WriteSyncId: idgen.GetId(),
	})
	if err != nil {
		logx.E(ctx, "Failed to AddSimilarQuestionSync err:%+v", err)
		return 0, err
	}
	return syncID, nil
}

// AddQASync 新增问答同步流水 (相似问场景调addSimilarQuestionSyncBatch接口)
func (l *VectorSyncLogic) AddQASync(ctx context.Context, qa *qaEntity.DocQA) (uint64, error) {
	now := time.Now()
	syncID, err := l.vecDao.CreateVectorSync(ctx, &vecEntity.VectorSync{
		Type:        vecEntity.VectorTypeQA,
		RelateID:    qa.ID,
		Status:      vecEntity.StatusSyncInit,
		Request:     "",
		TryTimes:    0,
		MaxTryTimes: vecEntity.MaxTryTimes,
		UpdateTime:  now,
		CreateTime:  now,
		ExtendedId:  0,
		WriteSyncId: idgen.GetId(),
	})
	if err != nil {
		logx.E(ctx, "Failed to AddQASync err:%+v", err)
		return 0, err
	}
	logx.I(ctx, "AddQASync|syncID:%d|qa:%+v", syncID, qa)
	return syncID, nil
}

// addRejectedQuestionSync 新增拒答问题同步流水
func (l *VectorSyncLogic) AddRejectedQuestionSync(ctx context.Context, rejectedQuestion *qaEntity.RejectedQuestion) (
	uint64, error) {
	now := time.Now()
	syncID, err := l.vecDao.CreateVectorSync(ctx, &vecEntity.VectorSync{
		Type:        vecEntity.VectorTypeRejectedQuestion,
		RelateID:    rejectedQuestion.ID,
		Status:      vecEntity.StatusSyncInit,
		Request:     "",
		UpdateTime:  now,
		CreateTime:  now,
		ExtendedId:  0,
		WriteSyncId: idgen.GetId(),
	})
	if err != nil {
		logx.E(ctx, "Failed to AddRejectedQuestionSync err:%+v", err)
	}
	return syncID, nil
}

// BatchDirectAddSegmentKnowledge 批量新增分片知识
func (l *VectorSyncLogic) BatchDirectAddSegmentKnowledge(ctx context.Context, robotID, appBizID uint64, segments []*segEntity.DocSegmentExtend,
	embeddingVersion uint64, embeddingName string, vectorLabels []*pb.VectorLabel) error {
	logx.I(ctx, "BatchDirectAddSegmentKnowledge|robotID:%d|%dsegments|embeddingVersion:%d|embeddingName:%s|%dvectorLabels",
		robotID, len(segments), embeddingVersion, embeddingName, len(vectorLabels))
	if len(segments) == 0 {
		logx.E(ctx, "BatchDirectAddSegmentKnowledge len(segments):%d|ignore", len(segments))
		return nil
	}

	knowledge := make([]*pb.KnowledgeData, 0)
	// sheetSyncMap := &sync.Map{}
	for _, seg := range segments {
		// todo 对于表格类型文档，如果sheet停用，则不入库(方案存在问题，一个seg可能对应多个sheet)
		// if interveneOriginDocBizID != 0 && (seg.FileType == model.FileTypeXlsx || seg.FileType == model.FileTypeXls ||
		//	seg.FileType == model.FileTypeCsv) {
		//	corpBizID, appBizID, _, _, err := d.SegmentCommonIDsToBizIDs(ctx, seg.CorpPrimaryId,
		//		seg.AppPrimaryId, 0, seg.DocID)
		//	if err != nil {
		//		logx.E(ctx, "SegmentCommonIDsToBizIDs|interveneOriginDocBizID:%d|SheetData:%s|err:%+v",
		//			interveneOriginDocBizID, seg.SheetData, err)
		//		return err
		//	}
		//	sheet, err := d.GetSheetFromDocSegment(ctx, seg, corpBizID, appBizID, interveneOriginDocBizID, sheetSyncMap)
		//	if err != nil {
		//		logx.E(ctx, "GetSheetFromDocSegment|interveneOriginDocBizID:%d|SheetData:%s|err:%+v",
		//			interveneOriginDocBizID, seg.SheetData, err)
		//		return err
		//	}
		//	if sheet != nil && sheet.IsDisabled == model.SegmentIsDisabled {
		//		logx.D(ctx, "BatchAddKnowledge skip|SheetName:%s|DocID:%d", sheet.SheetName, seg.DocID)
		//		continue
		//	}
		// }
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
		IndexId:            entity.SegmentReviewVersionID,
		DocType:            entity.DocTypeSegment,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingName,
		Knowledge:          knowledge,
		BotBizId:           appBizID,
	}
	logx.I(ctx, "BatchDirectAddSegmentKnowledge|req:%+v", req)
	rsp, err := l.rpc.RetrievalDirectIndex.BatchAddKnowledge(ctx, req)
	if err != nil {
		logx.E(ctx, "BatchDirectAddSegmentKnowledge|err:%v", err)
		return err
	}
	logx.I(ctx, "BatchDirectAddSegmentKnowledge|rsp:%+v", rsp)
	return nil
}

// BatchDirectDeleteSegmentKnowledge 批量删除分片知识
func (l *VectorSyncLogic) BatchDirectDeleteSegmentKnowledge(ctx context.Context, robotID uint64,
	segments []*segEntity.DocSegmentExtend, embeddingVersion uint64, embeddingModel string) error {
	if len(segments) == 0 {
		logx.E(ctx, "批量删除分片知识 len(segments):%d|ignore", len(segments))
		return nil
	}
	botBizID, err := l.rawSqlDao.GetBotBizIDByID(ctx, robotID)
	if err != nil {
		logx.E(ctx, "BatchDirectDeleteSegmentKnowledge GetBotBizIDByID:%+v err:%+v", robotID, err)
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
		IndexId:            entity.SegmentReviewVersionID,
		Data:               knowledge,
		DocType:            entity.DocTypeSegment,
		EmbeddingVersion:   embeddingVersion,
		EmbeddingModelName: embeddingModel,
		BotBizId:           botBizID,
	}
	logx.I(ctx, "BatchDirectDeleteSegmentKnowledge|req:%+v", req)
	if err = l.rpc.RetrievalDirectIndex.BatchDeleteKnowledge(ctx, req); err != nil {
		logx.E(ctx, "BatchDirectDeleteSegmentKnowledge|err:%v", err)
		return err
	}
	logx.I(ctx, "BatchDirectDeleteSegmentKnowledge done")
	return nil
}
