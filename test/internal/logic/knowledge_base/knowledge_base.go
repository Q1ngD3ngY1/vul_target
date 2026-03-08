package knowledge_base

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	pbknowledge "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	"time"
)

// DescribeKnowledgeBase 查询知识库信息
func DescribeKnowledgeBase(ctx context.Context, req *pb.DescribeKnowledgeBaseReq) (
	*pb.DescribeKnowledgeBaseRsp, error) {
	rsp := &pb.DescribeKnowledgeBaseRsp{}
	corpBizID := pkg.CorpBizID(ctx)
	knowledgeBizIds := make([]uint64, 0)
	for _, knowledgeBizId := range req.GetKnowledgeBizIds() {
		knowledgeBizIdUint64, err := util.CheckReqParamsIsUint64(ctx, knowledgeBizId)
		if err != nil {
			return nil, err
		}
		knowledgeBizIds = append(knowledgeBizIds, knowledgeBizIdUint64)
	}
	knowledgeBases, err := dao.GetKnowledgeBaseDao(nil).GetKnowledgeBases(ctx, corpBizID, knowledgeBizIds)
	if err != nil {
		return nil, err
	}
	knowledgeBaseMap := make(map[uint64]*model.KnowledgeBase)
	for _, knowledgeBase := range knowledgeBases {
		knowledgeBaseMap[knowledgeBase.KnowledgeBizId] = knowledgeBase
	}
	pbKnowledgeBases := make([]*pbknowledge.KnowledgeBaseInfo, 0)
	for _, knowledgeBizId := range knowledgeBizIds {
		pbKnowledgeBase := &pbknowledge.KnowledgeBaseInfo{
			KnowledgeBizId: knowledgeBizId,
		}
		knowledgeBase, ok := knowledgeBaseMap[knowledgeBizId]
		if ok {
			pbKnowledgeBase.ProcessingFlags = getProcessingFlags(knowledgeBase)
		}
		pbKnowledgeBases = append(pbKnowledgeBases, pbKnowledgeBase)
	}
	rsp.KnowledgeBases = pbKnowledgeBases
	return rsp, nil
}

// getProcessingFlags 遍历枚举值判断知识库是否包含各种状态标记
func getProcessingFlags(knowledgeBase *model.KnowledgeBase) []pbknowledge.KnowledgeBaseInfo_ProcessingFlag {
	processingFlags := make([]pbknowledge.KnowledgeBaseInfo_ProcessingFlag, 0)
	for val, _ := range pbknowledge.KnowledgeBaseInfo_ProcessingFlag_name {
		if val == 0 {
			continue
		}
		if knowledgeBase.HasProcessingFlag(uint64(val)) {
			processingFlags = append(processingFlags, pbknowledge.KnowledgeBaseInfo_ProcessingFlag(val))
		}
	}
	return processingFlags
}

// AddProcessingFlags 设置知识库处理标记
func AddProcessingFlags(ctx context.Context, corpBizID uint64, knowledgeBizIds []uint64,
	processingFlags []pbknowledge.KnowledgeBaseInfo_ProcessingFlag) error {
	// 先读取当前值
	knowledgeBases, err := dao.GetKnowledgeBaseDao(nil).GetKnowledgeBases(ctx, corpBizID, knowledgeBizIds)
	if err != nil {
		return err
	}
	knowledgeBaseMap := make(map[uint64]*model.KnowledgeBase)
	for _, knowledgeBase := range knowledgeBases {
		knowledgeBaseMap[knowledgeBase.KnowledgeBizId] = knowledgeBase
	}
	for _, knowledgeBizId := range knowledgeBizIds {
		knowledgeBase, ok := knowledgeBaseMap[knowledgeBizId]
		if !ok {
			knowledgeBase = &model.KnowledgeBase{
				CorpBizID:      corpBizID,
				KnowledgeBizId: knowledgeBizId,
				ProcessingFlag: 0,
				IsDeleted:      dao.IsNotDeleted,
				CreateTime:     time.Now(),
				UpdateTime:     time.Now(),
			}
		}
		oldProcessingFlag := knowledgeBase.ProcessingFlag
		for _, processingFlag := range processingFlags {
			knowledgeBase.AddProcessingFlag([]uint64{uint64(processingFlag)})
		}
		if oldProcessingFlag != knowledgeBase.ProcessingFlag {
			err = dao.GetKnowledgeBaseDao(nil).SetKnowledgeBase(ctx, corpBizID, knowledgeBase.KnowledgeBizId,
				knowledgeBase.ProcessingFlag)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// RemoveProcessingFlags 取消知识库处理标记
func RemoveProcessingFlags(ctx context.Context, corpBizID uint64, knowledgeBizIds []uint64,
	processingFlags []pbknowledge.KnowledgeBaseInfo_ProcessingFlag) error {
	// 先读取当前值
	knowledgeBases, err := dao.GetKnowledgeBaseDao(nil).GetKnowledgeBases(ctx, corpBizID, knowledgeBizIds)
	if err != nil {
		return err
	}
	knowledgeBaseMap := make(map[uint64]*model.KnowledgeBase)
	for _, knowledgeBase := range knowledgeBases {
		knowledgeBaseMap[knowledgeBase.KnowledgeBizId] = knowledgeBase
	}
	for _, knowledgeBizId := range knowledgeBizIds {
		knowledgeBase, ok := knowledgeBaseMap[knowledgeBizId]
		if !ok {
			// 如果不存在就不需求清理
			continue
		}
		oldProcessingFlag := knowledgeBase.ProcessingFlag
		for _, processingFlag := range processingFlags {
			knowledgeBase.RemoveProcessingFlag([]uint64{uint64(processingFlag)})
		}
		if oldProcessingFlag != knowledgeBase.ProcessingFlag {
			err = dao.GetKnowledgeBaseDao(nil).SetKnowledgeBase(ctx, corpBizID, knowledgeBase.KnowledgeBizId, knowledgeBase.ProcessingFlag)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// CheckProcessingFlag 检查处理标记
func CheckProcessingFlag(ctx context.Context, corpBizID uint64, knowledgeBizId uint64,
	flag pbknowledge.KnowledgeBaseInfo_ProcessingFlag) (bool, error) {
	knowledgeBases, err := dao.GetKnowledgeBaseDao(nil).GetKnowledgeBases(ctx, corpBizID, []uint64{knowledgeBizId})
	if err != nil {
		return false, err
	}
	if len(knowledgeBases) == 0 {
		return false, nil
	}
	return knowledgeBases[0].HasProcessingFlag(uint64(flag)), nil
}
