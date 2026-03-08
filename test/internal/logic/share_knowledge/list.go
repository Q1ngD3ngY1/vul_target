package share_knowledge

import (
	"context"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
)

func GetSpaceShareKnowledgeListExSelf(ctx context.Context,
	db dao.Dao,
	corpBizID, exStaffID uint64,
	spaceID, keyword string,
	pageNumber, pageSize uint32) (int64, []*pb.KnowledgeBaseInfo, error) {
	total, list, err := db.ListSpaceShareKnowledgeExSelf(ctx, corpBizID, exStaffID, spaceID, keyword, pageNumber, pageSize)
	if err != nil {
		log.ErrorContextf(ctx, "GetSpaceShareKnowledgeListExSelf ListSpaceShareKnowledgeExSelf fail, err=%+v", err)
		return 0, nil, err
	}
	result := make([]*pb.KnowledgeBaseInfo, 0, len(list))
	for idx := range list {
		baseInfo, _ := ConvertSharedKnowledgeBaseInfo(ctx, list[idx])
		result = append(result, baseInfo)
	}
	return total, result, nil
}
