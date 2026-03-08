package share_knowledge

import (
	"context"
	"database/sql"
	"errors"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
)

func GetModelAssociatedApps(ctx context.Context,
	db dao.Dao,
	corpBizID uint64,
	spaceID string,
	modelKeyword string) ([]*pb.KnowledgeBaseInfo, error) {
	log.InfoContextf(ctx, "GetModelAssociatedApps, corpBizID: %d, spaceID: %s, modelKeyword: %s", corpBizID, spaceID, modelKeyword)

	knowledgeBaseConfigs, err := dao.GetKnowledgeConfigDao(nil).
		GetKnowledgeConfigsByModelAssociated(ctx, corpBizID, modelKeyword)
	if err != nil {
		log.ErrorContextf(ctx, "GetModelAssociatedApps dao.GetKnowledgeConfigsByModelAssociated fail, err=%+v", err)
		return nil, err
	}

	if len(knowledgeBaseConfigs) == 0 {
		return nil, nil
	}

	knowledgeBizIDList := make([]uint64, 0, len(knowledgeBaseConfigs))
	for _, knowledgeBaseConfig := range knowledgeBaseConfigs {
		knowledgeBizIDList = append(knowledgeBizIDList, knowledgeBaseConfig.KnowledgeBizId)
	}

	knowledgeBaseInfoList, err := db.RetrieveBaseSharedKnowledge(ctx, corpBizID, knowledgeBizIDList)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		log.ErrorContextf(ctx, "GetModelAssociatedApps dao.RetrieveBaseSharedKnowledge fail, err=%+v", err)
		return nil, err
	}

	result := make([]*pb.KnowledgeBaseInfo, 0, len(knowledgeBaseInfoList))
	for _, shareKb := range knowledgeBaseInfoList {
		log.InfoContextf(ctx, "GetModelAssociatedApps, shareKb: %+v", shareKb)
		if spaceID != "" && shareKb.SpaceID != spaceID {
			continue
		}
		baseInfo, _ := ConvertSharedKnowledgeBaseInfo(ctx, shareKb)
		result = append(result, baseInfo)
	}

	return result, nil
}
