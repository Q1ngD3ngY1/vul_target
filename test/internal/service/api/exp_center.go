package api

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	logicApp "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/app"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	"github.com/spf13/cast"
)

// GetExpInfo 获得体验中心信息
func (s *Service) GetExpInfo(ctx context.Context, req *pb.GetExpInfoReq) (*pb.GetExpInfoRsp, error) {
	if req.GetExpInfoType() != pb.ExpInfoType_EXP_INFO_TYPE_KNOWLEDGE {
		return nil, errs.ErrParameterInvalid
	}
	appBizID, err := cast.ToUint64E(req.GetExpAppBizId())
	if err != nil {
		log.ErrorContextf(ctx, "GetExpInfo cast.ToUint64E err: %+v", err)
		return nil, errs.ErrParameterInvalid
	}
	app, err := client.GetAppInfo(ctx, appBizID, model.AppReleaseScenes)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	modelName, err := logicApp.GetAppNormalModelName(ctx, app)
	if err != nil {
		log.ErrorContextf(ctx, "GetExpInfo GetAppNormalModelName err: %+v", err)
		return nil, errs.ErrNotFoundModel
	}

	qaCount, err := dao.GetDocQaDao().GetDocQaCount(ctx, nil, &dao.DocQaFilter{
		CorpId:  app.CorpId,
		RobotId: app.Id,
	})
	if err != nil {
		log.ErrorContextf(ctx, "GetExpMetaInfo GetQAChunkCount err: %+v", err)
		return nil, errs.ErrGetKnowledgeFailed
	}
	segCount, err := dao.GetDocDao().GetDocCount(ctx, nil, &dao.DocFilter{
		CorpId:  app.CorpId,
		RobotId: app.Id,
	})
	if err != nil {
		log.ErrorContextf(ctx, "GetExpMetaInfo GetSegmentChunkCount err: %+v", err)
		return nil, errs.ErrGetKnowledgeFailed
	}
	tableNames, err := dao.GetDBTableDao().BatchGetTableName(ctx, app.CorpBizId, app.AppBizId)
	if err != nil {
		log.ErrorContextf(ctx, "GetExpMetaInfo BatchGetTableName err: %+v", err)
		return nil, errs.ErrGetKnowledgeFailed
	}
	return &pb.GetExpInfoRsp{
		ExpInfoType: req.GetExpInfoType(),
		KnowledgeExpInfos: []*pb.KnowledgeExpInfo{
			{
				QaCount:           uint64(qaCount),
				DocCount:          uint64(segCount),
				DbSourceTableName: tableNames,
				ModelName:         []string{modelName},
			},
		},
	}, nil
}
