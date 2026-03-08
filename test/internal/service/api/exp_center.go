package api

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	dbEntity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	logicApp "git.woa.com/adp/kb/kb-config/internal/logic/app"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"github.com/spf13/cast"
)

// GetExpInfo 获得体验中心信息
// 调用路径：/trpc.KEP.bot_admin_config_server.Admin/DescribeExpInfo -> here
// knowledge 是 admin 的体验中心数据提供方之一
func (s *Service) GetExpInfo(ctx context.Context, req *pb.GetExpInfoReq) (*pb.GetExpInfoRsp, error) {
	if req.GetExpInfoType() != pb.ExpInfoType_EXP_INFO_TYPE_KNOWLEDGE {
		return nil, errs.ErrParameterInvalid
	}
	appBizID, err := cast.ToUint64E(req.GetExpAppBizId())
	if err != nil {
		logx.E(ctx, "GetExpInfo cast.ToUint64E err: %+v", err)
		return nil, errs.ErrParameterInvalid
	}
	app, err := s.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, appBizID, entity.AppReleaseScenes)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	modelName, err := logicApp.GetAppNormalModelName(ctx, app)
	if err != nil {
		logx.E(ctx, "GetExpInfo GetAppNormalModelName err: %+v", err)
		return nil, errs.ErrNotFoundModel
	}

	qaCount, err := s.qaLogic.GetDocQaCount(ctx, nil, &qaEntity.DocQaFilter{
		CorpId:  app.CorpPrimaryId,
		RobotId: app.PrimaryId,
	})
	if err != nil {
		logx.E(ctx, "GetExpMetaInfo GetQAChunkCount err: %+v", err)
		return nil, errs.ErrGetKnowledgeFailed
	}
	segCount, err := s.docLogic.GetDocCount(ctx, nil, &docEntity.DocFilter{
		CorpId:  app.CorpPrimaryId,
		RobotId: app.PrimaryId,
	})
	if err != nil {
		logx.E(ctx, "GetExpMetaInfo GetSegmentChunkCount err: %+v", err)
		return nil, errs.ErrGetKnowledgeFailed
	}
	tableFilter := dbEntity.TableFilter{
		CorpBizID: app.CorpBizId,
		AppBizID:  app.BizId,
	}
	tables, _, err := s.dbLogic.DescribeTableList(ctx, &tableFilter)
	// tableNames, err := dao.GetDBTableDao().BatchGetTableName(ctx, app.CorpBizId, app.AppBizId)
	if err != nil {
		logx.E(ctx, "GetExpMetaInfo BatchGetTableName err: %+v", err)
		return nil, errs.ErrGetKnowledgeFailed
	}
	var tableNames []string
	for _, t := range tables {
		tableNames = append(tableNames, t.Name)
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
