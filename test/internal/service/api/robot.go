package api

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/common/v3/utils"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// GetPresignedURL 获取临时链接 不用临时密钥 临时密钥有过期时间
func (s *Service) GetPresignedURL(ctx context.Context, req *pb.GetPresignedURLReq) (*pb.GetPresignedURLRsp, error) {
	log.DebugContextf(ctx, "GetPresignedURLReq:%s", utils.Any2String(req))
	doc := &model.Doc{}
	typeKey := model.OfflineStorageTypeKey
	if req.GetTypeKey() == model.RealtimeStorageTypeKey {
		realtimeDoc, err := s.dao.GetRealtimeDocByID(ctx, req.GetBusinessId())
		if err != nil || realtimeDoc == nil {
			return nil, errs.ErrDocNotFound
		}
		doc.ID = realtimeDoc.ID
		doc.BusinessID = realtimeDoc.DocID
		doc.RobotID = realtimeDoc.RobotID
		doc.CorpID = realtimeDoc.CorpID
		doc.CosURL = realtimeDoc.CosUrl
		doc.FileName = realtimeDoc.FileName
		doc.FileType = realtimeDoc.FileType
		typeKey = model.RealtimeStorageTypeKey
	} else {
		offlineDoc, err := s.dao.GetDocByBizID(ctx, req.GetBusinessId(), knowClient.NotVIP)
		if err != nil || offlineDoc == nil {
			return nil, errs.ErrDocNotFound
		}
		// 注意:IsNoCheckRefer字段不要暴露到云上
		if !req.GetIsNoCheckRefer() && !offlineDoc.IsReferOpen() {
			log.DebugContextf(ctx, "文档ID:%d 未开启引用文档", offlineDoc.ID)
			return &pb.GetPresignedURLRsp{}, nil
		}
		doc = offlineDoc
	}
	app, err := s.dao.GetAppByID(ctx, doc.RobotID)
	if err != nil || app == nil {
		return nil, errs.ErrRobotNotFound
	}
	corp, err := s.dao.GetCorpByID(ctx, doc.CorpID)
	if err != nil || corp == nil {
		return nil, errs.ErrCorpNotFound
	}
	// 黑名单检查
	if s.isBlacklistPresignedURLUin(ctx, corp.Uin) {
		return nil, errs.ErrPermissionDenied
	}
	err = s.dao.CheckURLPrefix(ctx, doc.CorpID, corp.BusinessID, app.BusinessID, doc.CosURL)
	if err != nil {
		log.ErrorContextf(ctx, "GetPresignedURL|CheckURLPrefix failed, err:%+v", err)
		return nil, errs.ErrInvalidURL
	}
	url, err := s.dao.GetPresignedURLWithTypeKey(ctx, typeKey, doc.CosURL)
	if err != nil {
		return nil, errs.ErrSystem
	}
	return &pb.GetPresignedURLRsp{
		FileName:   doc.FileName,
		FileType:   doc.FileType,
		CosUrl:     doc.CosURL,
		Url:        url,
		Bucket:     "",
		IsDownload: doc.IsDownloadable,
	}, nil
}

// isBlacklistPresignedURLUin 是否是预览URL黑名单Uin
func (s *Service) isBlacklistPresignedURLUin(ctx context.Context, uin string) bool {
	isBlacklist, ok := utilConfig.GetWhitelistConfig().PresignedURLUinBlacklist[uin]
	if ok && isBlacklist {
		log.InfoContextf(ctx, "isBlacklistPresignedURLUin|uin:%s is in blacklist", uin)
		return true
	}
	return false
}

// getRobotByID 通过自增ID获取机器人
func (s *Service) getRobotByID(ctx context.Context, appID uint64) (*model.AppDB, error) {
	app, err := s.dao.GetAppByID(ctx, appID)
	if err != nil {
		return nil, err
	}
	if app == nil {
		return nil, errs.ErrRobotNotFound
	}
	return app, nil
}

// GetPresignedURLNoCheck 获取临时链接 不用临时密钥 临时密钥有过期时间，不进行黑名单校验内部调用
func (s *Service) GetPresignedURLNoCheck(ctx context.Context, req *pb.GetPresignedURLReq) (*pb.GetPresignedURLRsp,
	error) {
	doc, err := s.dao.GetDocByBizID(ctx, req.GetBusinessId(), knowClient.NotVIP)
	if err != nil || doc == nil {
		return nil, errs.ErrDocNotFound
	}
	// 注意:IsNoCheckRefer字段不要暴露到云上
	if !req.GetIsNoCheckRefer() && !doc.IsReferOpen() {
		log.DebugContextf(ctx, "文档ID:%d 未开启引用文档", doc.ID)
		return &pb.GetPresignedURLRsp{}, nil
	}
	app, err := s.dao.GetAppByID(ctx, doc.RobotID)
	if err != nil || app == nil {
		return nil, errs.ErrRobotNotFound
	}
	corp, err := s.dao.GetCorpByID(ctx, doc.CorpID)
	if err != nil || corp == nil {
		return nil, errs.ErrCorpNotFound
	}
	err = s.dao.CheckURLPrefix(ctx, doc.CorpID, corp.BusinessID, app.BusinessID, doc.CosURL)
	if err != nil {
		log.ErrorContextf(ctx, "GetPresignedURL|CheckURLPrefix failed, err:%+v", err)
		return nil, errs.ErrInvalidURL
	}
	url, err := s.dao.GetPresignedURL(ctx, doc.CosURL)
	if err != nil {
		return nil, errs.ErrSystem
	}
	return &pb.GetPresignedURLRsp{
		FileName: doc.FileName,
		FileType: doc.FileType,
		CosUrl:   doc.CosURL,
		Url:      url,
		Bucket:   "",
	}, nil
}
