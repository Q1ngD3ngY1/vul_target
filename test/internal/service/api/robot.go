package api

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// GetPresignedURL 获取临时链接 不用临时密钥 临时密钥有过期时间
func (s *Service) GetPresignedURL(ctx context.Context, req *pb.GetPresignedURLReq) (*pb.GetPresignedURLRsp, error) {
	logx.D(ctx, "GetPresignedURLReq:%s", req)
	doc := &docEntity.Doc{}
	typeKey := entity.OfflineStorageTypeKey
	if req.GetTypeKey() == entity.RealtimeStorageTypeKey {
		realtimeDoc, err := s.docLogic.GetRealtimeDocByID(ctx, req.GetBusinessId())
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
		typeKey = entity.RealtimeStorageTypeKey
	} else {
		offlineDoc, err := s.docLogic.GetDocByBizID(ctx, req.GetBusinessId(), knowClient.NotVIP)
		if err != nil || offlineDoc == nil {
			return nil, errs.ErrDocNotFound
		}
		// 注意:IsNoCheckRefer字段不要暴露到云上
		if !req.GetIsNoCheckRefer() && !offlineDoc.IsReferOpen() {
			logx.D(ctx, "文档ID:%d 未开启引用文档", offlineDoc.ID)
			return &pb.GetPresignedURLRsp{}, nil
		}
		doc = offlineDoc
	}
	app, err := s.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, doc.RobotID)
	if err != nil || app == nil {
		return nil, errs.ErrRobotNotFound
	}
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, doc.CorpID)
	// corp, err := s.dao.GetCorpByID(ctx, doc.CorpPrimaryId)
	if err != nil || corp == nil {
		return nil, errs.ErrCorpNotFound
	}
	// 黑名单检查
	if s.isBlacklistPresignedURLUin(ctx, corp.Uin) {
		return nil, errs.ErrPermissionDenied
	}
	err = s.s3.CheckURLPrefix(ctx, doc.CorpID, corp.GetCorpId(), app.BizId, doc.CosURL)
	if err != nil {
		logx.E(ctx, "GetPresignedURL|CheckURLPrefix failed, err:%+v", err)
		return nil, errs.ErrInvalidURL
	}
	url, err := s.s3.GetPreSignedURLWithTypeKey(ctx, typeKey, doc.CosURL, 0)
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
	isBlacklist, ok := config.GetWhitelistConfig().PresignedURLUinBlacklist[uin]
	if ok && isBlacklist {
		logx.I(ctx, "isBlacklistPresignedURLUin|uin:%s is in blacklist", uin)
		return true
	}
	return false
}

// GetPresignedURLNoCheck 获取临时链接 不用临时密钥 临时密钥有过期时间，不进行黑名单校验内部调用
func (s *Service) GetPresignedURLNoCheck(ctx context.Context, req *pb.GetPresignedURLReq) (*pb.GetPresignedURLRsp, error) {
	doc, err := s.docLogic.GetDocByBizID(ctx, req.GetBusinessId(), knowClient.NotVIP)
	if err != nil || doc == nil {
		return nil, errs.ErrDocNotFound
	}
	// 注意:IsNoCheckRefer字段不要暴露到云上
	if !req.GetIsNoCheckRefer() && !doc.IsReferOpen() {
		logx.D(ctx, "文档ID:%d 未开启引用文档", doc.ID)
		return &pb.GetPresignedURLRsp{}, nil
	}
	app, err := s.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, doc.RobotID)
	if err != nil || app == nil {
		return nil, errs.ErrRobotNotFound
	}
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, doc.CorpID)
	// corp, err := s.dao.GetCorpByID(ctx, doc.CorpPrimaryId)
	if err != nil || corp == nil {
		return nil, errs.ErrCorpNotFound
	}
	err = s.s3.CheckURLPrefix(ctx, doc.CorpID, corp.GetCorpId(), app.BizId, doc.CosURL)
	if err != nil {
		logx.E(ctx, "GetPresignedURL|CheckURLPrefix failed, err:%+v", err)
		return nil, errs.ErrInvalidURL
	}
	url, err := s.s3.GetPreSignedURL(ctx, doc.CosURL)
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
