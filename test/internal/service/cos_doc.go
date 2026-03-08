// Package service 业务逻辑层-客户COS文档
package service

import (
	"context"
	"errors"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	"sync"
)

// UploadCOSFileList 上传COS文件列表
//
//	@alias=/UploadCOSFileList
func (s *Service) UploadCOSFileList(ctx context.Context, req *knowledge.UploadCOSFileListReq) (*knowledge.UploadCOSFileListRsp, error) {
	log.InfoContextf(ctx, "UploadCOSFileList req:%+v", req)
	rsp := new(knowledge.UploadCOSFileListRsp)
	app, err := s.getAppByAppBizID(ctx, req.GetAppBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corp, err := s.dao.GetCorpByID(ctx, pkg.CorpID(ctx))
	if err != nil || corp == nil {
		return rsp, errs.ErrCorpNotFound
	}
	uin, _ := model.GetLoginUinAndSubAccountUin(ctx)
	credentialResponse, status, err := s.dao.AssumeServiceRole(ctx, uin,
		config.App().COSDocumentConfig.ServiceRole, 0, nil)
	if err != nil {
		return rsp, errs.ErrAssumeServiceRoleFailed
	}
	if status != knowledge.RoleStatusType_RoleStatusAvailable {
		return rsp, errs.ErrServiceRoleUnavailable
	}
	log.DebugContextf(ctx, "UploadCOSFileList, credentialResponse: %+v", credentialResponse)
	wg := sync.WaitGroup{}
	for _, v := range req.GetFileList() {
		wg.Add(1)
		go func(file *knowledge.COSFile) {
			defer wg.Done()
			doc.SaveCorpCosToInnerCos(ctx, s.dao, app, corp, file, credentialResponse.Credentials)
			rsp.FileList = append(rsp.FileList, file)
		}(v)
	}
	wg.Wait()
	return rsp, nil
}

// DescribeFileUploadInfo 查询文件上传信息
//
//	@alias=/DescribeFileUploadInfo
func (s *Service) DescribeFileUploadInfo(ctx context.Context, req *knowledge.DescribeFileUploadInfoReq) (*knowledge.DescribeFileUploadInfoRsp, error) {
	log.InfoContextf(ctx, "DescribeFileUploadInfo req:%+v", req)
	rsp := new(knowledge.DescribeFileUploadInfoRsp)
	app, err := s.getAppByAppBizID(ctx, req.GetAppBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corp, err := s.dao.GetCorpByID(ctx, pkg.CorpID(ctx))
	if err != nil || corp == nil {
		return rsp, errs.ErrCorpNotFound
	}
	docMap, err := doc.DescribeFileUploadInfo(ctx, app.ID, corp.ID, req.GetFileBizIds())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	for _, v := range req.GetFileBizIds() {
		corpCOSDoc, ok := docMap[v]
		if !ok {
			rsp.FileList = append(rsp.FileList, &knowledge.COSFile{
				FileBizId:  v,
				FileStatus: knowledge.FileStatusType_FileStatusUploadFailed,
			})
		} else {
			rsp.FileList = append(rsp.FileList, &knowledge.COSFile{
				FileBizId:   corpCOSDoc.BusinessID,
				FileStatus:  knowledge.FileStatusType(corpCOSDoc.Status),
				BizFileKey:  corpCOSDoc.BusinessCosURL,
				BizFileTag:  corpCOSDoc.BusinessCosTag,
				BizFileHash: corpCOSDoc.BusinessCosHash,
			})
		}
	}
	return rsp, nil
}

// DescribeFileUploadInfos 批量查询文件上传信息
//
//	@alias=/DescribeFileUploadInfos
func (s *Service) DescribeFileUploadInfos(ctx context.Context, req *knowledge.DescribeFileUploadInfoReq) (*knowledge.DescribeFileUploadInfoRsp, error) {
	log.InfoContextf(ctx, "DescribeFileUploadInfos req:%+v", req)
	return nil, errors.New("rpc DescribeFileUploadInfos of service Admin is not implemented")
}

// CommitCOSFileList 提交COS文件列表
//
//	@alias=/CommitCOSFileList
func (s *Service) CommitCOSFileList(ctx context.Context, req *knowledge.CommitCOSFileListReq) (*knowledge.CommitCOSFileListRsp, error) {
	return nil, errors.New("rpc CommitCOSFileList of service Admin is not implemented")
}
