package service

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/mathx/randx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"github.com/spf13/cast"
	cloudsts "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/sts/v20180813"

	knowledge "git.woa.com/adp/pb-go/kb/kb_config"
)

// UploadCOSFileList 上传COS文件列表
//
//	@alias=/UploadCOSFileList
func (s *Service) UploadCOSFileList(ctx context.Context, req *knowledge.UploadCOSFileListReq) (*knowledge.UploadCOSFileListRsp, error) {
	logx.I(ctx, "UploadCOSFileList req:%+v", req)
	rsp := new(knowledge.UploadCOSFileListRsp)
	corpID := contextx.Metadata(ctx).CorpID()
	staffID := contextx.Metadata(ctx).StaffID()
	appBizId := cast.ToUint64(req.GetAppBizId())
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, appBizId)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpID)
	if err != nil || corp == nil {
		return rsp, errs.ErrCorpNotFound
	}
	uin, _ := kbEntity.GetLoginUinAndSubAccountUin(ctx)
	credentialResponse, status, err := s.rpc.Cloud.AssumeServiceRole(ctx, uin,
		config.App().COSDocumentConfig.ServiceRole, 0, nil)
	if err != nil {
		return rsp, errs.ErrAssumeServiceRoleFailed
	}
	if status != knowledge.RoleStatusType_RoleStatusAvailable {
		return rsp, errs.ErrServiceRoleUnavailable
	}
	logx.D(ctx, "UploadCOSFileList, credentialResponse: %+v", credentialResponse)
	wg := sync.WaitGroup{}
	for _, v := range req.GetFileList() {
		wg.Add(1)
		go func(file *knowledge.COSFile) {
			defer wg.Done()
			s.SaveCorpCosToInnerCos(ctx, app.PrimaryId, appBizId, corpID, corp.CorpId, staffID,
				file, credentialResponse.Credentials)
			rsp.FileList = append(rsp.FileList, file)
		}(v)
	}
	wg.Wait()
	return rsp, nil
}

func (s *Service) SaveCorpCosToInnerCos(ctx context.Context, appID, appBizId, corpID, corpBizID, staffID uint64,
	file *knowledge.COSFile, credential *cloudsts.Credentials) {
	logx.D(ctx, "SaveCorpCosToInnerCos req:%+v", file)
	key := fmt.Sprintf(dao.LockSaveCorpCOSDoc, corpID, file.GetFileTag())
	if err := s.dao.Lock(ctx, key, 120*time.Second); err != nil {
		logx.E(ctx, "SaveCorpCosToInnerCos lock fail req:%+v, err:%+v", file, err)
		file.FileStatus = knowledge.FileStatusType_FileStatusUploading
		return
	}

	defer func() { _ = s.dao.UnLock(ctx, key) }()

	selectColumns := []string{docEntity.CorpCOSDocTblColBusinessID, docEntity.CorpCOSDocTblColStatus}
	filter := &docEntity.CorpCOSDocFilter{
		CorpID:  corpID,
		RobotID: appID,
		CosTag:  file.GetFileTag(),
	}
	doc, err := s.docLogic.GetDao().DescribeCorpCosDoc(ctx, selectColumns, filter)
	if err != nil {
		logx.E(ctx, "SaveCorpCosToInnerCos GetCorpCosDoc from db req:%+v, err:%+v", file, err)
		return
	}

	// 如果已经存在，则直接返回
	if doc != nil && doc.BusinessID != 0 {
		if slices.Contains([]uint32{
			uint32(knowledge.FileStatusType_FileStatusQueuing),
			uint32(knowledge.FileStatusType_FileStatusUploading),
			uint32(knowledge.FileStatusType_FileStatusTargetAlreadyExist),
			uint32(knowledge.FileStatusType_FileStatusUploadSucceeded),
		}, doc.Status) {
			file.FileBizId = doc.BusinessID
			return
		}
		// 其他状态支持重试
	} else {
		// 如果不存在，则创建一个新的文档 并下载上传内部cos
		doc = &docEntity.CorpCOSDoc{
			BusinessID: idgen.GetId(),
			CorpID:     corpID,
			RobotID:    appID,
			CosTag:     file.GetFileTag(),
			CosPath:    file.GetFileKey(),
			CosBucket:  file.GetFileBucket(),
			SyncTime:   time.Now(),
			StaffID:    staffID,
			Status:     uint32(knowledge.FileStatusType_FileStatusUploading),
		}
		err = s.docLogic.GetDao().CreateCorpCosDoc(ctx, doc)
		if err != nil {
			logx.E(ctx, "SaveCorpCosToInnerCos CreateCorpCosDoc to db req:%+v, err:%+v", file, err)
			return
		}
	}

	defer func() {
		logx.I(ctx, "Update CorpCosDoc Record. (docId: %d)", doc.ID)
		// 更新状态和内部cos信息
		updateColumns := []string{
			docEntity.CorpCOSDocTblColStatus,
			docEntity.CorpCOSDocTblColBusinessCosURL,
			docEntity.CorpCOSDocTblColBusinessCosHash,
			docEntity.CorpCOSDocTblColBusinessCosTag,
			docEntity.CorpCOSDocTblColSyncTime,
			docEntity.CorpCOSDocTblColUpdateTime,
		}
		filter := &docEntity.CorpCOSDocFilter{
			BusinessIDs: []uint64{doc.BusinessID},
			RobotID:     appID,
			CorpID:      corpID,
		}
		doc.UpdateTime = time.Now()
		err = s.docLogic.GetDao().ModifyCorpCosDoc(ctx, updateColumns, filter, doc)
		if err != nil {
			logx.E(ctx, "SaveCorpCosToInnerCos UpdateCorpCosDoc to db req:%+v, err:%+v", file, err)
			return
		}
	}()

	file.FileBizId = doc.BusinessID
	content, err := s.rpc.COS.GetCOSObject(ctx, credential, file.GetFileBucket(), "ap-guangzhou", file.GetFileKey())
	if err != nil {
		logx.E(ctx, "SaveCorpCosToInnerCos GetCOSObject req:%+v, err:%+v", file, err)
		return
	}
	fileName := filepath.Ext(file.GetFileKey())
	cosFileName := randx.RandomString(20) + strconv.Itoa(rand.Intn(10000000000)) + fileName
	// util.RandStr(20) + strconv.Itoa(util.New().Intn(10000000000)) + fileName
	cosPath := s.s3.GetCorpRobotCOSPath(ctx, corpBizID, appBizId, cosFileName)
	err = s.s3.PutObject(ctx, content, cosPath)
	if err != nil {
		doc.Status = uint32(knowledge.FileStatusType_FileStatusUploadFailed)
		doc.FailReason = fmt.Sprintf("SaveCorpCosToInnerCos PutObject err:%+v", err)
		logx.E(ctx, "SaveCorpCosToInnerCos PutObject cosPath:%s req:%+v, err:%+v", cosPath, file, err)
		return
	}
	objInfo, err := s.s3.StatObject(ctx, cosPath)
	if err != nil {
		doc.Status = uint32(knowledge.FileStatusType_FileStatusUploadFailed)
		doc.FailReason = fmt.Sprintf("SaveCorpCosToInnerCos StatObject err:%+v", err)
		logx.E(ctx, "SaveCorpCosToInnerCos StatObject req:%+v, err:%+v", file, err)
		return
	}
	doc.BusinessCosURL = cosPath
	doc.BusinessCosHash = objInfo.Hash
	doc.BusinessCosTag = objInfo.ETag
	doc.Status = uint32(knowledge.FileStatusType_FileStatusUploadSucceeded)
	return

}

// DescribeFileUploadInfo 查询文件上传信息
//
//	@alias=/DescribeUploadInfo
//	@alias=/DescribeFileUploadInfo
func (s *Service) DescribeFileUploadInfo(ctx context.Context, req *knowledge.DescribeFileUploadInfoReq) (*knowledge.DescribeFileUploadInfoRsp, error) {
	logx.I(ctx, "DescribeFileUploadInfo req:%+v", req)
	rsp := new(knowledge.DescribeFileUploadInfoRsp)
	corpID := contextx.Metadata(ctx).CorpID()
	appBizId := cast.ToUint64(req.GetAppBizId())
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, appBizId)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpID)
	if err != nil || corp == nil {
		return rsp, errs.ErrCorpNotFound
	}
	docMap, err := s.docLogic.DescribeFileUploadInfo(ctx, app.PrimaryId, corp.CorpPrimaryId, req.GetFileBizIds())
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
	logx.I(ctx, "DescribeFileUploadInfos req:%+v", req)
	return nil, errors.New("rpc DescribeFileUploadInfos of service Admin is not implemented")
}

// CommitCOSFileList 提交COS文件列表
//
//	@alias=/CommitCOSFileList
func (s *Service) CommitCOSFileList(ctx context.Context, req *knowledge.CommitCOSFileListReq) (*knowledge.CommitCOSFileListRsp, error) {
	return nil, errors.New("rpc CommitCOSFileList of service Admin is not implemented")
}
