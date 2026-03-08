package doc

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	cloudsts "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/sts/v20180813"
)

// SaveCorpCosToInnerCos 保存企业COS到内部COS
func SaveCorpCosToInnerCos(ctx context.Context, d dao.Dao, app *model.App, corp *model.Corp, file *knowledge.COSFile,
	credential *cloudsts.Credentials) {
	log.DebugContextf(ctx, "SaveCorpCosToInnerCos req:%+v", file)
	key := fmt.Sprintf(dao.LockSaveCorpCOSDoc, pkg.CorpID(ctx), file.GetFileTag())
	if err := d.Lock(ctx, key, 120*time.Second); err != nil {
		log.ErrorContextf(ctx, "SaveCorpCosToInnerCos lock fail req:%+v, err:%+v", file, err)
		file.FileStatus = knowledge.FileStatusType_FileStatusUploading
		return
	}

	defer func() { _ = d.UnLock(ctx, key) }()
	selectColumns := []string{dao.CorpCOSDocTblColBusinessID, dao.CorpCOSDocTblColStatus}
	filter := dao.CorpCOSDocFilter{
		CorpID:  corp.ID,
		RobotID: app.ID,
		CosTag:  file.GetFileTag(),
	}
	doc, err := dao.GetCorpCOSDocDao().GetCorpCosDoc(ctx, selectColumns, filter)
	if err != nil {
		log.ErrorContextf(ctx, "SaveCorpCosToInnerCos GetCorpCosDoc from db req:%+v, err:%+v", file, err)
		return
	}
	// 如果已经存在，则直接返回
	if doc.BusinessID != 0 {
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
		doc = &model.CorpCOSDoc{
			BusinessID: d.GenerateSeqID(),
			CorpID:     corp.ID,
			RobotID:    app.ID,
			CosTag:     file.GetFileTag(),
			CosPath:    file.GetFileKey(),
			CosBucket:  file.GetFileBucket(),
			SyncTime:   time.Now(),
			StaffID:    uint64(pkg.SID(ctx)),
			Status:     uint32(knowledge.FileStatusType_FileStatusUploading),
		}
		err = dao.GetCorpCOSDocDao().CreateCorpCosDoc(ctx, doc)
		if err != nil {
			log.ErrorContextf(ctx, "SaveCorpCosToInnerCos CreateCorpCosDoc to db req:%+v, err:%+v", file, err)
			return
		}
	}
	defer func() {
		// 更新状态和内部cos信息
		err = dao.GetCorpCOSDocDao().UpdateCorpCosDoc(ctx, doc)
		if err != nil {
			log.ErrorContextf(ctx, "SaveCorpCosToInnerCos UpdateCorpCosDoc to db req:%+v, err:%+v", file, err)
			return
		}
	}()
	file.FileBizId = doc.BusinessID
	content, err := d.GetCOSObject(ctx, credential, file.GetFileBucket(), "ap-guangzhou", file.GetFileKey())
	if err != nil {
		log.ErrorContextf(ctx, "SaveCorpCosToInnerCos GetCOSObject req:%+v, err:%+v", file, err)
		return
	}
	fileName := filepath.Ext(file.GetFileKey())
	cosFileName := util.RandStr(20) + strconv.Itoa(util.New().Intn(10000000000)) + fileName
	cosPath := d.GetCorpRobotCOSPath(ctx, corp.BusinessID, app.BusinessID, cosFileName)
	err = d.PutObject(ctx, content, cosPath)
	if err != nil {
		doc.Status = uint32(knowledge.FileStatusType_FileStatusUploadFailed)
		doc.FailReason = fmt.Sprintf("SaveCorpCosToInnerCos PutObject err:%+v", err)
		log.ErrorContextf(ctx, "SaveCorpCosToInnerCos PutObject cosPath:%s req:%+v, err:%+v", cosPath, file, err)
		return
	}
	objInfo, err := d.StatObject(ctx, cosPath)
	if err != nil {
		doc.Status = uint32(knowledge.FileStatusType_FileStatusUploadFailed)
		doc.FailReason = fmt.Sprintf("SaveCorpCosToInnerCos StatObject err:%+v", err)
		log.ErrorContextf(ctx, "SaveCorpCosToInnerCos StatObject req:%+v, err:%+v", file, err)
		return
	}
	doc.BusinessCosURL = cosPath
	doc.BusinessCosHash = objInfo.Hash
	doc.BusinessCosTag = objInfo.ETag
	doc.Status = uint32(knowledge.FileStatusType_FileStatusUploadSucceeded)
	return
}

// DescribeFileUploadInfo 获取上传文件列表
func DescribeFileUploadInfo(ctx context.Context, appID, corpID uint64, fieBizIDs []uint64) (map[uint64]*model.CorpCOSDoc, error) {
	docsMap := make(map[uint64]*model.CorpCOSDoc)
	selectColumns := []string{
		dao.CorpCOSDocTblColBusinessID,
		dao.CorpCOSDocTblColBusinessCosURL,
		dao.CorpCOSDocTblColBusinessCosHash,
		dao.CorpCOSDocTblColBusinessCosTag,
		dao.CorpCOSDocTblColIsDeleted,
		dao.CorpCOSDocTblColStatus,
		dao.CorpCOSDocTblColFailReason,
	}
	filter := dao.CorpCOSDocFilter{
		BusinessIDs: fieBizIDs,
		RobotID:     appID,
		CorpID:      corpID,
	}
	docs, err := dao.GetCorpCOSDocDao().GetCorpCosDocs(ctx, selectColumns, filter)
	if err != nil {
		log.ErrorContextf(ctx, "DescribeFileUploadInfo GetCorpCosDocByBizIDs err:%+v", err)
		return nil, err
	}
	for _, doc := range docs {
		docsMap[doc.BusinessID] = doc
	}
	return docsMap, nil
}
