package async

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/clientx/s3x"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/entity/finance"
	"git.woa.com/adp/pb-go/common"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	secapi "git.woa.com/sec-api/go/scurl"
	"github.com/spf13/cast"

	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	logicCommon "git.woa.com/adp/kb/kb-config/internal/logic/common"
	"git.woa.com/adp/kb/kb-config/internal/rpc"

	internalDao "git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/third_doc"
)

const (
	DocRefreshPrefix = "doc_refresh:dosage:"
	splitTag         = "|"
)

// DocRefreshScheduler 文档刷新任务， 包含腾讯文档、onedrive、sharepoint
type DocRefreshScheduler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity.DocRefreshParams
}

func registerDocRefreshScheduler(tc *taskCommon) {
	task_scheduler.Register(
		entity.RefreshThirdDocTask,
		func(t task_scheduler.Task, params entity.DocRefreshParams) task_scheduler.TaskHandler {
			return &DocRefreshScheduler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

// Prepare 数据准备
func (d *DocRefreshScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(ThirdDocRefresh) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	for docID, info := range d.p.FileInfo {
		log.DebugContextf(ctx, "task(ThirdDocRefresh) Prepare, FileInfo: %+v ", info)
		kv[fmt.Sprintf("%s%d", DocRefreshPrefix, docID)] = fmt.Sprintf("%d", docID)
	}
	return kv, nil
}

// Init 初始化
func (d *DocRefreshScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.InfoContextf(ctx, "task(ThirdDocRefresh) Init start")
	return nil
}

// Process 任务处理
func (d *DocRefreshScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(ThirdDocRefresh) Process, task: %+v, params: %+v", d.task, d.p)
	if len(d.p.EnvSet) > 0 { // 审核回调需要env-set
		contextx.Metadata(ctx).WithEnvSet(d.p.EnvSet)
	}
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(ThirdDocRefresh) Start k:%s, v:%s", k, v)
		key := k
		id := cast.ToUint64(v)
		tFileInfo, exists := d.p.FileInfo[id]
		if !exists {
			if err := progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(ThirdDocRefresh) Finish kv:%s err:%+v", key, err)
				return err
			}
			log.ErrorContextf(ctx, "task(ThirdDocRefresh) FileInfo kv:%s docID:%+v tFileInfo is nil", key, id)
			continue
		}
		if tFileInfo.FileID == "" || tFileInfo.RobotID == 0 || tFileInfo.OperationID == "" || tFileInfo.CorpID == 0 {
			if err := progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(ThirdDocRefresh) Finish kv:%s err:%+v", key, err)
				return err
			}
			log.ErrorContextf(ctx, "task(ThirdDocRefresh) FileInfo:%v docID:%+v tFileInfo is nil", tFileInfo, id)
			continue
		}
		appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, tFileInfo.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			logx.D(ctx, "task(DocResume) appDB.HasDeleted()|appID:%d", tFileInfo.RobotID)
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(DocResume) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}

		doc, err := d.docLogic.GetDocByID(ctx, id, appDB.BizId)
		if err != nil {
			logx.E(ctx, "task(ThirdDocRefresh) GetDocByBizID kv:%s err:%+v", key, err)
			return err
		}
		corp, err := d.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, doc.CorpID)
		// corp, err := d.dao.GetCorpByID(ctx, doc.CorpPrimaryId)
		if err != nil {
			logx.E(ctx, "AutoRunDocDiffTask GetCorpByID err: %+v", err)
			return err
		}

		staffInfo, err := d.rpc.PlatformAdmin.GetStaffByID(ctx, doc.StaffID)
		if err != nil {
			log.ErrorContextf(ctx, "task(TxDocRefresh) GetStaffByID err: %+v", err)
			return err
		}
		// TODO
		user, err := d.userLogic.DescribeExpUser(ctx, staffInfo.UserID)
		if err != nil {
			log.ErrorContextf(ctx, "task(TxDocRefresh) GetUserID failed, err:%+v", err)
			return err
		}
		if err = d.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{App: appDB}); err != nil {
			log.WarnContextf(ctx, "task(TxDocRefresh) CheckKnowledgeBaseQuota fail: %+v",
				logicCommon.ConvertErrMsg(ctx, d.rpc, 0, appDB.PrimaryId, err))
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(TxDocRefresh) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		var cosURL, cosHash string
		var fileSize uint64
		fileName := getFileNameByType(doc.FileType)
		cosPath := d.s3.GetCorpRobotCOSPath(ctx, corp.GetCorpId(), appDB.BizId, fileName)
		cosURL = cosPath
		switch d.p.SourceFrom {
		case docEntity.SourceFromOnedrive:
			// perf: 可以把one drive的 etag存储起来，然后先调用one drive的接口看下etag是否有变更，如果没有变更，甚至都不用同步到cos
			err = d.refreshOnedriveDoc(ctx, user.Uin, user.SubAccountUin, common.SourceFromType_SOURCE_FROM_TYPE_ONEDRIVE, tFileInfo, progress, cosURL)
			if err != nil {
				log.ErrorContextf(ctx, "task(TxDocRefresh) refreshOnedriveDoc k:%s err:%+v", key, err)
				return err
			}
			log.DebugContextf(ctx, "task(TxDocRefresh) StatObject kv:%s, cosPath is %v", key, cosURL)
			objectInfo, err := d.s3.StatObject(ctx, cosURL)
			if err != nil {
				log.ErrorContextf(ctx, "task(DocRefresh) key %v, StatObject err:%+v", key, err)
				return err
			}
			if objectInfo.Hash == doc.CosHash {
				log.DebugContextf(ctx, "task(DocRefresh) key %v, StatObject hash equal, skip refresh", key)
				err = progress.Finish(ctx, key)
				if err != nil {
					log.ErrorContextf(ctx, "task(DocRefresh) key %v, Finish err:%+v", key, err)
					return err
				}
				continue
			}
			cosHash = objectInfo.Hash
			fileSize = uint64(objectInfo.Size)
		}

		// -------
		updateDocFilter := &docEntity.DocFilter{
			IDs: []uint64{doc.ID}, CorpId: doc.CorpID, RobotId: doc.RobotID,
		}
		staffID := doc.StaffID
		if contextx.Metadata(ctx).StaffID() != 0 {
			staffID = contextx.Metadata(ctx).StaffID()
		}
		update := &docEntity.Doc{
			StaffID:    staffID,
			Status:     docEntity.DocStatusParseIng,
			UpdateTime: time.Now(),
			CosURL:     cosPath,
			CosHash:    cosHash,
			FileSize:   fileSize,
		}
		updateDocColumns := []string{docEntity.DocTblColStaffId, docEntity.DocTblColStatus,
			docEntity.DocTblColUpdateTime,
			docEntity.DocTblColCosURL, docEntity.DocTblColCosHash, docEntity.DocTblColFileSize}
		_, err = d.docLogic.UpdateLogicByDao(ctx, updateDocColumns, updateDocFilter, update)
		if err != nil {
			logx.E(ctx, "ReloadUpdateDoc|UpdateDocStatus|err:%+v", err)
			return err
		}

		doc.CosURL = cosPath
		doc.CosHash = cosHash
		requestID := contextx.TraceID(ctx)
		taskID, err := d.docLogic.SendDocParseWordCount(ctx, doc, requestID, "")
		if err != nil {
			return err
		}
		docParse := &docEntity.DocParse{
			DocID:        doc.ID,
			CorpID:       doc.CorpID,
			RobotID:      doc.RobotID,
			StaffID:      doc.StaffID,
			RequestID:    requestID,
			Type:         docEntity.DocParseTaskTypeWordCount,
			OpType:       docEntity.DocParseOpTypeWordCount,
			Status:       docEntity.DocParseIng,
			TaskID:       taskID,
			SourceEnvSet: contextx.Metadata(ctx).EnvSet(),
		}
		err = d.docLogic.CreateDocParseTask(ctx, docParse)
		if err != nil {
			return err
		}
		// --------

		if err := progress.Finish(ctx, key); err != nil {
			log.ErrorContextf(ctx, "task(TxDocRefresh) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(TxDocRefresh) Finish kv:%s", key)
	}
	return nil
}

// getImportThirdDocCosObj 获取刷新文档的cos对象
func (d *DocRefreshScheduler) getImportThirdDocCosObj(ctx context.Context, doc *docEntity.Doc,
	corpBizID, AppBizID uint64) (rpc.ImportTDocCosObj, error) {
	result := rpc.ImportTDocCosObj{}
	fileName := getFileNameByType(doc.FileType)
	uploadCosPath := d.s3.GetCorpRobotCOSPath(ctx, corpBizID, AppBizID, fileName)
	req2 := s3x.GetCredentialReq{
		TypeKey:       internalDao.DefaultStorageTypeKey,
		Path:          []string{uploadCosPath},
		StorageAction: gox.IfElse(len(fileName) == 0, s3x.ActionDownload, s3x.ActionUpload),
	}
	res, err := d.s3.GetCredentialWithTypeKey(ctx, &req2)
	if err != nil {
		logx.E(ctx, "ImportThirdFileProgress GetCredentialWithTypeKey error: %+v", err)
		return result, err
	}
	bucket, err := d.s3.GetBucketWithTypeKey(ctx, internalDao.DefaultStorageTypeKey)
	if err != nil {
		logx.E(ctx, "ImportThirdFileProgress GetBucketWithTypeKey error: %+v", err)
		return result, err
	}
	region, err := d.s3.GetRegionWithTypeKey(ctx, internalDao.DefaultStorageTypeKey)
	if err != nil {
		logx.E(ctx, "ImportThirdFileProgress GetRegionWithTypeKey error: %+v", err)
		return result, err
	}
	result = rpc.ImportTDocCosObj{
		Bucket:        bucket,
		Region:        region,
		CosPath:       uploadCosPath,
		TempSecretId:  res.Credentials.TmpSecretID,
		TempSecretKey: res.Credentials.TmpSecretKey,
		SecurityToken: res.Credentials.SessionToken,
	}
	logx.D(ctx, "ImportThirdFileProgress result: %+v", result)
	return result, nil
}

func (d *DocRefreshScheduler) refreshOnedriveDoc(ctx context.Context,
	uin, subUin string,
	sourceFrom common.SourceFromType,
	fileInfo *entity.DocRefreshFileInfo,
	progress *task_scheduler.Progress,
	cosPath string,
) error {
	fileID := fileInfo.FileID

	// step1. 获取accessToken
	authRsp, err := d.taskCommon.rpc.GetThirdDocPlatformAuthToken(ctx, uin, subUin, common.SourceFromType(d.p.SourceFrom))
	if err != nil {
		log.ErrorContextf(ctx, "task(TxDocRefresh) GetAuthToken err: %+v", err)
		// TODO: 是否需要finish
		// progress.Finish(ctx, fileID)
		return err
	}
	if !authRsp.GetAuthStatus() {
		log.ErrorContextf(ctx, "auth failed, fileID %s", fileID)
	}

	// step2. 获取文件下载地址

	docListRsp, err := d.rpc.ListOnedriveDoc(ctx,
		third_doc.WithAccessToken(authRsp.GetAccessToken()),
		third_doc.WithItemID(fileID),
	)
	if err != nil {
		log.ErrorContextf(ctx, "task(TxDocRefresh) ListDoc err: %+v", err)
		return err
	}
	if len(docListRsp.Docs) != 1 {
		log.ErrorContextf(ctx, "file id %s doc list length != 1", fileID)
	}
	// step3. 请求下载地址
	downloadURL := docListRsp.Docs[0].DownloadURL
	safeClient := secapi.NewSafeClient()
	httpReq, err := http.NewRequest(http.MethodGet, downloadURL, nil)
	if err != nil {
		log.ErrorContextf(ctx, "http.NewRequest fail: url(%s), err(%v)", downloadURL, err)
	}

	rsp, err := safeClient.Do(httpReq)
	if err != nil {
		log.ErrorContextf(ctx, "safeClient.Do fail: url(%s), err(%v)", downloadURL, err)
		return err
	}
	if rsp.StatusCode != http.StatusOK {
		log.ErrorContextf(ctx, "rsp.StatusCode != http.StatusOK: url(%s), statusCode(%d)", downloadURL, rsp.StatusCode)
	}

	defer rsp.Body.Close()
	// step4. 上传到cos
	err = d.s3.PutObjectByReader(ctx, cosPath, rsp.Body)
	// _, err = client.GetCosClient(ctx, &cred, cosObj.Bucket, cosObj.Region).Object.Put(ctx, cosObj.CosPath, rsp.Body, nil)
	if err != nil {
		log.ErrorContextf(ctx, "PutObject fail: url(%s), fileID(%s), err(%v)", downloadURL, fileID, err)
		return err
	}
	return nil
}

func (d *DocRefreshScheduler) refreshSharePointDoc(ctx context.Context) error {
	return nil
}

// Fail 任务失败
func (d *DocRefreshScheduler) Fail(ctx context.Context) error {

	return nil
}

// Stop 任务停止
func (d *DocRefreshScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *DocRefreshScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(TxDocRefresh) Done")
	return nil
}
