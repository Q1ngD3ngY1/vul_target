package async

import (
	"context"
	"fmt"
	"strings"
	"time"

	"git.woa.com/adp/common/x/clientx/s3x"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/mathx/randx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/entity/finance"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"

	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/logic/common"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
)

const (
	TxDocRefreshPrefix = "tx_doc_refresh:dosage:"
)

// TxDocRefreshTaskHandler 腾讯文档刷新任务
type TxDocRefreshTaskHandler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity.TxDocRefreshParams
}

func registerTxDocRefreshTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.TxDocRefreshTask,
		func(t task_scheduler.Task, params entity.TxDocRefreshParams) task_scheduler.TaskHandler {
			return &TxDocRefreshTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

// Prepare 数据准备
func (d *TxDocRefreshTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(TxDocRefresh) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	for docID, info := range d.p.TFileInfo {
		logx.D(ctx, "task(TxDocRefresh) Prepare, TFileInfo: %+v ", info)
		kv[fmt.Sprintf("%s%d", TxDocRefreshPrefix, docID)] = fmt.Sprintf("%d", docID)
	}
	return kv, nil
}

// Init 初始化
func (d *TxDocRefreshTaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.I(ctx, "task(TxDocRefresh) Init start")
	return nil
}

// Process 任务处理
func (d *TxDocRefreshTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(TxDocRefresh) Process, task: %+v, params: %+v", d.task, d.p)
	if len(d.p.EnvSet) > 0 { // 审核回调需要env-set
		contextx.Metadata(ctx).WithEnvSet(d.p.EnvSet)
	}
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(TxDocRefresh) Start k:%s, v:%s", k, v)
		key := k
		id := cast.ToUint64(v)

		tFileInfo, exists := d.p.TFileInfo[id]
		if !exists {
			if err := progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(TxDocRefresh) Finish kv:%s err:%+v", key, err)
				return err
			}
			logx.E(ctx, "task(TxDocRefresh) TFileInfo kv:%s docID:%+v tFileInfo is nil", key, id)
			continue
		}
		if tFileInfo.FileID == "" || tFileInfo.RobotID == 0 || tFileInfo.OperationID == "" || tFileInfo.CorpID == 0 {
			if err := progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(TxDocRefresh) Finish kv:%s err:%+v", key, err)
				return err
			}
			logx.E(ctx, "task(TxDocRefresh) TFileInfo:%v docID:%+v tFileInfo is nil", tFileInfo, id)
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
		appInfo, err := d.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, appDB.BizId, entity.AppTestScenes)
		if err != nil {
			logx.E(ctx, "task(TxDocRefresh) GetAppInfo err: %+v", err)
			return err
		}

		doc, err := d.docLogic.GetDocByID(ctx, id, appDB.BizId)
		if err != nil {
			logx.E(ctx, "task(TxDocRefresh) GetDocByBizID kv:%s err:%+v", key, err)
			return err
		}
		corp, err := d.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, doc.CorpID)
		// corp, err := d.dao.GetCorpByID(ctx, doc.CorpPrimaryId)
		if err != nil {
			logx.E(ctx, "AutoRunDocDiffTask GetCorpByID err: %+v", err)
			return err
		}
		if err = d.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{App: appDB}); err != nil {
			logx.W(ctx, "task(TxDocRefresh) CheckKnowledgeBaseQuota fail: %+v",
				common.ConvertErrMsg(ctx, d.rpc, 0, appDB.PrimaryId, err))
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(TxDocRefresh) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		if strings.HasPrefix(key, TxDocRefreshPrefix) {
			logx.D(ctx, "task(TxDocRefresh) GetDocByBizID kv:%s doc:%+v", key, doc)
			cosObj, err := d.getImportTDocCosObj(ctx, doc, corp.GetCorpId(), appDB.BizId)
			if err != nil {
				logx.E(ctx, "task(TxDocRefresh) getImportTDocCosObj kv:%s err:%+v", key, err)
				return err
			}
			// rsp, err := client.ImportTFileProgress(ctx, appInfo.GetUin(), appInfo.GetUin(), tFileInfo.FileID,
			//	tFileInfo.OperationID, cosObj)
			// if err != nil {
			//	logx.E(ctx, "task(TxDocRefresh) ImportTFileProgress kv:%s err:%+v", key, err)
			//	return err
			// }
			// if rsp.Response.Code != 200 {
			//	logx.E(ctx, "task(TxDocRefresh) ImportTFileProgress Code fail kv:%s rsp:%+v",
			//		key, rsp)
			//	continue
			// }
			var rsp rpc.ImportTFileProgressResponse
			maxRetries := 5
			newOperationID := tFileInfo.OperationID
			for i := 0; i < maxRetries; i++ {
				rsp, err = d.rpc.TDocLinker.ImportTFileProgress(ctx, appInfo.Uin, appInfo.Uin, tFileInfo.FileID,
					newOperationID, cosObj)
				if err != nil {
					logx.E(ctx, "task(TxDocRefresh) ImportTFileProgress kv:%s err:%+v", key, err)
					return err
				}
				if rsp.Response.Code != 200 {
					// 临时Key失效，重新生成
					if rsp.Response.Code == 10015 {
						operationID, err := d.rpc.TDocLinker.ImportTFile(ctx, appInfo.Uin, appInfo.Uin,
							doc.CustomerKnowledgeId)
						if err != nil {
							logx.E(ctx, "task(TxDocRefresh) ImportTFile err: %+v", err)
							return err
						}
						logx.E(ctx, "task(TxDocRefresh) kv:%s new operationID:%+v",
							key, operationID)
						newOperationID = operationID
						tFileInfo.OperationID = operationID
						d.p.TFileInfo[id] = tFileInfo
						continue
					}
					logx.E(ctx, "task(TxDocRefresh) ImportTFileProgress Code fail kv:%s rsp:%+v",
						key, rsp)
					break
				}
				if rsp.Response.Data.Progress == 100 {
					break
				}
				logx.I(ctx, "task(TxDocRefresh) ImportTFileProgress kv:%s Progress:%+v, retry %d/%d",
					key, rsp.Response.Data.Progress, i+1, maxRetries)
				// 最后一次重试不需要等待
				if i < maxRetries-1 {
					time.Sleep(1 * time.Second)
				}
			}
			if rsp.Response.Data.Progress != 100 {
				logx.I(ctx, "task(TxDocRefresh) ImportTFileProgress kv:%s Progress:%+v",
					key, rsp.Response.Data.Progress)
				continue
			}

			logx.I(ctx, "task(TxDocRefresh) CosHash diff kv:%s Data.CosHash:%+v doc.CosHash:%v",
				key, rsp.Response.Data.CosHash, doc.CosHash)
			if rsp.Response.Data.CosHash == doc.CosHash {
				logx.I(ctx, "task(TxDocRefresh) CosHash一致,不进行更新 kv:%s Progress:%+v doc:%v",
					key, rsp.Response.Data.Progress, doc)
				if err = progress.Finish(ctx, key); err != nil {
					logx.E(ctx, "task(TxDocRefresh) Finish kv:%s err:%+v", key, err)
					return err
				}
			}

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
				CosURL:     cosObj.CosPath, // rsp.Response.Data.Url,
				CosHash:    rsp.Response.Data.CosHash,
				FileSize:   rsp.Response.Data.Size,
			}
			updateDocColumns := []string{docEntity.DocTblColStaffId, docEntity.DocTblColStatus,
				docEntity.DocTblColUpdateTime,
				docEntity.DocTblColCosURL, docEntity.DocTblColCosHash, docEntity.DocTblColFileSize}
			_, err = d.docLogic.UpdateLogicByDao(ctx, updateDocColumns, updateDocFilter, update)
			if err != nil {
				logx.E(ctx, "ReloadUpdateDoc|UpdateDocStatus|err:%+v", err)
				d.markDocRefreshParseFailed(ctx, doc)
				return err
			}

			doc.CosURL = cosObj.CosPath // rsp.Response.Data.Url
			doc.CosHash = rsp.Response.Data.CosHash
			requestID := contextx.TraceID(ctx)
			taskID, err := d.docLogic.SendDocParseWordCount(ctx, doc, requestID, "")
			if err != nil {
				d.markDocRefreshParseFailed(ctx, doc)
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
				d.markDocRefreshParseFailed(ctx, doc)
				return err
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			logx.E(ctx, "task(TxDocRefresh) Finish kv:%s err:%+v", key, err)
			return err
		}
		logx.D(ctx, "task(TxDocRefresh) Finish kv:%s", key)
	}
	return nil
}

// getImportTDocCosObj 获取刷新文档的cos对象
func (d *TxDocRefreshTaskHandler) getImportTDocCosObj(ctx context.Context, doc *docEntity.Doc,
	corpBizID, AppBizID uint64) (rpc.ImportTDocCosObj, error) {
	result := rpc.ImportTDocCosObj{}
	fileName := getFileNameByType(doc.FileType)
	uploadCosPath := d.s3.GetCorpRobotCOSPath(ctx, corpBizID, AppBizID, fileName)
	req2 := s3x.GetCredentialReq{
		TypeKey:       dao.DefaultStorageTypeKey,
		Path:          []string{uploadCosPath},
		StorageAction: gox.IfElse(len(fileName) == 0, s3x.ActionDownload, s3x.ActionUpload),
	}
	res, err := d.s3.GetCredentialWithTypeKey(ctx, &req2)
	if err != nil {
		logx.E(ctx, "ImportTFileProgress GetCredentialWithTypeKey error: %+v", err)
		return result, err
	}
	bucket, err := d.s3.GetBucketWithTypeKey(ctx, dao.DefaultStorageTypeKey)
	if err != nil {
		logx.E(ctx, "ImportTFileProgress GetBucketWithTypeKey error: %+v", err)
		return result, err
	}
	region, err := d.s3.GetRegionWithTypeKey(ctx, dao.DefaultStorageTypeKey)
	if err != nil {
		logx.E(ctx, "ImportTFileProgress GetRegionWithTypeKey error: %+v", err)
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
	logx.D(ctx, "ImportTFileProgress result: %+v", result)
	return result, nil
}

// getFileNameByType 获取上传文件名
func getFileNameByType(fileType string) string {
	if len(fileType) == 0 {
		return ""
	}
	random := randx.RandomString(20, randx.WithMode(randx.AlphabetMode))
	return fmt.Sprintf("%s-%d.%s", random, idgen.GetId(), fileType)
}

func (d *TxDocRefreshTaskHandler) markDocRefreshParseFailed(ctx context.Context, doc *docEntity.Doc) {
	if doc == nil || doc.HasDeleted() {
		return
	}
	doc.Status = docEntity.DocStatusParseFail
	if err := d.docLogic.UpdateDocStatus(ctx, doc); err != nil {
		logx.E(ctx, "markDocRefreshParseFailed UpdateDocStatus error: %+v", err)
	}
}

// Fail 任务失败
func (d *TxDocRefreshTaskHandler) Fail(ctx context.Context) error {

	return nil
}

// Stop 任务停止
func (d *TxDocRefreshTaskHandler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *TxDocRefreshTaskHandler) Done(ctx context.Context) error {
	logx.D(ctx, "task(TxDocRefresh) Done")
	return nil
}
