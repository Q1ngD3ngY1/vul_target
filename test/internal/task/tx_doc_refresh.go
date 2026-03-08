// bot-knowledge-config-server
//
// @(#)tx_doc_refresh.go  星期三, 七月 16, 2025
// Copyright(c) 2025, zrwang@Tencent. All rights reserved.

package task

import (
	"context"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/service"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"go.opentelemetry.io/otel/trace"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

const (
	TxDocRefreshPrefix = "tx_doc_refresh:dosage:"
)

// TxDocRefreshScheduler 腾讯文档刷新任务
type TxDocRefreshScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    model.TxDocRefreshParams
}

func initTxDocRefreshScheduler() {
	task_scheduler.Register(
		model.TxDocRefreshTask,
		func(t task_scheduler.Task, params model.TxDocRefreshParams) task_scheduler.TaskHandler {
			return &TxDocRefreshScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *TxDocRefreshScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(TxDocRefresh) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	for docID, info := range d.p.TFileInfo {
		log.DebugContextf(ctx, "task(TxDocRefresh) Prepare, TFileInfo: %+v ", info)
		kv[fmt.Sprintf("%s%d", TxDocRefreshPrefix, docID)] = fmt.Sprintf("%d", docID)
	}
	return kv, nil
}

// Init 初始化
func (d *TxDocRefreshScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.InfoContextf(ctx, "task(TxDocRefresh) Init start")
	return nil
}

// Process 任务处理
func (d *TxDocRefreshScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(TxDocRefresh) Process, task: %+v, params: %+v", d.task, d.p)
	if len(d.p.EnvSet) > 0 { // 审核回调需要env-set
		ctx = pkg.WithEnvSet(ctx, d.p.EnvSet)
	}
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(TxDocRefresh) Start k:%s, v:%s", k, v)
		key := k
		id := cast.ToUint64(v)

		tFileInfo, exists := d.p.TFileInfo[id]
		if !exists {
			if err := progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(TxDocRefresh) Finish kv:%s err:%+v", key, err)
				return err
			}
			log.ErrorContextf(ctx, "task(TxDocRefresh) TFileInfo kv:%s docID:%+v tFileInfo is nil", key, id)
			continue
		}
		if tFileInfo.FileID == "" || tFileInfo.RobotID == 0 || tFileInfo.OperationID == "" || tFileInfo.CorpID == 0 {
			if err := progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(TxDocRefresh) Finish kv:%s err:%+v", key, err)
				return err
			}
			log.ErrorContextf(ctx, "task(TxDocRefresh) TFileInfo:%v docID:%+v tFileInfo is nil", tFileInfo, id)
			continue
		}
		appDB, err := d.dao.GetAppByID(ctx, tFileInfo.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			log.DebugContextf(ctx, "task(DocResume) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(DocResume) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		appInfo, err := client.GetAppInfo(ctx, appDB.BusinessID, model.AppTestScenes)
		if err != nil {
			log.ErrorContextf(ctx, "task(TxDocRefresh) GetAppInfo err: %+v", err)
			return err
		}

		doc, err := d.dao.GetDocByID(ctx, id, appInfo.GetAppBizId())
		if err != nil {
			log.ErrorContextf(ctx, "task(TxDocRefresh) GetDocByBizID kv:%s err:%+v", key, err)
			return err
		}
		corp, err := d.dao.GetCorpByID(ctx, doc.CorpID)
		if err != nil {
			log.ErrorContextf(ctx, "AutoRunDocDiffTask GetCorpByID err: %+v", err)
			return err
		}
		if err = service.CheckIsUsedCharSizeExceeded(ctx, d.dao, appInfo.GetAppBizId(), corp.ID); err != nil {
			log.WarnContextf(ctx, "task(TxDocRefresh) CheckIsUsedCharSizeExceeded fail: %+v",
				d.dao.ConvertErrMsg(ctx, 0, appInfo.GetId(), err))
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(TxDocRefresh) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		if strings.HasPrefix(key, TxDocRefreshPrefix) {
			log.DebugContextf(ctx, "task(TxDocRefresh) GetDocByBizID kv:%s doc:%+v", key, doc)
			cosObj, err := d.getImportTDocCosObj(ctx, doc, corp.BusinessID, appInfo.GetAppBizId())
			if err != nil {
				log.ErrorContextf(ctx, "task(TxDocRefresh) getImportTDocCosObj kv:%s err:%+v", key, err)
				return err
			}
			//rsp, err := client.ImportTFileProgress(ctx, appInfo.GetUin(), appInfo.GetUin(), tFileInfo.FileID,
			//	tFileInfo.OperationID, cosObj)
			//if err != nil {
			//	log.ErrorContextf(ctx, "task(TxDocRefresh) ImportTFileProgress kv:%s err:%+v", key, err)
			//	return err
			//}
			//if rsp.Response.Code != 200 {
			//	log.ErrorContextf(ctx, "task(TxDocRefresh) ImportTFileProgress Code fail kv:%s rsp:%+v",
			//		key, rsp)
			//	continue
			//}
			var rsp client.ImportTFileProgressResponse
			maxRetries := 5
			newOperationID := tFileInfo.OperationID
			for i := 0; i < maxRetries; i++ {
				rsp, err = client.ImportTFileProgress(ctx, appInfo.GetUin(), appInfo.GetUin(), tFileInfo.FileID,
					newOperationID, cosObj)
				if err != nil {
					log.ErrorContextf(ctx, "task(TxDocRefresh) ImportTFileProgress kv:%s err:%+v", key, err)
					return err
				}
				if rsp.Response.Code != 200 {
					// 临时Key失效，重新生成
					if rsp.Response.Code == 10015 {
						operationID, err := client.ImportTFile(ctx, appInfo.GetUin(), appInfo.GetUin(), doc.CustomerKnowledgeId)
						if err != nil {
							log.ErrorContextf(ctx, "task(TxDocRefresh) ImportTFile err: %+v", err)
							return err
						}
						log.ErrorContextf(ctx, "task(TxDocRefresh) kv:%s new operationID:%+v",
							key, operationID)
						newOperationID = operationID
						tFileInfo.OperationID = operationID
						d.p.TFileInfo[id] = tFileInfo
						continue
					}
					log.ErrorContextf(ctx, "task(TxDocRefresh) ImportTFileProgress Code fail kv:%s rsp:%+v",
						key, rsp)
					break
				}
				if rsp.Response.Data.Progress == 100 {
					break
				}
				log.InfoContextf(ctx, "task(TxDocRefresh) ImportTFileProgress kv:%s Progress:%+v, retry %d/%d",
					key, rsp.Response.Data.Progress, i+1, maxRetries)
				// 最后一次重试不需要等待
				if i < maxRetries-1 {
					time.Sleep(1 * time.Second)
				}
			}
			if rsp.Response.Data.Progress != 100 {
				log.InfoContextf(ctx, "task(TxDocRefresh) ImportTFileProgress kv:%s Progress:%+v",
					key, rsp.Response.Data.Progress)
				continue
			}

			log.InfoContextf(ctx, "task(TxDocRefresh) CosHash diff kv:%s Data.CosHash:%+v doc.CosHash:%v",
				key, rsp.Response.Data.CosHash, doc.CosHash)
			if rsp.Response.Data.CosHash == doc.CosHash {
				log.InfoContextf(ctx, "task(TxDocRefresh) CosHash一致,不进行更新 kv:%s Progress:%+v doc:%v",
					key, rsp.Response.Data.Progress, doc)
				if err = progress.Finish(ctx, key); err != nil {
					log.ErrorContextf(ctx, "task(TxDocRefresh) Finish kv:%s err:%+v", key, err)
					return err
				}
			}

			updateDocFilter := &dao.DocFilter{
				IDs: []uint64{doc.ID}, CorpId: doc.CorpID, RobotId: doc.RobotID,
			}
			staffID := doc.StaffID
			if pkg.StaffID(ctx) != 0 {
				staffID = pkg.StaffID(ctx)
			}
			update := &model.Doc{
				StaffID:    staffID,
				Status:     model.DocStatusParseIng,
				UpdateTime: time.Now(),
				CosURL:     cosObj.CosPath, //rsp.Response.Data.Url,
				CosHash:    rsp.Response.Data.CosHash,
				FileSize:   rsp.Response.Data.Size,
			}
			updateDocColumns := []string{dao.DocTblColStaffId, dao.DocTblColStatus, dao.DocTblColUpdateTime,
				dao.DocTblColCosURL, dao.DocTblColCosHash, dao.DocTblColFileSize}
			_, err = dao.GetDocDao().UpdateDoc(ctx, updateDocColumns, updateDocFilter, update)
			if err != nil {
				log.ErrorContextf(ctx, "ReloadUpdateDoc|UpdateDocStatus|err:%+v", err)
				return err
			}

			doc.CosURL = cosObj.CosPath //rsp.Response.Data.Url
			doc.CosHash = rsp.Response.Data.CosHash
			requestID := trace.SpanContextFromContext(ctx).TraceID().String()
			taskID, err := d.dao.SendDocParseWordCount(ctx, doc, requestID, "")
			if err != nil {
				return err
			}
			docParse := model.DocParse{
				DocID:     doc.ID,
				CorpID:    doc.CorpID,
				RobotID:   doc.RobotID,
				StaffID:   doc.StaffID,
				RequestID: requestID,
				Type:      model.DocParseTaskTypeWordCount,
				OpType:    model.DocParseOpTypeWordCount,
				Status:    model.DocParseIng,
				TaskID:    taskID,
			}
			err = d.dao.CreateDocParse(ctx, nil, docParse)
			if err != nil {
				return err
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			log.ErrorContextf(ctx, "task(TxDocRefresh) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(TxDocRefresh) Finish kv:%s", key)
	}
	return nil
}

// getImportTDocCosObj 获取刷新文档的cos对象
func (d *TxDocRefreshScheduler) getImportTDocCosObj(ctx context.Context, doc *model.Doc,
	corpBizID, AppBizID uint64) (client.ImportTDocCosObj, error) {
	result := client.ImportTDocCosObj{}
	pathList := make([]string, 0)
	fileName := getFileNameByType(doc.FileType, d.dao)
	uploadCosPath := d.dao.GetCorpRobotCOSPath(ctx, corpBizID, AppBizID, fileName)
	pathList = []string{uploadCosPath}
	typeKey := dao.DefaultStorageTypeKey
	res, err := d.dao.GetCredentialWithTypeKey(ctx, typeKey, pathList,
		utils.When(len(fileName) == 0, model.ActionDownload, model.ActionUpload))
	if err != nil {
		log.ErrorContextf(ctx, "ImportTFileProgress GetCredentialWithTypeKey error: %+v", err)
		return result, err
	}
	bucket, err := d.dao.GetBucketWithTypeKey(ctx, typeKey)
	if err != nil {
		log.ErrorContextf(ctx, "ImportTFileProgress GetBucketWithTypeKey error: %+v", err)
		return result, err
	}
	region, err := d.dao.GetRegionWithTypeKey(ctx, typeKey)
	if err != nil {
		log.ErrorContextf(ctx, "ImportTFileProgress GetRegionWithTypeKey error: %+v", err)
		return result, err
	}
	result = client.ImportTDocCosObj{
		Bucket:        bucket,
		Region:        region,
		CosPath:       uploadCosPath,
		TempSecretId:  res.Credentials.TmpSecretID,
		TempSecretKey: res.Credentials.TmpSecretKey,
		SecurityToken: res.Credentials.SessionToken,
	}
	log.DebugContextf(ctx, "ImportTFileProgress result: %+v", result)
	return result, nil
}

// getFileNameByType 获取上传文件名
func getFileNameByType(fileType string, d dao.Dao) string {
	if len(fileType) == 0 {
		return ""
	}
	return fmt.Sprintf("%s-%d.%s", util.RandStr(20), d.GenerateSeqID(), fileType)
}

// Fail 任务失败
func (d *TxDocRefreshScheduler) Fail(ctx context.Context) error {

	return nil
}

// Stop 任务停止
func (d *TxDocRefreshScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *TxDocRefreshScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(TxDocRefresh) Done")
	return nil
}
