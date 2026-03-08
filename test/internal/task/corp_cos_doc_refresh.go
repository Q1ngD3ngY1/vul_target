package task

import (
	"context"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/service"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	"go.opentelemetry.io/otel/trace"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

const (
	CorpCOSDocRefreshPrefix = "corp_cos_doc_refresh:dosage:"
)

// CorpCOSDocRefreshScheduler 客户COS文档刷新任务
type CorpCOSDocRefreshScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    model.CorpCOSDocRefreshParams
}

func initCorpCOSDocRefreshScheduler() {
	task_scheduler.Register(
		model.CorpCOSDocRefreshTask,
		func(t task_scheduler.Task, params model.CorpCOSDocRefreshParams) task_scheduler.TaskHandler {
			return &CorpCOSDocRefreshScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *CorpCOSDocRefreshScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(CorpCOSDocRefresh) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	for _, doc := range d.p.Docs {
		log.DebugContextf(ctx, "task(CorpCOSDocRefresh) Prepare, doc: %+v ", doc)
		kv[fmt.Sprintf("%s%d", CorpCOSDocRefreshPrefix, doc.ID)] = fmt.Sprintf("%d", doc.ID)
	}
	return kv, nil
}

// Init 初始化
func (d *CorpCOSDocRefreshScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.InfoContextf(ctx, "task(CorpCOSDocRefresh) Init start")
	return nil
}

// Process 任务处理
func (d *CorpCOSDocRefreshScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	log.DebugContextf(ctx, "task(CorpCOSDocRefresh) Process, task: %+v, params: %+v", d.task, d.p)
	if len(d.p.EnvSet) > 0 { // 审核回调需要env-set
		ctx = pkg.WithEnvSet(ctx, d.p.EnvSet)
	}
	var docs = make(map[uint64]*model.Doc)
	for _, v := range d.p.Docs {
		docs[v.ID] = v
	}
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(CorpCOSDocRefresh) Start k:%s, v:%s", k, v)
		key := k
		id := cast.ToUint64(v)

		doc, exists := docs[id]
		if !exists {
			if err := progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(CorpCOSDocRefresh) Finish kv:%s err:%+v", key, err)
				return err
			}
			log.ErrorContextf(ctx, "task(CorpCOSDocRefresh) doc kv:%s docID:%+v doc is nil", key, id)
			continue
		}
		appDB, err := d.dao.GetAppByID(ctx, doc.RobotID)
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
			log.ErrorContextf(ctx, "task(CorpCOSDocRefresh) GetAppInfo err: %+v", err)
			return err
		}

		doc, err = d.dao.GetDocByID(ctx, id, appInfo.GetAppBizId())
		if err != nil {
			log.ErrorContextf(ctx, "task(CorpCOSDocRefresh) GetDocByBizID kv:%s err:%+v", key, err)
			return err
		}
		corp, err := d.dao.GetCorpByID(ctx, doc.CorpID)
		if err != nil {
			log.ErrorContextf(ctx, "AutoRunDocDiffTask GetCorpByID err: %+v", err)
			return err
		}
		if err = service.CheckIsUsedCharSizeExceeded(ctx, d.dao, appInfo.GetAppBizId(), corp.ID); err != nil {
			log.WarnContextf(ctx, "task(CorpCOSDocRefresh) CheckIsUsedCharSizeExceeded fail: %+v",
				d.dao.ConvertErrMsg(ctx, 0, appInfo.GetId(), err))
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(CorpCOSDocRefresh) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		columns := []string{dao.CorpCOSDocTblColID, dao.CorpCOSDocTblColBusinessID, dao.CorpCOSDocTblColCosBucket,
			dao.CorpCOSDocTblColCosHash, dao.CorpCOSDocTblColCosPath}
		filter := dao.CorpCOSDocFilter{BusinessIDs: []uint64{cast.ToUint64(doc.CustomerKnowledgeId)}}
		corpCOSInfo, err := dao.GetCorpCOSDocDao().GetCorpCosDoc(ctx, columns, filter)
		if err != nil {
			log.ErrorContextf(ctx, "task(CorpCOSDocRefresh) GetCorpCosDoc kv:%s err:%+v", key, err)
			return err
		}
		if corpCOSInfo.ID == 0 {
			log.ErrorContextf(ctx, "task(CorpCOSDocRefresh) GetCorpCosDoc kv:%s is empty", key)
			return err
		}
		if strings.HasPrefix(key, CorpCOSDocRefreshPrefix) {
			log.DebugContextf(ctx, "task(CorpCOSDocRefresh) GetDocByBizID kv:%s doc:%+v", key, doc)
			credentialResponse, status, err := d.dao.AssumeServiceRole(ctx, appInfo.GetUin(),
				config.App().COSDocumentConfig.ServiceRole, 0, nil)
			if err != nil {
				return errs.ErrAssumeServiceRoleFailed
			}
			if status != knowledge.RoleStatusType_RoleStatusAvailable {
				return errs.ErrServiceRoleUnavailable
			}
			content, err := d.dao.GetCOSObject(ctx, credentialResponse.Credentials, corpCOSInfo.CosBucket,
				"ap-guangzhou", corpCOSInfo.CosPath)
			if err != nil {
				log.ErrorContextf(ctx, "GetCOSObject corpCOS:%+v, err:%+v", corpCOSInfo, err)
				return err
			}
			fileName := filepath.Ext(corpCOSInfo.CosPath)
			cosFileName := util.RandStr(20) + strconv.Itoa(util.New().Intn(10000000000)) + fileName
			cosPath := d.dao.GetCorpRobotCOSPath(ctx, corp.BusinessID, appDB.BusinessID, cosFileName)
			err = d.dao.PutObject(ctx, content, cosPath)
			if err != nil {
				log.ErrorContextf(ctx, "PutObject corpCOS:%+v, err:%+v", corpCOSInfo, err)
				return err
			}
			objInfo, err := d.dao.StatObject(ctx, cosPath)
			if err != nil {
				log.ErrorContextf(ctx, "StatObject corpCOS:%+v, err:%+v", corpCOSInfo, err)
				return err
			}

			log.InfoContextf(ctx, "task(CorpCOSDocRefresh) CosHash diff kv:%s Data.CosHash:%+v doc.CosHash:%v",
				key, objInfo.Hash, doc.CosHash)
			if objInfo.Hash == doc.CosHash {
				log.InfoContextf(ctx, "task(CorpCOSDocRefresh) CosHash一致,不进行更新 kv:%s doc:%v", key, doc)
				if err = progress.Finish(ctx, key); err != nil {
					log.ErrorContextf(ctx, "task(CorpCOSDocRefresh) Finish kv:%s err:%+v", key, err)
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
				CosURL:     cosPath,
				CosHash:    objInfo.Hash,
				FileSize:   uint64(objInfo.Size),
			}
			updateDocColumns := []string{dao.DocTblColStaffId, dao.DocTblColStatus, dao.DocTblColUpdateTime,
				dao.DocTblColCosURL, dao.DocTblColCosHash, dao.DocTblColFileSize}
			_, err = dao.GetDocDao().UpdateDoc(ctx, updateDocColumns, updateDocFilter, update)
			if err != nil {
				log.ErrorContextf(ctx, "ReloadUpdateDoc|UpdateDocStatus|err:%+v", err)
				return err
			}

			// 更新用户cos文件信息
			corpCOSInfo.SyncTime = time.Now()
			corpCOSInfo.StaffID = staffID
			corpCOSInfo.BusinessCosURL = cosPath
			corpCOSInfo.BusinessCosHash = objInfo.Hash
			corpCOSInfo.BusinessCosTag = objInfo.ETag
			// 更新状态和内部cos信息
			err = dao.GetCorpCOSDocDao().UpdateCorpCosDoc(ctx, corpCOSInfo)
			if err != nil {
				log.ErrorContextf(ctx, "UpdateCorpCosDoc to db corpCOSInfo:%+v, err:%+v", corpCOSInfo, err)
			}

			doc.CosURL = cosPath
			doc.CosHash = objInfo.Hash
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
			log.ErrorContextf(ctx, "task(CorpCOSDocRefresh) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(CorpCOSDocRefresh) Finish kv:%s", key)
	}
	return nil
}

// Fail 任务失败
func (d *CorpCOSDocRefreshScheduler) Fail(ctx context.Context) error {

	return nil
}

// Stop 任务停止
func (d *CorpCOSDocRefreshScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *CorpCOSDocRefreshScheduler) Done(ctx context.Context) error {
	log.DebugContextf(ctx, "task(CorpCOSDocRefresh) Done")
	return nil
}
