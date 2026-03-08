package async

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/trace"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/mathx/randx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	financeEntity "git.woa.com/adp/kb/kb-config/internal/entity/finance"
	"git.woa.com/adp/kb/kb-config/internal/logic/common"
	doc "git.woa.com/adp/kb/kb-config/internal/logic/document"
	"git.woa.com/adp/kb/kb-config/internal/logic/finance"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"

	knowledge "git.woa.com/adp/pb-go/kb/kb_config"
)

const (
	CorpCOSDocRefreshPrefix = "corp_cos_doc_refresh:dosage:"
)

// CorpCOSDocRefreshScheduler 客户COS文档刷新任务
type CorpCOSDocRefreshScheduler struct {
	rpc          *rpc.RPC
	s3           dao.S3
	docLogic     *doc.Logic
	financeLogic *finance.Logic
	task         task_scheduler.Task
	p            entity.CorpCOSDocRefreshParams
}

func registerCorpCOSDocRefreshScheduler(tc *taskCommon, rpc *rpc.RPC, s3 dao.S3, docLogic *doc.Logic, financeLogic *finance.Logic) {
	task_scheduler.Register(
		entity.CorpCOSDocRefreshTask,
		func(t task_scheduler.Task, params entity.CorpCOSDocRefreshParams) task_scheduler.TaskHandler {
			return &CorpCOSDocRefreshScheduler{
				rpc:          rpc,
				s3:           s3,
				docLogic:     docLogic,
				financeLogic: financeLogic,
				task:         t,
				p:            params,
			}
		},
	)
}

// Prepare 数据准备
func (d *CorpCOSDocRefreshScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(CorpCOSDocRefresh) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	for _, doc := range d.p.Docs {
		logx.D(ctx, "task(CorpCOSDocRefresh) Prepare, doc: %+v ", doc)
		kv[fmt.Sprintf("%s%d", CorpCOSDocRefreshPrefix, doc.ID)] = fmt.Sprintf("%d", doc.ID)
	}
	return kv, nil
}

// Init 初始化
func (d *CorpCOSDocRefreshScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.I(ctx, "task(CorpCOSDocRefresh) Init start")
	return nil
}

// Process 任务处理
func (d *CorpCOSDocRefreshScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	logx.D(ctx, "task(CorpCOSDocRefresh) Process, task: %+v, params: %+v", d.task, d.p)
	if len(d.p.EnvSet) > 0 { // 审核回调需要env-set
		contextx.Metadata(ctx).WithEnvSet(d.p.EnvSet)
	}
	var docs = make(map[uint64]*docEntity.Doc)
	for _, v := range d.p.Docs {
		docs[v.ID] = v
	}
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(CorpCOSDocRefresh) Start k:%s, v:%s", k, v)
		key := k
		id := cast.ToUint64(v)

		doc, exists := docs[id]
		if !exists {
			if err := progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(CorpCOSDocRefresh) Finish kv:%s err:%+v", key, err)
				return err
			}
			logx.E(ctx, "task(CorpCOSDocRefresh) doc kv:%s docID:%+v doc is nil", key, id)
			continue
		}
		appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryId(ctx, doc.RobotID)
		if err != nil {
			return err
		}
		if appDB.IsDeleted {
			logx.D(ctx, "task(CorpCOSDocRefresh) appDB.HasDeleted()|appID:%d", appDB.PrimaryId)
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(CorpCOSDocRefresh) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		appInfo, err := d.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, appDB.BizId, entity.AppTestScenes)
		if err != nil {
			logx.E(ctx, "task(CorpCOSDocRefresh) GetAppInfo err: %+v", err)
			return err
		}

		doc, err = d.docLogic.GetDocByID(ctx, id, appInfo.BizId)
		if err != nil {
			logx.E(ctx, "task(CorpCOSDocRefresh) GetDocByBizID kv:%s err:%+v", key, err)
			return err
		}
		corp, err := d.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, doc.CorpID)
		if err != nil {
			logx.E(ctx, "task(CorpCOSDocRefresh) DescribeCorpByPrimaryId err: %+v", err)
			return err
		}
		if err = d.financeLogic.CheckKnowledgeBaseQuota(ctx, financeEntity.CheckQuotaReq{App: appInfo}); err != nil {
			logx.W(ctx, "task(CorpCOSDocRefresh) CheckKnowledgeBaseQuota fail: %+v",
				common.ConvertErrMsg(ctx, d.rpc, 0, appInfo.PrimaryId, err))
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(CorpCOSDocRefresh) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}

		columns := []string{docEntity.CorpCOSDocTblColID, docEntity.CorpCOSDocTblColBusinessID, docEntity.CorpCOSDocTblColCosBucket,
			docEntity.CorpCOSDocTblColCosHash, docEntity.CorpCOSDocTblColCosPath}
		filter := &docEntity.CorpCOSDocFilter{BusinessIDs: []uint64{cast.ToUint64(doc.CustomerKnowledgeId)}}
		corpCOSInfo, err := d.docLogic.GetDao().DescribeCorpCosDoc(ctx, columns, filter)
		if err != nil {
			logx.E(ctx, "task(CorpCOSDocRefresh) DescribeCorpCosDoc kv:%s err:%+v", key, err)
			return err
		}
		if corpCOSInfo.ID == 0 {
			logx.E(ctx, "task(CorpCOSDocRefresh) DescribeCorpCosDoc kv:%s is empty", key)
			return err
		}
		if strings.HasPrefix(key, CorpCOSDocRefreshPrefix) {
			logx.D(ctx, "task(CorpCOSDocRefresh) GetDocByBizID kv:%s doc:%+v", key, doc)
			credentialResponse, status, err := d.rpc.Cloud.AssumeServiceRole(ctx, appInfo.Uin,
				config.App().COSDocumentConfig.ServiceRole, 0, nil)
			if err != nil {
				return errs.ErrAssumeServiceRoleFailed
			}
			if status != knowledge.RoleStatusType_RoleStatusAvailable {
				return errs.ErrServiceRoleUnavailable
			}
			content, err := d.rpc.COS.GetCOSObject(ctx, credentialResponse.Credentials, corpCOSInfo.CosBucket,
				"ap-guangzhou", corpCOSInfo.CosPath)
			if err != nil {
				logx.E(ctx, "GetCOSObject corpCOS:%+v, err:%+v", corpCOSInfo, err)
				return err
			}
			fileName := filepath.Ext(corpCOSInfo.CosPath)
			// cosFileName := util.RandStr(20) + strconv.Itoa(util.New().Intn(10000000000)) + fileName
			cosFileName := randx.RandomString(20) + strconv.Itoa(rand.Intn(10000000000)) + fileName
			cosPath := d.s3.GetCorpRobotCOSPath(ctx, corp.CorpId, appDB.BizId, cosFileName)
			err = d.s3.PutObject(ctx, content, cosPath)
			if err != nil {
				logx.E(ctx, "PutObject corpCOS:%+v, err:%+v", corpCOSInfo, err)
				return err
			}
			objInfo, err := d.s3.StatObject(ctx, cosPath)
			if err != nil {
				logx.E(ctx, "StatObject corpCOS:%+v, err:%+v", corpCOSInfo, err)
				return err
			}

			logx.I(ctx, "task(CorpCOSDocRefresh) CosHash diff kv:%s Data.CosHash:%+v doc.CosHash:%v",
				key, objInfo.Hash, doc.CosHash)
			if objInfo.Hash == doc.CosHash {
				logx.I(ctx, "task(CorpCOSDocRefresh) CosHash一致,不进行更新 kv:%s doc:%v", key, doc)
				if err = progress.Finish(ctx, key); err != nil {
					logx.E(ctx, "task(CorpCOSDocRefresh) Finish kv:%s err:%+v", key, err)
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
				CosURL:     cosPath,
				CosHash:    objInfo.Hash,
				FileSize:   uint64(objInfo.Size),
			}
			updateDocColumns := []string{docEntity.DocTblColStaffId, docEntity.DocTblColStatus, docEntity.DocTblColUpdateTime,
				docEntity.DocTblColCosURL, docEntity.DocTblColCosHash, docEntity.DocTblColFileSize}
			_, err = d.docLogic.GetDao().UpdateDoc(ctx, updateDocColumns, updateDocFilter, update)
			if err != nil {
				logx.E(ctx, "ReloadUpdateDoc|UpdateDocStatus|err:%+v", err)
				return err
			}

			// 更新用户cos文件信息
			corpCOSInfo.SyncTime = time.Now()
			corpCOSInfo.StaffID = staffID
			corpCOSInfo.BusinessCosURL = cosPath
			corpCOSInfo.BusinessCosHash = objInfo.Hash
			corpCOSInfo.BusinessCosTag = objInfo.ETag
			corpCOSInfo.UpdateTime = time.Now()

			updateCorpCOSColumns := []string{docEntity.CorpCOSDocTblColSyncTime, docEntity.CorpCOSDocTblColStaffID,
				docEntity.CorpCOSDocTblColBusinessCosURL, docEntity.CorpCOSDocTblColBusinessCosHash,
				docEntity.CorpCOSDocTblColBusinessCosTag, docEntity.CorpCOSDocTblColUpdateTime}

			filter := &docEntity.CorpCOSDocFilter{BusinessIDs: []uint64{corpCOSInfo.BusinessID}}
			// 更新状态和内部cos信息
			err = d.docLogic.GetDao().ModifyCorpCosDoc(ctx, updateCorpCOSColumns, filter, corpCOSInfo)
			if err != nil {
				logx.E(ctx, "UpdateCorpCosDoc to db corpCOSInfo:%+v, err:%+v", corpCOSInfo, err)
			}

			doc.CosURL = cosPath
			doc.CosHash = objInfo.Hash
			requestID := trace.SpanContextFromContext(ctx).TraceID().String()
			taskID, err := d.docLogic.SendDocParseWordCount(ctx, doc, requestID, "")
			if err != nil {
				return err
			}
			docParse := &docEntity.DocParse{
				DocID:     doc.ID,
				CorpID:    doc.CorpID,
				RobotID:   doc.RobotID,
				StaffID:   doc.StaffID,
				RequestID: requestID,
				Type:      docEntity.DocParseTaskTypeWordCount,
				OpType:    docEntity.DocParseOpTypeWordCount,
				Status:    docEntity.DocParseIng,
				TaskID:    taskID,
			}
			err = d.docLogic.GetDao().CreateDocParseTask(ctx, docParse)
			if err != nil {
				return err
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			logx.E(ctx, "task(CorpCOSDocRefresh) Finish kv:%s err:%+v", key, err)
			return err
		}
		logx.D(ctx, "task(CorpCOSDocRefresh) Finish kv:%s", key)
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
	logx.D(ctx, "task(CorpCOSDocRefresh) Done")
	return nil
}
