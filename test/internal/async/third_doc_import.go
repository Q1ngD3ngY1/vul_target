package async

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/mapx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/pb-go/common"
	"git.woa.com/adp/pb-go/kb/kb_config"
	knowledge "git.woa.com/adp/pb-go/kb/kb_config"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	secapi "git.woa.com/sec-api/go/scurl"
	"github.com/spf13/cast"

	"git.woa.com/adp/kb/kb-config/internal/entity"
	thirdDocEntity "git.woa.com/adp/kb/kb-config/internal/entity/third_doc"
	"git.woa.com/adp/kb/kb-config/internal/util"
)

const (
	ImportSplitTag = "|"
)

type MigrateThirdDocTask struct {
	*taskCommon
	task           task_scheduler.Task
	p              entity.MigrateThirdPartyDocParams
	successMigrate map[uint64]*model.TThirdDocMigrateProgress
	failMigrate    map[uint64]*model.TThirdDocMigrateProgress
}

func registerMigrateOnedriveDocScheduler(tc *taskCommon) {
	task_scheduler.Register(
		entity.MigrateThirdDocTask,
		func(t task_scheduler.Task, params entity.MigrateThirdPartyDocParams) task_scheduler.TaskHandler {
			return &MigrateThirdDocTask{
				taskCommon:     tc,
				task:           t,
				p:              params,
				successMigrate: make(map[uint64]*model.TThirdDocMigrateProgress),
				failMigrate:    make(map[uint64]*model.TThirdDocMigrateProgress),
			}
		},
	)
}

func (d *MigrateThirdDocTask) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	kv := make(task_scheduler.TaskKV)
	switch common.SourceFromType(d.p.SourceFrom) {
	case common.SourceFromType_SOURCE_FROM_TYPE_ONEDRIVE:
		// 准备 downloadURL
		operationIDs := d.p.OperationIDs
		operationIDsProgress, err := d.thirdDocLogic.GetThirdDocLogic(common.SourceFromType(d.p.SourceFrom)).GetImportProgress(ctx, &knowledge.GetMigrateThirdPartyProcessReq{
			OperationIds: slicex.Map(operationIDs, func(e uint64) string {
				return convx.Uint64ToString(e)
			}),
		})

		if err != nil {
			log.ErrorContextf(ctx, "GetImportProgress fail: err(%v)", err)
			return nil, err
		}
		authTokenRsp, err := d.taskCommon.rpc.GetThirdDocPlatformAuthToken(ctx, d.p.Uin, d.p.SUin, common.SourceFromType(d.p.SourceFrom))
		if err != nil {
			log.ErrorContextf(ctx, "GetAuthToken failed, err: %v", err)
			return kv, err
		}
		// 如果用户取消授权，让任务失败
		if !authTokenRsp.GetAuthStatus() {
			log.ErrorContextf(ctx, "GetAuthToken fail: authStatus(%t)", authTokenRsp.GetAuthStatus())
			return kv, nil
		}
		authToken := authTokenRsp.GetAccessToken()

		failedOperationRecord := make([]uint64, 0)

		for _, operationItem := range operationIDsProgress.MigrateThirdPartyProgressInfo {
			// Perf: 这里可以减少一次请求， 直接构建微软的request, 不用从 onedrive 拉取文件信息
			// https://graph.microsoft.com/v1.0/me/drive/items/%s/content
			docInfo, err := d.rpc.ListOnedriveDoc(ctx,
				thirdDocEntity.WithAccessToken(authToken),
				thirdDocEntity.WithItemID(operationItem.ItemId),
			)
			if err != nil {
				opID, _ := convx.StringToUint64(operationItem.OperationId)
				failedOperationRecord = append(failedOperationRecord, opID)
				continue
			}
			// docType 映射
			if docInfo != nil && len(docInfo.Docs) > 0 {
				// 不用再过滤，返回给用户的内容已经过滤了
				//fileType, ok := filter.FilterType[docInfo.Docs[0].MimeType]
				//if !ok {
				//	continue
				//}
				// fileType := oneDriveConfig.FileSuffixAndTypeMap[docInfo.Docs[0].MimeType]
				// utils.
				fileType := util.GetFileExt(docInfo.Docs[0].Name)
				kv[fmt.Sprintf("%d", operationItem.OperationId)] = fmt.Sprintf("%s%s%s%s%d", docInfo.Docs[0].DownloadURL, ImportSplitTag, fileType, ImportSplitTag, docInfo.Docs[0].Size)
			}
		}
	case common.SourceFromType_SOURCE_FROM_TYPE_SHAREPOINT:
		// 准备 downloadURL
	}

	return kv, nil
}

func (d *MigrateThirdDocTask) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	return nil
}

// Process 任务处理
func (d *MigrateThirdDocTask) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	// 从 onedrive 下载文件，上传到 cos
	log.DebugContextf(ctx, "task(MigrateOneDrive) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		strSli := strings.Split(v, ImportSplitTag)
		if len(strSli) != 3 {
			log.ErrorContextf(ctx, "task(MigrateOneDrive) Process, task: %+v, params: %+v, ImportSplitTag(%s) split fail: %s", d.task, d.p, ImportSplitTag, v)
			continue
		}
		// TODO: filesize 用microsoft的还是cos的?
		downloadURL, fileType, fileSize := strSli[0], strSli[1], cast.ToInt64(strSli[2])

		operationID := cast.ToUint64(k)
		safeClient := secapi.NewSafeClient()
		httpReq, err := http.NewRequest(http.MethodGet, downloadURL, nil)
		if err != nil {
			log.ErrorContextf(ctx, "http.NewRequest fail: url(%s), err(%v)", downloadURL, err)
			continue
		}
		rsp, err := safeClient.Do(httpReq)
		if err != nil {
			log.ErrorContextf(ctx, "safeClient.Do fail: url(%s), err(%v)", downloadURL, err)
			continue
		}
		defer rsp.Body.Close()
		if rsp.StatusCode != http.StatusOK {
			log.ErrorContextf(ctx, "fetch onedrive content failed: url:%s statusCode:%d", downloadURL, rsp.StatusCode)
			continue
		}

		fileName := getFileNameByType(fileType)
		uploadCosPath := d.s3.GetCorpRobotCOSPath(ctx, d.p.CorpBizID, d.p.AppBizID, fileName)

		err = d.s3.PutObjectByReader(ctx, uploadCosPath, rsp.Body)
		if err != nil {
			log.ErrorContextf(ctx, "client.GetCosClient(tmpCredRes.Credentials).Object.Put fail: err(%v)", err)
			continue
		}
		cosObjInfo, err := d.s3.StatObject(ctx, uploadCosPath)
		if err != nil {
			log.ErrorContextf(ctx, "s3.StatObject fail: err(%v)", err)
			continue
		}

		etag := cosObjInfo.ETag
		crc := cosObjInfo.Hash
		d.successMigrate[operationID] = &model.TThirdDocMigrateProgress{
			OperationID: operationID,
			Status:      int32(kb_config.MigrateStatus_MIGRATE_STATUS_SUCCESS),
			CosETag:     etag,
			CosHash:     crc,
			CosURL:      fmt.Sprintf("%s.cos.%s.myqcloud.com%s", d.s3.GetBucket(ctx), d.s3.GetRegion(ctx), uploadCosPath),
			FileSize:    fileSize,
		}
	}

	for _, v := range d.p.OperationIDs {
		if err := progress.Finish(ctx, cast.ToString(v)); err != nil {
			log.ErrorContextf(ctx, "task(MigrateOneDrive) Finish fail: err(%v)", err)
			return err
		}
	}
	return nil
}

// Fail 任务失败
func (d *MigrateThirdDocTask) Fail(_ context.Context) error {
	return nil
}

// Stop 任务停止
func (d *MigrateThirdDocTask) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *MigrateThirdDocTask) Done(ctx context.Context) error {
	// 更新迁移进度库, success 状态为 3, fail 状态为 0
	successMap := d.successMigrate
	failSlices := slicex.Diff(d.p.OperationIDs, mapx.Keys(d.successMigrate))
	failMap := slicex.MapKV(failSlices, func(e uint64) (uint64, *model.TThirdDocMigrateProgress) {
		return e, &model.TThirdDocMigrateProgress{
			OperationID: e,
			Status:      int32(kb_config.MigrateStatus_MIGRATE_STATUS_FAIL),
		}
	})
	err := d.thirdDocLogic.GetThirdDocLogic(common.SourceFromType(d.p.SourceFrom)).UpdateImportProgress(ctx, successMap, failMap)
	if err != nil {
		log.ErrorContextf(ctx, "thirdDao.UpdateMigrateProgress fail: err(%v)", err)
		return err
	}
	return nil
}
