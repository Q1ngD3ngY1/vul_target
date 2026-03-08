package third_document

import (
	"context"
	"strings"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/mapx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/pb-go/common"
	knowledge "git.woa.com/adp/pb-go/kb/kb_config"
	"github.com/spf13/cast"

	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	thirdDocDao "git.woa.com/adp/kb/kb-config/internal/dao/third_document"
	"git.woa.com/adp/kb/kb-config/internal/dao/user"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/entity/third_doc"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

const (
	RoleAdmin    = "Administrator;"
	createdByTag = ";"
)

type OnedriveDocLogic struct {
	ThirdDocDao *thirdDocDao.OnedriveDao
	rpc         *rpc.RPC
	userDao     user.Dao
}

func NewOnedriveDocLogic(rpc *rpc.RPC, thirdDao *thirdDocDao.OnedriveDao, userDao user.Dao) *OnedriveDocLogic {
	return &OnedriveDocLogic{
		ThirdDocDao: thirdDao,
		rpc:         rpc,
		userDao:     userDao,
	}
}

func (t *OnedriveDocLogic) ListDoc(ctx context.Context, req *knowledge.ListThirdPartyDocReq) (*knowledge.ListThirdPartyDocRsp, error) {
	rsp := &knowledge.ListThirdPartyDocRsp{
		ThirdPartyDocDetailInfo: make([]*knowledge.ThirdPartyDocDetailInfo, 0),
	}
	// 从 platform 拿授权
	uin, subUin := contextx.Metadata(ctx).Uin(), contextx.Metadata(ctx).SubAccountUin()
	checkRsp, err := t.rpc.GetThirdDocPlatformAuthToken(ctx, uin, subUin, req.GetSourceFrom())
	if err != nil {
		log.ErrorContextf(ctx, "GetAuthToken fail: err(%v)", err)
		return nil, err
	}
	// 没有授权
	if !checkRsp.GetAuthStatus() {
		log.WarnContextf(ctx, "current user is not auth, uin is %v, subuin is %v", uin, subUin)
		return nil, errs.ErrThirdDocWithoutAuth
	}

	// 调用 onedrive 接口获取文档列表, 组装 onedrive 需要的参数
	listDocOptions := make([]third_doc.ListDocOption, 0)
	listDocOptions = append(listDocOptions, third_doc.WithAccessToken(checkRsp.GetAccessToken()))
	if len(req.GetNextLink()) > 0 {
		listDocOptions = append(listDocOptions, third_doc.WithNextLink(req.GetNextLink()))
	} else {
		listDocOptions = append(listDocOptions,
			third_doc.WithFolderID(req.GetFolderId()),
			third_doc.WithKeyword(req.GetKeyword()),
			third_doc.WithExtra("$select", []string{"id", "name", "size", "lastModifiedDateTime", "file", "createdBy", "folder"}),
		)
	}

	docList, err := t.rpc.ListOnedriveDoc(ctx, listDocOptions...)

	if err != nil {
		logx.ErrorContextf(ctx, "ListDoc fail: err(%v)", err)
		return nil, err
	}

	docListFilter := make([]*third_doc.CommonDocInfo, 0)
	// 针对文件类型限制的 size 进行过滤
	for _, docInfo := range docList.Docs {
		fileType := util.GetFileExt(docInfo.Name)
		if strings.Contains(docInfo.CreatedBy, createdByTag) {
			createNameArr := strings.Split(docInfo.CreatedBy, createdByTag)
			docInfo.CreatedBy = createNameArr[len(createNameArr)-1]
		}

		if docInfo.IsFolder {
			docListFilter = append(docListFilter, docInfo)
		}
		size, ok := config.App().RobotDefault.FileTypeSize[fileType]
		if !ok || docInfo.Size > int64(size) {
			continue
		}
		docListFilter = append(docListFilter, docInfo)
	}

	rsp.NextLink = docList.NextLink
	for _, docItem := range docListFilter {
		rsp.ThirdPartyDocDetailInfo = append(rsp.ThirdPartyDocDetailInfo, &knowledge.ThirdPartyDocDetailInfo{
			ItemId:         docItem.ID,
			Title:          docItem.Name,
			IsFolder:       docItem.IsFolder,
			FileType:       docItem.MimeType,
			CreatorName:    docItem.CreatedBy,
			LastModifyTime: docItem.LastModifiedTime,
			Size:           docItem.Size,
			Url:            docItem.DownloadURL,
		})
	}

	return rsp, nil
}

// Migrate 从 onedrive 获取文档列表，然后上传到 cos
func (t *OnedriveDocLogic) ImportDoc(ctx context.Context, req *knowledge.MigrateThirdPartyDocReq) (*knowledge.MigrateThirdPartyDocRsp, error) {
	rsp := &knowledge.MigrateThirdPartyDocRsp{
		ItemIdAndOperationIdPair: make([]*knowledge.ItemIDAndOperationIDPair, 0),
	}
	// 校验参数
	if len(req.GetItemIds()) <= 0 || len(req.GetItemIds()) > config.App().ThirdDocConfig.OneDrive.FilterUploadCount {
		return rsp, errx.ErrBadRequest
	}

	appBizID, _ := convx.StringToUint64(req.GetAppBizId())
	// _, err := t.dao.GetAppByID(ctx, appBizID)
	_, err := t.rpc.AppAdmin.DescribeAppById(ctx, appBizID)
	if err != nil {
		log.ErrorContextf(ctx, "DescribeAppByID call fail: appBizID is %v, err is %v", appBizID, err)
		return nil, err
	}

	// 授权
	uin, subUin := contextx.Metadata(ctx).Uin(), contextx.Metadata(ctx).SubAccountUin()
	checkRsp, err := t.rpc.GetThirdDocPlatformAuthToken(ctx, uin, subUin, req.GetSourceFrom())
	if err != nil {
		log.ErrorContextf(ctx, "GetAuthToken request failed, err is %v", err)
		return nil, err
	}
	if !checkRsp.GetAuthStatus() {
		log.ErrorContextf(ctx, "current user is not not auth, uin is %v, subUin is %v", uin, subUin)
		return nil, errs.ErrThirdDocWithoutAuth
	}

	fileIDAndOpIDMap := slicex.MapKV(req.GetItemIds(), func(itemID string) (string, uint64) {
		operationID := idgen.GetId()
		return itemID, cast.ToUint64(operationID)
	})

	// 1. 写入当前迁移状态到db
	err = t.ThirdDocDao.Migrate(ctx, int32(req.GetSourceFrom()), appBizID, fileIDAndOpIDMap)
	if err != nil {
		log.ErrorContextf(ctx, "ImportDoc fail: err(%v)", err)
		return rsp, err
	}

	// 2. 创建迁移任务
	corpID := contextx.Metadata(ctx).CorpID()
	corp, err := t.rpc.PlatformAdmin.DescribeCorpByBizId(ctx, corpID)
	if err != nil {
		log.ErrorContextf(ctx, "DescribeCorp call fail: err is %v", err)
		return rsp, err
	}
	err = scheduler.NewImportThirdDocTask(ctx, appBizID, corp.CorpId, fileIDAndOpIDMap)
	if err != nil {
		logx.ErrorContextf(ctx, "create import third doc task failed: %v", err)
		return nil, err
	}

	// 3. 返回任务id
	mapx.Range(fileIDAndOpIDMap, func(fileID string, operationID uint64) {
		rsp.ItemIdAndOperationIdPair = append(rsp.ItemIdAndOperationIdPair, &knowledge.ItemIDAndOperationIDPair{
			ItemId:      fileID,
			OperationId: cast.ToString(operationID),
		})
	})
	return rsp, nil
}

// ImportProgress 获取第三方文档导入进度
func (t *OnedriveDocLogic) GetImportProgress(ctx context.Context, req *knowledge.GetMigrateThirdPartyProcessReq) (*knowledge.GetMigrateThirdPartyProcessRsp, error) {
	rsp := &knowledge.GetMigrateThirdPartyProcessRsp{}
	operationIDs := slicex.Map(req.GetOperationIds(), func(operationID string) uint64 {
		return cast.ToUint64(operationID)
	})
	progressData, err := t.ThirdDocDao.GetMigrateProgress(ctx, operationIDs)
	if err != nil {
		log.ErrorContextf(ctx, "GetMigrateProgress fail, operationID is %v, err is%v", operationIDs, err)
		return nil, err
	}

	rsp.MigrateThirdPartyProgressInfo = slicex.Map(progressData, func(progress *model.TThirdDocMigrateProgress) *knowledge.MigrateThirdPartyProgressInfo {
		return &knowledge.MigrateThirdPartyProgressInfo{
			OperationId:   cast.ToString(progress.OperationID),
			MigrateStatus: knowledge.MigrateStatus(progress.Status),
			Url:           progress.CosURL,
			ETag:          progress.CosETag,
			CosHash:       progress.CosHash,
			Size:          progress.FileSize,
			ItemId:        progress.FileID,
		}
	})

	return rsp, nil
}

// RefreshDoc 刷新第三方文档
func (t *OnedriveDocLogic) RefreshDoc(ctx context.Context, isAuto bool, docs []*docEntity.Doc) error {
	thirdDocFileInfo := make([]*entity.DocRefreshFileInfo, 0)
	for _, doc := range docs {
		if doc.Status != docEntity.DocStatusWaitRelease && doc.Status != docEntity.DocStatusReleaseSuccess {
			if isAuto {
				log.WarnContextf(ctx, "RefreshOnedriveDoc doc status is not wait release or release success, doc: %+v", doc)
				continue
			}
			return errs.ErrRefreshTxDocStatusFail
		}
		if time.Unix(0, 0).Before(doc.ExpireEnd) && time.Now().After(doc.ExpireEnd) {
			log.WarnContextf(ctx, "RefreshOnedriveDoc status Expire, doc: %+v", doc)
			continue
		}
		staffInfo, err := t.rpc.PlatformAdmin.GetStaffByID(ctx, doc.StaffID)
		if err != nil {
			log.ErrorContextf(ctx, "Get staff failed, err is %v", err)
			return err
		}
		userInfo, err := t.userDao.DescribeUserByID(ctx, staffInfo.UserID)
		if err != nil {
			log.ErrorContextf(ctx, "RefreshOnedriveDoc GetUserByID err: %+v", err)
			return err
		}

		rsp, err := t.rpc.GetThirdDocPlatformAuthToken(ctx, userInfo.Uin, userInfo.SubAccountUin, common.SourceFromType_SOURCE_FROM_TYPE_ONEDRIVE)
		if err != nil {
			if isAuto {
				// 定时任务自动刷新,未授权跳过
				log.DebugContextf(ctx, "RefreshOnedriveDoc CheckUserAuth rsp.Response.Code != 200, isAuto: %v uin: %s",
					isAuto, userInfo.Uin)
				continue
			}
			log.ErrorContextf(ctx, "RefreshOnedriveDoc ImportTFile err: %+v", err)
			return err
		}
		log.DebugContextf(ctx, "RefreshOnedriveDoc CheckUserAuth rsp: %+v", rsp)
		if rsp == nil || !rsp.AuthStatus {
			continue
		}

		operationID := idgen.GetId()
		thirdDocFileInfo = append(thirdDocFileInfo, &entity.DocRefreshFileInfo{
			DocID:       doc.ID,
			CorpID:      doc.CorpID,
			StaffID:     doc.StaffID,
			RobotID:     doc.RobotID,
			FileID:      doc.CustomerKnowledgeId,
			OperationID: cast.ToString(operationID),
		})
	}

	// isAuto 如果是自动刷新任务，需要更新所有文档下次执行时间
	if isAuto {
		log.DebugContextf(ctx, "RefreshOnedriveDoc isAuto: %v", isAuto)
	}

	taskID, err := scheduler.NewThirdDocRefreshTask(ctx, docEntity.SourceFromOnedrive, thirdDocFileInfo)
	if err != nil {
		log.ErrorContextf(ctx, "RefreshOnedriveDoc NewThirdDocRefreshTask err: %+v", err)
		return err
	}

	log.DebugContextf(ctx, "RefreshOnedriveDoc NewThirdDocRefreshTask taskID: %v", taskID)
	return nil
}

// UpdateImportProgress 更新第三方文档导入进度
func (t *OnedriveDocLogic) UpdateImportProgress(ctx context.Context, success, fail map[uint64]*model.TThirdDocMigrateProgress) error {
	return t.ThirdDocDao.UpdateMigrateProgress(ctx, success, fail)
}
