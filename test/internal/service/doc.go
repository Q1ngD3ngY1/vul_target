// Package service 业务逻辑层
package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"git.woa.com/adp/common/x/gox/ptrx"

	"git.woa.com/adp/common/x/logx/auditx"
	"git.woa.com/adp/kb/kb-config/internal/entity/finance"

	md "github.com/JohannesKaufmann/html-to-markdown"
	"github.com/PuerkitoBio/goquery"
	rd "github.com/go-shiori/go-readability"
	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"gorm.io/gorm"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/clientx/s3x"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/mapx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/gox/stringx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/mathx/randx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/common/x/utilx/validx"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/category"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	logicCommon "git.woa.com/adp/kb/kb-config/internal/logic/common"
	logicDoc "git.woa.com/adp/kb/kb-config/internal/logic/document"
	"git.woa.com/adp/kb/kb-config/internal/logic/user"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	pm "git.woa.com/adp/pb-go/platform/platform_manager"
	secapi "git.woa.com/sec-api/go/scurl"
)

// DescribeStorageCredential 获取临时密钥
// 特别说明：C侧链接可以分享，需要在C侧进行图片上传或者文档上传，需要获取临时密钥
// 1. C侧分享出去的连接，当前登录的Corp和App可能不是归属关系
// 2. 临时密钥这里不强制校验AppID和CorpID是否强制是归属关系
// 3. 路径的拼接方式已经保证了获取的权限只能是当前Corp下的，不存在跨Corp越权问题
// 4. 通过FileType参数收敛权限：FileType为空的话，只有下载权限，不为空的话，指定了固定的路径上传
// 5. 由于1、2，可能存在下面的情况：
//   - public/corpA/appA/xxx.xx 正常是这种情况
//   - public/corpB/appA/xxx.xx C侧分享的情况，这里其实corpB下没有appA，只是存储corpB通过appA分享链接操作的数据（图片等）
//   - corp/corpA/appA/doc/xxx.xx 正常是这种情况
//   - corp/corpB/appA/doc/xxx.xx C侧分享的情况，这里其实corpB下没有appA，只是存储corpB通过appA分享链接操作的数据（文档等）
//   - 补充说明：corp和app不匹配的情况存在于分享链接的情况，其他情况不应该出现，也不应该跨数据访问
func (s *Service) DescribeStorageCredential(ctx context.Context, req *pb.DescribeStorageCredentialReq) (
	*pb.DescribeStorageCredentialRsp, error) {
	logx.I(ctx, "DescribeStorageCredential|req:%+v", req)
	corpID := contextx.Metadata(ctx).CorpID()
	corpBizId, err := s.getCorpByID(ctx, corpID)
	if err != nil || corpBizId == 0 {
		return nil, errs.ErrCorpNotFound
	}
	pathList := make([]string, 0)
	var fileCosPath, imageCosPath, uploadCosPath string
	fileName := s.getFileNameByType(req.GetFileType())
	if req.GetBotBizId() != "" {
		app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
		// 背景：给C端分享出去的链接使用上传图片或者文档，此时当前登录的Corp和App可能不是归属关系
		// 这里不强制校验AppID和CorpID是否强制是归属关系
		// 下面路径的拼接已经保证了获取的权限只能是当前Corp下的，不存在跨Corp越权问题
		if err != nil && !errx.Is(err, errs.ErrCorpAppNotEqual) {
			return nil, errs.ErrRobotNotFound
		}
		if len(fileName) == 0 {
			// corpCosPath是按企业维度维护的cos数据
			// robotCosPath是按照机器人维度维护的cos数据
			// 文档资料是按照机器人维度进行隔离，所以后续使用robotCosPath对外，corpCosPath只对存量的历史数据开放
			corpCosPath := s.s3.GetCorpCOSPath(ctx, corpID)
			fileCosPath = s.s3.GetCorpRobotCOSPath(ctx, corpBizId, app.BizId, "")
			imageCosPath = s.s3.GetCorpAppImagePath(ctx, corpBizId, app.BizId, "")
			pathList = []string{corpCosPath, fileCosPath, imageCosPath}
		} else {
			// 区分Cos公有权限场景还是私有权限场景
			if req.GetIsPublic() {
				uploadCosPath = s.s3.GetCorpAppImagePath(ctx, corpBizId, app.BizId, fileName)
			} else {
				uploadCosPath = s.s3.GetCorpRobotCOSPath(ctx, corpBizId, app.BizId, fileName)
			}
			pathList = []string{uploadCosPath}
		}
	} else {
		if len(fileName) == 0 {
			imageCosPath = s.s3.GetCorpImagePath(ctx, corpBizId)
			pathList = append(pathList, imageCosPath)
		} else {
			uploadCosPath = filepath.Join(s.s3.GetCorpImagePath(ctx, corpBizId), fileName)
			pathList = append(pathList, uploadCosPath)
		}
	}

	typeKey := dao.DefaultStorageTypeKey
	if len(req.GetTypeKey()) > 0 {
		typeKey = req.GetTypeKey()
	}
	req2 := s3x.GetCredentialReq{
		TypeKey:       typeKey,
		Path:          pathList,
		StorageAction: gox.IfElse[string](len(fileName) == 0, s3x.ActionDownload, s3x.ActionUpload),
	}
	res, err := s.s3.GetCredentialWithTypeKey(ctx, &req2)
	if err != nil {
		return nil, err
	}
	bucket, err := s.s3.GetBucketWithTypeKey(ctx, typeKey)
	if err != nil {
		return nil, err
	}
	region, err := s.s3.GetRegionWithTypeKey(ctx, typeKey)
	if err != nil {
		return nil, err
	}
	storageType, err := s.s3.GetTypeWithTypeKey(ctx, typeKey)
	if err != nil {
		return nil, err
	}
	rsp := &pb.DescribeStorageCredentialRsp{
		Credentials: &pb.DescribeStorageCredentialRsp_Credentials{
			Token:        res.Credentials.SessionToken,
			TmpSecretId:  res.Credentials.TmpSecretID,
			TmpSecretKey: res.Credentials.TmpSecretKey,
		},
		ExpiredTime: uint32(res.ExpiredTime),
		StartTime:   uint32(res.StartTime),
		Bucket:      bucket,
		Region:      region,
		FilePath:    fileCosPath,
		ImagePath:   imageCosPath,
		UploadPath:  uploadCosPath,
		CorpUin:     0,
		Type:        storageType,
	}
	logx.I(ctx, "DescribeStorageCredential|rsp:%+v", rsp)
	return rsp, nil
}

// ListDoc 文档列表
func (s *Service) ListDoc(ctx context.Context, req *pb.ListDocReq) (*pb.ListDocRsp, error) {
	logx.I(ctx, "ListDoc Req:%+v", req)
	rsp := new(pb.ListDocRsp)
	if req.GetBotBizId() == "" {
		return nil, errs.ErrParams
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if req.GetQueryType() == "" {
		req.QueryType = docEntity.DocQueryTypeFileName
	}
	docListReq, err := s.getDocListReq(ctx, req, app.CorpPrimaryId, app.PrimaryId, app.IsShared)
	if err != nil {
		return rsp, err
	}
	total, docs, err := s.docLogic.GetDocListByListReq(ctx, docListReq)
	if err != nil {
		return rsp, errs.ErrGetDocListFail
	}
	staffIDs, docIDs := make([]uint64, 0, len(docs)), make([]uint64, 0, len(docs))
	cateIDMap := make(map[uint64]struct{})
	for _, doc := range docs {
		docIDs = append(docIDs, doc.ID)
		staffIDs = append(staffIDs, doc.StaffID)
		if doc.CategoryID != 0 {
			cateIDMap[uint64(doc.CategoryID)] = struct{}{}
		}
	}
	cateIDs := maps.Keys(cateIDMap)
	cateMap, err := s.cateLogic.DescribeCateByIDs(ctx, cateEntity.DocCate, cateIDs)
	if err != nil {
		logx.E(ctx, "获取分类信息失败 err:%+v", err)
		// API调用使用的出参，先只打印ERROR日志，不返回错误，避免现网有脏数据影响文档列表的展示，观察一段时间后再放开
		// TODO: return nil, pkg.ErrCateNotFound
	}
	maxProcessUnstableStatusDocCount := 10 // 最大处理非稳定状态时间过长文档数，避免处理太多导致接口超时
	processUnstableStatusDocCount := 0
	for _, doc := range docs {
		if !doc.IsStableStatus() && doc.Status != docEntity.DocStatusReleasing {
			// 兜底策略：避免文档一直阻塞在非稳定状态（除了发布中状态由admin维护，不需要兜底）
			if processUnstableStatusDocCount >= maxProcessUnstableStatusDocCount {
				// 最大处理非稳定状态时间过长文档数，避免处理太多导致接口超时
				continue
			}
			// 如果是非稳定状态时间过长，打印ERROR日志，并更新成失败状态
			s.docLogic.ProcessUnstableStatusDoc(ctx, doc)
			processUnstableStatusDocCount++
		}
	}
	qaNums, err := s.qaLogic.GetDocQANum(ctx, app.CorpPrimaryId, app.PrimaryId, docIDs)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	// 检查文档是否在发布中
	releasingDocIdMap, err := s.docLogic.GetReleasingDocId(ctx, app.PrimaryId, docIDs)
	if err != nil {
		logx.E(ctx, "获取发布中的文档失败 err:%+v", err)
		return rsp, errs.ErrSystem
	}
	latestRelease, err := s.releaseLogic.GetLatestRelease(ctx, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	mapDocID2AttrLabels, err := s.labelLogic.GetDocAttributeLabelDetail(ctx, app.PrimaryId, docIDs)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	docParsesFailMap, err := s.docParsesMap(ctx, docs)
	if err != nil {
		return rsp, errs.ErrDocParseTaskNotFound
	}
	docAuditFailMap, _ := s.docAuditMap(ctx, docs, app)
	rsp.Total = total
	rsp.List = make([]*pb.ListDocRsp_Doc, 0, len(docs))
	// 获取员工名称
	staffs, err := s.rpc.PlatformAdmin.DescribeStaffList(ctx, &pm.DescribeStaffListReq{
		StaffIds: staffIDs,
	})
	if err != nil { // 失败降级为返回员工ID
		logx.E(ctx, "ListDoc get staff name staffIDs:%v, error:%v", staffIDs, err)
	}
	for _, doc := range docs {
		docPb := logicDoc.DbDoc2PbDoc(ctx, releasingDocIdMap, doc, latestRelease, qaNums, mapDocID2AttrLabels,
			docParsesFailMap, docAuditFailMap, cateMap, app.IsShared)
		if staff, ok := staffs[doc.StaffID]; ok { // 赋值员工名称
			docPb.StaffName = staff.GetNickName()
		} else { // 没取到返回员工ID
			docPb.StaffName = cast.ToString(doc.StaffID)
		}
		rsp.List = append(rsp.List, docPb)
	}
	return rsp, nil
}

func (s *Service) docAuditMap(ctx context.Context, docs []*docEntity.Doc, app *entity.App) (
	map[uint64]releaseEntity.AuditStatus, error) {
	docParseFailIDs := make([]uint64, 0)
	for _, doc := range docs {
		if doc.Status == docEntity.DocStatusAuditFail {
			docParseFailIDs = append(docParseFailIDs, doc.ID)
			logx.D(ctx, "docAuditMap doc.ID:%d", doc.ID)
		}
	}
	docAuditFailMap, err := s.auditLogic.GetBizAuditStatusByRelateIDs(ctx, app.PrimaryId, app.CorpPrimaryId, docParseFailIDs)
	if err != nil {
		return docAuditFailMap, errs.ErrAuditNotFound
	}
	return docAuditFailMap, nil
}

func (s *Service) isAllowRetry(ctx context.Context, docID uint64, docStatus uint32,
	docParsesFailMap map[uint64]docEntity.DocParse,
	docAuditFailMap map[uint64]releaseEntity.AuditStatus) bool {
	if docParsesFailMap == nil {
		return false
	}
	if docStatus == docEntity.DocStatusAuditFail {
		if docAuditFail, ok := docAuditFailMap[docID]; ok && docAuditFail.Status == releaseEntity.AuditStatusTimeoutFail {
			return true
		}
		return false
	}
	if docStatus == docEntity.DocStatusParseImportFail {
		return true
	}
	logx.D(ctx, "isAllowRetry docParsesFailMap:%+v, "+
		"docID:%d, docStatus:%d", docParsesFailMap, docID, docStatus)
	if len(docParsesFailMap) == 0 {
		return false
	}
	docParsesFail := docEntity.DocParse{}
	ok := false
	if docParsesFail, ok = docParsesFailMap[docID]; !ok {
		return false
	}
	if docParsesFail.Status == docEntity.DocParseCallBackCancel {
		return true
	}
	result := &pb.FileParserCallbackReq{}
	err := jsonx.UnmarshalFromString(docParsesFail.Result, result)
	if err != nil {
		logx.E(ctx, "isAllowRetry UnmarshalFromString err:%+v, "+
			"docID:%d, docStatus:%d", err, docID, docStatus)
		return true
	}
	return s.getIsAllowRetry(result.ErrorCode)
}

func (s *Service) getIsAllowRetry(errorCode string) bool {
	if conf, ok := config.App().DocParseError[errorCode]; ok {
		return conf.IsAllowRetry
	}
	return config.App().DocParseErrorDefault.IsAllowRetry
}

func (s *Service) docParsesMap(ctx context.Context, docs []*docEntity.Doc) (map[uint64]docEntity.DocParse, error) {
	logx.D(ctx, "docParsesMap docs:%+v", docs)
	docParseFailIDs := make([]uint64, 0)
	for _, doc := range docs {
		if doc.Status == docEntity.DocStatusParseFail {
			docParseFailIDs = append(docParseFailIDs, doc.ID)
		}
	}
	docParsesMap := make(map[uint64]docEntity.DocParse, len(docParseFailIDs))
	if len(docParseFailIDs) == 0 {
		return docParsesMap, nil
	}
	docParses, err := s.docLogic.GetDocParseByDocIDs(ctx, docParseFailIDs, docs[0].RobotID)
	if err != nil {
		return docParsesMap, errs.ErrDocParseTaskNotFound
	}
	for _, v := range docParses {
		if _, ok := docParsesMap[v.DocID]; !ok {
			docParsesMap[v.DocID] = *v
		}
	}
	logx.D(ctx, "docParsesMap docParsesMap:%+v", docParsesMap)
	return docParsesMap, nil
}

func (s *Service) getDocListReq(ctx context.Context, req *pb.ListDocReq, corpID, robotID uint64, isShared bool) (*docEntity.DocListReq, error) {
	validityStatus, status, err := s.getDocExpireStatus(req.GetStatus(), isShared)
	if err != nil {
		return nil, err
	}
	err = s.checkQueryType(req.GetQueryType())
	if err != nil {
		return nil, err
	}
	var cateIDs []uint64
	if req.GetCateBizId() != cateEntity.AllCateID {
		cateID, err := s.cateLogic.VerifyCateBiz(ctx, cateEntity.DocCate, corpID, uint64(req.GetCateBizId()), robotID)
		if err != nil {
			return nil, err
		}
		if req.GetShowCurrCate() == docEntity.ShowCurrCate { // 只展示当前分类的数据
			cateIDs = append(cateIDs, cateID)
		} else {
			cateIDs, err = s.getCateChildrenIDs(ctx, cateEntity.DocCate, corpID, robotID, cateID)
			if err != nil {
				return nil, err
			}
		}
	}

	mapFilterFlag := make(map[string]bool)
	for _, filterFlag := range req.GetFilterFlag() {
		if !docEntity.IsValidDocFilterFlag(filterFlag.Flag) {
			return nil, errs.ErrDocFilterFlagFail
		}
		mapFilterFlag[filterFlag.Flag] = filterFlag.Value
	}

	docListReq := &docEntity.DocListReq{
		CorpID:         corpID,
		RobotID:        robotID,
		FileName:       req.GetQuery(),
		QueryType:      req.GetQueryType(),
		FileTypes:      req.GetFileTypes(),
		FilterFlag:     mapFilterFlag,
		Page:           req.GetPageNumber(),
		PageSize:       req.GetPageSize(),
		Status:         status,
		ValidityStatus: validityStatus,
		Opts:           []uint32{docEntity.DocOptDocImport},
		CateIDs:        cateIDs,
	}

	if req.GetEnableScope() != pb.RetrievalEnableScope_ENABLE_SCOPE_TYPE_UNKNOWN {
		docListReq.EnableScope = ptrx.Uint32(uint32(req.GetEnableScope()))
	}

	return docListReq, nil
}

// checkCanSaveDoc 判断用户是否能上传文档
func (s *Service) checkCanSaveDoc(ctx context.Context, botBizID uint64, fileName, fileType string) error {
	staffID := contextx.Metadata(ctx).StaffID()
	staff, err := s.rpc.PlatformAdmin.GetStaffByID(ctx, staffID)
	if err != nil || staff == nil {
		return errs.ErrStaffNotFound
	}
	if len(strings.TrimSuffix(fileName, "."+fileType)) == 0 {
		return errs.ErrInvalidFileName
	}
	if !util.CheckFileType(ctx, fileName, fileType) {
		return errs.ErrFileExtNotMatch
	}
	return nil
}

func checkAndGetEnableScope(ctx context.Context, app *entity.App,
	enableScopeInReq pb.RetrievalEnableScope) pb.RetrievalEnableScope {
	enableScope := enableScopeInReq
	if enableScope == pb.RetrievalEnableScope_ENABLE_SCOPE_TYPE_UNKNOWN {
		// 默认知识库为开发域生效；共享知识库默认为开发域/发布域都生效； 企点和历史api用户兼容
		enableScope = gox.IfElse(app.IsShared,
			pb.RetrievalEnableScope_ENABLE_SCOPE_TYPE_ALL, pb.RetrievalEnableScope_ENABLE_SCOPE_TYPE_DEV)
		logx.W(ctx, "EnableScope is empty in req, set to %s default enableScope (appBizId:%d)",
			enableScope.String(), app.BizId)
	}
	return enableScope
}

// SaveDoc 保存文档
func (s *Service) SaveDoc(ctx context.Context, req *pb.SaveDocReq) (*pb.SaveDocRsp, error) {
	logx.I(ctx, "SaveDoc req:%+v", req)
	rsp := new(pb.SaveDocRsp)
	if req.GetBotBizId() == "" || req.GetCosHash() == "" {
		return rsp, errs.ErrWrapf(errs.ErrParameterInvalid, "BotBizId CosHash")
	}

	key := fmt.Sprintf(dao.LockForSaveDoc, req.GetBotBizId(), req.GetCosHash())
	if err := s.dao.Lock(ctx, key, 120*time.Second); err != nil {
		return nil, errs.ErrSameDocUploading
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()

	expireStart, expireEnd, err := util.CheckReqStartEndTime(ctx, req.GetExpireStart(), req.GetExpireEnd())
	if err != nil {
		return nil, err
	}
	fileSize, err := validx.CheckAndParseUint64(req.GetSize())
	if err != nil {
		return nil, err
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, app.CorpPrimaryId)
	// corp, err := s.dao.GetCorpByID(ctx, app.CorpPrimaryId)
	if err != nil || corp == nil {
		return rsp, errs.ErrCorpNotFound
	}

	if err = s.s3.CheckURLFile(ctx, app.CorpPrimaryId, corp.GetCorpId(), app.BizId,
		req.CosUrl, req.ETag); err != nil {
		logx.E(ctx, "SaveDoc|CheckURLFile failed, err:%+v", err)
		return rsp, errs.ErrInvalidURL
	}

	if req.GetSource() == docEntity.SourceFromWeb {
		originalURL := strings.TrimSpace(req.GetOriginalUrl())
		if originalURL == "" {
			logx.W(ctx, "SaveDoc|Source|%d|OriginalUrl为空", req.GetSource())
			return rsp, errs.ErrParams
		}
		if utf8.RuneCountInString(originalURL) > 2048 {
			logx.E(ctx, "SaveDoc|OriginalUrl长度超过2048字符限制")
			return rsp, errs.ErrInvalidURL
		}
	}
	if req.GetSource() == docEntity.SourceFromTxDoc && req.GetCustomerKnowledgeId() == "" {
		logx.W(ctx, "SaveDoc|Source|%d|CustomerKnowledgeId为空", req.GetSource())
		return rsp, errs.ErrParams
	}

	if err = s.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{App: app}); err != nil {
		return rsp, logicCommon.ConvertErrMsg(ctx, s.rpc, 0, app.CorpPrimaryId, err)
	}
	if err := s.checkCanSaveDoc(ctx, app.BizId, req.GetFileName(), req.GetFileType()); err != nil {
		return rsp, err
	}
	if len(req.GetAttrLabels()) > 0 {
		req.AttrRange = docEntity.AttrRangeCondition
	} else {
		req.AttrRange = docEntity.AttrRangeAll
	}
	attrs, labels, err := s.checkAttributeLabelRefer(ctx, app.PrimaryId, config.App().AttributeLabel.DocAttrLimit,
		config.App().AttributeLabel.DocAttrLabelLimit, req.GetAttrRange(), req.GetAttrLabels())
	if err != nil {
		return rsp, err
	}

	enableScope := checkAndGetEnableScope(ctx, app, req.GetEnableScope())

	// 导入问答仅支持xlsx格式
	if req.Opt == docEntity.DocOptBatchImport && (req.GetFileType() != docEntity.FileTypeXlsx &&
		req.GetFileType() != docEntity.FileTypeNumbers) {
		return rsp, errs.ErrDocQAFileFail
	}
	// 如果是导入excel，需要判别文件不是文档导入，如果是文档导入的excel就不需要检查表头了
	if (req.GetFileType() == docEntity.FileTypeXlsx || req.GetFileType() == docEntity.FileTypeNumbers) &&
		req.Opt != docEntity.DocOptDocImport {
		if rsp, err := s.checkXlsx(ctx, app.CorpPrimaryId, app.PrimaryId, req.GetCosUrl(), contextx.Metadata(ctx).Uin(),
			app.BizId); rsp != nil || err != nil {
			return rsp, err
		}
	}
	// 校验是否有重复文档
	isDuplicate, rsp, err := s.docLogic.CheckDuplicateFile(ctx, req, app.CorpPrimaryId, app.PrimaryId)
	logx.I(ctx, "save|isDuplicate:%t (doc:%s)", isDuplicate, req.FileName)
	if err != nil {
		return nil, err
	}
	if isDuplicate {
		return rsp, nil
	}
	auditFlag, err := util.GetFileAuditFlag(req.GetFileType())
	if err != nil {
		return rsp, err
	}
	size, err := s.checkDocXlsxCharSize(ctx, req, app, fileSize)
	if err != nil {
		return rsp, err
	}
	// todo neckyang 校验自定义切分规则配置
	var cateID uint64
	if req.GetCateBizId() != "" && req.GetCateBizId() != "0" {
		var catBizID uint64
		catBizID, err = util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
		if err != nil {
			return nil, err
		}
		if catBizID == 0 {
			logx.W(ctx, "SaveDoc|CateBizId is 0, catBizID:%d", catBizID)
			return nil, errs.ErrCateNotFound
		}
		cateID, err = s.cateLogic.VerifyCateBiz(ctx, cateEntity.DocCate, app.CorpPrimaryId, catBizID, app.PrimaryId)
	} else {
		cateID, err = s.cateLogic.DescribeRobotUncategorizedCateID(ctx, cateEntity.DocCate, app.CorpPrimaryId, app.PrimaryId)
	}
	if err != nil {
		return nil, err
	}
	staffID := contextx.Metadata(ctx).StaffID()
	doc := s.newDoc(ctx, req, app, app.CorpPrimaryId, staffID, expireStart, expireEnd, uint64(enableScope), fileSize, auditFlag, size, cateID)
	docAttributeLabelsFromPB, err := fillDocAttributeLabelsFromPB(ctx, req.GetAttrLabels(), true, attrs, labels)
	if err != nil {
		return nil, err
	}
	if err := s.docLogic.CreateDoc(ctx, staffID, doc, docAttributeLabelsFromPB); err != nil {
		logx.E(ctx, "SaveDoc CreateDoc err: %+v", err)
		if errs.Is(err, errs.ErrFileSizeTooBig) || errs.Is(err, errs.ErrUnSupportFileType) {
			return nil, err
		}
		return nil, errs.ErrSystem
	}
	auditx.Create(auditx.BizDocument).App(app.BizId).Space(app.SpaceId).Log(ctx, doc.BusinessID, doc.FileName)
	rsp.DocBizId = doc.BusinessID
	// 最后更新容量使用情况
	objectInfo, err := s.s3.StatObject(ctx, doc.CosURL)
	if err == nil && objectInfo != nil && objectInfo.Size > 0 {
		doc.FileSize = uint64(objectInfo.Size)
	}
	if doc.FileSize != 0 {
		err := s.financeLogic.UpdateAppCapacityUsage(ctx, entity.CapacityUsage{
			StorageCapacity:   gox.IfElse(doc.Source == docEntity.SourceFromCorpCOSDoc, 0, int64(doc.FileSize)),
			ComputeCapacity:   int64(doc.FileSize),
			KnowledgeCapacity: int64(doc.FileSize),
		}, doc.RobotID, doc.CorpID)
		if err != nil {
			return rsp, errs.ErrUpdateRobotUsedCharSizeFail
		}
	}
	logx.I(ctx, "SaveDoc|rsp:%+v", rsp)
	return rsp, nil
}

// checkXlsx 检查问答模板文件是否符合要求
func (s *Service) checkXlsx(ctx context.Context, corpID, robotID uint64, cosURL string, uin string,
	appBizID uint64) (*pb.SaveDocRsp, error) {
	body, err := s.s3.GetObject(ctx, cosURL)
	if err != nil {
		return nil, errs.ErrSystem
	}
	// 将配置中文件头翻译成ctx中语言
	var checkHead []string
	for _, v := range docEntity.ExcelTplHead[:docEntity.ExcelTplQaEnableScopeIndex+1] {
		checkHead = append(checkHead, i18n.Translate(ctx, v))
	}
	logx.I(ctx, "checkXlsx checkHead:%v", checkHead)
	rows, bs, err := util.CheckXlsxContent(ctx, cosURL, 0, config.App().DocQA.ImportMaxLength,
		checkHead, body, s.checkRow, uin, appBizID)
	if err != nil {
		if !errors.Is(err, errs.ErrExcelContent) {
			return nil, err
		}
		key := cosURL + ".check.xlsx"
		if err := s.s3.PutObject(ctx, bs, key); err != nil {
			return nil, errs.ErrSystem
		}
		url, err := s.s3.GetPreSignedURL(ctx, key)
		if err != nil {
			return nil, errs.ErrSystem
		}
		return &pb.SaveDocRsp{
			ErrorMsg:      i18n.Translate(ctx, i18nkey.KeyFileDataErrorPleaseDownloadErrorFile),
			ErrorLink:     url,
			ErrorLinkText: i18n.Translate(ctx, i18nkey.KeyDownload),
		}, nil
	}

	allCates, err := s.cateLogic.DescribeCateList(ctx, category.QACate, corpID, robotID)
	if err != nil {
		return nil, errs.ErrSystem
	}

	tree := category.BuildCateTree(allCates)
	for _, row := range rows {
		_, cate := category.GetCatePath(row)
		tree.Create(cate)
	}

	limit := config.App().DocQA.CateNodeLimit
	if tree.NodeCount()-1 > limit {
		return nil, errs.ErrWrapf(errs.ErrCodeCateCountExceed, i18n.Translate(ctx, i18nkey.KeyQACategoryCountExceeded),
			limit)
	}

	return nil, nil
}

// checkRow check每一行的内容
func (s *Service) checkRow(ctx context.Context, i int, row []string, questions *sync.Map, uin string, appBizID uint64,
	uniqueImgHost *sync.Map) string {
	ok, cates := category.GetCatePath(row)
	if !ok {
		return i18n.Translate(ctx, i18nkey.KeyCategoryErrorPleaseRefill)
	}

	for _, cate := range cates {
		if err := checkCateName(ctx, cate); err != nil {
			return errx.Msg(err)
		}
	}

	if len(row) < docEntity.ExcelTplHeadLen-docEntity.ExcelTpOptionalLen {
		return i18n.Translate(ctx, i18nkey.KeyQuestionOrAnswerEmptyPleaseFill)
	}

	answer := strings.TrimSpace(row[docEntity.ExcelTplAnswerIndex])
	question := strings.TrimSpace(row[docEntity.ExcelTplQuestionIndex])
	if question == "" || answer == "" {
		return i18n.Translate(ctx, i18nkey.KeyQuestionOrAnswerEmptyPleaseFill)
	}

	if _, ok := questions.Load(question); ok {
		logx.I(context.Background(), "checkRow|question:%s", question)
		return i18n.Translate(ctx, i18nkey.KeyQAKnowledgeBaseDuplicateCorpus, question)
	}
	questions.Store(question, i)
	// 检查问题描述
	if len(row) >= docEntity.ExcelTplQuestionDescIndex+1 {
		questionDesc := strings.TrimSpace(row[docEntity.ExcelTplQuestionDescIndex])
		if err := s.qaLogic.CheckQuestionDesc(ctx, questionDesc); err != nil {
			return errx.Msg(err)
		}
	}
	// 检查相似问
	if len(row) >= docEntity.ExcelTplSimilarQuestionIndex+1 {
		simQuestions := strings.TrimSpace(row[docEntity.ExcelTplSimilarQuestionIndex])
		sqs := stringx.SplitAndRemoveEmpty(simQuestions, "\n")
		if len(sqs) > 0 {
			if _, err := s.qaLogic.CheckSimilarQuestionNumLimit(ctx, len(sqs), 0, 0); err != nil {
				return i18n.Translate(ctx, i18nkey.KeyQAKnowledgeBaseInfo, question, errx.Msg(err))
			}
			if _, _, err := s.qaLogic.CheckSimilarQuestionContent(ctx, question, sqs); err != nil {
				return i18n.Translate(ctx, i18nkey.KeyQAKnowledgeBaseInfo, question, errx.Msg(err))
			}
			/* 增加相似问,一起判断是否重复, 不判断, 和api接口行为保持一致
			   for _, sq := range sqs {
			      questions[sq] = i
			   }
			*/
		}
	}
	mdAnswer, err := util.CheckQaImgURLSafeToMD(ctx, answer, uin, appBizID, uniqueImgHost)
	if err != nil {
		logx.W(ctx, "ModifyQA Answer ConvertDocQaHtmlToMD err:%d", err)
		return i18n.Translate(ctx, i18nkey.KeyQAKnowledgeBaseInfo, question, errx.Msg(err))
	}
	videoUrls, err := util.CheckVideoUrls(mdAnswer)
	if err != nil {
		return i18n.Translate(ctx, i18nkey.KeyQAKnowledgeBaseInfo, answer, errx.Msg(err))
	}
	for _, videoUrl := range videoUrls {
		u, err := url.Parse(videoUrl)
		if err != nil {
			return i18n.Translate(ctx, i18nkey.KeyQAKnowledgeBaseInfo, answer, errx.Msg(err))
		}
		if u.Host != config.App().Storage.VideoDomain {
			return i18n.Translate(ctx, i18nkey.KeyQAKnowledgeBaseInfo, answer, i18nkey.KeyExternalVideoLinksNotSupported)
		}
		// 去掉前面的斜线
		path := strings.TrimPrefix(u.Path, "/")
		objectInfo, err := s.s3.StatObject(context.Background(), path)
		if err != nil || objectInfo == nil {
			logx.W(context.Background(), "checkRow|StatObject:%+v err:%v", objectInfo, err)
			return i18n.Translate(ctx, i18nkey.KeyQAKnowledgeBaseInfo, answer, i18nkey.KeyInvalidOrUnreachableVideoUrl)
		}
	}

	// 检查时间有效期格式是否满足
	if err := checkInDataValidity(ctx, row); err != nil {
		return errx.Msg(err)
	}
	// 检查自定义参数是否满足
	if docEntity.ExcelTplCustomParamIndex+1 > len(row) {
		if err := s.qaLogic.CheckQuestionAndAnswer(ctx, question, answer); err != nil {
			return errx.Msg(err)
		}
		return ""
	}
	customParam := strings.TrimSpace(row[docEntity.ExcelTplCustomParamIndex])
	questionDesc := strings.TrimSpace(row[docEntity.ExcelTplQuestionDescIndex])
	if err := s.qaLogic.CheckQAAndDescAndParam(ctx, question, answer, questionDesc, customParam); err != nil {
		return errx.Msg(err)
	}
	return ""
}

func (s *Service) newDoc(ctx context.Context, req *pb.SaveDocReq, app *entity.App,
	corpID, staffID, expireStart, expireEnd, enableScope, fileSize uint64,
	auditFlag uint32, size int, cateId uint64) *docEntity.Doc {
	isDownloadable := false
	if req.GetIsRefer() && req.GetReferUrlType() == docEntity.ReferURLTypePreview {
		isDownloadable = req.GetIsDownload()
	}
	// 计算下次更新时间
	nextUpdateTime := time.Unix(0, 0).Add(8 * time.Hour)
	if req.GetSource() == docEntity.SourceFromTxDoc && req.GetUpdatePeriodInfo().GetUpdatePeriodH() != 0 {
		nextUpdateTime = logicDoc.GetDocNextUpdateTime(ctx, req.GetUpdatePeriodInfo().GetUpdatePeriodH())
	}
	doc := &docEntity.Doc{
		BusinessID:          idgen.GetId(),
		RobotID:             app.PrimaryId,
		CorpID:              corpID,
		StaffID:             staffID,
		FileName:            req.GetFileName(),
		FileType:            req.GetFileType(),
		FileSize:            fileSize,
		CosURL:              req.GetCosUrl(),
		Bucket:              s.s3.GetBucket(ctx),
		CosHash:             req.GetCosHash(),
		Status:              docEntity.DocStatusParseIng,
		IsDeleted:           false,
		Source:              req.GetSource(),
		WebURL:              req.GetWebUrl(),
		AuditFlag:           auditFlag,
		CharSize:            uint64(size),
		NextAction:          docEntity.DocNextActionAdd,
		IsRefer:             req.GetIsRefer(),
		AttrRange:           req.GetAttrRange(),
		ReferURLType:        req.GetReferUrlType(),
		ExpireStart:         time.Unix(int64(expireStart), 0),
		ExpireEnd:           time.Unix(int64(expireEnd), 0),
		Opt:                 s.getSaveDocOpt(req),
		CategoryID:          uint32(cateId),
		OriginalURL:         req.GetOriginalUrl(),
		CustomerKnowledgeId: req.GetCustomerKnowledgeId(),
		IsDownloadable:      isDownloadable,
		UpdatePeriodH:       req.GetUpdatePeriodInfo().GetUpdatePeriodH(),
		NextUpdateTime:      nextUpdateTime,
		SplitRule:           req.GetSplitRule(),
		EnableScope:         uint32(enableScope),
	}
	// if req.GetSource() == model.SourceFromTxDoc && req.GetUpdatePeriodInfo().GetUpdatePeriodH() != 0 {
	//	doc.NextUpdateTime = nextUpdateTime
	// }
	for _, attrFlag := range req.GetAttributeFlags() {
		doc.AddAttributeFlag([]uint64{uint64(math.Pow(2, float64(attrFlag)))})
	}
	return doc
}

func (s *Service) getSaveDocOpt(req *pb.SaveDocReq) uint32 {
	// 兼容历史数据，保证如果不是xlsx文件格式，是文档导入类型
	if req.Opt == docEntity.DocOptNormal && (req.FileType != docEntity.FileTypeXlsx &&
		req.FileType != docEntity.FileTypeNumbers) {
		return docEntity.DocOptDocImport
	}
	return req.Opt
}

func (s *Service) checkDocXlsxCharSize(ctx context.Context, req *pb.SaveDocReq, app *entity.App, fileSize uint64) (int,
	error) {
	if fileSize > config.App().RobotDefault.MaxFileSize {
		return 0, errs.ErrFileSizeTooBig
	}
	objectInfo, err := s.s3.StatObject(ctx, req.GetCosUrl())
	if err != nil || objectInfo == nil {
		return 0, errs.ErrSystem
	}
	if objectInfo.Size > int64(config.App().RobotDefault.MaxFileSize) {
		return 0, errs.ErrFileSizeTooBig
	}
	// 如果是xlsx，但是是文档导入，不计算大小
	if (strings.ToLower(req.GetFileType()) != docEntity.FileTypeXlsx &&
		strings.ToLower(req.GetFileType()) != docEntity.FileTypeNumbers) ||
		req.Opt == docEntity.DocOptDocImport {
		return 0, nil
	}
	// 以下是问答导入流程
	size, err := s.parseDocXlsxCharSize(ctx, req.GetFileName(), req.GetCosUrl(), req.GetFileType())
	if err != nil {
		return 0, err
	}
	if err = s.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{
		App:                  app,
		NewCharSize:          uint64(size),
		NewKnowledgeCapacity: fileSize,
		NewStorageCapacity:   gox.IfElse(req.GetSource() == docEntity.SourceFromCorpCOSDoc, 0, fileSize),
		NewComputeCapacity:   fileSize,
	}); err != nil {
		return size, err
	}
	return size, nil
}

func (s *Service) parseDocXlsxCharSize(ctx context.Context, fileName, cosURL, fileType string) (int, error) {
	done := make(chan any)
	var size int
	var err error
	go func() {
		defer gox.Recover()
		charSize, charErr := s.docLogic.ParseDocXlsxCharSize(ctx, fileName, cosURL, fileType)
		size = charSize
		err = charErr
		close(done)
	}()
	select {
	case <-ctx.Done():
		return size, errs.ErrParseFileTimeout
	case <-done:
		if err != nil {
			return size, errs.ErrSystem
		}
	}
	return size, nil
}

// DeleteDoc 删除文档
func (s *Service) DeleteDoc(ctx context.Context, req *pb.DeleteDocReq) (*pb.DeleteDocRsp, error) {
	logx.I(ctx, "DeleteDoc Req:%+v", req)
	rsp := new(pb.DeleteDocRsp)
	if len(req.GetDocBizIds()) == 0 && len(req.GetIds()) == 0 {
		return rsp, errs.ErrWrapf(errs.ErrParameterInvalid, i18n.Translate(ctx, i18nkey.KeyDocumentIDCountZero))
	}
	limit := config.GetMainConfig().BatchInterfaceLimit.DeleteDocMaxLimit
	if limit > 0 && (len(req.GetIds()) > limit || len(req.GetDocBizIds()) > limit) {
		return rsp, errs.ErrWrapf(errs.ErrParameterInvalid, i18n.Translate(ctx, i18nkey.KeyDocumentIDCountExceedLimit), limit)
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	ids := slicex.Unique(req.GetIds())
	bizIds := slicex.Unique(req.GetDocBizIds())
	var docs map[uint64]*docEntity.Doc
	if len(ids) > 0 {
		reqIDs, err := util.CheckReqSliceUint64(ctx, ids)
		if err != nil {
			return nil, err
		}
		docs, err = s.docLogic.GetDocByIDs(ctx, reqIDs, app.PrimaryId)
		if err != nil {
			return rsp, errs.ErrDocNotFound
		}
	} else {
		bizIDReq, err := util.CheckReqSliceUint64(ctx, bizIds)
		if err != nil {
			return nil, err
		}
		docs, err = s.docLogic.GetDocByBizIDs(ctx, bizIDReq, app.PrimaryId)
		if err != nil {
			return rsp, errs.ErrDocNotFound
		}
	}
	// 先加锁, 防止并发删除同一个文档
	// 兼容ids参数和docBizIds参数两种场景
	docBizIds := make([]uint64, 0)
	for _, doc := range docs {
		docBizIds = append(docBizIds, doc.BusinessID)
	}
	err = logicCommon.LockByBizIds(ctx, s.dao, dao.LockForModifyOrDeleteDoc, 2*time.Second, docBizIds)
	defer logicCommon.UnlockByBizIds(trpc.CloneContext(ctx), s.dao, dao.LockForModifyOrDeleteDoc, docBizIds)
	if err != nil {
		return rsp, errs.ErrDocIsModifyingOrDeleting
	}
	logx.I(ctx, "DeleteDoc getAppByAppBizID ok, app:%+v", app)
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	staffID, corpID := contextx.Metadata(ctx).StaffID(), contextx.Metadata(ctx).CorpID()
	logx.I(ctx, "DeleteDoc staffID:%v, corpID:%v", staffID, corpID)
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpID)
	// corp, err := s.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		logx.E(ctx, "GetCorpByID err: %+v", err)
		return rsp, err
	}
	notDeletedDocs := make([]*docEntity.Doc, 0, len(docs))
	notDeletedDocBizIDs := make([]uint64, 0)
	docIds := make([]uint64, 0, len(docs))
	for _, doc := range docs {
		docIds = append(docIds, doc.ID)
	}
	// 检查文档是否在发布中
	releasingDocIdMap, err := s.docLogic.GetReleasingDocId(ctx, app.PrimaryId, docIds)
	if err != nil {
		logx.E(ctx, "获取发布中的文档失败 err:%+v", err)
		return rsp, errs.ErrSystem
	}
	for _, doc := range docs {
		if doc.CorpID != corpID || doc.RobotID != app.PrimaryId {
			return rsp, errs.ErrPermissionDenied
		}
		if doc.HasDeleted() {
			continue
		}
		if _, ok := releasingDocIdMap[doc.ID]; ok {
			return rsp, errs.ErrDocIsRelease
		}
		if !doc.IsAllowDelete() {
			return rsp, errs.ErrDocForbidDelete
		}
		if doc.IsProcessing([]uint64{docEntity.DocProcessingFlagHandlingDocDiffTask}) {
			return rsp, errs.ErrDocDiffTaskRunIng
		}
		notDeletedDocs = append(notDeletedDocs, doc)
		notDeletedDocBizIDs = append(notDeletedDocBizIDs, doc.BusinessID)
	}
	if len(notDeletedDocs) == 0 {
		return rsp, nil
	}
	if err = s.docLogic.DeleteDocs(ctx, staffID, app.PrimaryId, app.BizId, notDeletedDocs); err != nil {
		return nil, errs.ErrSystem
	}
	err = s.taskLogic.InvalidDocDiffTask(ctx, corp.GetCorpId(), app.BizId, notDeletedDocBizIDs)
	if err != nil {
		// 更新对比任务失败不影响文档的删除流程
		logx.W(ctx, "DeleteDoc|InvalidDocDiffTask|err:%+v", err)
	}
	for _, doc := range notDeletedDocs {
		auditx.Delete(auditx.BizDocument).App(app.BizId).Space(app.SpaceId).Log(ctx, doc.BusinessID, doc.FileName)
	}
	return rsp, nil
}

// CheckDocReferWorkFlow 检查文档引用的工作流
func (s *Service) CheckDocReferWorkFlow(ctx context.Context, req *pb.CheckDocReferWorkFlowReq) (
	*pb.CheckDocReferWorkFlowRsp, error) {
	logx.I(ctx, "CheckDocReferWorkFlow Req:%+v", req)
	rsp := new(pb.CheckDocReferWorkFlowRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	if app == nil {
		return rsp, errs.ErrRobotNotFound
	}
	docBizIds, err := util.CheckReqSliceUint64(ctx, req.GetDocBizIds())
	if err != nil {
		return nil, err
	}
	logx.I(ctx, "CheckDocReferWorkFlow|docBizIds:%+v", docBizIds)
	workFlowList, err := s.docLogic.GetWorkflowListByDoc(ctx, req)
	if err != nil {
		return rsp, err
	}
	logx.D(ctx, "CheckDocReferWorkFlow|workFlowList:%+v", workFlowList)
	rsp.List = workFlowList
	return rsp, nil
}

// CheckWebDocIsMulti 检查网页文档是否多层级
func (s *Service) CheckWebDocIsMulti(ctx context.Context, req *pb.CheckWebDocIsMultiReq) (
	*pb.CheckWebDocIsMultiRsp, error) {
	logx.I(ctx, "CheckWebDocIsMulti Req:%+v", req)
	rsp := new(pb.CheckWebDocIsMultiRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetAppBizId())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	if app == nil {
		return rsp, errs.ErrRobotNotFound
	}
	docBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		return nil, err
	}
	if docBizId == 0 {
		return nil, errs.ErrParamsNotExpected
	}

	requestID := trace.SpanContextFromContext(ctx).TraceID().String()
	isMulti, err := s.docLogic.GetWebDocIsMult(ctx, requestID, req.GetAppBizId(), req.GetDocBizId())
	if err != nil {
		logx.W(ctx, "获取网页文档元数据失败 err:%+v", err)
		return rsp, nil
	}

	logx.D(ctx, "GetWebDocIsMult|isMulti:%+v", isMulti)
	rsp.IsMulti = isMulti
	return rsp, nil
}

func (s *Service) getPendingDoc(ctx context.Context, robotID uint64) (map[uint64]struct{}, error) {
	corpID := contextx.Metadata(ctx).CorpID()
	latestRelease, err := s.releaseLogic.GetLatestRelease(ctx, corpID, robotID)
	if err != nil {
		return nil, err
	}
	if latestRelease == nil {
		return nil, nil
	}
	if latestRelease.IsPublishDone() {
		return nil, nil
	}
	releaseDocs, err := s.releaseLogic.GetReleaseDoc(ctx, latestRelease)
	if err != nil {
		return nil, err
	}
	return releaseDocs, nil
}

// ReferDoc 是否引用文档链接
func (s *Service) ReferDoc(ctx context.Context, req *pb.ReferDocReq) (*pb.ReferDocRsp, error) {
	rsp := new(pb.ReferDocRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, convx.Uint64ToString(req.GetBotBizId()))
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	var doc *docEntity.Doc
	if req.DocId > 0 {
		doc, err = s.docLogic.GetDocByID(ctx, req.GetDocId(), app.PrimaryId)
	} else {
		doc, err = s.docLogic.GetDocByBizID(ctx, req.GetDocBizId(), app.PrimaryId)
	}
	if err != nil || doc == nil {
		return rsp, errs.ErrDocNotFound
	}
	if doc.CorpID != app.CorpPrimaryId || doc.RobotID != app.PrimaryId {
		return rsp, errs.ErrPermissionDenied
	}
	if doc.IsDeleted {
		return rsp, errs.ErrDocHasDeleted
	}
	if !doc.IsAllowRefer() {
		return rsp, errs.ErrForbidRefer
	}
	referAfter := &docEntity.Doc{
		ID:         doc.ID,
		BusinessID: doc.BusinessID,
		RobotID:    app.PrimaryId,
		CorpID:     doc.CorpID,
		StaffID:    doc.StaffID,
		IsRefer:    req.GetIsRefer(),
	}
	err = s.docLogic.ReferDoc(ctx, referAfter)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	return rsp, nil
}

// GenerateQA 开始/重新生成QA
func (s *Service) GenerateQA(ctx context.Context, req *pb.GenerateQAReq) (*pb.GenerateQARsp, error) {
	logx.I(ctx, "GenerateQA Req:%+v", req)
	rsp := new(pb.GenerateQARsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	if len(req.GetDocBizIds()) > config.App().DocQA.GenerateQALimit {
		return rsp, errs.ErrGenerateQALimitFail
	}
	docBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetDocBizIds())
	if err != nil {
		return nil, err
	}
	docs, err := s.docLogic.GetDocByBizIDs(ctx, docBizIDs, app.PrimaryId)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	if len(docs) == 0 {
		return rsp, errs.ErrDocNotFound
	}
	docIds := make([]uint64, 0, len(docs))
	for _, doc := range docs {
		docIds = append(docIds, doc.ID)
	}
	// 检查文档是否在发布中
	releasingDocIdMap, err := s.docLogic.GetReleasingDocId(ctx, app.PrimaryId, docIds)
	if err != nil {
		logx.E(ctx, "获取发布中的文档失败 err:%+v", err)
		return rsp, errs.ErrSystem
	}
	for _, doc := range docs {
		if doc.CorpID != app.CorpPrimaryId || doc.RobotID != app.PrimaryId {
			return rsp, errs.ErrPermissionDenied
		}
		if doc.HasDeleted() {
			return rsp, errs.ErrDocHasDeleted
		}
		if !doc.IsAllowCreateQA() {
			return rsp, errs.ErrDocStatusNotAllowReCreateQA
		}
		if !doc.IsDocTypeCreateQA() {
			return rsp, errs.ErrCreateQADocType
		}
		if doc.CharSize > config.App().RobotDefault.DocToQAMaxCharSize {
			return rsp, errs.ErrDocCharSizeNotAllowCreateQA
		}
		if _, ok := releasingDocIdMap[doc.ID]; ok {
			return rsp, errs.ErrDocIsRelease
		}
		if doc.IsProcessing([]uint64{docEntity.DocProcessingFlagHandlingDocDiffTask}) {
			return rsp, errs.ErrDocDiffTaskRunIng
		}
		generating, err := s.taskLogic.GetDocQATaskGenerating(ctx, app.CorpPrimaryId, app.PrimaryId, doc.ID)
		if err != nil {
			logx.E(ctx, "GenerateQA|GetDocQATaskGenerating|查询文档是否有进行中任务失败 err:%+v", err)
			return rsp, err
		}
		if generating {
			logx.I(ctx, "GenerateQA|GetDocQATaskGenerating|文档已有正在进行中任务|%v|doc|%v",
				generating, doc)
			return rsp, errs.ErrGeneratingFail
		}
	}
	qaTask := &qaEntity.DocQATask{
		CorpID:  app.CorpPrimaryId,
		RobotID: app.PrimaryId,
	}
	staffID := contextx.Metadata(ctx).StaffID()
	if err = s.taskLogic.GenerateQA(ctx, staffID, mapx.Values(docs), qaTask); err != nil {
		return rsp, errs.ErrSystem
	}
	return rsp, nil
}

// ListSelectDoc 获取文档下拉列表
func (s *Service) ListSelectDoc(ctx context.Context, req *pb.ListSelectDocReq) (*pb.ListSelectDocRsp, error) {
	logx.I(ctx, "ListSelectDoc Req:%+v", req)
	rsp := new(pb.ListSelectDocRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, app.CorpPrimaryId)
	// corp, err := s.dao.GetCorpByID(ctx, app.CorpPrimaryId)
	if err != nil {
		return rsp, errs.ErrCorpNotFound
	}
	if config.IsVipApp(corp.GetUin(), app.BizId) {
		// TODO: 该接口后续需要优化，支持百万级文档的超大应用
		return rsp, nil
	}
	validityStatus, status, err := s.getDocExpireStatus(req.GetStatus(), app.IsShared)
	if err != nil {
		return nil, err
	}
	fileTypes := []string{docEntity.FileTypeDocx, docEntity.FileTypeMD, docEntity.FileTypeTxt, docEntity.FileTypePdf,
		docEntity.FileTypePptx, docEntity.FileTypePpt, docEntity.FileTypeDoc, docEntity.FileTypePng,
		docEntity.FileTypeJpg, docEntity.FileTypeNumbers, docEntity.FileTypePages, docEntity.FileTypeKeyNote,
		docEntity.FileTypeJpeg, docEntity.FileTypeWps, docEntity.FileTypePPsx, docEntity.FileTypeTiff,
		docEntity.FileTypeBmp,
		docEntity.FileTypeGif, docEntity.FileTypeHtml, docEntity.FileTypeMhtml}

	offset, limit := utilx.Page(req.GetPageNumber(), req.GetPageSize(), 200)
	docFilter := &docEntity.DocFilter{
		CorpId:  app.CorpPrimaryId,
		RobotId: app.PrimaryId,
		// FileNameOrAuditName:             listReq.FileName,
		// QueryType:                       listReq.QueryType,
		FileNameSubStrOrAuditNameSubStr: req.GetFileName(),
		FileTypes:                       fileTypes,
		ValidityStatus:                  validityStatus,
		Status:                          status,
		Offset:                          offset,
		Limit:                           limit,
	}

	total, err := s.docLogic.GetDocCount(ctx, docEntity.DocTblColList, docFilter)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	if total == 0 {
		return rsp, nil
	}
	docFilter.Offset, docFilter.Limit = utilx.Page(1, total)
	list, err := s.docLogic.GetDocList(ctx, docEntity.DocTblColList, docFilter)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	rsp.List = make([]*pb.ListSelectDocRsp_Option, 0, len(list))
	for _, item := range list {
		rsp.List = append(rsp.List, &pb.ListSelectDocRsp_Option{
			Text:     item.GetFileNameByStatus(),
			Value:    fmt.Sprintf("%d", item.BusinessID),
			CharSize: item.CharSize,
			FileType: item.FileType,
		})
	}
	return rsp, nil
}

// FetchURLContent 抓取网页内容
func (s *Service) FetchURLContent(ctx context.Context, req *pb.FetchURLContentReq) (*pb.FetchURLContentRsp, error) {
	botBizID := convx.Uint64ToString(req.GetBotBizId())
	fetchURL := strings.TrimSpace(req.GetUrl())
	logx.D(ctx, "botBizID:%d, fetchURL(%s)", botBizID, fetchURL)
	rsp := new(pb.FetchURLContentRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}

	// 调用底座解析服务
	if config.GetMainConfig().FetchURLUseWebParser {
		requestID := contextx.TraceID(ctx)
		title, content, err := s.docLogic.FetURLContent(ctx, requestID, app.BizId, fetchURL)
		if err != nil {
			return rsp, err
		}
		rsp.Title = title
		rsp.Content = content
		return rsp, nil
	}

	// 使用安全http请求
	parsedURL, err := url.Parse(fetchURL)
	if err != nil {
		logx.E(ctx, "校验url失败:url(%s) , err(%v)", fetchURL, err)
		return rsp, errs.ErrInvalidURL
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		logx.E(ctx, "Invalid Scheme: url(%s) , err(%v)", fetchURL, err)
		return rsp, errs.ErrInvalidURL
	}
	safeClient := secapi.NewSafeClient()
	httpReq, err := http.NewRequest("GET", fetchURL, nil)
	if err != nil {
		logx.E(ctx, "http.NewRequest fail: url(%s), err(%v)", fetchURL, err)
		return rsp, errs.ErrInvalidURL
	}
	// 基于安全请求的客户端，发起安全请求
	httpRsp, err := safeClient.Do(httpReq)
	if err != nil {
		logx.E(ctx, "safeClient.Do fail: url(%s), err(%v)", fetchURL, err)
		return rsp, errs.ErrFetchURLFail
	}
	if httpRsp.StatusCode != 200 {
		logx.E(ctx, "抓取内容失败:url:%s statusCode:%d", fetchURL, httpRsp.StatusCode)
		return rsp, errs.ErrFetchURLFail
	}
	by, err := io.ReadAll(httpRsp.Body)
	if err != nil {
		logx.E(ctx, "io.ReadAll fail:url(%s)  err(%v)", fetchURL, err)
		return rsp, errs.ErrInvalidURL
	}
	html := string(by)
	if html == "" {
		logx.E(ctx, "抓取内容为空 url(%v)", fetchURL)
		return rsp, errs.ErrFetchURLFail
	}
	if len(html) > 2*1024*1024 {
		logx.E(ctx, "抓取内容过长 url(%v)", fetchURL)
		return rsp, errs.ErrFetchURLTooBig
	}
	rsp.Title, err = getTitle(ctx, html)
	if err != nil {
		return rsp, err
	}
	rsp.Content, err = readability(ctx, parsedURL, html)
	if err != nil {
		return rsp, err
	}
	return rsp, nil
}

func readability(ctx context.Context, pageURL *url.URL, html string) (content string, err error) {
	p := rd.NewParser()
	doc, err := p.Parse(strings.NewReader(html), pageURL)
	if err != nil {
		logx.E(ctx, "readability p.Parse err %v", err)
		return "", err
	}

	converter := md.NewConverter("", true, nil)
	md, err := converter.ConvertString(doc.Content)
	if err != nil {
		logx.E(ctx, "readability converter.ConvertString err %v", err)
		return "", err
	}

	return md, nil
}

func getTitle(ctx context.Context, html string) (string, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		logx.E(ctx, "getTitle err %v", err)
		return "", err
	}
	var title string
	for _, s := range []string{"h1", "h2", "h3"} {
		doc.Find(s).EachWithBreak(func(i int, s *goquery.Selection) bool {
			text := strings.TrimSpace(s.Text())
			if len(text) > 0 {
				title = text
				return false
			}
			return true
		})
		if len(title) > 0 {
			break
		}
	}

	if len(title) == 0 {
		p := rd.NewParser()
		doc, err := p.Parse(strings.NewReader(html), nil)
		if err == nil {
			title = strings.TrimSpace(doc.Title)
		}
	}
	if len(title) == 0 {
		title = strings.TrimSpace(doc.Find("title").First().Text())
	}
	if len(title) == 0 {
		title = "未命名网页"
	}
	return stringx.ToUTF8(title), nil
}

// DescribeDoc 获取文档详情
func (s *Service) DescribeDoc(ctx context.Context, req *pb.DescribeDocReq) (*pb.DescribeDocRsp, error) {
	logx.I(ctx, "DescribeDoc Req:%+v", req)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		return nil, err
	}
	doc, err := s.docLogic.GetDocByBizIDAndAppID(ctx, app.CorpPrimaryId, app.PrimaryId, docBizID, docEntity.DocTblColList)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errs.ErrDocNotFound
		}
		return nil, errs.ErrSystem
	}
	qaNums, err := s.qaLogic.GetDocQANum(ctx, app.CorpPrimaryId, app.PrimaryId, []uint64{doc.ID})
	if err != nil {
		return nil, errs.ErrSystem
	}
	// 检查文档是否在发布中
	releasingDocIdMap, err := s.docLogic.GetReleasingDocId(ctx, app.PrimaryId, []uint64{doc.ID})
	if err != nil {
		logx.E(ctx, "获取发布中的文档失败 err:%+v", err)
		return nil, errs.ErrSystem
	}
	latestRelease, err := s.releaseLogic.GetLatestRelease(ctx, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		return nil, errs.ErrSystem
	}
	mapDocID2AttrLabels, err := s.labelLogic.GetDocAttributeLabelDetail(ctx, app.PrimaryId, []uint64{doc.ID})
	if err != nil {
		return nil, errs.ErrSystem
	}
	_, isReleasing := releasingDocIdMap[doc.ID]
	cateList, err := s.cateLogic.DescribeCateList(ctx, cateEntity.DocCate, doc.CorpID, doc.RobotID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	tree := category.BuildCateTree(cateList)
	cateNamePath, cateBizIdPath := tree.Path(ctx, uint64(doc.CategoryID))
	logx.D(ctx, "get cate path--docBizId:%d, cateNamePath:%+v, cateBizIdPath:%+v", doc.BusinessID, cateNamePath, cateBizIdPath)
	cateBizId := uint64(0)
	if len(cateBizIdPath) > 0 {
		cateBizId = cateBizIdPath[len(cateBizIdPath)-1]
	} else {
		return nil, errs.ErrCateNotFound
	}
	cateNamePath = append([]string{i18n.Translate(ctx, category.AllCateName)}, cateNamePath...)
	cateBizIdPath = append([]uint64{category.AllCateID}, cateBizIdPath...)
	updatePeriodH := doc.UpdatePeriodH
	if doc.Source == docEntity.SourceFromWeb {
		requestID := trace.SpanContextFromContext(ctx).TraceID().String()
		updatePeriodH, err = s.docLogic.GetDocUpdateFrequency(ctx, requestID, req.GetBotBizId(), req.GetDocBizId())
		if err != nil {
			logx.W(ctx, "获取网页文档更新频率失败 err:%+v", err)
			// return nil, errs.ErrSystem
			// 降级处理,下游接口失败不影响 获取文档详情其他内容
		}
	}

	pbDoc := &pb.DescribeDocRsp{
		DocBizId:            doc.BusinessID,
		FileName:            doc.FileName,
		CosUrl:              doc.CosURL,
		Reason:              doc.Message,
		UpdateTime:          doc.UpdateTime.Unix(),
		Status:              doc.StatusCorrect(),
		StatusDesc:          i18n.Translate(ctx, doc.StatusDesc(latestRelease.IsPublishPause())),
		FileType:            doc.FileType,
		IsRefer:             doc.IsRefer,
		QaNum:               qaNums[doc.ID][qaEntity.QAIsNotDeleted],
		IsDeleted:           doc.HasDeleted(),
		Source:              doc.Source,
		SourceDesc:          doc.DocSourceDesc(),
		IsAllowRestart:      !isReleasing && doc.IsAllowCreateQA(),
		IsDeletedQa:         qaNums[doc.ID][qaEntity.QAIsNotDeleted] == 0 && qaNums[doc.ID][qaEntity.QAIsDeleted] != 0,
		IsCreatingQa:        doc.IsCreatingQaV1(),
		IsAllowDelete:       !isReleasing && doc.IsAllowDelete(),
		IsAllowRefer:        doc.IsAllowRefer(),
		IsCreatedQa:         doc.IsCreatedQA,
		DocCharSize:         doc.CharSize,
		IsAllowEdit:         !isReleasing && doc.IsAllowEdit(),
		AttrRange:           doc.AttrRange,
		AttrLabels:          fillPBAttrLabels(mapDocID2AttrLabels[doc.ID]),
		CateBizId:           cateBizId,
		CustomerKnowledgeId: doc.CustomerKnowledgeId,
		IsDisabled:          false, // 知识库概念统一后该字段已废弃
		IsDownload:          doc.IsDownloadable,
		SplitRule:           doc.SplitRule,
		UpdatePeriodInfo:    &pb.UpdatePeriodInfo{UpdatePeriodH: updatePeriodH},
		CateBizIdPath:       cateBizIdPath,
		CateNamePath:        cateNamePath,
		EnableScope:         pb.RetrievalEnableScope(doc.EnableScope),
	}
	if doc.FileNameInAudit != "" {
		pbDoc.FileName = doc.FileNameInAudit
	}
	for k, v := range docEntity.AttributeFlagMap {
		if doc.HasAttributeFlag(k) {
			pbDoc.AttributeFlags = append(pbDoc.AttributeFlags, v)
		}
	}
	if pbDoc.Status == docEntity.DocStatusWaitRelease || pbDoc.Status == docEntity.DocStatusReleaseSuccess {
		// 所有的待发布和已发布状态都变为导入成功
		pbDoc.StatusDesc = i18n.Translate(ctx, i18nkey.KeyImportComplete)
	}
	return pbDoc, nil
}

// DescribeDocs 批量获取文档详情
func (s *Service) DescribeDocs(ctx context.Context, req *pb.DescribeDocsReq) (*pb.DescribeDocsRsp, error) {
	logx.I(ctx, "DescribeDocs Req:%+v", req)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil || app == nil {
		return nil, errs.ErrRobotNotFound
	}
	docBizIDs, err := util.BatchCheckReqParamsIsUint64(ctx, req.GetDocBizIds())
	if err != nil {
		return nil, err
	}
	if len(docBizIDs) > config.GetMainConfig().BatchInterfaceLimit.GeneralMaxLimit {
		return nil, errs.ErrDescribeDocLimit
	}
	docs, err := s.docLogic.GetDocByBizIDs(ctx, docBizIDs, app.PrimaryId)
	if err != nil || len(docs) == 0 {
		return nil, errs.ErrDocNotFound
	}
	// 校验文档的CorpID是否与应用的CorpID一致
	for _, doc := range docs {
		if app.CorpPrimaryId != doc.CorpID {
			logx.W(ctx, "DescribeDocs|doc not belong to app|appCorpID:%d|docCorpID:%d|docBizID:%d",
				app.CorpPrimaryId, doc.CorpID, doc.BusinessID)
			return nil, errs.ErrPermissionDenied
		}
	}
	docIDs, cateIDs := make([]uint64, 0, len(docs)), make([]uint64, 0, len(docs))
	for _, doc := range docs {
		docIDs = append(docIDs, doc.ID)
		cateIDs = append(cateIDs, uint64(doc.CategoryID))
	}
	qaNums, err := s.qaLogic.GetDocQANum(ctx, app.CorpPrimaryId, app.PrimaryId, docIDs)
	if err != nil {
		return nil, errs.ErrSystem
	}
	// 检查文档是否在发布中
	releasingDocIdMap, err := s.docLogic.GetReleasingDocId(ctx, app.PrimaryId, docIDs)
	if err != nil {
		logx.E(ctx, "获取发布中的文档失败 err:%+v", err)
		return nil, errs.ErrSystem
	}
	latestRelease, err := s.releaseLogic.GetLatestRelease(ctx, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		return nil, errs.ErrSystem
	}
	mapDocID2AttrLabels, err := s.labelLogic.GetDocAttributeLabelDetail(ctx, app.PrimaryId, docIDs)
	if err != nil {
		return nil, errs.ErrSystem
	}
	cateMap, err := s.cateLogic.DescribeCateByIDs(ctx, cateEntity.DocCate, cateIDs)
	if err != nil {
		return nil, errs.ErrCateNotFound
	}

	docDetails := s.getDocDetails(ctx, docs, qaNums, releasingDocIdMap, latestRelease, mapDocID2AttrLabels, cateMap)
	return &pb.DescribeDocsRsp{Docs: docDetails}, nil
}

// getDocDetails 获取文档详情
func (s *Service) getDocDetails(ctx context.Context, docs map[uint64]*docEntity.Doc, qaNums map[uint64]map[uint32]uint32,
	pendingDoc map[uint64]struct{}, latestRelease *releaseEntity.Release,
	mapDocID2AttrLabels map[uint64][]*labelEntity.AttrLabel,
	cateMap map[uint64]*cateEntity.CateInfo) []*pb.DescribeDocsRsp_DocDetail {
	docDetails := make([]*pb.DescribeDocsRsp_DocDetail, 0)
	for _, doc := range docs {
		_, ok := pendingDoc[doc.ID]
		docDetail := &pb.DescribeDocsRsp_DocDetail{
			DocBizId:       doc.BusinessID,
			FileName:       doc.GetRealFileName(),
			CosUrl:         doc.CosURL,
			Reason:         doc.Message,
			UpdateTime:     doc.UpdateTime.Unix(),
			Status:         doc.StatusCorrect(),
			StatusDesc:     doc.StatusDesc(latestRelease.IsPublishPause()),
			FileType:       doc.FileType,
			IsRefer:        doc.IsRefer,
			QaNum:          qaNums[doc.ID][qaEntity.QAIsNotDeleted],
			IsDeleted:      doc.HasDeleted(),
			Source:         doc.Source,
			SourceDesc:     doc.DocSourceDesc(),
			IsAllowRestart: !ok && doc.IsAllowCreateQA(),
			IsDeletedQa:    qaNums[doc.ID][qaEntity.QAIsNotDeleted] == 0 && qaNums[doc.ID][qaEntity.QAIsDeleted] != 0,
			IsCreatingQa:   doc.IsCreatingQaV1(),
			IsAllowDelete:  !ok && doc.IsAllowDelete(),
			IsAllowRefer:   doc.IsAllowRefer(),
			IsCreatedQa:    doc.IsCreatedQA,
			DocCharSize:    doc.CharSize,
			IsAllowEdit:    !ok && doc.IsAllowEdit(),
			AttrRange:      doc.AttrRange,
			AttrLabels:     fillPBAttrLabels(mapDocID2AttrLabels[doc.ID]),
			ReferUrl:       s.referURL(ctx, doc),
		}
		if cate, ok := cateMap[uint64(doc.CategoryID)]; ok {
			if cate != nil {
				docDetail.CateBizId = cate.BusinessID
			}
		}
		docDetails = append(docDetails, docDetail)
	}
	return docDetails
}

// referURL 获取引用链接地址
func (s *Service) referURL(ctx context.Context, d *docEntity.Doc) string {
	if d == nil {
		return ""
	}

	// 未开启【展示参考来源】
	if !d.IsReferOpen() {
		return ""
	}

	// 用户自定义 URL
	if d.ReferURLType == docEntity.ReferURLTypeUserDefined {
		return d.WebURL
	}

	// 网页导入的原始链接
	if d.Source == docEntity.SourceFromWeb && d.ReferURLType == docEntity.ReferURLTypeWebDocURL {
		return d.OriginalURL
	}

	// 文档预览链接（ADP COS 链接）
	signURL, err := s.s3.GetPreSignedURLWithTypeKey(ctx, entity.OfflineStorageTypeKey, d.CosURL, 0)
	if err != nil {
		logx.E(ctx, "ReferURL GetPreSignedURLWithTypeKey doc:%+v err:%v", d, err)
		return ""
	}
	return signURL
}

// ModifyDoc 修改文档
func (s *Service) ModifyDoc(ctx context.Context, req *pb.ModifyDocReq) (*pb.ModifyDocRsp, error) {
	logx.I(ctx, "ModifyDoc Req:%+v", req)
	staffID := contextx.Metadata(ctx).StaffID()
	// 先加锁，防止并发修改
	docBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		return nil, err
	}
	err = logicCommon.LockByBizIds(ctx, s.dao, dao.LockForModifyOrDeleteDoc, 2*time.Second, []uint64{docBizId})
	defer logicCommon.UnlockByBizIds(ctx, s.dao, dao.LockForModifyOrDeleteDoc, []uint64{docBizId})
	if err != nil {
		return nil, errs.ErrDocIsModifyingOrDeleting
	}
	// todo neckyang 校验拆分规则
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
	}
	doc, err := s.docLogic.GetDocByBizID(ctx, docBizId, app.PrimaryId)
	if err != nil || doc == nil {
		return nil, errs.ErrDocNotFound
	}
	if err = s.isDocAllowedToModify(ctx, *doc, *app, app.CorpPrimaryId); err != nil {
		return nil, err
	}
	// if slices.Contains(req.GetModifyTypes(), pb.ModifyDocReq_UPDATE_TX_DOC_REFRESH) {
	//	if doc.Source != model.SourceFromTxDoc {
	//		return nil, pkg.ErrParams
	//	}
	//	err := logicDoc.RefreshTxDoc(ctx, false, []*model.Doc{doc}, s.dao)
	//	if err != nil {
	//		logx.E(ctx, "RefreshTxDoc failed, err:%v", err)
	//		return nil, pkg.ErrSystem
	//	}
	//	return &pb.ModifyDocRsp{}, nil
	// }

	if len(req.GetModifyTypes()) > 0 {
		if err = s.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{App: app}); err != nil {
			return nil, logicCommon.ConvertErrMsg(ctx, s.rpc, 0, app.CorpPrimaryId, err)
		}
		err = s.docLogic.ModifyItemsAction(ctx, app, doc, req)
		if err != nil {
			logx.E(ctx, "ModifyItemsAction failed, err:%v", err)
			return nil, err
		}
		auditx.Modify(auditx.BizDocument).App(app.BizId).Space(app.SpaceId).Log(ctx, doc.BusinessID, doc.GetDocFileName())
		return &pb.ModifyDocRsp{}, nil
	}

	if len(req.GetAttrLabels()) > 0 {
		req.AttrRange = docEntity.AttrRangeCondition
	} else {
		req.AttrRange = docEntity.AttrRangeAll
	}
	attrs, labels, err := s.checkAttributeLabelRefer(ctx, app.PrimaryId, config.App().AttributeLabel.DocAttrLimit,
		config.App().AttributeLabel.DocAttrLabelLimit, req.GetAttrRange(), req.GetAttrLabels())
	if err != nil {
		return nil, err
	}
	isDocAttributeLabelChange, err := s.labelLogic.IsDocAttributeLabelChange(ctx, app.PrimaryId, doc.ID, doc.AttrRange,
		req.GetAttrRange(), req.GetAttrLabels())
	if err != nil {
		return nil, errs.ErrSystem
	}
	isNeedPublish := false
	if isDocAttributeLabelChange || doc.Status == docEntity.DocStatusUpdateFail {
		isNeedPublish = true
	}
	expireStart, expireEnd, err := util.CheckReqStartEndTime(ctx, req.GetExpireStart(), req.GetExpireEnd())
	if err != nil {
		return nil, err
	}
	// 如果有效期发生变更，需要更新记录
	if doc.ExpireStart.Unix() != int64(expireStart) || doc.ExpireEnd.Unix() != int64(expireEnd) {
		isNeedPublish = true
	}
	if isNeedPublish {
		doc.Status = docEntity.DocStatusUpdating
	}
	if isNeedPublish && !doc.IsNextActionAdd() {
		doc.NextAction = docEntity.DocNextActionUpdate
	}
	doc.IsRefer = req.GetIsRefer()
	doc.AttrRange = req.GetAttrRange()
	doc.ReferURLType = req.GetReferUrlType()
	doc.WebURL = req.GetWebUrl()
	doc.ExpireStart = time.Unix(int64(expireStart), 0)
	doc.ExpireEnd = time.Unix(int64(expireEnd), 0)
	doc.CustomerKnowledgeId = req.GetCustomerKnowledgeId()
	doc.StaffID = staffID
	// 文档属性标记需要先置0再重新赋值
	doc.AttributeFlag = 0
	for _, attrFlag := range req.GetAttributeFlags() {
		doc.AddAttributeFlag([]uint64{uint64(math.Pow(2, float64(attrFlag)))})
	}
	doc.IsDownloadable = false
	if req.GetIsRefer() && req.GetReferUrlType() == docEntity.ReferURLTypePreview {
		doc.IsDownloadable = req.GetIsDownload()
	}
	if req.GetSplitRule() != "" {
		doc.SplitRule = req.GetSplitRule()
	}
	docAttributeLabelsFromPB, err := fillDocAttributeLabelsFromPB(ctx, req.GetAttrLabels(), true, attrs, labels)
	if err != nil {
		return nil, err
	}

	var cateID uint64
	if req.GetCateBizId() != "" && req.GetCateBizId() != "0" {
		catBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
		if err != nil {
			return nil, err
		}
		cateID, err = s.cateLogic.VerifyCateBiz(ctx, cateEntity.DocCate, app.CorpPrimaryId, catBizID, app.PrimaryId)
	} else {
		cateID, err = s.cateLogic.DescribeRobotUncategorizedCateID(ctx, cateEntity.DocCate, app.CorpPrimaryId, app.PrimaryId)
	}
	if err != nil {
		return nil, err
	}
	if doc.CategoryID != uint32(cateID) {
		isNeedPublish = true
	}
	doc.CategoryID = uint32(cateID)

	enableScope := req.GetEnableScope()

	if enableScope != pb.RetrievalEnableScope_ENABLE_SCOPE_TYPE_UNKNOWN && uint32(enableScope) != doc.EnableScope {
		logx.I(ctx, "enableScope in req is not equals to origin doc , enable_scope changed. (%d -> %d)",
			enableScope, doc.EnableScope)
		doc.EnableScope = uint32(enableScope)
		isNeedPublish = true
	}

	if err = s.docLogic.UpdateDoc(ctx, staffID, doc, isNeedPublish, docAttributeLabelsFromPB); err != nil {
		return nil, errs.ErrSystem
	}
	auditx.Modify(auditx.BizDocument).App(app.BizId).Space(app.SpaceId).Log(ctx, doc.BusinessID, doc.GetDocFileName())
	return &pb.ModifyDocRsp{}, nil
}

// BatchModifyDoc 批量修改文档应用链接，过期时间
func (s *Service) BatchModifyDoc(ctx context.Context, req *pb.BatchModifyDocReq) (*pb.BatchModifyDocRsp, error) {
	logx.I(ctx, "BatchModifyDoc Req:%+v", req)
	rsp := new(pb.BatchModifyDocRsp)
	staffID := contextx.Metadata(ctx).StaffID()
	// 先加锁，防止并发修改
	docBizIds, err := util.CheckReqSliceUint64(ctx, req.GetDocBizIds())
	if err != nil {
		return nil, err
	}
	docBizIds = slicex.Unique(docBizIds)
	err = logicCommon.LockByBizIds(ctx, s.dao, dao.LockForModifyOrDeleteDoc, 2*time.Second, docBizIds)
	defer logicCommon.UnlockByBizIds(ctx, s.dao, dao.LockForModifyOrDeleteDoc, docBizIds)
	if err != nil {
		return rsp, errs.ErrDocIsModifyingOrDeleting
	}

	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err != nil {
		return nil, errs.ErrSystem
	}
	if err = app.IsWriteable(); err != nil {
		logx.E(ctx, "BatchModifyDoc app.IsWriteable, err:%v", err)
		return nil, err
	}
	docs, err := s.docLogic.GetDocByBizIDs(ctx, docBizIds, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "BatchModifyDoc GetDocByBusinessIDs, err:%v", err)
		return nil, errs.ErrSystem
	}
	if len(docs) == 0 {
		return nil, errs.ErrDocNotFound
	}
	docIds := make([]uint64, 0, len(docs))
	for _, doc := range docs {
		docIds = append(docIds, doc.ID)
	}
	// 检查文档是否在发布中
	releasingDocIdMap, err := s.docLogic.GetReleasingDocId(ctx, app.PrimaryId, docIds)
	if err != nil {
		logx.E(ctx, "获取发布中的文档失败 err:%+v", err)
		return nil, errs.ErrSystem
	}

	// 检查参数是否正确
	// 1.检查批量到期时间
	expireEnd, err := util.CheckReqParamsIsUint64(ctx, req.GetExpireEnd())
	if err != nil {
		return nil, err
	}
	// https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800123709548 支持按分钟级别判断
	if expireEnd > 0 && time.Unix(int64(expireEnd), 0).Before(time.Now()) {
		return nil, errs.ErrInvalidExpireTime
	}
	// 打开关闭应用链接不需要发布；修改适用范围；自定义到期时间需要发布
	isNeedUpdateMap := make(map[uint64]int)
	var changedDoc []*docEntity.Doc
	for _, doc := range docs {
		if doc.Status == docEntity.DocStatusCharExceeded {
			return rsp, errs.ErrDocNotAllowEdit
		}
		if doc.CorpID != app.CorpPrimaryId || doc.RobotID != app.PrimaryId {
			logx.I(ctx, "BatchModifyDoc doc permission Denied! docInfo:%+v,corpID:%+v,robotID:%+v", doc,
				app.CorpPrimaryId, app.PrimaryId)
			return rsp, errs.ErrPermissionDenied
		}

		if doc.HasDeleted() {
			return rsp, errs.ErrDocHasDeleted
		}
		if _, ok := releasingDocIdMap[doc.ID]; ok {
			return rsp, errs.ErrDocIsRelease
		}

		if req.GetActionType() == docEntity.BatchModifyDefault || req.GetActionType() == docEntity.BatchModifyRefer {
			doc.IsRefer = req.GetIsRefer()
			doc.ReferURLType = req.GetReferUrlType()
			doc.WebURL = req.GetWebUrl()
			doc.IsDownloadable = false
			if req.GetIsRefer() && req.GetReferUrlType() == docEntity.ReferURLTypePreview {
				doc.IsDownloadable = req.GetIsDownload()
			}
		}

		if req.GetActionType() == docEntity.BatchModifyDefault {
			if !doc.IsAllowEdit() {
				return rsp, errs.ErrDocNotAllowEdit
			}
			if req.GetEnableScope() != pb.RetrievalEnableScope_ENABLE_SCOPE_TYPE_UNKNOWN {
				logx.I(ctx, "BatchModifyDoc | Modify EnableScope..")
				if doc.EnableScope != uint32(req.GetEnableScope()) {
					isNeedUpdateMap[doc.ID] = 1
				}
				doc.EnableScope = uint32(req.GetEnableScope())
			} else {
				logx.I(ctx, "BatchModifyDoc | Modify expireEnd..")
				if doc.ExpireEnd.Unix() != int64(expireEnd) {
					isNeedUpdateMap[doc.ID] = 1
					doc.Status = docEntity.DocStatusUpdating
					if !doc.IsNextActionAdd() {
						doc.NextAction = docEntity.DocNextActionUpdate
					}
				}
				doc.ExpireEnd = time.Unix(int64(expireEnd), 0)
			}

		}

		if req.GetActionType() == docEntity.BatchModifyExpiredTime {
			if !doc.IsAllowEdit() {
				return rsp, errs.ErrDocNotAllowEdit
			}
			if doc.ExpireEnd.Unix() != int64(expireEnd) {
				isNeedUpdateMap[doc.ID] = 1
				doc.Status = docEntity.DocStatusUpdating
				if !doc.IsNextActionAdd() {
					doc.NextAction = docEntity.DocNextActionUpdate
				}
			}
			doc.ExpireEnd = time.Unix(int64(expireEnd), 0)
		}

		if req.GetActionType() == docEntity.BatchModifyUpdatePeriod {
			if !doc.IsAllowEdit() {
				return rsp, errs.ErrDocNotAllowEdit
			}
			if doc.Source != docEntity.SourceFromTxDoc {
				// 不是腾讯文档类型，不支持更新时间周期
				continue
			}
			nextUpdateTime := logicDoc.GetDocNextUpdateTime(ctx, req.GetUpdatePeriodInfo().GetUpdatePeriodH())
			doc.UpdatePeriodH = req.GetUpdatePeriodInfo().GetUpdatePeriodH()
			doc.NextUpdateTime = nextUpdateTime
		}

		doc.StaffID = staffID

		if req.GetActionType() == docEntity.BatchModifyUpdateEnableScope {
			if !doc.IsAllowEdit() {
				return rsp, errs.ErrDocNotAllowEdit
			}
			if req.GetEnableScope() != pb.RetrievalEnableScope_ENABLE_SCOPE_TYPE_UNKNOWN {
				if doc.EnableScope != uint32(req.GetEnableScope()) {
					isNeedUpdateMap[doc.ID] = 1
				}
				doc.EnableScope = uint32(req.GetEnableScope())
			}
		}

		changedDoc = append(changedDoc, doc)
	}
	if err = s.docLogic.BatchUpdateDoc(ctx, staffID, changedDoc, isNeedUpdateMap); err != nil {
		logx.E(ctx, "BatchUpdateDoc, err:%v", err)
		return nil, errs.ErrSystem
	}
	for _, doc := range docs {
		auditx.Modify(auditx.BizDocument).App(app.BizId).Space(app.SpaceId).Log(ctx, doc.BusinessID, doc.FileName)
	}
	return rsp, nil

}

// ModifyDocStatus 修改文档状态
func (s *Service) ModifyDocStatus(ctx context.Context, req *pb.ModifyDocStatusReq) (*pb.ModifyDocStatusRsp, error) {
	logx.I(ctx, "ModifyDocStatus Req:%s", req)
	rsp := new(pb.ModifyDocStatusRsp)
	if err := s.checkLogin(ctx); err != nil {
		return nil, err
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		return nil, err
	}
	err = logicCommon.LockByBizIds(ctx, s.dao, dao.LockForModifyOrDeleteDoc, 2*time.Second, []uint64{docBizID})
	defer logicCommon.UnlockByBizIds(ctx, s.dao, dao.LockForModifyOrDeleteDoc, []uint64{docBizID})
	if err != nil {
		return nil, errs.ErrDocIsModifyingOrDeleting
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetAppBizId())
	if err != nil || app == nil {
		return nil, errs.ErrSystem
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
	}

	doc, err := s.docLogic.GetDocByBizID(ctx, docBizID, app.PrimaryId)
	if err != nil || doc == nil {
		return nil, errs.ErrDocNotFound
	}
	if err = s.isDocAllowedToModify(ctx, *doc, *app, app.CorpPrimaryId); err != nil {
		return nil, err
	}
	logx.I(ctx, "ModifyDocStatus doc:%+v", jsonx.MustMarshalToString(doc))
	isNeedPublish := true
	doc.Status = docEntity.DocStatusUpdating
	if isNeedPublish && !doc.IsNextActionAdd() {
		doc.NextAction = docEntity.DocNextActionUpdate
	}
	staffID := contextx.Metadata(ctx).StaffID()
	if err = s.docLogic.UpdateDocDisableState(ctx, staffID, doc, req.GetIsDisabled()); err != nil {
		return nil, errs.ErrSystem
	}
	if req.GetIsDisabled() {
		auditx.Disable(auditx.BizDocument).App(app.BizId).Space(app.SpaceId).Log(ctx, doc.BusinessID, doc.GetDocFileName())
	} else {
		auditx.Enable(auditx.BizDocument).App(app.BizId).Space(app.SpaceId).Log(ctx, doc.BusinessID, doc.GetDocFileName())
	}
	return rsp, nil
}

// GetDocPreview 获取临时链接 不用临时密钥 临时密钥有过期时间
func (s *Service) GetDocPreview(ctx context.Context, req *pb.GetDocPreviewReq) (rsp *pb.GetDocPreviewRsp, err error) {
	logx.I(ctx, "GetDocPreview|req:%+v", req)
	if err := s.checkLogin(ctx); err != nil {
		return nil, err
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		return nil, err
	}
	var app *entity.App
	if req.GetTypeKey() == entity.RealtimeStorageTypeKey {
		app, err = s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
		// 这里不强制校验AppID和CorpID是否强制是归属关系
		// C侧分享链接出去的情况：当前登录的Corp和App可能不是归属关系
		if err != nil && !errors.Is(err, errs.ErrCorpAppNotEqual) {
			return nil, errs.ErrRobotNotFound
		}
		if app == nil {
			return nil, errs.ErrRobotNotFound
		}
		// 指定实时文档
		rsp, err = s.getRealtimeDocPreview(ctx, app, docBizID)
	} else {
		app, err = s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
		if err != nil && !errors.Is(err, errs.ErrCorpAppNotEqual) {
			return nil, errs.ErrRobotNotFound
		}
		if app == nil {
			return nil, errs.ErrRobotNotFound
		}
		// 先获取doc用于权限校验
		doc, docErr := s.docLogic.GetDocByBizID(ctx, docBizID, app.PrimaryId)
		if docErr != nil || doc == nil {
			logx.W(ctx, "GetDocPreview|GetDocByBizID failed|docBizID:%d|err:%v", docBizID, docErr)
			return nil, errs.ErrDocNotFound
		}

		// 文档与应用的corpid必须一致
		if app.CorpPrimaryId != doc.CorpID {
			logx.W(ctx, "GetDocPreview|doc not belong to app|appCorpID:%d|docCorpID:%d", app.CorpPrimaryId, doc.CorpID)
			return nil, errs.ErrPermissionDenied
		}

		// 跨企业访问需要判断参考来源refer字段：ErrCorpAppNotEqual(B端跨企业) 或 corpID=0(C端用户)
		loginCorpID := contextx.Metadata(ctx).CorpID()
		if errors.Is(err, errs.ErrCorpAppNotEqual) || loginCorpID == 0 {
			//  公开的知识库，比如应用模板场景查看示例文档，也需要放通越权校验
			isPublicCorp := slices.Contains(config.GetMainConfig().PublicCorpPrimaryIds, app.CorpPrimaryId)
			if !isPublicCorp && !doc.IsReferOpen() {
				logx.W(ctx, "GetDocPreview|cross-corp access denied|appCorpID:%d", app.CorpPrimaryId)
				return nil, errs.ErrPermissionDenied
			}
		}

		rsp, err = s.getOfflineDocPreview(ctx, app, docBizID, req.GetBotBizId(), doc)
	}
	if err != nil {
		return nil, err
	}
	logx.I(ctx, "GetDocPreview|rsp:%+v", rsp)
	return rsp, nil
}

// BatchDownloadDoc 批量下载文档
func (s *Service) BatchDownloadDoc(ctx context.Context, req *pb.BatchDownloadDocReq) (
	rsp *pb.BatchDownloadDocRsp, err error) {
	logx.I(ctx, "BatchDownloadDoc|req:%+v", req)
	corpID := contextx.Metadata(ctx).CorpID()
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	docBizIDs, err := util.BatchCheckReqParamsIsUint64(ctx, req.GetDocBizIds())
	if err != nil {
		return nil, err
	}
	if len(docBizIDs) > config.App().RobotDefault.BatchDownloadDocMaxLimit {
		return nil, errs.ErrBatchDownloadMaxCount
	}
	// todo: 对 docBizIDs 做越权校验
	// todo: BatchDownloadDoc 只会查询到当前PrimaryId下的docID,看看是否有必要做docBizIDs的越权

	appDB, err := s.rpc.AppAdmin.DescribeAppById(ctx, botBizID)
	if err != nil {
		logx.E(ctx, "prepareTokenDosage GetAppInfo err: %+v", err)
		return nil, err
	}
	if appDB.IsDeleted {
		return nil, errs.ErrAppNotFound
	}
	if appDB.CorpPrimaryId != corpID {
		return rsp, errs.ErrPermissionDenied
	}
	rsp, err = s.docLogic.BatchDownloadDoc(ctx, appDB.PrimaryId, docBizIDs, s.dao)
	if err != nil {
		logx.E(ctx, "BatchDownloadDoc err: %+v", err)
		return nil, err
	}
	for _, docRsp := range rsp.DocList {
		auditx.Download(auditx.BizDocument).App(botBizID).Space(appDB.SpaceId).Log(ctx, docRsp.DocBizId, docRsp.FileName)
	}
	logx.I(ctx, "BatchDownloadDoc|rsp:%+v", rsp)
	return rsp, nil
}

// getRealtimeDocPreview 实时文档预览
func (s *Service) getRealtimeDocPreview(ctx context.Context, app *entity.App, docID uint64) (
	*pb.GetDocPreviewRsp, error) {
	corpID := contextx.Metadata(ctx).CorpID()
	corpBizId, err := s.getCorpByID(ctx, corpID)
	if err != nil || corpBizId == 0 {
		return nil, errs.ErrCorpNotFound
	}
	doc, err := s.docLogic.GetRealtimeDocByID(ctx, docID)
	if err != nil || doc == nil {
		return nil, errs.ErrDocNotFound
	}
	logx.I(ctx, "getRealtimeDocPreview|doc:%+v, corp:%d", doc, corpBizId)
	err = s.s3.CheckURLPrefix(ctx, doc.CorpID, corpBizId, app.BizId, doc.CosUrl)
	if err != nil {
		logx.E(ctx, "getRealtimeDocPreview|CheckURLPrefix failed, err:%+v", err)
		return nil, errs.ErrInvalidURL
	}
	signURL, err := s.s3.GetPreSignedURLWithTypeKey(ctx, entity.RealtimeStorageTypeKey, doc.CosUrl, 0)
	if err != nil {
		return nil, errs.ErrSystem
	}
	return &pb.GetDocPreviewRsp{
		FileName: doc.FileName,
		FileType: doc.FileType,
		CosUrl:   doc.CosUrl,
		Url:      signURL,
		Bucket:   "",
	}, nil
}

// getOfflineDocPreview 离线文档预览
func (s *Service) getOfflineDocPreview(ctx context.Context, app *entity.App, docBizID uint64, botBizID string, doc *docEntity.Doc) (
	*pb.GetDocPreviewRsp, error) {
	if app == nil {
		return nil, errs.ErrRobotNotFound
	}
	if doc == nil {
		return nil, errs.ErrDocNotFound
	}
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, doc.CorpID)
	// corp, err := s.dao.GetCorpByID(ctx, doc.CorpPrimaryId)
	if err != nil || corp == nil {
		return nil, errs.ErrCorpNotFound
	}

	// 因为无法确认doc是默认知识库的还是共享知识库的，所以需要通过doc中的robotid去获取应用or共享知识库的bizid
	req := &appconfig.ListAppBaseInfoReq{
		AppPrimaryIds: []uint64{doc.RobotID},
		PageNumber:    1,
		PageSize:      1,
	}
	apps, _, err := s.rpc.AppAdmin.ListAppBaseInfo(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("GetAppBaseInfo err: %w", err)
	}
	if len(apps) == 0 {
		return nil, errs.ErrAppNotFound
	}
	appBizId := apps[0].BizId

	err = s.s3.CheckURLPrefix(ctx, doc.CorpID, corp.GetCorpId(), appBizId, doc.CosURL)
	if err != nil {
		logx.E(ctx, "getOfflineDocPreview|CheckURLPrefix failed, err:%+v", err)
		return nil, errs.ErrInvalidURL
	}
	signURL, err := s.s3.GetPreSignedURLWithTypeKey(ctx, entity.OfflineStorageTypeKey, doc.CosURL, 0)
	if err != nil {
		return nil, errs.ErrSystem
	}
	parseUrl, err := s.docLogic.GetDocParseResUrl(ctx, doc.ID, apps[0].PrimaryId)
	if err != nil {
		logx.E(ctx, "getOfflineDocPreview|GetDocParseResUrl failed, err:%+v", err)
		return nil, errs.ErrSystem
	}
	return &pb.GetDocPreviewRsp{
		FileName:            doc.FileName,
		FileType:            doc.FileType,
		CosUrl:              doc.CosURL,
		Url:                 signURL,
		Bucket:              "",
		NewName:             doc.GetFileNameByStatus(),
		ParseResultCosUrl:   parseUrl,
		IsDownload:          doc.IsDownloadable,
		Source:              doc.Source,
		CustomerKnowledgeId: doc.CustomerKnowledgeId,
	}, nil
}

func (s *Service) getDocExpireStatus(status []uint32, isShared bool) (uint32, []uint32, error) {
	var validityStatus uint32
	if len(status) == 0 {
		return validityStatus, status, nil
	}
	var newStatus []uint32
	for i := range status {
		switch status[i] { // 预留后续会有未生效、生效中状态
		case docEntity.DocStatusExpired:
			validityStatus = docEntity.DocExpiredStatus
		case docEntity.DocStatusCharExceeded:
			// 超量状态扩展显示出失败稳态转超量的状态
			newStatus = append(newStatus,
				docEntity.DocStatusCharExceeded,
				docEntity.DocStatusParseImportFailCharExceeded,
				docEntity.DocStatusAuditFailCharExceeded,
				docEntity.DocStatusUpdateFailCharExceeded,
				docEntity.DocStatusCreateIndexFailCharExceeded,
				docEntity.DocStatusAppealFailedCharExceeded)
		case docEntity.DocStatusWaitRelease:
			newStatus = append(newStatus, docEntity.DocStatusWaitRelease)
			if isShared {
				// 共享知识库，需要兼容从应用知识库人工转换成共享知识库的情况
				newStatus = append(newStatus, docEntity.DocStatusReleaseSuccess)
			}
		default:
			newStatus = append(newStatus, status[i])
		}
	}
	// 如果选择了状态，但是没有选择已过期，那就是未过期
	if validityStatus != docEntity.DocExpiredStatus && len(newStatus) > 0 {
		validityStatus = docEntity.DocUnExpiredStatus
	}
	return validityStatus, newStatus, nil
}

// checkQueryType 校验查询类型
func (s *Service) checkQueryType(fileType string) error {
	if fileType != docEntity.DocQueryTypeFileName && fileType != docEntity.DocQueryTypeAttribute {
		return errs.ErrParamsNotExpected
	}
	return nil
}

// StopDocParse 终止文档解析
func (s *Service) StopDocParse(ctx context.Context, req *pb.StopDocParseReq) (*pb.StopDocParseRsp, error) {
	logx.I(ctx, "StopDocParse Req:%+v", req)
	rsp := new(pb.StopDocParseRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}

	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		return nil, err
	}
	doc, err := s.docLogic.GetDocByBizID(ctx, docBizID, app.PrimaryId)
	if err != nil {
		return rsp, err
	}
	taskID := ""
	docParse, err := s.docLogic.GetDocParseByDocIDAndTypeAndStatus(ctx, doc.ID, docEntity.DocParseTaskTypeWordCount,
		docEntity.DocParseIng, doc.RobotID)
	if err != nil {
		logx.W(ctx, "GetDocParseByDocIDAndTypeAndStatus failed, err: %+v", err)
		// (兼容干预中的终止)如果文档在干预中且解析任务未找到，则直接更新文档状态
		if errors.Is(err, errs.ErrDocParseTaskNotFound) && doc.IsProcessing([]uint64{
			docEntity.DocProcessingFlagSegmentIntervene}) {
			logx.W(ctx, "GetDocParseByDocIDAndTypeAndStatus failed, err: %+v", err)
			doc.Message = config.App().DocParseStop.Msg
			doc.Status = docEntity.DocStatusParseFail
			err = s.docLogic.UpdateDocStatusAndCharSize(ctx, doc)
			if err != nil {
				return rsp, err
			}
			return rsp, nil
		}
		return rsp, errs.ErrDocParseTaskNotFound
	}

	requestID := contextx.TraceID(ctx)
	taskID = docParse.TaskID
	err = s.docLogic.StopDocParseTask(ctx, taskID, requestID, app.BizId)
	if err != nil {
		return rsp, errs.ErrStopDocParseFail
	}
	doc.Message = config.App().DocParseStop.Msg
	doc.Status = docEntity.DocStatusParseFail
	doc.StaffID = contextx.Metadata(ctx).StaffID()
	err = s.docLogic.UpdateDocStatusAndCharSize(ctx, doc)
	if err != nil {
		return rsp, errs.ErrUpdateDocStatusFail
	}
	docParse.Status = docEntity.DocParseCallBackCancel
	docParse.RequestID = requestID
	docParse.UpdateTime = time.Now()

	updateColumns := []string{docEntity.DocParseTblColStatus, docEntity.DocParseTblColRequestID,
		docEntity.DocParseTblColUpdateTime}

	err = s.docLogic.UpdateDocParseTask(ctx, updateColumns, docParse)
	if err != nil {
		return rsp, errs.ErrUpdateDocParseTaskStatusFail
	}
	return rsp, nil
}

// RetryDocParse 重试文档解析
func (s *Service) RetryDocParse(ctx context.Context, req *pb.RetryDocParseReq) (*pb.RetryDocParseRsp, error) {
	logx.I(ctx, "RetryDocParse Req:%+v", req)
	rsp := new(pb.RetryDocParseRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	docBizIDs, err := util.BatchCheckReqParamsIsUint64(ctx, req.GetDocBizIds())
	if err != nil {
		return nil, err
	}
	limit := config.GetMainConfig().BatchInterfaceLimit.RetryDocParseMaxLimit
	if limit > 0 && (len(docBizIDs) > limit) {
		return rsp, errs.ErrWrapf(errs.ErrParameterInvalid,
			i18n.Translate(ctx, i18nkey.KeyDocumentIDCountExceedLimit), limit)
	}
	// 如果批量字段为空,兼容老接口单个操作字段
	if len(docBizIDs) == 0 {
		docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
		if err != nil {
			return nil, err
		}
		if docBizID == 0 {
			return nil, errs.ErrParams
		}
		docBizIDs = append(docBizIDs, docBizID)
	}
	if err = s.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{App: app}); err != nil {
		return rsp, logicCommon.ConvertErrMsg(ctx, s.rpc, 0, app.CorpPrimaryId, err)
	}
	docs, err := s.docLogic.GetDocByBizIDs(ctx, docBizIDs, app.PrimaryId)
	if err != nil {
		return rsp, err
	}

	parsedDocs := make([]*docEntity.Doc, 0)
	for _, doc := range docs {
		if doc.RobotID != app.PrimaryId {
			return rsp, errs.ErrWrapf(errs.ErrDocNotFound, "当前应用中不存在该文档")
		}
		parsedDocs = append(parsedDocs, doc)
	}

	docParsesFailMap, err := s.docParsesMap(ctx, parsedDocs)
	if err != nil {
		return rsp, errs.ErrDocParseTaskNotFound
	}
	docAuditFailMap, _ := s.docAuditMap(ctx, parsedDocs, app)

	// 获取企业信息
	staffBizID, staffID, corpBizID := contextx.Metadata(ctx).StaffBizID(), contextx.Metadata(ctx).StaffID(), contextx.Metadata(ctx).CorpBizID()
	successCount := 0
	failedDocs := make([]uint64, 0)
	for _, doc := range parsedDocs {
		if !s.isAllowRetry(ctx, doc.ID, doc.Status, docParsesFailMap, docAuditFailMap) {
			logx.W(ctx, "文档当前状态不可重试解析, docBizID:%d, robotID:%d, appID:%d",
				doc.BusinessID, doc.RobotID, app.PrimaryId)
			failedDocs = append(failedDocs, doc.BusinessID)
			continue
		}
		if doc.RobotID != app.PrimaryId {
			logx.W(ctx, "文档不属于当前应用, docBizID:%d, robotID:%d, appID:%d",
				doc.BusinessID, doc.RobotID, app.PrimaryId)
			failedDocs = append(failedDocs, doc.BusinessID)
			continue
			// return rsp, errs.ErrWrapf(errs.ErrDocNotFound, i18n.Translate(ctx, i18nkey.KeyDocumentNotInCurrentApp))
		}

		docParses, err := s.docLogic.DocParseCanBeRetried(ctx, doc.ID, docEntity.DocParseTaskTypeWordCount,
			[]uint32{docEntity.DocParseCallBackFailed, docEntity.DocParseCallBackCancel,
				docEntity.DocParseCallBackCharSizeExceeded},
			doc.RobotID)
		if err != nil {
			logx.E(ctx, "获取可重试文档解析任务失败, docBizID:%d, err:%v", doc.BusinessID, err)
			failedDocs = append(failedDocs, doc.BusinessID)
			continue
			// return rsp, errs.ErrDocParseTaskFailNotFound
		}

		if len(docParses) == 0 {
			// (兼容干预中的重试)如果文档在干预中且解析任务未找到，则重新提交干预异步任务
			if doc.IsProcessing([]uint64{docEntity.DocProcessingFlagSegmentIntervene}) {
				// 获取切片数据
				docCommon := &segEntity.DocSegmentCommon{
					AppID:      app.PrimaryId,
					AppBizID:   app.BizId,
					CorpID:     app.CorpPrimaryId,
					CorpBizID:  corpBizID,
					StaffID:    staffID,
					StaffBizID: staffBizID,
					DocBizID:   doc.BusinessID,
					DocID:      doc.ID,
					DataSource: uint32(logicDoc.GetDataSource(ctx, doc.SplitRule)),
				}
				// 审核
				auditFlag, err := util.GetFileAuditFlag(doc.FileType)
				if err != nil {
					logx.E(ctx, "获取审核标志失败, docBizID:%d, err:%v", doc.BusinessID, err)
					failedDocs = append(failedDocs, doc.BusinessID)
					continue
					// return rsp, err
				}
				_, err = s.docLogic.CreateDocParsingIntervention(ctx, docCommon, auditFlag, doc)
				if err != nil {
					logx.E(ctx, "创建文档解析干预任务失败, docBizID:%d, err:%v", doc.BusinessID, err)
					failedDocs = append(failedDocs, doc.BusinessID)
					continue
					// logx.E(ctx, "RetryDocParse|RetryDocParse|err:%+v", err)
					// return nil, errs.ErrRetryDocParseTaskFail
				}
				successCount++
				continue
				// return rsp, nil
			}
			logx.E(ctx, "未找到可重试的文档解析任务, docBizID:%d", doc.BusinessID)
			failedDocs = append(failedDocs, doc.BusinessID)
			continue
			// return rsp, errs.ErrDocParseTaskFailNotFound
		}
		docParse := docParses[0]
		if err = s.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{App: app}); err != nil {
			return rsp, logicCommon.ConvertErrMsg(ctx, s.rpc, 0, app.CorpPrimaryId, err)
		}
		requestID := contextx.TraceID(ctx)
		if docParse.Status == docEntity.DocParseCallBackFailed {
			err = s.docLogic.RetryDocParseTask(ctx, docParse.TaskID, requestID, app.BizId)
			if err != nil {
				logx.E(ctx, "重试文档解析任务失败, docBizID:%d, err:%v", doc.BusinessID, err)
				failedDocs = append(failedDocs, doc.BusinessID)
				continue
				// return rsp, errs.ErrRetryDocParseTaskFail
			}
			docParse.Status = docEntity.DocParseIng
			docParse.Result = ""
			docParse.RequestID = requestID
			docParse.UpdateTime = time.Now()
			updateColumns := []string{
				docEntity.DocTblColUpdateTime,
				docEntity.DocParseTblColStatus,
				docEntity.DocParseTblColResult,
				docEntity.DocParseTblColRequestID,
			}
			err = s.docLogic.UpdateDocParseTask(ctx, updateColumns, docParse)
			if err != nil {
				logx.E(ctx, "更新文档解析任务状态失败, docBizID:%d, err:%v", doc.BusinessID, err)
				failedDocs = append(failedDocs, doc.BusinessID)
				continue
				// return rsp, errs.ErrUpdateDocParseTaskStatusFail
			}
			doc.Status = docEntity.DocStatusParseIng
		} else if docParse.Status == docEntity.DocParseCallBackCancel {
			taskID, err := s.docLogic.SendDocParseWordCount(ctx, doc, requestID, "")
			if err != nil {
				logx.E(ctx, "发送文档字数解析任务失败, docBizID:%d, err:%v", doc.BusinessID, err)
				failedDocs = append(failedDocs, doc.BusinessID)
				continue
				// return rsp, errs.ErrRetryDocParseTaskFail
			}
			newDocParse := &docEntity.DocParse{
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
			err = s.docLogic.CreateDocParseTask(ctx, newDocParse)
			if err != nil {
				logx.E(ctx, "创建文档解析任务失败, docBizID:%d, err:%v", doc.BusinessID, err)
				failedDocs = append(failedDocs, doc.BusinessID)
				continue
				// return rsp, errs.ErrRetryDocParseTaskFail
			}
			doc.Status = docEntity.DocStatusParseIng
		} else {
			docParse.Status = docEntity.DocParseCallBackFinish
			updateColumns := []string{docEntity.DocParseTblColStatus, docEntity.DocParseTblColUpdateTime}
			if err = s.docLogic.UpdateDocParseTask(ctx, updateColumns, docParse); err != nil {
				logx.E(ctx, "更新文档解析任务状态失败, docBizID:%d, err:%v", doc.BusinessID, err)
				failedDocs = append(failedDocs, doc.BusinessID)
				continue
				// return rsp, errs.ErrUpdateDocParseTaskStatusFail
			}
			if config.FileAuditSwitch() {
				doc.Status = docEntity.DocStatusAuditIng
				doc.AuditFlag = docEntity.AuditFlagWait
				if err = s.docLogic.CreateDocAudit(ctx, doc, docParse.SourceEnvSet); err != nil {
					logx.E(ctx, "创建文档审核失败, docBizID:%d, err:%v", doc.BusinessID, err)
					failedDocs = append(failedDocs, doc.BusinessID)
					continue
					// return rsp, errs.ErrCreateAuditFail
				}
			} else {
				doc.Status = docEntity.DocStatusCreatingIndex
				go func(rCtx context.Context) {
					if err = s.docLogic.DocParseSegment(rCtx, nil, doc, false); err != nil {
						logx.E(ctx, "设置文档无需审核失败, docBizID:%d, err:%v", doc.BusinessID, err)
						failedDocs = append(failedDocs, doc.BusinessID)
						return
					}
				}(trpc.CloneContext(ctx))
			}
		}

		doc.Message = ""
		doc.StaffID = contextx.Metadata(ctx).StaffID()
		err = s.docLogic.UpdateDocStatusAndCharSize(ctx, doc)
		if err != nil {
			logx.E(ctx, "RetryDocParse|UpdateDocStatusAndCharSize|err:%+v", err)
			failedDocs = append(failedDocs, doc.BusinessID)
			continue
			// return rsp, err
		}
		successCount++
		auditx.Recover(auditx.BizDocument).Corp(corpBizID).App(app.BizId).Space(app.SpaceId).Log(ctx, doc.BusinessID, doc.GetDocFileName())
	}
	if len(failedDocs) > 0 {
		logx.W(ctx, "Paritially DocParse Failed, SuccessCount:%d, FailedCount:%d, FailedDocBizIds:%v",
			successCount, len(failedDocs), failedDocs)
		if successCount == 0 {
			return rsp, errs.ErrRetryDocParseTaskFail
		}
	}
	logx.I(ctx, "RetryDocParse|SuccessCount:%d, FailedCount:%d", successCount, len(failedDocs))
	return rsp, nil
}

// ModifyDocAttrRange 批量修改文档的适用范围
func (s *Service) ModifyDocAttrRange(ctx context.Context, req *pb.ModifyDocAttrRangeReq) (*pb.ModifyDocAttrRangeRsp,
	error) {
	logx.I(ctx, "ModifyDocAttrRange req:%+v", req)
	staffID := contextx.Metadata(ctx).StaffID()
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrSystem
	}
	docBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetDocBizIds())
	if err != nil {
		return nil, err
	}
	docs, err := s.docLogic.GetDocByBizIDs(ctx, docBizIDs, app.PrimaryId)
	if err != nil || len(docs) == 0 {
		return nil, errs.ErrDocNotFound
	}
	docIds := make([]uint64, 0, len(docs))
	for _, doc := range docs {
		docIds = append(docIds, doc.ID)
	}
	// 检查文档是否在发布中
	releasingDocIdMap, err := s.docLogic.GetReleasingDocId(ctx, app.PrimaryId, docIds)
	if err != nil {
		logx.E(ctx, "获取发布中的文档失败 err:%+v", err)
		return nil, errs.ErrSystem
	}
	if len(req.GetAttrLabels()) > 0 {
		req.AttrRange = docEntity.AttrRangeCondition
	} else {
		req.AttrRange = docEntity.AttrRangeAll
	}
	attrs, labels, err := s.checkAttributeLabelRefer(ctx, app.PrimaryId, config.App().AttributeLabel.DocAttrLimit,
		config.App().AttributeLabel.DocAttrLabelLimit, req.GetAttrRange(), req.GetAttrLabels())
	if err != nil {
		return nil, err
	}
	needUpdateDocs := make([]*docEntity.Doc, 0, len(docs))
	for _, doc := range docs {
		if doc.CorpID != app.CorpPrimaryId || doc.RobotID != app.PrimaryId {
			return nil, errs.ErrPermissionDenied
		}
		if doc.HasDeleted() {
			return nil, errs.ErrDocHasDeleted
		}
		if !doc.IsAllowEdit() {
			return nil, errs.ErrDocNotAllowEdit
		}
		if _, ok := releasingDocIdMap[doc.ID]; ok {
			return nil, errs.ErrDocIsRelease
		}
		isDocAttributeLabelChange, err := s.labelLogic.IsDocAttributeLabelChange(ctx, app.PrimaryId, doc.ID, doc.AttrRange,
			req.GetAttrRange(), req.GetAttrLabels())
		if err != nil {
			return nil, errs.ErrSystem
		}
		if isDocAttributeLabelChange {
			doc.AttrRange = req.GetAttrRange()
			doc.Status = docEntity.DocStatusUpdating
			if !doc.IsNextActionAdd() {
				doc.NextAction = docEntity.DocNextActionUpdate
			}
			doc.StaffID = staffID
			needUpdateDocs = append(needUpdateDocs, doc)
		}
	}
	if len(needUpdateDocs) == 0 {
		return &pb.ModifyDocAttrRangeRsp{}, nil
	}

	docAttrs, err := fillDocAttributeLabelsFromPB(ctx, req.GetAttrLabels(), true, attrs, labels)
	if err != nil {
		return nil, err
	}
	if err = s.docLogic.UpdateDocAttrRange(ctx, staffID, needUpdateDocs, docAttrs); err != nil {
		return nil, errs.ErrSystem
	}

	for _, doc := range needUpdateDocs {
		auditx.Modify(auditx.BizDocument).App(app.BizId).Space(app.SpaceId).Log(ctx, doc.BusinessID, doc.GetDocFileName(), "LabelRange")
	}
	return &pb.ModifyDocAttrRangeRsp{}, nil
}

// RetryDocAudit 重试文档审核
func (s *Service) RetryDocAudit(ctx context.Context, req *pb.RetryDocAuditReq) (*pb.RetryDocAuditRsp, error) {
	rsp := new(pb.RetryDocAuditRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	logx.D(ctx, "重试文档审核 RetryDocAudit 失败 app:%+v error:%+v", app, err)
	if err != nil {
		return rsp, errs.ErrDocCannotBeSubmittedForAudit
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		return nil, err
	}
	doc, err := s.docLogic.GetDocByBizID(ctx, docBizID, app.PrimaryId)
	logx.D(ctx, "重试文档审核 RetryDocAudit 失败 1 doc:%+v err:%+v", doc, err)
	if err != nil {
		return rsp, err
	}
	if doc.Status != docEntity.DocStatusAuditFail {
		logx.D(ctx, "重试文档审核 RetryDocAudit 2 失败 doc:%+v err:%+v", doc, err)
		return rsp, errs.ErrDocCannotBeSubmittedForAudit
	}
	err = s.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{
		App:                  app,
		NewCharSize:          doc.CharSize,
		NewKnowledgeCapacity: doc.FileSize,
		NewStorageCapacity:   gox.IfElse(doc.Source == docEntity.SourceFromCorpCOSDoc, 0, doc.FileSize),
		NewComputeCapacity:   doc.FileSize,
	})
	if err != nil {
		return rsp, err
	}
	audits, err := s.auditLogic.GetBizAuditStatusByRelateIDs(ctx, app.PrimaryId, app.CorpPrimaryId, []uint64{doc.ID})
	logx.D(ctx, "重试文档审核 RetryDocAudit 失败 audits:%+v err:%+v", audits, err)
	if err != nil {
		return rsp, errs.ErrDocCannotBeSubmittedForAudit
	}
	if audit, ok := audits[doc.ID]; !ok || audit.Status != releaseEntity.AuditStatusTimeoutFail {
		return rsp, errs.ErrDocCannotBeSubmittedForAudit
	}
	if err = s.docLogic.CreateDocAudit(ctx, doc, contextx.Metadata(ctx).EnvSet()); err != nil {
		return rsp, errs.ErrCreateAuditFail
	}
	doc.Status = docEntity.DocStatusAuditIng
	doc.AuditFlag = docEntity.AuditFlagWait
	doc.Message = ""
	doc.StaffID = contextx.Metadata(ctx).StaffID()
	if err := s.docLogic.UpdateDocStatusAndCharSize(ctx, doc); err != nil {
		return rsp, errs.ErrUpdateDocStatusFail
	}
	return rsp, nil
}

func (s *Service) getValidityDocCount(ctx context.Context, robotID, corpID uint64) (uint64, error) {
	// req := docEntity.DocListReq{
	// 	CorpPrimaryId:  corpID,
	// 	AppPrimaryId: robotID,
	// 	FileTypes: []string{docEntity.FileTypeDocx, docEntity.FileTypeMD, docEntity.FileTypeTxt, docEntity.FileTypePdf,
	// 		docEntity.FileTypeXlsx, docEntity.FileTypePptx, docEntity.FileTypePpt,
	// 		docEntity.FileTypeDoc, docEntity.FileTypeXls,
	// 		docEntity.FileTypePng, docEntity.FileTypeJpg, docEntity.FileTypeJpeg, docEntity.FileTypeCsv},
	// 	Page:           1,
	// 	PageSize:       1,
	// 	Status:         s.getValidityDocStatus(),
	// 	ValidityStatus: docEntity.DocUnExpiredStatus,
	// 	Opts:           []uint32{docEntity.DocOptDocImport},
	// }
	offset, limit := utilx.Page(1, 1)
	docFilter := &docEntity.DocFilter{
		CorpId:  corpID,
		RobotId: robotID,
		FileTypes: []string{docEntity.FileTypeDocx, docEntity.FileTypeMD, docEntity.FileTypeTxt, docEntity.FileTypePdf,
			docEntity.FileTypeXlsx, docEntity.FileTypePptx, docEntity.FileTypePpt, docEntity.FileTypeKeyNote,
			docEntity.FileTypeDoc, docEntity.FileTypeXls, docEntity.FileTypeNumbers, docEntity.FileTypePages,
			docEntity.FileTypePng, docEntity.FileTypeJpg, docEntity.FileTypeJpeg, docEntity.FileTypeCsv},
		Offset:         offset,
		Limit:          limit,
		Status:         s.getValidityDocStatus(),
		ValidityStatus: docEntity.DocUnExpiredStatus,
		Opts:           []uint32{docEntity.DocOptDocImport},
	}
	total, err := s.docLogic.GetDocCount(ctx, docEntity.DocParseTblColList, docFilter)
	// total, _, err := s.dao.GetDocList(ctx, &req)
	if err != nil {
		return 0, err
	}
	return uint64(total), nil
}

func (s *Service) getValidityDocStatus() []uint32 {
	return []uint32{
		docEntity.DocStatusWaitRelease,
		docEntity.DocStatusReleasing,
		docEntity.DocStatusReleaseSuccess,
		docEntity.DocStatusUpdating,
		docEntity.DocStatusUpdateFail,
	}
}

func (s *Service) getFileNameByType(fileType string) string {
	if len(fileType) == 0 {
		return ""
	}
	random := randx.RandomString(20, randx.WithMode(randx.AlphabetMode))
	return fmt.Sprintf("%s-%d.%s", random, idgen.GetId(), fileType)
}

// getCorpByID 通过ID获取企业信息
func (s *Service) getCorpByID(ctx context.Context, id uint64) (corpBizID uint64, err error) {
	loginUserType := contextx.Metadata(ctx).LoginUserType()
	switch loginUserType {
	case entity.LoginUserExpType:
		session, err := s.checkSession(ctx)
		if err != nil {
			return 0, err
		}
		user, err := s.userLogic.DescribeExpUser(ctx, session.ID)
		if err != nil {
			return 0, err
		}
		return user.BusinessID, nil
	}
	corp, err := s.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, id)
	if err != nil {
		logx.E(ctx, "getCorpByID DescribeCorpByPrimaryId err: %+v", err)
		return 0, err
	}
	return corp.GetCorpId(), nil
}

// RenameDoc 文档重命名
func (s *Service) RenameDoc(ctx context.Context, req *pb.RenameDocReq) (*pb.RenameDocRsp, error) {
	rsp := new(pb.RenameDocRsp)
	log.DebugContext(ctx, "RenameDoc REQ: ", req)
	staffID, corpID := contextx.Metadata(ctx).StaffID(), contextx.Metadata(ctx).CorpID()
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		logx.E(ctx, "文档重命名失败 CheckReqBotBizIDUint64 err: %+v", err)
		return nil, err
	}
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, botBizID)
	// app, err := s.dao.GetAppByAppBizID(ctx, botBizID)
	if err != nil {
		logx.E(ctx, "文档重命名失败 GetAppByAppBizID err: %+v", err)
		return nil, err
	}
	if corpID != 0 && corpID != app.CorpPrimaryId {
		return nil, errs.ErrWrapf(errs.ErrCorpAppNotEqual, i18n.Translate(ctx, i18nkey.KeyEnterpriseAppAffiliationMismatch))
	}

	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		logx.E(ctx, "文档重命名失败 CheckReqParamsIsUint64 err: %+v", err)
		return nil, err
	}
	doc, err := s.docLogic.GetDocByBizID(ctx, docBizID, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "文档重命名失败 GetDocByBizID err: %+v", err)
		return nil, err
	}
	if doc.RobotID != app.PrimaryId {
		return rsp, errs.ErrWrapf(errs.ErrDocNotFound, i18n.Translate(ctx, i18nkey.KeyDocumentNotInCurrentApp))
	}
	if !doc.CanRename() {
		logx.E(ctx, "文档重命名失败, 当前状态: %+v", doc.StatusDesc(false))
		return nil, errs.ErrDocCannotRename
	}
	if doc.GetRealFileName() == req.NewName {
		logx.E(ctx, "文档名称未修改")
		return nil, errs.ErrDocNameNotChanged
	}
	if filepath.Ext(req.NewName) != filepath.Ext(doc.FileName) {
		logx.E(ctx, "文档重命名失败, 文档名称后缀不一致, 原文档名: %+v, 新文档名: %+v",
			doc.FileName, req.NewName)
		return nil, errs.ErrDocNameExtNotMatch
	}
	if util.FileNameNoSuffix(req.NewName) == "" {
		logx.E(ctx, "文档重命名失败, 文档名称是空的, 原文档名: %+v, 新文档名: %+v",
			doc.FileName, req.NewName)
		return nil, errs.ErrDocNameVerifyFailed
	}
	doc.FileNameInAudit = req.NewName

	if err := s.docLogic.RenameDoc(ctx, staffID, app, doc); err != nil {
		logx.E(ctx, "文档重命名失败 RenameDoc err: %+v", err)
		return nil, err
	}

	fileName := doc.GetDocFileName()
	if fileName == req.NewName {
		fileName = doc.FileName
	}

	auditx.Modify(auditx.BizDocument).App(app.BizId).Space(app.SpaceId).Log(ctx, doc.BusinessID, fileName, req.NewName)

	return rsp, nil
}

// ResumeDoc 超量失效恢复
func (s *Service) ResumeDoc(ctx context.Context, req *pb.ResumeDocReq) (*pb.ResumeDocRsp, error) {
	rsp := new(pb.ResumeDocRsp)
	staffID, corpID := contextx.Metadata(ctx).StaffID(), contextx.Metadata(ctx).CorpID()
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		logx.E(ctx, "恢复文档失败 CheckReqBotBizIDUint64 err: %+v", err)
		return nil, err
	}
	log.DebugContext(ctx, "ResumeDoc REQ: ", req)
	bot, err := s.rpc.AppAdmin.DescribeAppById(ctx, botBizID)
	// bot, err := s.dao.GetAppByAppBizID(ctx, botBizID)
	if err != nil {
		logx.E(ctx, "恢复文档失败 GetAppByAppBizID err: %+v", err)
		return nil, err
	}
	if bot == nil {
		return nil, errs.ErrAppNotFound
	}
	newCtx := util.SetMultipleMetaData(ctx, bot.SpaceId, bot.Uin)
	// 字符数超限不可执行
	if err = s.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{App: bot}); err != nil {
		return rsp, logicCommon.ConvertErrMsg(ctx, s.rpc, 0, bot.CorpPrimaryId, err)
	}
	docBizIDs, err := util.BatchCheckReqParamsIsUint64(newCtx, req.GetDocBizIds())
	if err != nil {
		logx.E(newCtx, "Resume Doc Failed. BatchCheckReqParamsIsUint64 err: %+v", err)
		return rsp, err
	}
	docM, err := s.docLogic.GetDocByBizIDs(newCtx, docBizIDs, bot.PrimaryId)
	if err != nil {
		logx.E(ctx, "Resume Doc Failed. GetDocByBizIDs err: %+v", err)
		return rsp, err
	}
	docExceededTimes := []entity.DocExceededTime{}
	for _, doc := range docM {
		if !doc.IsCharSizeExceeded() {
			continue
		}
		docExceededTimes = append(docExceededTimes, entity.DocExceededTime{
			BizID:      doc.BusinessID,
			UpdateTime: doc.UpdateTime,
		})
		if err := s.resumeDoc(newCtx, doc); err != nil {
			logx.E(newCtx, "恢复部分文档失败 resumeDoc err: %+v", err)
			continue
		}
		auditx.Recover(auditx.BizDocument).App(bot.BizId).Space(bot.SpaceId).Log(newCtx, doc.BusinessID, doc.FileName)
	}
	if err := scheduler.NewDocResumeTask(newCtx, corpID, bot.PrimaryId, staffID, docExceededTimes); err != nil {
		logx.E(newCtx, "Resume Doc Failed. CreateDocResumeTask err: %+v", err)
	}
	return rsp, nil
}

func (s *Service) resumeDoc(ctx context.Context, doc *docEntity.Doc) error {
	switch doc.Status {
	case docEntity.DocStatusCharExceeded:
		doc.Status = docEntity.DocStatusResuming
	case docEntity.DocStatusUpdateFailCharExceeded:
		doc.Status = docEntity.DocStatusUpdateFailResuming
	case docEntity.DocStatusParseImportFailCharExceeded:
		doc.Status = docEntity.DocStatusParseImportFailResuming
	case docEntity.DocStatusAuditFailCharExceeded:
		doc.Status = docEntity.DocStatusAuditFailResuming
	case docEntity.DocStatusCreateIndexFailCharExceeded:
		doc.Status = docEntity.DocStatusCreateIndexFailResuming
	case docEntity.DocStatusExpiredCharExceeded:
		doc.Status = docEntity.DocStatusExpiredResuming
	case docEntity.DocStatusAppealFailedCharExceeded:
		doc.Status = docEntity.DocStatusAppealFailedResuming
	default:
		// 不可恢复
		return nil
	}
	updateDocFilter := &docEntity.DocFilter{
		IDs: []uint64{doc.ID}, CorpId: doc.CorpID, RobotId: doc.RobotID,
	}
	update := &docEntity.Doc{
		StaffID:    contextx.Metadata(ctx).StaffID(),
		UpdateTime: time.Now(),
		Status:     doc.Status,
	}
	updateDocColumns := []string{docEntity.DocTblColStaffId, docEntity.DocTblColStatus, docEntity.DocTblColUpdateTime}
	_, err := s.docLogic.UpdateLogicByDao(ctx, updateDocColumns, updateDocFilter, update)
	if err != nil {
		return err
	}
	return nil
}

// listDocIDs 获取Doc ID
func listDocIDs(details map[uint64]*docEntity.Doc) []uint64 {
	values := maps.Values(details)
	var docIDs []uint64
	for _, value := range values {
		docIDs = append(docIDs, value.ID)
	}
	return docIDs
}

// GroupDoc Doc分组
func (s *Service) GroupDoc(ctx context.Context, req *pb.GroupObjectReq) (*pb.GroupObjectRsp, error) {
	logx.I(ctx, "GroupDoc Req:%+v", req)
	rsp := new(pb.GroupObjectRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	var cateID uint64
	cateBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
	if err != nil {
		return nil, err
	}
	if cateID, err = s.cateLogic.VerifyCateBiz(ctx, cateEntity.DocCate, app.CorpPrimaryId, cateBizID, app.PrimaryId); err != nil {
		return rsp, errs.ErrCateNotFound
	}
	var details map[uint64]*docEntity.Doc
	var docIDs []uint64
	ids := slicex.Unique(req.GetBizIds())
	details, err = s.docLogic.GetDocByBizIDs(ctx, ids, app.PrimaryId)
	if err != nil {
		return rsp, errs.ErrDocNotFound
	}
	// 检查文档是否在发布中
	releasingDocIdMap, err := s.docLogic.GetReleasingDocId(ctx, app.PrimaryId, docIDs)
	if err != nil {
		logx.E(ctx, "获取发布中的文档失败 err:%+v", err)
		return rsp, errs.ErrSystem
	}
	latestRelease, err := s.releaseLogic.GetLatestRelease(ctx, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	for _, doc := range details {
		_, ok := releasingDocIdMap[doc.ID]
		if !doc.IsStableStatus() || ok {
			// 非稳定状态的文档不能修改分组，因为后续重新学习会导致状态和最终结果不可控
			statusDesc := doc.StatusDesc(latestRelease.IsPublishPause())
			if statusDesc == "" {
				statusDesc = "处理中"
			}
			return rsp, errs.ErrWrapf(errs.ErrDocStatusNotStable, i18nkey.KeyDocumentCannotModifyGroup,
				doc.BusinessID, statusDesc)
		}
	}

	docIDs = listDocIDs(details)
	if err = s.cateLogic.GroupCateObject(ctx, cateEntity.DocCate, docIDs, cateID, app); err != nil {
		return rsp, errs.ErrSystem
	}

	return rsp, nil
}

// ReportKnowledgeOperationLog 知识型操作日志上报
// TODO(ericjwang): 哪里在用？貌似没流量，代码里也搜不到内部调用
func (s *Service) ReportKnowledgeOperationLog(ctx context.Context, req *pb.ReportKnowledgeOperationLogReq) (
	*pb.ReportKnowledgeOperationLogRsp, error) {
	logx.I(ctx, "ReportKnowledgeOperationLog Req:%+v", req)
	rsp := new(pb.ReportKnowledgeOperationLogRsp)
	operationBizIds, err := util.CheckReqSliceUint64(ctx, req.GetOperationBizIds())
	if err != nil {
		return nil, err
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetAppBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if app.IsDeleted {
		return rsp, nil
	}
	if req.GetOperationType() != user.SyncInfoTypeDoc && req.GetOperationType() != user.SyncInfoTypeQA {
		return nil, errs.ErrParamsNotExpected
	}
	logx.D(ctx, "ReportKnowledgeOperationLog|OperationType:%s|corpID:%+d|botBizID:%+d|operationBizIds:%+v",
		req.GetOperationType(), app.CorpPrimaryId, app.BizId, operationBizIds)

	// if doc.CorpPrimaryId != corpID || doc.AppPrimaryId != app.ID {
	//	logx.I(ctx, "BatchModifyDoc doc permission Denied! docInfo:%+v,corpID:%+v,robotID:%+v", doc,
	//		corpID, app.ID)
	//	return rsp, pkg.ErrPermissionDenied
	// }
	return rsp, err
}

func (s *Service) getShareKnowledgeValidityDocCount(ctx context.Context, appBizID uint64) (uint64, error) {
	shareKnowledges, err := s.kbDao.GetAppShareKGList(ctx, appBizID)
	if err != nil {
		return 0, err
	}
	knowledgesBizIDs := make([]uint64, 0, len(shareKnowledges))
	for _, v := range shareKnowledges {
		if v.KnowledgeBizID == appBizID {
			continue
		}
		knowledgesBizIDs = append(knowledgesBizIDs, v.KnowledgeBizID)
	}
	// robots, err := s.dao.GetRobotList(ctx, 0, "", knowledgesBizIDs, 0, 1, uint32(len(knowledgesBizIDs)))
	appListReq := appconfig.ListAppBaseInfoReq{
		AppBizIds:  knowledgesBizIDs,
		PageNumber: 1,
		PageSize:   uint32(len(knowledgesBizIDs)),
	}
	apps, _, err := s.rpc.AppAdmin.ListAppBaseInfo(ctx, &appListReq)
	if err != nil {
		return 0, err
	}
	appPrimaryIds := slicex.Pluck(apps, func(v *entity.AppBaseInfo) uint64 { return v.PrimaryId }) // 主键 id
	if len(appPrimaryIds) == 0 {
		return 0, nil
	}
	total, err := s.docLogic.GetDocCount(ctx, nil, &docEntity.DocFilter{
		RobotIDs: appPrimaryIds,
	})
	logx.D(ctx, "getShareKnowledgeValidityDocCount appBizID:%d robots:%v total:%d", appBizID, appPrimaryIds,
		total)
	return uint64(total), err
}
