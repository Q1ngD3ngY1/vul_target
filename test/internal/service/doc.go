// Package service 业务逻辑层
package service

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	logicDocQa "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc_qa"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/permissions"

	logicCommon "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/common"
	logicDoc "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"

	"gorm.io/gorm"

	knowClient "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"

	"git.woa.com/dialogue-platform/common/v3/errors"

	"golang.org/x/exp/maps"

	md "github.com/JohannesKaufmann/html-to-markdown"
	"github.com/PuerkitoBio/goquery"
	rd "github.com/go-shiori/go-readability"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/trace"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/metadata"
	"git.woa.com/baicaoyuan/moss/types/mapx"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/doc_diff_task"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
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
	log.InfoContextf(ctx, "DescribeStorageCredential|req:%+v", req)
	corpID := pkg.CorpID(ctx)
	corp, err := s.getCorpByID(ctx, corpID)
	if err != nil || corp == nil {
		return nil, errs.ErrCorpNotFound
	}
	pathList := make([]string, 0)
	var fileCosPath, imageCosPath, uploadCosPath string
	fileName := s.getFileNameByType(req.GetFileType())
	if req.GetBotBizId() != "" {
		botBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetBotBizId())
		if err != nil {
			return nil, err
		}
		app, err := s.getAppByAppBizID(ctx, botBizID)
		// 背景：给C端分享出去的链接使用上传图片或者文档，此时当前登录的Corp和App可能不是归属关系
		// 这里不强制校验AppID和CorpID是否强制是归属关系
		// 下面路径的拼接已经保证了获取的权限只能是当前Corp下的，不存在跨Corp越权问题
		if err != nil && !errors.Is(err, errs.ErrCorpAppNotEqual) {
			return nil, errs.ErrRobotNotFound
		}
		if len(fileName) == 0 {
			// corpCosPath是按企业维度维护的cos数据
			// robotCosPath是按照机器人维度维护的cos数据
			// 文档资料是按照机器人维度进行隔离，所以后续使用robotCosPath对外，corpCosPath只对存量的历史数据开放
			corpCosPath := s.dao.GetCorpCOSPath(ctx, corpID)
			fileCosPath = s.dao.GetCorpRobotCOSPath(ctx, corp.BusinessID, app.BusinessID, "")
			imageCosPath = s.dao.GetCorpAppImagePath(ctx, corp.BusinessID, app.BusinessID, "")
			pathList = []string{corpCosPath, fileCosPath, imageCosPath}
		} else {
			// 区分Cos公有权限场景还是私有权限场景
			if req.GetIsPublic() {
				uploadCosPath = s.dao.GetCorpAppImagePath(ctx, corp.BusinessID, app.BusinessID, fileName)
			} else {
				uploadCosPath = s.dao.GetCorpRobotCOSPath(ctx, corp.BusinessID, app.BusinessID, fileName)
			}
			pathList = []string{uploadCosPath}
		}
	} else {
		if len(fileName) == 0 {
			imageCosPath = s.dao.GetCorpImagePath(ctx, corp.BusinessID)
			pathList = append(pathList, imageCosPath)
		} else {
			uploadCosPath = filepath.Join(s.dao.GetCorpImagePath(ctx, corp.BusinessID), fileName)
			pathList = append(pathList, uploadCosPath)
		}
	}

	typeKey := dao.DefaultStorageTypeKey
	if len(req.GetTypeKey()) > 0 {
		typeKey = req.GetTypeKey()
	}
	res, err := s.dao.GetCredentialWithTypeKey(ctx, typeKey, pathList,
		utils.When(len(fileName) == 0, model.ActionDownload, model.ActionUpload))
	if err != nil {
		return nil, err
	}
	bucket, err := s.dao.GetBucketWithTypeKey(ctx, typeKey)
	if err != nil {
		return nil, err
	}
	region, err := s.dao.GetRegionWithTypeKey(ctx, typeKey)
	if err != nil {
		return nil, err
	}
	storageType, err := s.dao.GetStorageTypeWithTypeKey(ctx, typeKey)
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
	log.InfoContextf(ctx, "DescribeStorageCredential|rsp:%+v", rsp)
	return rsp, nil
}

// ListDoc 文档列表
func (s *Service) ListDoc(ctx context.Context, req *pb.ListDocReq) (*pb.ListDocRsp, error) {
	log.InfoContextf(ctx, "ListDoc Req:%+v", req)
	rsp := new(pb.ListDocRsp)
	corpID := pkg.CorpID(ctx)
	if req.GetBotBizId() == "" {
		return nil, errs.ErrParams
	}
	botBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if req.GetQueryType() == "" {
		req.QueryType = model.DocQueryTypeFileName
	}
	docListReq, err := s.getDocListReq(ctx, req, corpID, app.ID, app.IsShared)
	if err != nil {
		return rsp, err
	}
	total, docs, err := logicDoc.GetDocList(ctx, docListReq)
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
	cateMap, err := s.dao.GetCateByIDs(ctx, model.DocCate, cateIDs)
	if err != nil {
		log.ErrorContextf(ctx, "获取分类信息失败 err:%+v", err)
		// API调用使用的出参，先只打印ERROR日志，不返回错误，避免现网有脏数据影响文档列表的展示，观察一段时间后再放开
		// TODO: return nil, errs.ErrCateNotFound
	}
	maxProcessUnstableStatusDocCount := 10 // 最大处理非稳定状态时间过长文档数，避免处理太多导致接口超时
	processUnstableStatusDocCount := 0
	for _, doc := range docs {
		if !doc.IsStableStatus() && doc.Status != model.DocStatusReleasing {
			// 兜底策略：避免文档一直阻塞在非稳定状态（除了发布中状态由admin维护，不需要兜底）
			if processUnstableStatusDocCount >= maxProcessUnstableStatusDocCount {
				// 最大处理非稳定状态时间过长文档数，避免处理太多导致接口超时
				continue
			}
			// 如果是非稳定状态时间过长，打印ERROR日志，并更新成失败状态
			logicDoc.ProcessUnstableStatusDoc(ctx, doc)
			processUnstableStatusDocCount++
		}
	}
	qaNums, err := s.dao.GetDocQANum(ctx, corpID, app.ID, docIDs)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	// 检查文档是否在发布中
	releasingDocIdMap, err := logicDoc.GetReleasingDocId(ctx, app.ID, docIDs)
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的文档失败 err:%+v", err)
		return rsp, errs.ErrSystem
	}
	latestRelease, err := s.dao.GetLatestRelease(ctx, corpID, app.ID)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	mapDocID2AttrLabels, err := s.dao.GetDocAttributeLabelDetail(ctx, app.ID, docIDs)
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
	//获取员工名称
	staffByID, err := client.ListCorpStaffByIds(ctx, pkg.CorpBizID(ctx), staffIDs)
	if err != nil { //失败降级为返回员工ID
		log.ErrorContextf(ctx, "ListDbSource get staff name err:%v,staffIDs:%v", err, staffIDs)
	}
	for _, doc := range docs {
		docPb := logicDoc.DbDoc2PbDoc(ctx, releasingDocIdMap, doc, latestRelease, qaNums, mapDocID2AttrLabels,
			docParsesFailMap, docAuditFailMap, cateMap, app.IsShared)
		if staffName, ok := staffByID[doc.StaffID]; ok { //赋值员工名称
			docPb.StaffName = staffName
		} else { //没取到返回员工ID
			docPb.StaffName = cast.ToString(doc.StaffID)
		}
		rsp.List = append(rsp.List, docPb)
	}
	return rsp, nil
}

func (s *Service) docAuditMap(ctx context.Context, docs []*model.Doc, app *model.App) (
	map[uint64]model.AuditStatus, error) {
	docParseFailIDs := make([]uint64, 0)
	for _, doc := range docs {
		if doc.Status == model.DocStatusAuditFail {
			docParseFailIDs = append(docParseFailIDs, doc.ID)
			log.DebugContextf(ctx, "docAuditMap doc.ID:%d", doc.ID)
		}
	}
	docAuditFailMap, err := s.dao.GetBizAuditStatusByRelateIDs(ctx, app.ID, app.CorpID, docParseFailIDs)
	if err != nil {
		return docAuditFailMap, errs.ErrAuditNotFound
	}
	return docAuditFailMap, nil
}

func (s *Service) isAllowRetry(ctx context.Context, docID uint64, docStatus uint32,
	docParsesFailMap map[uint64]model.DocParse,
	docAuditFailMap map[uint64]model.AuditStatus) bool {
	if docParsesFailMap == nil {
		return false
	}
	if docStatus == model.DocStatusAuditFail {
		if docAuditFail, ok := docAuditFailMap[docID]; ok && docAuditFail.Status == model.AuditStatusTimeoutFail {
			return true
		}
		return false
	}
	if docStatus == model.DocStatusParseImportFail {
		return true
	}
	log.DebugContextf(ctx, "isAllowRetry docParsesFailMap:%+v, "+
		"docID:%d, docStatus:%d", docParsesFailMap, docID, docStatus)
	if len(docParsesFailMap) == 0 {
		return false
	}
	docParsesFail := model.DocParse{}
	ok := false
	if docParsesFail, ok = docParsesFailMap[docID]; !ok {
		return false
	}
	if docParsesFail.Status == model.DocParseCallBackCancel {
		return true
	}
	result := &knowledge.FileParserCallbackReq{}
	err := jsoniter.UnmarshalFromString(docParsesFail.Result, result)
	if err != nil {
		log.ErrorContextf(ctx, "isAllowRetry UnmarshalFromString err:%+v, "+
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

func (s *Service) docParsesMap(ctx context.Context, docs []*model.Doc) (map[uint64]model.DocParse, error) {
	log.DebugContextf(ctx, "docParsesMap docs:%+v", docs)
	docParseFailIDs := make([]uint64, 0)
	for _, doc := range docs {
		if doc.Status == model.DocStatusParseFail {
			docParseFailIDs = append(docParseFailIDs, doc.ID)
		}
	}
	docParsesMap := make(map[uint64]model.DocParse, len(docParseFailIDs))
	if len(docParseFailIDs) == 0 {
		return docParsesMap, nil
	}
	docParses, err := s.dao.GetDocParseByDocIDs(ctx, docParseFailIDs, docs[0].RobotID)
	if err != nil {
		return docParsesMap, errs.ErrDocParseTaskNotFound
	}
	for _, v := range docParses {
		if _, ok := docParsesMap[v.DocID]; !ok {
			docParsesMap[v.DocID] = v
		}
	}
	log.DebugContextf(ctx, "docParsesMap docParsesMap:%+v", docParsesMap)
	return docParsesMap, nil
}

func (s *Service) getDocListReq(ctx context.Context, req *pb.ListDocReq, corpID, robotID uint64, isShared bool) (*model.DocListReq,
	error) {
	validityStatus, status, err := s.getDocExpireStatus(req.GetStatus(), isShared)
	if err != nil {
		return nil, err
	}
	err = s.checkQueryType(req.GetQueryType())
	if err != nil {
		return nil, err
	}
	var cateIDs []uint64
	if req.GetCateBizId() != model.AllCateID {
		cateID, err := s.dao.CheckCateBiz(ctx, model.DocCate, corpID, uint64(req.GetCateBizId()), robotID)
		if err != nil {
			return nil, err
		}
		if req.GetShowCurrCate() == model.ShowCurrCate { //只展示当前分类的数据
			cateIDs = append(cateIDs, cateID)
		} else {
			cateIDs, err = s.getCateChildrenIDs(ctx, model.DocCate, corpID, robotID, cateID)
			if err != nil {
				return nil, err
			}
		}
	}

	mapFilterFlag := make(map[string]bool)
	for _, filterFlag := range req.GetFilterFlag() {
		if !dao.IsValidDocFilterFlag(filterFlag.Flag) {
			return nil, errs.ErrDocFilterFlagFail
		}
		mapFilterFlag[filterFlag.Flag] = filterFlag.Value
	}

	return &model.DocListReq{
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
		Opts:           []uint32{model.DocOptDocImport},
		CateIDs:        cateIDs,
	}, nil
}

// checkCanSaveDoc 判断用户是否能上传文档
func (s *Service) checkCanSaveDoc(ctx context.Context, robotID uint64, fileName, fileType string) error {
	staffID := pkg.StaffID(ctx)
	staff, err := s.dao.GetStaffByID(ctx, staffID)
	if err != nil || staff == nil {
		return errs.ErrStaffNotFound
	}
	if len(strings.TrimSuffix(fileName, "."+fileType)) == 0 {
		return errs.ErrInvalidFileName
	}
	if !util.CheckFileType(ctx, fileName, fileType) {
		return errs.ErrFileExtNotMatch
	}
	if err := s.isInTestMode(ctx, staff.CorpID, robotID, nil); err != nil {
		return err
	}
	return nil
}

// SaveDoc 保存文档
func (s *Service) SaveDoc(ctx context.Context, req *pb.SaveDocReq) (*pb.SaveDocRsp, error) {
	log.InfoContextf(ctx, "SaveDoc req:%+v", req)
	rsp := new(pb.SaveDocRsp)
	key := fmt.Sprintf(dao.LockForSaveDoc, req.GetCosHash())
	if err := s.dao.Lock(ctx, key, 120*time.Second); err != nil {
		return nil, errs.ErrSameDocUploading
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()
	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)

	expireStart, expireEnd, err := util.CheckReqStartEndTime(ctx, req.GetExpireStart(), req.GetExpireEnd())
	if err != nil {
		return nil, err
	}
	fileSize, err := util.CheckReqParamsIsUint64(ctx, req.GetSize())
	if err != nil {
		return nil, err
	}
	botBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	corp, err := s.dao.GetCorpByID(ctx, app.CorpID)
	if err != nil || corp == nil {
		return rsp, errs.ErrCorpNotFound
	}

	if err = s.dao.CheckURLFile(ctx, app.CorpID, corp.BusinessID, app.BusinessID,
		req.CosUrl, req.ETag); err != nil {
		log.ErrorContextf(ctx, "SaveDoc|CheckURLFile failed, err:%+v", err)
		return rsp, errs.ErrInvalidURL
	}

	if req.GetSource() == model.SourceFromWeb {
		originalURL := strings.TrimSpace(req.GetOriginalUrl())
		if originalURL == "" {
			log.WarnContextf(ctx, "SaveDoc|Source|%d|OriginalUrl为空", req.GetSource())
			return rsp, errs.ErrParams
		}
		if utf8.RuneCountInString(originalURL) > 2048 {
			log.ErrorContextf(ctx, "SaveDoc|OriginalUrl长度超过2048字符限制")
			return rsp, errs.ErrInvalidURL
		}
	}
	if req.GetSource() == model.SourceFromTxDoc && req.GetCustomerKnowledgeId() == "" {
		log.WarnContextf(ctx, "SaveDoc|Source|%d|CustomerKnowledgeId为空", req.GetSource())
		return rsp, errs.ErrParams
	}

	if err = CheckIsUsedCharSizeExceeded(ctx, s.dao, botBizID, corpID); err != nil {
		return rsp, s.dao.ConvertErrMsg(ctx, 0, app.CorpID, err)
	}
	if err := s.checkCanSaveDoc(ctx, app.ID, req.GetFileName(), req.GetFileType()); err != nil {
		return rsp, err
	}
	if len(req.GetAttrLabels()) > 0 {
		req.AttrRange = model.AttrRangeCondition
	} else {
		req.AttrRange = model.AttrRangeAll
	}
	attrs, labels, err := s.checkAttributeLabelRefer(ctx, app.ID, config.App().AttributeLabel.DocAttrLimit,
		config.App().AttributeLabel.DocAttrLabelLimit, req.GetAttrRange(), req.GetAttrLabels())
	if err != nil {
		return rsp, err
	}

	// 导入问答仅支持xlsx格式
	if req.Opt == model.DocOptBatchImport && req.GetFileType() != model.FileTypeXlsx {
		return rsp, errs.ErrDocQAFileFail
	}
	// 如果是导入excel，需要判别文件不是文档导入，如果是文档导入的excel就不需要检查表头了
	if req.GetFileType() == model.FileTypeXlsx && req.Opt != model.DocOptDocImport {
		if !app.IsShared {
			releaseCount, err := logicDocQa.GetDocQaReleaseCount(ctx, corpID, app.ID)
			if err != nil {
				return rsp, errs.ErrGetReleaseFail
			}
			if releaseCount >= int64(config.App().RobotDefault.QaReleaseMaxLimit) {
				return rsp, errs.ErrReleaseQaMaxCount
			}
		}
		if rsp, err := s.checkXlsx(ctx, corpID, app.ID, req.GetCosUrl(), pkg.Uin(ctx), app.BusinessID); rsp != nil || err != nil {
			return rsp, err
		}
	} else {
		if !app.IsShared {
			releaseCount, err := logicDoc.GetDocReleaseCount(ctx, corpID, app.ID)
			if err != nil {
				return rsp, errs.ErrGetReleaseFail
			}
			log.InfoContextf(ctx, "save|releaseCount:%d", releaseCount)
			if releaseCount >= int64(config.App().RobotDefault.DocReleaseMaxLimit) {
				return rsp, errs.ErrReleaseMaxCount
			}
		}
		// 校验是否有重复文档
		isDuplicate, rsp, err := logicDoc.CheckDuplicateFile(ctx, s.dao, req, corpID, app.ID)
		if err != nil {
			return nil, err
		}
		if isDuplicate {
			return rsp, nil
		}
	}
	auditFlag, err := s.getAuditFlag(req.GetFileType())
	if err != nil {
		return rsp, err
	}
	size, err := s.checkDocXlsxCharSize(ctx, req, app, fileSize)
	if err != nil {
		return rsp, err
	}
	// todo neckyang 校验自定义切分规则配置
	var cateID uint64
	if req.GetCateBizId() != "" {
		var catBizID uint64
		catBizID, err = util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
		if err != nil {
			return nil, err
		}
		if catBizID == 0 {
			return nil, errs.ErrCateNotFound
		}
		cateID, err = s.dao.CheckCateBiz(ctx, model.DocCate, corpID, catBizID, app.ID)
	} else {
		cateID, err = s.dao.GetRobotUncategorizedCateID(ctx, model.DocCate, corpID, app.ID)
	}
	if err != nil {
		return nil, err
	}
	doc := s.newDoc(ctx, req, app, corpID, staffID, expireStart, expireEnd, fileSize, auditFlag, size, cateID)
	docAttributeLabelsFromPB, err := fillDocAttributeLabelsFromPB(ctx, req.GetAttrLabels(), true, attrs, labels)
	if err != nil {
		return nil, err
	}
	if err := s.dao.CreateDoc(ctx, staffID, doc, docAttributeLabelsFromPB); err != nil {
		log.ErrorContextf(ctx, "SaveDoc CreateDoc err: %+v", err)
		return nil, errs.ErrSystem
	}
	rsp.DocBizId = doc.BusinessID
	_ = s.dao.AddOperationLog(ctx, model.DocEventAdd, corpID, app.ID, req, rsp, nil, doc)
	log.InfoContextf(ctx, "SaveDoc|rsp:%+v", rsp)
	return rsp, nil
}

func (s *Service) newDoc(ctx context.Context, req *pb.SaveDocReq, app *model.App,
	corpID, staffID, expireStart, expireEnd, fileSize uint64,
	auditFlag uint32, size int, cateId uint64) *model.Doc {
	isDownloadable := false
	if req.GetIsRefer() && req.GetReferUrlType() == model.ReferURLTypePreview {
		isDownloadable = req.GetIsDownload()
	}
	// 计算下次更新时间
	nextUpdateTime := time.Unix(0, 0).Add(8 * time.Hour)
	if req.GetSource() == model.SourceFromTxDoc && req.GetUpdatePeriodInfo().GetUpdatePeriodH() != 0 {
		nextUpdateTime = logicDoc.GetDocNextUpdateTime(ctx, req.GetUpdatePeriodInfo().GetUpdatePeriodH())
	}
	doc := &model.Doc{
		BusinessID:          s.dao.GenerateSeqID(),
		RobotID:             app.ID,
		CorpID:              corpID,
		StaffID:             staffID,
		FileName:            req.GetFileName(),
		FileType:            req.GetFileType(),
		FileSize:            fileSize,
		CosURL:              req.GetCosUrl(),
		Bucket:              s.dao.GetBucket(ctx),
		CosHash:             req.GetCosHash(),
		Status:              model.DocStatusParseIng,
		IsDeleted:           model.DocIsNotDeleted,
		Source:              req.GetSource(),
		WebURL:              req.GetWebUrl(),
		AuditFlag:           auditFlag,
		CharSize:            uint64(size),
		NextAction:          model.DocNextActionAdd,
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
	}
	//if req.GetSource() == model.SourceFromTxDoc && req.GetUpdatePeriodInfo().GetUpdatePeriodH() != 0 {
	//	doc.NextUpdateTime = nextUpdateTime
	//}
	for _, attrFlag := range req.GetAttributeFlags() {
		doc.AddAttributeFlag([]uint64{uint64(math.Pow(2, float64(attrFlag)))})
	}
	return doc
}

func (s *Service) getSaveDocOpt(req *pb.SaveDocReq) uint32 {
	// 兼容历史数据，保证如果不是xlsx文件格式，是文档导入类型
	if req.Opt == model.DocOptNormal && req.FileType != "xlsx" {
		return model.DocOptDocImport
	}
	return req.Opt
}

func (s *Service) checkDocXlsxCharSize(ctx context.Context, req *pb.SaveDocReq, app *model.App, fileSize uint64) (int,
	error) {
	if fileSize > config.App().RobotDefault.MaxFileSize {
		return 0, errs.ErrFileSizeTooBig
	}
	objectInfo, err := s.dao.StatObject(ctx, req.GetCosUrl())
	if err != nil || objectInfo == nil {
		return 0, errs.ErrSystem
	}
	if objectInfo.Size > int64(config.App().RobotDefault.MaxFileSize) {
		return 0, errs.ErrFileSizeTooBig
	}
	// 如果是xlsx，但是是文档导入，不计算大小
	if strings.ToLower(req.GetFileType()) != model.FileTypeXlsx || req.Opt == model.DocOptDocImport {
		return 0, nil
	}
	size, err := s.parseDocXlsxCharSize(ctx, req.GetFileName(), req.GetCosUrl(), req.GetFileType())
	if err != nil {
		return 0, err
	}

	if err := CheckIsCharSizeExceeded(ctx, s.dao, app.BusinessID, app.CorpID, int64(size)); err != nil {
		return size, err
	}
	return size, nil
}

func (s *Service) parseDocXlsxCharSize(ctx context.Context, fileName, cosURL, fileType string) (int, error) {
	done := make(chan any)
	var size int
	var err error
	go func() {
		defer errors.PanicHandler()
		charSize, charErr := s.dao.ParseDocXlsxCharSize(ctx, fileName, cosURL, fileType)
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

func (s *Service) getAuditFlag(fileType string) (uint32, error) {
	if !config.FileAuditSwitch() {
		return model.AuditFlagNoNeed, nil
	}
	return util.GetAuditFlag(fileType)
}

// DeleteDoc 删除文档
func (s *Service) DeleteDoc(ctx context.Context, req *pb.DeleteDocReq) (*pb.DeleteDocRsp, error) {
	log.InfoContextf(ctx, "DeleteDoc Req:%+v", req)
	rsp := new(pb.DeleteDocRsp)
	var err error
	if len(req.GetDocBizIds()) == 0 && len(req.GetIds()) == 0 {
		return rsp, errs.ErrWrapf(errs.ErrParameterInvalid, i18n.Translate(ctx, i18nkey.KeyDocumentIDCountZero))
	}
	limit := utilConfig.GetMainConfig().BatchInterfaceLimit.DeleteDocMaxLimit
	if limit > 0 && (len(req.GetIds()) > limit || len(req.GetDocBizIds()) > limit) {
		return rsp, errs.ErrWrapf(errs.ErrParameterInvalid, i18n.Translate(ctx, i18nkey.KeyDocumentIDCountExceedLimit), limit)
	}
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	corpID := pkg.CorpID(ctx)
	releaseCount, err := logicDoc.GetDocReleaseCount(ctx, corpID, app.ID)
	if err != nil {
		return rsp, errs.ErrGetReleaseFail
	}
	ids := slicex.Unique(req.GetIds())
	bizIds := slicex.Unique(req.GetDocBizIds())
	var docs map[uint64]*model.Doc
	if len(ids) > 0 {
		reqIDs, err := util.CheckReqSliceUint64(ctx, ids)
		if err != nil {
			return nil, err
		}
		docs, err = s.dao.GetDocByIDs(ctx, reqIDs, app.ID)
		if err != nil {
			return rsp, errs.ErrDocNotFound
		}
	} else {
		bizIDReq, err := util.CheckReqSliceUint64(ctx, bizIds)
		if err != nil {
			return nil, err
		}
		docs, err = s.dao.GetDocByBizIDs(ctx, bizIDReq, app.ID)
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
	defer logicCommon.UnlockByBizIds(ctx, s.dao, dao.LockForModifyOrDeleteDoc, docBizIds)
	if err != nil {
		return rsp, errs.ErrDocIsModifyingOrDeleting
	}
	log.InfoContextf(ctx, "DeleteDoc getAppByAppBizID ok, app:%+v", app)
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	log.InfoContextf(ctx, "DeleteDoc staffID:%v, corpID:%v", staffID, corpID)
	corp, err := s.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		log.ErrorContextf(ctx, "GetCorpByID err: %+v", err)
		return rsp, err
	}
	// 是否在评测模式
	if err := s.isInTestMode(ctx, corpID, app.ID, nil); err != nil {
		return rsp, err
	}
	notDeletedDocs := make([]*model.Doc, 0, len(docs))
	notDeletedDocBizIDs := make([]uint64, 0)
	docIds := make([]uint64, 0, len(docs))
	for _, doc := range docs {
		docIds = append(docIds, doc.ID)
	}
	// 检查文档是否在发布中
	releasingDocIdMap, err := logicDoc.GetReleasingDocId(ctx, app.ID, docIds)
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的文档失败 err:%+v", err)
		return rsp, errs.ErrSystem
	}
	for _, doc := range docs {
		if doc.CorpID != corpID || doc.RobotID != app.ID {
			return rsp, errs.ErrPermissionDenied
		}
		if doc.HasDeleted() {
			continue
		}
		if !app.IsShared && doc.Status == model.DocStatusReleaseSuccess && releaseCount >= int64(config.App().
			RobotDefault.
			DocReleaseMaxLimit) {
			return rsp, errs.ErrReleaseMaxCount
		}
		if _, ok := releasingDocIdMap[doc.ID]; ok {
			return rsp, errs.ErrDocIsRelease
		}
		if !doc.IsAllowDelete() {
			return rsp, errs.ErrDocForbidDelete
		}
		if doc.IsProcessing([]uint64{model.DocProcessingFlagHandlingDocDiffTask}) {
			return rsp, errs.ErrDocDiffTaskRunIng
		}
		notDeletedDocs = append(notDeletedDocs, doc)
		notDeletedDocBizIDs = append(notDeletedDocBizIDs, doc.BusinessID)
	}
	if len(notDeletedDocs) == 0 {
		return rsp, nil
	}
	if err = s.dao.DeleteDocs(ctx, staffID, app.BusinessID, notDeletedDocs); err != nil {
		return nil, errs.ErrSystem
	}
	err = doc_diff_task.InvalidDocDiffTask(ctx, corp.BusinessID, app.BusinessID, notDeletedDocBizIDs)
	if err != nil {
		// 更新对比任务失败不影响文档的删除流程
		log.WarnContextf(ctx, "DeleteDoc|InvalidDocDiffTask|err:%+v", err)
	}

	_ = s.dao.AddOperationLog(ctx, model.DocEventDel, corpID, app.GetAppID(), req, rsp, nil, nil)
	return rsp, nil
}

// CheckDocReferWorkFlow 检查文档引用的工作流
func (s *Service) CheckDocReferWorkFlow(ctx context.Context, req *pb.CheckDocReferWorkFlowReq) (
	*pb.CheckDocReferWorkFlowRsp, error) {
	log.InfoContextf(ctx, "CheckDocReferWorkFlow Req:%+v", req)
	rsp := new(pb.CheckDocReferWorkFlowRsp)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
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
	log.InfoContextf(ctx, "CheckDocReferWorkFlow|docBizIds:%+v", docBizIds)
	workFlowList, err := logicDoc.GetWorkflowListByDoc(ctx, req)
	if err != nil {
		return rsp, err
	}
	log.DebugContextf(ctx, "CheckDocReferWorkFlow|workFlowList:%+v", workFlowList)
	rsp.List = workFlowList
	return rsp, nil
}

func (s *Service) getPendingDoc(ctx context.Context, robotID uint64) (map[uint64]struct{}, error) {
	corpID := pkg.CorpID(ctx)
	latestRelease, err := s.dao.GetLatestRelease(ctx, corpID, robotID)
	if err != nil {
		return nil, err
	}
	if latestRelease == nil {
		return nil, nil
	}
	if latestRelease.IsPublishDone() {
		return nil, nil
	}
	releaseDocs, err := s.dao.GetReleaseDoc(ctx, latestRelease)
	if err != nil {
		return nil, err
	}
	return releaseDocs, nil
}

// ReferDoc 是否引用文档链接
func (s *Service) ReferDoc(ctx context.Context, req *pb.ReferDocReq) (*pb.ReferDocRsp, error) {
	rsp := new(pb.ReferDocRsp)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	var doc *model.Doc
	if isExistUInt64(req.DocId) {
		doc, err = s.dao.GetDocByID(ctx, req.GetDocId(), app.ID)
	} else {
		doc, err = s.dao.GetDocByBizID(ctx, req.GetDocBizId(), app.ID)
	}
	if err != nil || doc == nil {
		return rsp, errs.ErrDocNotFound
	}
	corpID := pkg.CorpID(ctx)
	if doc.CorpID != corpID || doc.RobotID != app.ID {
		return rsp, errs.ErrPermissionDenied
	}
	if doc.IsDeleted == model.DocIsDeleted {
		return rsp, errs.ErrDocHasDeleted
	}
	if !doc.IsAllowRefer() {
		return rsp, errs.ErrForbidRefer
	}
	referAfter := &model.Doc{
		ID:      req.GetDocId(),
		IsRefer: req.GetIsRefer(),
	}
	err = s.dao.ReferDoc(ctx, referAfter)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	referBefore := &model.Doc{
		ID:      doc.ID,
		IsRefer: doc.IsRefer,
	}
	_ = s.dao.AddOperationLog(ctx, model.DocEventRefer, corpID, app.GetAppID(), req, rsp, referBefore, referAfter)
	return rsp, nil
}

// GenerateQA 开始/重新生成QA
func (s *Service) GenerateQA(ctx context.Context, req *pb.GenerateQAReq) (*pb.GenerateQARsp, error) {
	log.InfoContextf(ctx, "GenerateQA Req:%+v", req)
	rsp := new(pb.GenerateQARsp)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	corpID := pkg.CorpID(ctx)
	if err := s.isInTestMode(ctx, corpID, app.ID, nil); err != nil {
		return rsp, err
	}
	if len(req.GetDocBizIds()) > config.App().DocQA.GenerateQALimit {
		return rsp, errs.ErrGenerateQALimitFail
	}
	docBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetDocBizIds())
	if err != nil {
		return nil, err
	}
	docs, err := s.dao.GetDocByBizIDs(ctx, docBizIDs, app.ID)
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
	releasingDocIdMap, err := logicDoc.GetReleasingDocId(ctx, app.ID, docIds)
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的文档失败 err:%+v", err)
		return rsp, errs.ErrSystem
	}
	for _, doc := range docs {
		if doc.CorpID != corpID || doc.RobotID != app.ID {
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
		if doc.IsProcessing([]uint64{model.DocProcessingFlagHandlingDocDiffTask}) {
			return rsp, errs.ErrDocDiffTaskRunIng
		}
		generating, err := s.dao.GetDocQATaskGenerating(ctx, corpID, app.ID, doc.ID)
		if err != nil {
			log.ErrorContextf(ctx, "GenerateQA|GetDocQATaskGenerating|查询文档是否有进行中任务失败 err:%+v", err)
			return rsp, err
		}
		if generating {
			log.InfoContextf(ctx, "GenerateQA|GetDocQATaskGenerating|文档已有正在进行中任务|%v|doc|%v",
				generating, doc)
			return rsp, errs.ErrGeneratingFail
		}
	}
	qaTask := &model.DocQATask{
		CorpID:  corpID,
		RobotID: app.ID,
	}
	staffID := pkg.StaffID(ctx)
	if err = s.dao.GenerateQA(ctx, staffID, mapx.Values(docs), qaTask, app.BusinessID); err != nil {
		return rsp, errs.ErrSystem
	}
	return rsp, nil
}

// GetSelectDoc Deprecate 获取文档下拉列表
func (s *Service) GetSelectDoc(ctx context.Context, req *pb.GetSelectDocReq) (*pb.GetSelectDocRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := &pb.GetSelectDocRsp{}
	return rsp, nil
}

// ListSelectDoc 获取文档下拉列表
func (s *Service) ListSelectDoc(ctx context.Context, req *pb.ListSelectDocReq) (*pb.ListSelectDocRsp, error) {
	log.InfoContextf(ctx, "ListSelectDoc Req:%+v", req)
	rsp := new(pb.ListSelectDocRsp)
	corpID := pkg.CorpID(ctx)
	corp, err := s.dao.GetCorpByID(ctx, corpID)
	if err != nil {
		return rsp, errs.ErrCorpNotFound
	}
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	if knowClient.IsVipApp(corp.Uin, botBizID) {
		// TODO: 该接口后续需要优化，支持百万级文档的超大应用
		return rsp, nil
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	validityStatus, status, err := s.getDocExpireStatus(req.GetStatus(), app.IsShared)
	if err != nil {
		return nil, err
	}
	fileTypes := []string{model.FileTypeDocx, model.FileTypeMD, model.FileTypeTxt, model.FileTypePdf,
		model.FileTypePptx, model.FileTypePpt, model.FileTypeDoc, model.FileTypePng, model.FileTypeJpg,
		model.FileTypeJpeg, model.FileTypeWps, model.FileTypePPsx, model.FileTypeTiff, model.FileTypeBmp,
		model.FileTypeGif, model.FileTypeHtml, model.FileTypeMhtml}
	total, _, err := s.dao.GetDocList(ctx, &model.DocListReq{
		CorpID:         corpID,
		RobotID:        app.ID,
		FileName:       req.GetFileName(),
		QueryType:      model.DocQueryTypeFileName,
		FileTypes:      fileTypes,
		Page:           1,
		PageSize:       1,
		Status:         status,
		ValidityStatus: validityStatus,
	})
	if err != nil {
		return rsp, errs.ErrSystem
	}
	if total == 0 {
		return rsp, nil
	}
	_, list, err := s.dao.GetDocList(ctx, &model.DocListReq{
		CorpID:         corpID,
		RobotID:        app.ID,
		FileName:       req.GetFileName(),
		QueryType:      model.DocQueryTypeFileName,
		FileTypes:      fileTypes,
		Page:           1,
		PageSize:       uint32(total),
		Status:         status,
		ValidityStatus: validityStatus,
	})
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
	botBizID, fetchURL := req.GetBotBizId(), strings.TrimSpace(req.GetUrl())
	log.DebugContextf(ctx, "botBizID:%d, fetchURL(%s)", botBizID, fetchURL)
	rsp := new(pb.FetchURLContentRsp)
	if _, err := s.getAppByAppBizID(ctx, botBizID); err != nil {
		return rsp, errs.ErrRobotNotFound
	}

	// 调用底座解析服务
	if utilConfig.GetMainConfig().FetchURLUseWebParser {
		requestID := trace.SpanContextFromContext(ctx).TraceID().String()
		title, content, err := s.dao.FetURLContent(ctx, requestID, botBizID, fetchURL)
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
		log.ErrorContextf(ctx, "校验url失败:url(%s) , err(%v)", fetchURL, err)
		return rsp, errs.ErrInvalidURL
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		log.ErrorContextf(ctx, "Invalid Scheme: url(%s) , err(%v)", fetchURL, err)
		return rsp, errs.ErrInvalidURL
	}
	safeClient := secapi.NewSafeClient()
	httpReq, err := http.NewRequest("GET", fetchURL, nil)
	if err != nil {
		log.ErrorContextf(ctx, "http.NewRequest fail: url(%s), err(%v)", fetchURL, err)
		return rsp, errs.ErrInvalidURL
	}
	// 基于安全请求的客户端，发起安全请求
	httpRsp, err := safeClient.Do(httpReq)
	if err != nil {
		log.ErrorContextf(ctx, "safeClient.Do fail: url(%s), err(%v)", fetchURL, err)
		return rsp, errs.ErrFetchURLFail
	}
	if httpRsp.StatusCode != 200 {
		log.ErrorContextf(ctx, "抓取内容失败:url:%s statusCode:%d", fetchURL, httpRsp.StatusCode)
		return rsp, errs.ErrFetchURLFail
	}
	by, err := io.ReadAll(httpRsp.Body)
	if err != nil {
		log.ErrorContextf(ctx, "io.ReadAll fail:url(%s)  err(%v)", fetchURL, err)
		return rsp, errs.ErrInvalidURL
	}
	html := string(by)
	if html == "" {
		log.ErrorContextf(ctx, "抓取内容为空 url(%v)", fetchURL)
		return rsp, errs.ErrFetchURLFail
	}
	if len(html) > 2*1024*1024 {
		log.ErrorContextf(ctx, "抓取内容过长 url(%v)", fetchURL)
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
		log.ErrorContextf(ctx, "readability p.Parse err %v", err)
		return "", err
	}

	converter := md.NewConverter("", true, nil)
	md, err := converter.ConvertString(doc.Content)
	if err != nil {
		log.ErrorContextf(ctx, "readability converter.ConvertString err %v", err)
		return "", err
	}

	return md, nil
}

func getTitle(ctx context.Context, html string) (string, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		log.ErrorContextf(ctx, "getTitle err %v", err)
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
	return string(pkg.ToUTF8([]byte(title))), nil
}

// DescribeDoc 获取文档详情
func (s *Service) DescribeDoc(ctx context.Context, req *pb.DescribeDocReq) (*pb.DescribeDocRsp, error) {
	log.InfoContextf(ctx, "DescribeDoc Req:%+v", req)
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		return nil, err
	}
	doc, err := logicDoc.GetDocByBizID(ctx, corpID, app.ID, docBizID, dao.DocTblColList)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errs.ErrDocNotFound
		}
		return nil, errs.ErrSystem
	}
	qaNums, err := s.dao.GetDocQANum(ctx, corpID, app.ID, []uint64{doc.ID})
	if err != nil {
		return nil, errs.ErrSystem
	}
	// 检查文档是否在发布中
	releasingDocIdMap, err := logicDoc.GetReleasingDocId(ctx, app.ID, []uint64{doc.ID})
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的文档失败 err:%+v", err)
		return nil, errs.ErrSystem
	}
	latestRelease, err := s.dao.GetLatestRelease(ctx, corpID, app.ID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	mapDocID2AttrLabels, err := s.dao.GetDocAttributeLabelDetail(ctx, app.ID, []uint64{doc.ID})
	if err != nil {
		return nil, errs.ErrSystem
	}
	_, isReleasing := releasingDocIdMap[doc.ID]
	cate, err := s.dao.GetCateByID(ctx, model.DocCate, uint64(doc.CategoryID), doc.CorpID, doc.RobotID)
	if err != nil {
		return nil, errs.ErrCateNotFound
	}

	updatePeriodH := doc.UpdatePeriodH
	if doc.Source == model.SourceFromWeb {
		requestID := trace.SpanContextFromContext(ctx).TraceID().String()
		updatePeriodH, err = s.dao.GetDocUpdateFrequency(ctx, requestID, req.GetBotBizId(), req.GetDocBizId())
		if err != nil {
			log.WarnContextf(ctx, "获取网页文档更新频率失败 err:%+v", err)
			//return nil, errs.ErrSystem
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
		QaNum:               qaNums[doc.ID][model.QAIsNotDeleted],
		IsDeleted:           doc.HasDeleted(),
		Source:              doc.Source,
		SourceDesc:          doc.DocSourceDesc(),
		IsAllowRestart:      !isReleasing && doc.IsAllowCreateQA(),
		IsDeletedQa:         qaNums[doc.ID][model.QAIsNotDeleted] == 0 && qaNums[doc.ID][model.QAIsDeleted] != 0,
		IsCreatingQa:        doc.IsCreatingQaV1(),
		IsAllowDelete:       !isReleasing && doc.IsAllowDelete(),
		IsAllowRefer:        doc.IsAllowRefer(),
		IsCreatedQa:         doc.IsCreatedQA,
		DocCharSize:         doc.CharSize,
		IsAllowEdit:         !isReleasing && doc.IsAllowEdit(),
		AttrRange:           doc.AttrRange,
		AttrLabels:          fillPBAttrLabels(mapDocID2AttrLabels[doc.ID]),
		CateBizId:           cate.BusinessID,
		CustomerKnowledgeId: doc.CustomerKnowledgeId,
		IsDisabled:          doc.IsDisable(),
		IsDownload:          doc.IsDownloadable,
		SplitRule:           doc.SplitRule,
		UpdatePeriodInfo:    &pb.UpdatePeriodInfo{UpdatePeriodH: updatePeriodH},
	}
	if doc.FileNameInAudit != "" {
		pbDoc.FileName = doc.FileNameInAudit
	}
	for k, v := range model.AttributeFlagMap {
		if doc.HasAttributeFlag(k) {
			pbDoc.AttributeFlags = append(pbDoc.AttributeFlags, v)
		}
	}
	return pbDoc, nil
}

// DescribeDocs 批量获取文档详情
func (s *Service) DescribeDocs(ctx context.Context, req *pb.DescribeDocsReq) (*pb.DescribeDocsRsp, error) {
	log.InfoContextf(ctx, "DescribeDocs Req:%+v", req)
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	docBizIDs, err := util.BatchCheckReqParamsIsUint64(ctx, req.GetDocBizIds())
	if err != nil {
		return nil, err
	}
	if len(docBizIDs) > utilConfig.GetMainConfig().BatchInterfaceLimit.GeneralMaxLimit {
		return nil, errs.ErrDescribeDocLimit
	}
	docs, err := s.dao.GetDocByBizIDs(ctx, docBizIDs, app.ID)
	if err != nil || len(docs) == 0 {
		return nil, errs.ErrDocNotFound
	}
	docIDs, cateIDs := make([]uint64, 0, len(docs)), make([]uint64, 0, len(docs))
	for _, doc := range docs {
		docIDs = append(docIDs, doc.ID)
		cateIDs = append(cateIDs, uint64(doc.CategoryID))
	}
	qaNums, err := s.dao.GetDocQANum(ctx, corpID, app.ID, docIDs)
	if err != nil {
		return nil, errs.ErrSystem
	}
	// 检查文档是否在发布中
	releasingDocIdMap, err := logicDoc.GetReleasingDocId(ctx, app.ID, docIDs)
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的文档失败 err:%+v", err)
		return nil, errs.ErrSystem
	}
	latestRelease, err := s.dao.GetLatestRelease(ctx, corpID, app.ID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	mapDocID2AttrLabels, err := s.dao.GetDocAttributeLabelDetail(ctx, app.ID, docIDs)
	if err != nil {
		return nil, errs.ErrSystem
	}
	cateMap, err := s.dao.GetCateByIDs(ctx, model.DocCate, cateIDs)
	if err != nil {
		return nil, errs.ErrCateNotFound
	}

	docDetails := getDocDetails(docs, qaNums, releasingDocIdMap, latestRelease, mapDocID2AttrLabels, cateMap)
	return &pb.DescribeDocsRsp{Docs: docDetails}, nil
}

// getDocDetails 获取文档详情
func getDocDetails(docs map[uint64]*model.Doc, qaNums map[uint64]map[uint32]uint32,
	pendingDoc map[uint64]struct{}, latestRelease *model.Release, mapDocID2AttrLabels map[uint64][]*model.AttrLabel,
	cateMap map[uint64]*model.CateInfo) []*pb.DescribeDocsRsp_DocDetail {
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
			QaNum:          qaNums[doc.ID][model.QAIsNotDeleted],
			IsDeleted:      doc.HasDeleted(),
			Source:         doc.Source,
			SourceDesc:     doc.DocSourceDesc(),
			IsAllowRestart: !ok && doc.IsAllowCreateQA(),
			IsDeletedQa:    qaNums[doc.ID][model.QAIsNotDeleted] == 0 && qaNums[doc.ID][model.QAIsDeleted] != 0,
			IsCreatingQa:   doc.IsCreatingQaV1(),
			IsAllowDelete:  !ok && doc.IsAllowDelete(),
			IsAllowRefer:   doc.IsAllowRefer(),
			IsCreatedQa:    doc.IsCreatedQA,
			DocCharSize:    doc.CharSize,
			IsAllowEdit:    !ok && doc.IsAllowEdit(),
			AttrRange:      doc.AttrRange,
			AttrLabels:     fillPBAttrLabels(mapDocID2AttrLabels[doc.ID]),
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

// ModifyDoc 修改文档
func (s *Service) ModifyDoc(ctx context.Context, req *pb.ModifyDocReq) (*pb.ModifyDocRsp, error) {
	log.InfoContextf(ctx, "ModifyDoc Req:%+v", req)
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
	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	// todo neckyang 校验拆分规则
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
	}
	if err := s.isInTestMode(ctx, corpID, app.ID, nil); err != nil {
		return nil, err
	}
	releaseCount, err := logicDoc.GetDocReleaseCount(ctx, corpID, app.ID)
	if err != nil {
		return nil, errs.ErrGetReleaseFail
	}
	doc, err := s.dao.GetDocByBizID(ctx, docBizId, app.ID)
	if err != nil || doc == nil {
		return nil, errs.ErrDocNotFound
	}
	if err = s.isDocAllowedToModify(ctx, *doc, *app, corpID); err != nil {
		return nil, err
	}
	//if slices.Contains(req.GetModifyTypes(), pb.ModifyDocReq_UPDATE_TX_DOC_REFRESH) {
	//	if doc.Source != model.SourceFromTxDoc {
	//		return nil, errs.ErrParams
	//	}
	//	err := logicDoc.RefreshTxDoc(ctx, false, []*model.Doc{doc}, s.dao)
	//	if err != nil {
	//		log.ErrorContextf(ctx, "RefreshTxDoc failed, err:%v", err)
	//		return nil, errs.ErrSystem
	//	}
	//	return &pb.ModifyDocRsp{}, nil
	//}

	if len(req.GetModifyTypes()) > 0 {
		if err = CheckIsUsedCharSizeExceeded(ctx, s.dao, botBizID, corpID); err != nil {
			return nil, s.dao.ConvertErrMsg(ctx, 0, app.CorpID, err)
		}
		err = logicDoc.ModifyItemsAction(ctx, s.dao, app, doc, req)
		if err != nil {
			log.ErrorContextf(ctx, "ModifyItemsAction failed, err:%v", err)
			return nil, err
		}
		return &pb.ModifyDocRsp{}, nil
	}

	if len(req.GetAttrLabels()) > 0 {
		req.AttrRange = model.AttrRangeCondition
	} else {
		req.AttrRange = model.AttrRangeAll
	}
	attrs, labels, err := s.checkAttributeLabelRefer(ctx, app.ID, config.App().AttributeLabel.DocAttrLimit,
		config.App().AttributeLabel.DocAttrLabelLimit, req.GetAttrRange(), req.GetAttrLabels())
	if err != nil {
		return nil, err
	}
	isDocAttributeLabelChange, err := s.isDocAttributeLabelChange(ctx, app.ID, doc.ID, doc.AttrRange,
		req.GetAttrRange(), req.GetAttrLabels())
	if err != nil {
		return nil, errs.ErrSystem
	}
	oldDoc := doc
	isNeedPublish := false
	if isDocAttributeLabelChange || doc.Status == model.DocStatusUpdateFail {
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
		doc.Status = model.DocStatusUpdating
	}
	if isNeedPublish && !doc.IsNextActionAdd() {
		doc.NextAction = model.DocNextActionUpdate
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
	if req.GetIsRefer() && req.GetReferUrlType() == model.ReferURLTypePreview {
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
	if req.GetCateBizId() != "" {
		catBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
		if err != nil {
			return nil, err
		}
		cateID, err = s.dao.CheckCateBiz(ctx, model.DocCate, corpID, catBizID, app.ID)
	} else {
		cateID, err = s.dao.GetRobotUncategorizedCateID(ctx, model.DocCate, corpID, app.ID)
	}
	if err != nil {
		return nil, err
	}
	if doc.CategoryID != uint32(cateID) {
		isNeedPublish = true
	}
	doc.CategoryID = uint32(cateID)

	if !app.IsShared && isNeedPublish && oldDoc.Status == model.DocStatusReleaseSuccess &&
		releaseCount >= int64(config.App().RobotDefault.DocReleaseMaxLimit) {
		return nil, errs.ErrReleaseMaxCount
	}

	if err = s.dao.UpdateDoc(ctx, staffID, doc, isNeedPublish, docAttributeLabelsFromPB); err != nil {
		return nil, errs.ErrSystem
	}
	_ = s.dao.AddOperationLog(ctx, model.DocEventEdit, corpID, app.GetAppID(), req, nil, oldDoc, doc)
	return &pb.ModifyDocRsp{}, nil
}

// BatchModifyDoc 批量修改文档应用链接，过期时间
func (s *Service) BatchModifyDoc(ctx context.Context, req *pb.BatchModifyDocReq) (*pb.BatchModifyDocRsp, error) {
	log.InfoContextf(ctx, "BatchModifyDoc Req:%+v", req)
	rsp := new(pb.BatchModifyDocRsp)
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

	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}

	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	if err = app.IsWriteable(); err != nil {
		log.ErrorContextf(ctx, "BatchModifyDoc app.IsWriteable, err:%v", err)
		return nil, err
	}
	if err := s.isInTestMode(ctx, corpID, app.ID, nil); err != nil {
		log.ErrorContextf(ctx, "BatchModifyDoc app.isInTestMode, err:%v", err)
		return nil, err
	}
	docs, err := s.dao.GetDocByBizIDs(ctx, docBizIds, app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "BatchModifyDoc GetDocByBusinessIDs, err:%v", err)
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
	releasingDocIdMap, err := logicDoc.GetReleasingDocId(ctx, app.ID, docIds)
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的文档失败 err:%+v", err)
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
	releaseCount, err := logicDoc.GetDocReleaseCount(ctx, corpID, app.ID)
	if err != nil {
		return rsp, errs.ErrGetReleaseFail
	}
	// 打开关闭应用链接不需要发布；修改适用范围；自定义到期时间需要发布
	isNeedUpdateMap := make(map[uint64]int)
	var changedDoc []*model.Doc
	for _, doc := range docs {
		if doc.Status == model.DocStatusCharExceeded {
			return rsp, errs.ErrDocNotAllowEdit
		}
		if doc.CorpID != corpID || doc.RobotID != app.ID {
			log.InfoContextf(ctx, "BatchModifyDoc doc permission Denied! docInfo:%+v,corpID:%+v,robotID:%+v", doc,
				corpID, app.ID)
			return rsp, errs.ErrPermissionDenied
		}

		if doc.HasDeleted() {
			return rsp, errs.ErrDocHasDeleted
		}
		if _, ok := releasingDocIdMap[doc.ID]; ok {
			return rsp, errs.ErrDocIsRelease
		}

		if req.GetActionType() == model.BatchModifyDefault || req.GetActionType() == model.BatchModifyRefer {
			doc.IsRefer = req.GetIsRefer()
			doc.ReferURLType = req.GetReferUrlType()
			doc.WebURL = req.GetWebUrl()
			doc.IsDownloadable = false
			if req.GetIsRefer() && req.GetReferUrlType() == model.ReferURLTypePreview {
				doc.IsDownloadable = req.GetIsDownload()
			}
		}

		if req.GetActionType() == model.BatchModifyDefault || req.GetActionType() == model.BatchModifyExpiredTime {
			if !doc.IsAllowEdit() {
				return rsp, errs.ErrDocNotAllowEdit
			}
			if doc.ExpireEnd.Unix() != int64(expireEnd) {
				isNeedUpdateMap[doc.ID] = 1
				doc.Status = model.DocStatusUpdating
				if !doc.IsNextActionAdd() {
					doc.NextAction = model.DocNextActionUpdate
				}
			}
			doc.ExpireEnd = time.Unix(int64(expireEnd), 0)

		}

		if req.GetActionType() == model.BatchModifyUpdatePeriod {
			if !doc.IsAllowEdit() {
				return rsp, errs.ErrDocNotAllowEdit
			}
			if doc.Source != model.SourceFromTxDoc {
				// 不是腾讯文档类型，不支持更新时间周期
				continue
			}
			nextUpdateTime := logicDoc.GetDocNextUpdateTime(ctx, req.GetUpdatePeriodInfo().GetUpdatePeriodH())
			doc.UpdatePeriodH = req.GetUpdatePeriodInfo().GetUpdatePeriodH()
			doc.NextUpdateTime = nextUpdateTime
		}

		if !app.IsShared && doc.Status == model.DocStatusReleaseSuccess && len(isNeedUpdateMap) > 0 &&
			releaseCount >= int64(config.App().RobotDefault.DocReleaseMaxLimit) {
			return rsp, errs.ErrReleaseMaxCount
		}
		doc.IsDownloadable = req.GetIsDownload()
		doc.StaffID = staffID
		changedDoc = append(changedDoc, doc)
	}
	if err = s.dao.BatchUpdateDoc(ctx, staffID, changedDoc, isNeedUpdateMap); err != nil {
		log.ErrorContextf(ctx, "BatchUpdateDoc, err:%v", err)
		return nil, errs.ErrSystem
	}
	_ = s.dao.AddOperationLog(ctx, model.DocEventEdit, corpID, app.GetAppID(), req, nil, nil, nil)
	return rsp, nil

}

// ModifyDocStatus 修改文档状态
func (s *Service) ModifyDocStatus(ctx context.Context, req *pb.ModifyDocStatusReq) (*pb.ModifyDocStatusRsp, error) {
	log.InfoContextf(ctx, "ModifyDocStatus Req:%+v", util.Object2String(req))
	rsp := new(pb.ModifyDocStatusRsp)
	if err := s.checkLogin(ctx); err != nil {
		return nil, err
	}
	startID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		return nil, err
	}
	appBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
	if err != nil {
		log.ErrorContextf(ctx, "ModifyDocStatus appBizID err:%v", err)
		return nil, err
	}
	err = logicCommon.LockByBizIds(ctx, s.dao, dao.LockForModifyOrDeleteDoc, 2*time.Second, []uint64{docBizID})
	defer logicCommon.UnlockByBizIds(ctx, s.dao, dao.LockForModifyOrDeleteDoc, []uint64{docBizID})
	if err != nil {
		return nil, errs.ErrDocIsModifyingOrDeleting
	}
	app, err := s.getAppByAppBizID(ctx, appBizID)
	if err != nil || app == nil {
		return nil, errs.ErrSystem
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
	}
	releaseCount, err := logicDoc.GetDocReleaseCount(ctx, corpID, app.ID)
	if err != nil {
		return rsp, errs.ErrGetReleaseFail
	}

	doc, err := s.dao.GetDocByBizID(ctx, docBizID, app.ID)
	if err != nil || doc == nil {
		return nil, errs.ErrDocNotFound
	}
	if err = s.isDocAllowedToModify(ctx, *doc, *app, corpID); err != nil {
		return nil, err
	}
	if !app.IsShared && doc.Status == model.DocStatusReleaseSuccess && releaseCount >= int64(config.App().RobotDefault.
		DocReleaseMaxLimit) {
		return rsp, errs.ErrReleaseMaxCount
	}
	log.InfoContextf(ctx, "ModifyDocStatus doc:%+v", util.Object2String(doc))
	// 文档已经是停用状态，则不需要再更新，直接退出
	if req.GetIsDisabled() && doc.HasAttributeFlag(model.DocAttributeFlagDisable) {
		return nil, errs.ErrDocIsDisabled
	}
	// 文档已经是启用状态，则不需要再更新，直接退出
	if !req.GetIsDisabled() && !doc.HasAttributeFlag(model.DocAttributeFlagDisable) {
		return nil, errs.ErrDocIsEnabled
	}
	oldDoc := doc
	isNeedPublish := true
	doc.Status = model.DocStatusUpdating
	if isNeedPublish && !doc.IsNextActionAdd() {
		doc.NextAction = model.DocNextActionUpdate
	}
	if err = s.dao.UpdateDocDisableState(ctx, startID, doc, req.GetIsDisabled()); err != nil {
		return nil, errs.ErrSystem
	}
	_ = s.dao.AddOperationLog(ctx, model.DocEventEdit, corpID, app.GetAppID(), req, nil, oldDoc, doc)
	return rsp, nil
}

// GetDocPreview 获取临时链接 不用临时密钥 临时密钥有过期时间
func (s *Service) GetDocPreview(ctx context.Context, req *pb.GetDocPreviewReq) (rsp *pb.GetDocPreviewRsp, err error) {
	log.InfoContextf(ctx, "GetDocPreview|req:%+v", req)
	if err := s.checkLogin(ctx); err != nil {
		return nil, err
	}
	var app *model.App
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		return nil, err
	}
	if req.GetTypeKey() == model.RealtimeStorageTypeKey {
		app, err = s.getAppByAppBizID(ctx, botBizID)
		// 这里不强制校验AppID和CorpID是否强制是归属关系
		// C侧分享链接出去的情况：当前登录的Corp和App可能不是归属关系
		if err != nil && !errors.Is(err, errs.ErrCorpAppNotEqual) {
			return nil, errs.ErrRobotNotFound
		}
		// 指定实时文档
		rsp, err = s.getRealtimeDocPreview(ctx, app, docBizID)
	} else {
		app, err = s.getAppByAppBizID(ctx, botBizID)
		if err != nil {
			return nil, err
		}
		// 默认离线文档
		rsp, err = s.getOfflineDocPreview(ctx, app, docBizID, req.GetBotBizId())
	}
	if err != nil {
		return nil, err
	}
	log.InfoContextf(ctx, "GetDocPreview|rsp:%+v", rsp)
	return rsp, nil
}

// BatchDownloadDoc 批量下载文档
func (s *Service) BatchDownloadDoc(ctx context.Context, req *pb.BatchDownloadDocReq) (
	rsp *pb.BatchDownloadDocRsp, err error) {
	log.InfoContextf(ctx, "BatchDownloadDoc|req:%+v", req)
	corpID := pkg.CorpID(ctx)
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
	appInfo, err := client.GetAppInfo(ctx, botBizID, model.AppTestScenes)
	if err != nil {
		log.ErrorContextf(ctx, "prepareTokenDosage GetAppInfo err: %+v", err)
		return nil, err
	}
	if appInfo.IsDelete {
		return nil, errs.ErrAppNotFound
	}
	if appInfo.GetCorpId() != corpID {
		return rsp, errs.ErrPermissionDenied
	}
	rsp, err = logicDoc.BatchDownloadDoc(ctx, appInfo.Id, docBizIDs, s.dao)
	if err != nil {
		log.ErrorContextf(ctx, "BatchDownloadDoc err: %+v", err)
		return nil, err
	}
	log.InfoContextf(ctx, "BatchDownloadDoc|rsp:%+v", rsp)
	return rsp, nil
}

// getRealtimeDocPreview 实时文档预览
func (s *Service) getRealtimeDocPreview(ctx context.Context, app *model.App, docID uint64) (
	*pb.GetDocPreviewRsp, error) {
	corpID := pkg.CorpID(ctx)
	corp, err := s.getCorpByID(ctx, corpID)
	if err != nil || corp == nil {
		return nil, errs.ErrCorpNotFound
	}
	doc, err := s.dao.GetRealtimeDocByID(ctx, docID)
	if err != nil || doc == nil {
		return nil, errs.ErrDocNotFound
	}
	log.InfoContextf(ctx, "getRealtimeDocPreview|doc:%+v, corp:%+v", doc, corp)
	err = s.dao.CheckURLPrefix(ctx, doc.CorpID, corp.BusinessID, app.BusinessID, doc.CosUrl)
	if err != nil {
		log.ErrorContextf(ctx, "getRealtimeDocPreview|CheckURLPrefix failed, err:%+v", err)
		return nil, errs.ErrInvalidURL
	}
	signURL, err := s.dao.GetPresignedURLWithTypeKey(ctx, model.RealtimeStorageTypeKey, doc.CosUrl)
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
func (s *Service) getOfflineDocPreview(ctx context.Context, app *model.App, docBizID uint64, botBizID string) (
	*pb.GetDocPreviewRsp, error) {
	doc, err := s.dao.GetDocByBizID(ctx, docBizID, app.ID)
	if err != nil || doc == nil {
		return nil, errs.ErrDocNotFound
	}
	corp, err := s.dao.GetCorpByID(ctx, doc.CorpID)
	if err != nil || corp == nil {
		return nil, errs.ErrCorpNotFound
	}
	err = s.dao.CheckURLPrefix(ctx, doc.CorpID, corp.BusinessID, app.BusinessID, doc.CosURL)
	if err != nil {
		log.ErrorContextf(ctx, "getOfflineDocPreview|CheckURLPrefix failed, err:%+v", err)
		return nil, errs.ErrInvalidURL
	}
	signURL, err := s.dao.GetPresignedURLWithTypeKey(ctx, model.OfflineStorageTypeKey, doc.CosURL)
	if err != nil {
		return nil, errs.ErrSystem
	}
	parseUrl, err := logicDoc.GetDocParseResUrl(ctx, s.dao, doc.ID, app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "getOfflineDocPreview|GetDocParseResUrl failed, err:%+v", err)
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
		case model.DocStatusExpired:
			validityStatus = model.DocExpiredStatus
		case model.DocStatusCharExceeded:
			// 超量状态扩展显示出失败稳态转超量的状态
			newStatus = append(newStatus,
				model.DocStatusCharExceeded,
				model.DocStatusParseImportFailCharExceeded,
				model.DocStatusAuditFailCharExceeded,
				model.DocStatusUpdateFailCharExceeded,
				model.DocStatusCreateIndexFailCharExceeded,
				model.DocStatusAppealFailedCharExceeded)
		case model.DocStatusWaitRelease:
			newStatus = append(newStatus, model.DocStatusWaitRelease)
			if isShared {
				// 共享知识库，需要兼容从应用知识库人工转换成共享知识库的情况
				newStatus = append(newStatus, model.DocStatusReleaseSuccess)
			}
		default:
			newStatus = append(newStatus, status[i])
		}
	}
	// 如果选择了状态，但是没有选择已过期，那就是未过期
	if validityStatus != model.DocExpiredStatus && len(newStatus) > 0 {
		validityStatus = model.DocUnExpiredStatus
	}
	return validityStatus, newStatus, nil
}

// checkQueryType 校验查询类型
func (s *Service) checkQueryType(fileType string) error {
	if fileType != model.DocQueryTypeFileName && fileType != model.DocQueryTypeAttribute {
		return errs.ErrParamsNotExpected
	}
	return nil
}

// StopDocParse 终止文档解析
func (s *Service) StopDocParse(ctx context.Context, req *pb.StopDocParseReq) (*pb.StopDocParseRsp, error) {
	log.InfoContextf(ctx, "StopDocParse Req:%+v", req)
	rsp := new(pb.StopDocParseRsp)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
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
	doc, err := s.dao.GetDocByBizID(ctx, docBizID, app.ID)
	if err != nil {
		return rsp, err
	}
	taskID := ""
	docParse, err := s.dao.GetDocParseByDocIDAndTypeAndStatus(ctx, doc.ID, model.DocParseTaskTypeWordCount,
		model.DocParseIng, doc.RobotID)
	if err != nil {
		// (兼容干预中的终止)如果文档在干预中且解析任务未找到，则直接更新文档状态
		if errors.Is(err, errs.ErrDocParseTaskNotFound) && doc.IsProcessing([]uint64{
			model.DocProcessingFlagSegmentIntervene}) {
			log.WarnContextf(ctx, "GetDocParseByDocIDAndTypeAndStatus failed, err: %+v", err)
			doc.Message = config.App().DocParseStop.Msg
			doc.Status = model.DocStatusParseFail
			err = s.dao.UpdateDocStatusAndCharSize(ctx, doc)
			if err != nil {
				return rsp, err
			}
			return rsp, nil
		}
		return rsp, errs.ErrDocParseTaskNotFound
	}

	requestID := trace.SpanContextFromContext(ctx).TraceID().String()
	taskID = docParse.TaskID
	err = s.dao.StopDocParseTask(ctx, taskID, requestID, app.BusinessID)
	if err != nil {
		return rsp, errs.ErrStopDocParseFail
	}
	doc.Message = config.App().DocParseStop.Msg
	doc.Status = model.DocStatusParseFail
	doc.StaffID = pkg.StaffID(ctx)
	err = s.dao.UpdateDocStatusAndCharSize(ctx, doc)
	if err != nil {
		return rsp, errs.ErrUpdateDocStatusFail
	}
	docParse.Status = model.DocParseCallBackCancel
	docParse.RequestID = requestID
	err = s.dao.UpdateDocParseTask(ctx, docParse)
	if err != nil {
		return rsp, errs.ErrUpdateDocParseTaskStatusFail
	}

	return rsp, nil
}

// RetryDocParse 重试文档解析
func (s *Service) RetryDocParse(ctx context.Context, req *pb.RetryDocParseReq) (*pb.RetryDocParseRsp, error) {
	log.InfoContextf(ctx, "RetryDocParse Req:%+v", req)
	rsp := new(pb.RetryDocParseRsp)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
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
	limit := utilConfig.GetMainConfig().BatchInterfaceLimit.RetryDocParseMaxLimit
	if limit > 0 && (len(req.GetDocBizIds()) > limit) {
		return rsp, errs.ErrWrapf(errs.ErrParameterInvalid,
			i18n.Translate(ctx, i18nkey.KeyDocumentIDCountExceedLimit), limit)
	}
	corpID := pkg.CorpID(ctx)
	if !app.IsShared {
		releaseCount, err := logicDoc.GetDocReleaseCount(ctx, corpID, app.ID)
		if err != nil {
			return rsp, errs.ErrGetReleaseFail
		}
		log.InfoContextf(ctx, "RetryDocParse|releaseCount:%d", releaseCount)
		if releaseCount >= int64(config.App().RobotDefault.DocReleaseMaxLimit) {
			return rsp, errs.ErrReleaseMaxCount
		}
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

	if err = CheckIsUsedCharSizeExceeded(ctx, s.dao, app.BusinessID, app.CorpID); err != nil {
		return rsp, s.dao.ConvertErrMsg(ctx, 0, app.CorpID, err)
	}
	//for _, docBizId := range docBizIDs {
	//
	//}
	docs, err := s.dao.GetDocByBizIDs(ctx, docBizIDs, app.ID)
	if err != nil {
		return rsp, err
	}

	//docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	//if err != nil {
	//	return nil, err
	//}
	//doc, err := s.dao.GetDocByBizID(ctx, docBizID, app.ID)
	//if err != nil {
	//	return rsp, err
	//}
	var parsesDocs []*model.Doc
	for _, doc := range docs {
		parsesDocs = append(parsesDocs, doc)
	}
	docParsesFailMap, err := s.docParsesMap(ctx, parsesDocs)
	if err != nil {
		return rsp, errs.ErrDocParseTaskNotFound
	}
	docAuditFailMap, _ := s.docAuditMap(ctx, parsesDocs, app)

	successCount := 0
	failedDocs := make([]uint64, 0)
	for _, doc := range docs {
		if !s.isAllowRetry(ctx, doc.ID, doc.Status, docParsesFailMap, docAuditFailMap) {
			log.WarnContextf(ctx, "文档当前状态不可重试解析, docBizID:%d, robotID:%d, appID:%d",
				doc.BusinessID, doc.RobotID, app.ID)
			failedDocs = append(failedDocs, doc.BusinessID)
			continue
		}
		if doc.RobotID != app.ID {
			log.WarnContextf(ctx, "文档不属于当前应用, docBizID:%d, robotID:%d, appID:%d",
				doc.BusinessID, doc.RobotID, app.ID)
			failedDocs = append(failedDocs, doc.BusinessID)
			continue
			//return rsp, errs.ErrWrapf(errs.ErrDocNotFound, i18n.Translate(ctx, i18nkey.KeyDocumentNotInCurrentApp))
		}
		// 获取企业信息
		staffBizID, staffID, corpBizID, corpID := pkg.StaffBizID(ctx), pkg.StaffID(ctx), pkg.CorpBizID(ctx), pkg.CorpID(ctx)
		docParses, err := s.dao.DocParseCanBeRetried(ctx, doc.ID, model.DocParseTaskTypeWordCount,
			[]uint32{model.DocParseCallBackFailed, model.DocParseCallBackCancel, model.DocParseCallBackCharSizeExceeded},
			doc.RobotID)
		if err != nil {
			log.ErrorContextf(ctx, "获取可重试文档解析任务失败, docBizID:%d, err:%v", doc.BusinessID, err)
			failedDocs = append(failedDocs, doc.BusinessID)
			continue
			//return rsp, errs.ErrDocParseTaskFailNotFound
		}
		if len(docParses) == 0 {
			// (兼容干预中的重试)如果文档在干预中且解析任务未找到，则重新提交干预异步任务
			if doc.IsProcessing([]uint64{model.DocProcessingFlagSegmentIntervene}) {
				// 获取切片数据
				docCommon := &model.DocSegmentCommon{
					AppID:      app.ID,
					AppBizID:   botBizID,
					CorpID:     corpID,
					CorpBizID:  corpBizID,
					StaffID:    staffID,
					StaffBizID: staffBizID,
					DocBizID:   doc.BusinessID,
					DocID:      doc.ID,
					DataSource: uint32(logicDoc.GetDataSource(ctx, doc.SplitRule)),
				}
				// 审核
				auditFlag, err := s.getAuditFlag(doc.FileType)
				if err != nil {
					log.ErrorContextf(ctx, "获取审核标志失败, docBizID:%d, err:%v", doc.BusinessID, err)
					failedDocs = append(failedDocs, doc.BusinessID)
					continue
					//return rsp, err
				}
				_, err = logicDoc.CreateDocParsingIntervention(ctx, s.dao, docCommon, auditFlag, doc)
				if err != nil {
					log.ErrorContextf(ctx, "创建文档解析干预任务失败, docBizID:%d, err:%v", doc.BusinessID, err)
					failedDocs = append(failedDocs, doc.BusinessID)
					continue
					//log.ErrorContextf(ctx, "RetryDocParse|RetryDocParse|err:%+v", err)
					//return nil, errs.ErrRetryDocParseTaskFail
				}
				successCount++
				continue
				//return rsp, nil
			}
			log.ErrorContextf(ctx, "未找到可重试的文档解析任务, docBizID:%d", doc.BusinessID)
			failedDocs = append(failedDocs, doc.BusinessID)
			continue
			//return rsp, errs.ErrDocParseTaskFailNotFound
		}
		docParse := docParses[0]
		if err = CheckIsUsedCharSizeExceeded(ctx, s.dao, app.BusinessID, app.CorpID); err != nil {
			return rsp, s.dao.ConvertErrMsg(ctx, 0, app.CorpID, err)
		}
		requestID := trace.SpanContextFromContext(ctx).TraceID().String()
		if docParse.Status == model.DocParseCallBackFailed {
			err = s.dao.RetryDocParseTask(ctx, docParse.TaskID, requestID, app.BusinessID)
			if err != nil {
				log.ErrorContextf(ctx, "重试文档解析任务失败, docBizID:%d, err:%v", doc.BusinessID, err)
				failedDocs = append(failedDocs, doc.BusinessID)
				continue
				//return rsp, errs.ErrRetryDocParseTaskFail
			}
			docParse.Status = model.DocParseIng
			docParse.Result = ""
			docParse.RequestID = requestID
			err = s.dao.UpdateDocParseTask(ctx, docParse)
			if err != nil {
				log.ErrorContextf(ctx, "更新文档解析任务状态失败, docBizID:%d, err:%v", doc.BusinessID, err)
				failedDocs = append(failedDocs, doc.BusinessID)
				continue
				//return rsp, errs.ErrUpdateDocParseTaskStatusFail
			}
			doc.Status = model.DocStatusParseIng
		} else if docParse.Status == model.DocParseCallBackCancel {
			taskID, err := s.dao.SendDocParseWordCount(ctx, doc, requestID, "")
			if err != nil {
				log.ErrorContextf(ctx, "发送文档字数解析任务失败, docBizID:%d, err:%v", doc.BusinessID, err)
				failedDocs = append(failedDocs, doc.BusinessID)
				continue
				//return rsp, errs.ErrRetryDocParseTaskFail
			}
			newDocParse := model.DocParse{
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
			err = s.dao.CreateDocParseTask(ctx, newDocParse)
			if err != nil {
				log.ErrorContextf(ctx, "创建文档解析任务失败, docBizID:%d, err:%v", doc.BusinessID, err)
				failedDocs = append(failedDocs, doc.BusinessID)
				continue
				//return rsp, errs.ErrRetryDocParseTaskFail
			}
			doc.Status = model.DocStatusParseIng
		} else {
			docParse.Status = model.DocParseCallBackFinish
			if err = s.dao.UpdateDocParseTask(ctx, docParse); err != nil {
				log.ErrorContextf(ctx, "更新文档解析任务状态失败, docBizID:%d, err:%v", doc.BusinessID, err)
				failedDocs = append(failedDocs, doc.BusinessID)
				continue
				//return rsp, errs.ErrUpdateDocParseTaskStatusFail
			}
			if config.App().AuditSwitch {
				doc.Status = model.DocStatusAuditIng
				doc.AuditFlag = model.AuditFlagWait
				if err = s.dao.CreateDocAudit(ctx, doc, docParse.SourceEnvSet); err != nil {
					log.ErrorContextf(ctx, "创建文档审核失败, docBizID:%d, err:%v", doc.BusinessID, err)
					failedDocs = append(failedDocs, doc.BusinessID)
					continue
					//return rsp, errs.ErrCreateAuditFail
				}
			} else {
				if err = s.dao.NoNeedAuditDoc(ctx, doc); err != nil {
					log.ErrorContextf(ctx, "设置文档无需审核失败, docBizID:%d, err:%v", doc.BusinessID, err)
					failedDocs = append(failedDocs, doc.BusinessID)
					continue
					//return rsp, err
				}
			}
		}
		doc.Message = ""
		doc.StaffID = pkg.StaffID(ctx)
		err = s.dao.UpdateDocStatusAndCharSize(ctx, doc)
		if err != nil {
			log.ErrorContextf(ctx, "更新文档状态和字符数失败, docBizID:%d, err:%v", doc.BusinessID, err)
			failedDocs = append(failedDocs, doc.BusinessID)
			continue
			//return rsp, err
		}
		successCount++
	}
	if len(failedDocs) > 0 {
		log.WarnContextf(ctx, "部分文档重试失败, 成功数:%d, 失败数:%d, 失败的文档BizIDs:%v",
			successCount, len(failedDocs), failedDocs)
		if successCount == 0 {
			return rsp, errs.ErrRetryDocParseTaskFail
		}
	}
	log.InfoContextf(ctx, "文档重试解析完成, 成功数:%d, 失败数:%d", successCount, len(failedDocs))
	return rsp, nil
}

// ModifyDocAttrRange 批量修改文档的适用范围
func (s *Service) ModifyDocAttrRange(ctx context.Context, req *pb.ModifyDocAttrRangeReq) (*pb.ModifyDocAttrRangeRsp,
	error) {
	log.InfoContextf(ctx, "ModifyDocAttrRange req:%+v", req)
	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	robot, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	if err := s.isInTestMode(ctx, corpID, robot.ID, nil); err != nil {
		return nil, err
	}
	docBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetDocBizIds())
	if err != nil {
		return nil, err
	}
	docs, err := s.dao.GetDocByBizIDs(ctx, docBizIDs, robot.ID)
	if err != nil || len(docs) == 0 {
		return nil, errs.ErrDocNotFound
	}
	releaseCount, err := logicDoc.GetDocReleaseCount(ctx, corpID, robot.ID)
	if err != nil {
		return nil, errs.ErrGetReleaseFail
	}
	docIds := make([]uint64, 0, len(docs))
	for _, doc := range docs {
		docIds = append(docIds, doc.ID)
	}
	// 检查文档是否在发布中
	releasingDocIdMap, err := logicDoc.GetReleasingDocId(ctx, robot.ID, docIds)
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的文档失败 err:%+v", err)
		return nil, errs.ErrSystem
	}
	if len(req.GetAttrLabels()) > 0 {
		req.AttrRange = model.AttrRangeCondition
	} else {
		req.AttrRange = model.AttrRangeAll
	}
	attrs, labels, err := s.checkAttributeLabelRefer(ctx, robot.ID, config.App().AttributeLabel.DocAttrLimit,
		config.App().AttributeLabel.DocAttrLabelLimit, req.GetAttrRange(), req.GetAttrLabels())
	if err != nil {
		return nil, err
	}
	needUpdateDocs := make([]*model.Doc, 0, len(docs))
	for _, doc := range docs {
		if doc.CorpID != corpID || doc.RobotID != robot.ID {
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
		if !robot.IsShared && doc.Status == model.DocStatusReleaseSuccess &&
			releaseCount >= int64(config.App().RobotDefault.DocReleaseMaxLimit) {
			return nil, errs.ErrReleaseMaxCount
		}
		isDocAttributeLabelChange, err := s.isDocAttributeLabelChange(ctx, robot.ID, doc.ID, doc.AttrRange,
			req.GetAttrRange(), req.GetAttrLabels())
		if err != nil {
			return nil, errs.ErrSystem
		}
		if isDocAttributeLabelChange {
			doc.AttrRange = req.GetAttrRange()
			doc.Status = model.DocStatusUpdating
			if !doc.IsNextActionAdd() {
				doc.NextAction = model.DocNextActionUpdate
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
	if err = s.dao.UpdateDocAttrRange(ctx, staffID, needUpdateDocs, docAttrs); err != nil {
		return nil, errs.ErrSystem
	}
	_ = s.dao.AddOperationLog(ctx, model.DocEventEdit, corpID, robot.GetAppID(), req, nil, nil, nil)
	return &pb.ModifyDocAttrRangeRsp{}, nil
}

// RetryDocAudit 重试文档审核
func (s *Service) RetryDocAudit(ctx context.Context, req *pb.RetryDocAuditReq) (*pb.RetryDocAuditRsp, error) {
	rsp := new(pb.RetryDocAuditRsp)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	robot, err := s.getAppByAppBizID(ctx, botBizID)
	log.DebugContextf(ctx, "重试文档审核 RetryDocAudit 失败 robot:%+v err:%+v", robot, err)
	if err != nil {
		return rsp, errs.ErrDocCannotBeSubmittedForAudit
	}
	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		return nil, err
	}
	doc, err := s.dao.GetDocByBizID(ctx, docBizID, robot.ID)
	log.DebugContextf(ctx, "重试文档审核 RetryDocAudit 失败 1 doc:%+v err:%+v", doc, err)
	if err != nil {
		return rsp, err
	}
	if doc.Status != model.DocStatusAuditFail {
		log.DebugContextf(ctx, "重试文档审核 RetryDocAudit 2 失败 doc:%+v err:%+v", doc, err)
		return rsp, errs.ErrDocCannotBeSubmittedForAudit
	}
	if err := CheckIsCharSizeExceeded(ctx, s.dao, botBizID, robot.CorpID, int64(doc.CharSize)); err != nil {
		return rsp, err
	}
	audits, err := s.dao.GetBizAuditStatusByRelateIDs(ctx, robot.ID, robot.CorpID, []uint64{doc.ID})
	log.DebugContextf(ctx, "重试文档审核 RetryDocAudit 失败 audits:%+v err:%+v", audits, err)
	if err != nil {
		return rsp, errs.ErrDocCannotBeSubmittedForAudit
	}
	if audit, ok := audits[doc.ID]; !ok || audit.Status != model.AuditStatusTimeoutFail {
		return rsp, errs.ErrDocCannotBeSubmittedForAudit
	}
	if err = s.dao.CreateDocAudit(ctx, doc, metadata.Metadata(ctx).EnvSet()); err != nil {
		return rsp, errs.ErrCreateAuditFail
	}
	doc.Status = model.DocStatusAuditIng
	doc.AuditFlag = model.AuditFlagWait
	doc.Message = ""
	doc.StaffID = pkg.StaffID(ctx)
	if err := s.dao.UpdateDocStatusAndCharSize(ctx, doc); err != nil {
		return rsp, errs.ErrUpdateDocStatusFail
	}
	return rsp, nil
}

func (s *Service) getValidityDocCount(ctx context.Context, robotID, corpID uint64) (uint64, error) {
	req := model.DocListReq{
		CorpID:  corpID,
		RobotID: robotID,
		FileTypes: []string{model.FileTypeDocx, model.FileTypeMD, model.FileTypeTxt, model.FileTypePdf,
			model.FileTypeXlsx, model.FileTypePptx, model.FileTypePpt, model.FileTypeDoc, model.FileTypeXls,
			model.FileTypePng, model.FileTypeJpg, model.FileTypeJpeg, model.FileTypeCsv},
		Page:           1,
		PageSize:       1,
		Status:         s.getValidityDocStatus(),
		ValidityStatus: model.DocUnExpiredStatus,
		Opts:           []uint32{model.DocOptDocImport},
	}
	total, _, err := s.dao.GetDocList(ctx, &req)
	if err != nil {
		return 0, err
	}
	return total, nil
}

func (s *Service) getValidityDocStatus() []uint32 {
	return []uint32{
		model.DocStatusWaitRelease,
		model.DocStatusReleasing,
		model.DocStatusReleaseSuccess,
		model.DocStatusUpdating,
		model.DocStatusUpdateFail,
	}
}

func (s *Service) getFileNameByType(fileType string) string {
	if len(fileType) == 0 {
		return ""
	}
	return fmt.Sprintf("%s-%d.%s", util.RandStr(20), s.dao.GenerateSeqID(), fileType)
}

// getCorpByID 通过ID获取企业信息
func (s *Service) getCorpByID(ctx context.Context, id uint64) (*model.Corp, error) {
	loginUserType := pkg.LoginUserType(ctx)
	switch loginUserType {
	case model.LoginUserExpType:
		session, err := s.checkSession(ctx)
		if err != nil {
			return nil, err
		}
		user, err := s.dao.GetExpUserByID(ctx, session.ID)
		if err != nil {
			return nil, err
		}
		return &model.Corp{
			SID:        user.SID,
			BusinessID: user.BusinessID,
			FullName:   user.Cellphone,
			Cellphone:  user.Cellphone,
			Status:     user.Status,
			CreateTime: user.CreateTime,
			UpdateTime: user.UpdateTime,
		}, nil
	}
	return s.dao.GetCorpByID(ctx, id)
}

// RenameDoc 文档重命名
func (s *Service) RenameDoc(ctx context.Context, req *pb.RenameDocReq) (*pb.RenameDocRsp, error) {
	rsp := new(pb.RenameDocRsp)
	log.DebugContext(ctx, "RenameDoc REQ: ", req)
	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		log.ErrorContextf(ctx, "文档重命名失败 CheckReqBotBizIDUint64 err: %+v", err)
		return nil, err
	}
	app, err := client.GetAppInfo(ctx, botBizID, model.AppTestScenes)
	if err != nil {
		log.ErrorContextf(ctx, "文档重命名失败 GetAppByAppBizID err: %+v", err)
		return nil, err
	}
	if corpID != 0 && corpID != app.GetCorpId() {
		return nil, errs.ErrWrapf(errs.ErrCorpAppNotEqual, i18n.Translate(ctx, i18nkey.KeyEnterpriseAppAffiliationMismatch))
	}
	releaseCount, err := logicDoc.GetDocReleaseCount(ctx, corpID, app.GetId())
	if err != nil {
		return rsp, errs.ErrGetReleaseFail
	}

	docBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetDocBizId())
	if err != nil {
		log.ErrorContextf(ctx, "文档重命名失败 CheckReqParamsIsUint64 err: %+v", err)
		return nil, err
	}
	doc, err := s.dao.GetDocByBizID(ctx, docBizID, app.GetId())
	if err != nil {
		log.ErrorContextf(ctx, "文档重命名失败 GetDocByBizID err: %+v", err)
		return nil, err
	}
	if doc.RobotID != app.GetId() {
		return rsp, errs.ErrWrapf(errs.ErrDocNotFound, i18n.Translate(ctx, i18nkey.KeyDocumentNotInCurrentApp))
	}
	if !doc.CanRename() {
		log.ErrorContextf(ctx, "文档重命名失败, 当前状态: %+v", doc.StatusDesc(false))
		return nil, errs.ErrDocCannotRename
	}
	if doc.GetRealFileName() == req.NewName {
		log.ErrorContextf(ctx, "文档名称未修改")
		return nil, errs.ErrDocNameNotChanged
	}
	if filepath.Ext(req.NewName) != filepath.Ext(doc.FileName) {
		log.ErrorContextf(ctx, "文档重命名失败, 文档名称后缀不一致, 原文档名: %+v, 新文档名: %+v",
			doc.FileName, req.NewName)
		return nil, errs.ErrDocNameExtNotMatch
	}
	if util.FileNameNoSuffix(req.NewName) == "" {
		log.ErrorContextf(ctx, "文档重命名失败, 文档名称是空的, 原文档名: %+v, 新文档名: %+v",
			doc.FileName, req.NewName)
		return nil, errs.ErrDocNameVerifyFailed
	}
	if !app.GetIsShareKnowledgeBase() && doc.Status == model.DocStatusReleaseSuccess &&
		releaseCount >= int64(config.App().RobotDefault.DocReleaseMaxLimit) {
		return nil, errs.ErrReleaseMaxCount
	}
	doc.FileNameInAudit = req.NewName

	if err := s.dao.RenameDoc(ctx, staffID, app, doc); err != nil {
		log.ErrorContextf(ctx, "文档重命名失败 RenameDoc err: %+v", err)
		return nil, err
	}
	_ = s.dao.AddOperationLog(ctx, model.DocEventRename, corpID, app.GetId(), req, rsp, doc.FileName, doc.FileNameInAudit)

	return rsp, nil
}

// ResumeDoc 超量失效恢复
func (s *Service) ResumeDoc(ctx context.Context, req *pb.ResumeDocReq) (*pb.ResumeDocRsp, error) {
	rsp := new(pb.ResumeDocRsp)
	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		log.ErrorContextf(ctx, "恢复文档失败 CheckReqBotBizIDUint64 err: %+v", err)
		return nil, err
	}
	log.DebugContext(ctx, "ResumeDoc REQ: ", req)
	bot, err := s.dao.GetAppByAppBizID(ctx, botBizID)
	if err != nil {
		log.ErrorContextf(ctx, "恢复文档失败 GetAppByAppBizID err: %+v", err)
		return nil, err
	}
	if bot == nil {
		return nil, errs.ErrAppNotFound
	}
	ctx = pkg.WithSpaceID(ctx, bot.SpaceID)
	// 字符数超限不可执行
	if err = CheckIsUsedCharSizeExceeded(ctx, s.dao, botBizID, corpID); err != nil {
		return rsp, s.dao.ConvertErrMsg(ctx, 0, bot.CorpID, err)
	}
	docBizIDs, err := util.BatchCheckReqParamsIsUint64(ctx, req.GetDocBizIds())
	if err != nil {
		log.ErrorContextf(ctx, "恢复文档失败 BatchCheckReqParamsIsUint64 err: %+v", err)
		return rsp, err
	}
	docM, err := s.dao.GetDocByBizIDs(ctx, docBizIDs, bot.ID)
	if err != nil {
		log.ErrorContextf(ctx, "恢复文档失败 GetDocByBizIDs err: %+v", err)
		return rsp, err
	}
	docExceededTimes := []model.DocExceededTime{}
	for _, doc := range docM {
		if !doc.IsCharSizeExceeded() {
			continue
		}
		docExceededTimes = append(docExceededTimes, model.DocExceededTime{
			BizID:      doc.BusinessID,
			UpdateTime: doc.UpdateTime,
		})
		if err := s.resumeDoc(ctx, doc); err != nil {
			log.ErrorContextf(ctx, "恢复部分文档失败 resumeDoc err: %+v", err)
			continue
		}
	}
	if err := s.dao.CreateDocResumeTask(ctx, corpID, bot.ID, staffID, docExceededTimes); err != nil {
		log.ErrorContextf(ctx, "恢复文档失败 CreateDocResumeTask err: %+v", err)
	}
	return rsp, nil
}

func (s *Service) resumeDoc(ctx context.Context, doc *model.Doc) error {
	switch doc.Status {
	case model.DocStatusCharExceeded:
		doc.Status = model.DocStatusResuming
	case model.DocStatusUpdateFailCharExceeded:
		doc.Status = model.DocStatusUpdateFailResuming
	case model.DocStatusParseImportFailCharExceeded:
		doc.Status = model.DocStatusParseImportFailResuming
	case model.DocStatusAuditFailCharExceeded:
		doc.Status = model.DocStatusAuditFailResuming
	case model.DocStatusCreateIndexFailCharExceeded:
		doc.Status = model.DocStatusCreateIndexFailResuming
	case model.DocStatusExpiredCharExceeded:
		doc.Status = model.DocStatusExpiredResuming
	case model.DocStatusAppealFailedCharExceeded:
		doc.Status = model.DocStatusAppealFailedResuming
	default:
		// 不可恢复
		return nil
	}
	updateDocFilter := &dao.DocFilter{
		IDs: []uint64{doc.ID}, CorpId: doc.CorpID, RobotId: doc.RobotID,
	}
	update := &model.Doc{
		StaffID:    pkg.StaffID(ctx),
		UpdateTime: time.Now(),
		Status:     doc.Status,
	}
	updateDocColumns := []string{dao.DocTblColStaffId, dao.DocTblColStatus, dao.DocTblColUpdateTime}
	_, err := dao.GetDocDao().UpdateDoc(ctx, updateDocColumns, updateDocFilter, update)
	if err != nil {
		return err
	}
	return nil
}

// listDocIDs 获取Doc ID
func listDocIDs(details map[uint64]*model.Doc) []uint64 {
	values := maps.Values(details)
	var docIDs []uint64
	for _, value := range values {
		docIDs = append(docIDs, value.ID)
	}
	return docIDs
}

// GroupDoc Doc分组
func (s *Service) GroupDoc(ctx context.Context, req *pb.GroupObjectReq) (*pb.GroupObjectRsp, error) {
	log.InfoContextf(ctx, "GroupDoc Req:%+v", req)
	rsp := new(pb.GroupObjectRsp)
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	releaseCount, err := logicDoc.GetDocReleaseCount(ctx, corpID, app.ID)
	if err != nil {
		return rsp, errs.ErrGetReleaseFail
	}
	if !app.IsShared && releaseCount >= int64(config.App().RobotDefault.DocReleaseMaxLimit) {
		return rsp, errs.ErrReleaseMaxCount
	}
	var cateID uint64
	cateBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
	if err != nil {
		return nil, err
	}
	if cateID, err = s.dao.CheckCateBiz(ctx, model.DocCate, corpID, cateBizID, app.ID); err != nil {
		return rsp, errs.ErrCateNotFound
	}
	var details map[uint64]*model.Doc
	var docIDs []uint64
	ids := slicex.Unique(req.GetBizIds())
	details, err = s.dao.GetDocByBizIDs(ctx, ids, app.ID)
	if err != nil {
		return rsp, errs.ErrDocNotFound
	}
	// 检查文档是否在发布中
	releasingDocIdMap, err := logicDoc.GetReleasingDocId(ctx, app.ID, docIDs)
	if err != nil {
		log.ErrorContextf(ctx, "获取发布中的文档失败 err:%+v", err)
		return rsp, errs.ErrSystem
	}
	latestRelease, err := s.dao.GetLatestRelease(ctx, corpID, app.ID)
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
			return rsp, errs.ErrWrapf(errs.ErrDocStatusNotStable, i18n.Translate(ctx, i18nkey.KeyDocumentCannotModifyGroup),
				doc.BusinessID, statusDesc)
		}
	}

	docIDs = listDocIDs(details)
	if err = dao.GetCateDao(model.DocCate).GroupCateObject(ctx, s.dao, model.DocCate, docIDs, cateID, app); err != nil {
		return rsp, errs.ErrSystem
	}

	return rsp, nil
}

// ReportKnowledgeOperationLog 知识型操作日志上报
func (s *Service) ReportKnowledgeOperationLog(ctx context.Context, req *pb.ReportKnowledgeOperationLogReq) (
	*pb.ReportKnowledgeOperationLogRsp, error) {
	log.InfoContextf(ctx, "ReportKnowledgeOperationLog Req:%+v", req)
	rsp := new(pb.ReportKnowledgeOperationLogRsp)
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetAppBizId())
	if err != nil {
		return nil, err
	}
	operationBizIds, err := util.CheckReqSliceUint64(ctx, req.GetOperationBizIds())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if app.HasDeleted() {
		return rsp, nil
	}
	if req.GetOperationType() != permissions.SyncInfoTypeDoc && req.GetOperationType() != permissions.SyncInfoTypeQA {
		return nil, errs.ErrParamsNotExpected
	}
	log.DebugContextf(ctx, "ReportKnowledgeOperationLog|OperationType:%s|corpID:%+d|botBizID:%+d"+
		"|operationBizIds:%+v", req.GetOperationType(), corpID, app.BusinessID, operationBizIds)

	//if doc.CorpID != corpID || doc.RobotID != app.ID {
	//	log.InfoContextf(ctx, "BatchModifyDoc doc permission Denied! docInfo:%+v,corpID:%+v,robotID:%+v", doc,
	//		corpID, app.ID)
	//	return rsp, errs.ErrPermissionDenied
	//}
	return rsp, err
}

func (s *Service) getShareKnowledgeValidityDocCount(ctx context.Context, appBizID uint64) (uint64, error) {
	shareKnowledges, err := dao.GetAppShareKGDao().GetAppShareKGList(ctx, appBizID)
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
	robots, err := s.dao.GetRobotList(ctx, 0, "", knowledgesBizIDs, 0, 1, uint32(len(knowledgesBizIDs)))
	if err != nil {
		return 0, err
	}
	knowledgeIDs := make([]uint64, 0, len(robots))
	for _, v := range robots {
		knowledgeIDs = append(knowledgeIDs, v.ID)
	}
	if len(knowledgeIDs) == 0 {
		return 0, nil
	}
	total, err := dao.GetDocDao().GetDocCount(ctx, nil, &dao.DocFilter{
		RobotIDs: knowledgeIDs,
	})
	log.DebugContextf(ctx, "getShareKnowledgeValidityDocCount appBizID:%d robots:%v total:%d", appBizID, knowledgeIDs, total)
	return uint64(total), err
}
