package document

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/common/x/utilx"
	appconfig "git.woa.com/adp/pb-go/app/app_config"
	"git.woa.com/adp/pb-go/common"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"golang.org/x/exp/slices"
	"gorm.io/gorm"

	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	async "git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/category"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/internal/entity/label"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	fileManagerServer "git.woa.com/adp/pb-go/kb/parse_engine/file_manager_server"
	fileParseCommon "git.woa.com/adp/pb-go/kb/parse_engine/file_parse_common"
	"git.woa.com/adp/pb-go/resource_gallery/resource_gallery"
)

// CreateDoc 创建doc（异步计算字符、审核、生成文档分段）
func (l *Logic) CreateDoc(ctx context.Context, staffID uint64, doc *docEntity.Doc,
	attributeLabelReq *label.UpdateDocAttributeLabelReq) error {
	gormClient, err := knowClient.GormClient(ctx, docEntity.DocTableName, doc.RobotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "UpdateDoc get GormClient err:%v,robotID:%v", err, doc.RobotID)
		return err
	}

	if err := gormClient.Transaction(func(tx *gorm.DB) error {
		if err := l.docDao.CreateDoc(ctx, doc, tx); err != nil {
			logx.E(ctx, "Failed to create doc error. err:%+v", err)
			return err
		}

		logx.I(ctx, "CreateDoc success. doc:%+v", doc)

		logx.I(ctx, "UpdateDocAttributeLabelByTx. doc:%+v, attributeLabelReq:%+v", doc, attributeLabelReq)

		if err := l.labelDao.UpdateDocAttributeLabelByTx(ctx, doc.RobotID, doc.ID, attributeLabelReq, tx); err != nil {
			return err
		}
		// 增加是否批量导入操作，如果是批量导入操作，才会作为excel解析处理
		if doc.IsBatchImport() && doc.IsExcel() {
			if err := l.sendExcelImportNotice(ctx, staffID, doc); err != nil {
				return err
			}
			if err := scheduler.NewExcelToQATask(ctx, doc.RobotID, entity.ExcelToQAParams{
				CorpID: doc.CorpID, StaffID: staffID, RobotID: doc.RobotID, DocID: doc.ID, EnvSet: contextx.Metadata(ctx).EnvSet(),
			}); err != nil {
				logx.E(ctx, "Create ExcelToQATask err:%+v", err)
				return err
			}
			return nil
		}
		requestID := contextx.TraceID(ctx)
		taskID, err := l.SendDocParseWordCount(ctx, doc, requestID, "")
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
		// 这里原先是有一个tx的，但是tx这个变量没有用到
		err = l.docDao.CreateDocParseTask(ctx, docParse)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "创建文档失败 err:%+v", err)
		return err
	}
	return nil
}

func (l *Logic) getFileParseModelConfig(ctx context.Context, corpId uint64, robot *entity.AppBaseInfo) (*common.FileParseModel, error) {
	logx.D(ctx, "getFileParseModelConfig (knowledgeBizId:%d)", robot.BizId)
	corp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, corpId)
	if err != nil {
		return nil, err
	}
	corpBizId := corp.GetCorpId()

	fileParseModel := &common.FileParseModel{}
	var kbConfig *kbe.KnowledgeConfig

	if robot.IsShared {
		kbConfigs, err := l.kbDao.GetShareKnowledgeConfigs(ctx, corpBizId, []uint64{robot.BizId},
			[]uint32{uint32(pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL)})
		if err != nil {
			logx.W(ctx, "ExtractEmbeddingModelOfKB (knowledgeBizId:%d) err:%+v",
				robot.BizId, err)
			return fileParseModel, err
		}
		if len(kbConfigs) > 0 {
			kbConfig = kbConfigs[0]
		}
	} else {
		kbConfigs, err := l.kbDao.DescribeAppKnowledgeConfig(ctx, corpBizId, robot.BizId, robot.BizId)
		if err != nil {
			logx.W(ctx, "ExtractEmbeddingModelOfKB (knowledgeBizId:%d) err:%+v",
				robot.BizId, err)
			return fileParseModel, err
		}
		if len(kbConfigs) > 0 {
			filteredKbConfigs := slicex.Filter(kbConfigs, func(item *kbe.KnowledgeConfig) bool {
				return item.Type == uint32(pb.KnowledgeBaseConfigType_FILE_PARSE_MODEL)
			})
			if len(filteredKbConfigs) > 0 {
				kbConfig = filteredKbConfigs[0]
			}
		}
	}

	fileParseModelConfigStr := ""
	if kbConfig != nil {
		// qa学习的过程在默认知识库一定是评测端的配置
		fileParseModelConfigStr = kbConfig.PreviewConfig
		if robot.IsShared {
			fileParseModelConfigStr = kbConfig.Config
		}
	}
	return l.ConvertStr2FileParseModelConfigItem(ctx, fileParseModelConfigStr)
}

func (l *Logic) ConvertStr2FileParseModelConfigItem(ctx context.Context, str string) (*common.FileParseModel, error) {
	logx.D(ctx, "---------------ConvertStr2FileParseModelConfigItem str:%s", str)
	res := &common.FileParseModel{}
	if str != "" && jsonx.Valid([]byte(str)) {
		err := jsonx.Unmarshal([]byte(str), res)
		if err != nil {
			logx.W(ctx, "ConvertStr2FileParseModelConfigItem jsonx.Unmarshal err: %+v", err)
			return nil, errs.ErrSystem
		}
	}
	if res.GetModelName() == "" {
		// 如果为空，直接获取默认的生成模型配置
		value, exist := modelType2DefaultConfigCache.Get(entity.ModelCategoryFileParse)
		if exist {
			rsp, ok := value.(*resource_gallery.GetDefaultModelConfigRsp)
			if !ok {
				logx.E(ctx, "get model(%s) info from local cache failed", entity.ModelCategoryFileParse)
				exist = false
			} else {
				logx.D(ctx, "get model(%s) info from local cache: %+v", entity.ModelCategoryFileParse, rsp)
				res.ModelName = rsp.GetModelName()
				res.AliasName = rsp.GetAliasName()
				res.ModelId = rsp.GetModelName()
			}
		}
		if !exist {
			rsp, err := l.rpc.Resource.GetDefaultModelConfig(ctx, entity.ModelCategoryFileParse)
			logx.D(ctx, "ConvertStr2FileParseModelConfigItem GetDefaultModelConfig rsp:%+v, err:%+v", rsp, err)
			if err != nil {
				logx.E(ctx, "ConvertStr2FileParseModelConfigItem GetModelInfoByModelName err: %+v", err)
				return nil, errs.ErrSystem
			} else {
				modelType2DefaultConfigCache.Set(entity.ModelCategoryFileParse, rsp)
				res.ModelName = rsp.GetModelName()
				res.AliasName = rsp.GetAliasName()
				res.ModelId = rsp.GetModelName()
			}
		}
	}
	return res, nil
}

func (l *Logic) getThirdModelConfig(ctx context.Context, corpId uint64, appBaseInfo *entity.AppBaseInfo) (*fileParseCommon.ThirdModelConfig, error) {
	fileParseModel, err := l.getFileParseModelConfig(ctx, corpId, appBaseInfo)
	if err != nil {
		return nil, err
	}
	logx.D(ctx, "fileParseModel:%+v", fileParseModel)
	thirdModelConfig := &fileParseCommon.ThirdModelConfig{
		ModelName: fileParseModel.AliasName,
		ModelId:   fileParseModel.ModelName,
	}
	for _, supportedFileType := range fileParseModel.SupportedFiles {
		thirdModelConfig.SupportedFiles = append(thirdModelConfig.SupportedFiles, &fileParseCommon.SupportedFileType{
			FileExt:      supportedFileType.FileExt,
			MaxSizeBytes: supportedFileType.MaxSizeBytes,
			Description:  supportedFileType.Description,
		})
	}
	docParseThirdParseConfigParam := &docEntity.DocParseThirdParseConfigParam{
		FormulaEnhancement: fileParseModel.FormulaEnhancement,
		LLMEnhancement:     fileParseModel.LargeLanguageModelEnhancement,
		EnhancementMode:    fileParseModel.EnhancementMode,
		OutputHtmlTable:    fileParseModel.OutputHtmlTable,
	}
	thirdModelConfig.Param, err = jsonx.MarshalToString(docParseThirdParseConfigParam)
	if err != nil {
		logx.E(ctx, "getThirdModelConfig Marshal err:%v", err)
		return nil, err
	}
	return thirdModelConfig, nil
}

// SendDocParseWordCount 文档提交解析统计字符数
func (l *Logic) SendDocParseWordCount(ctx context.Context, doc *docEntity.Doc,
	requestID string, originFileType string) (string, error) {
	appBaseInfo, err := l.rpc.AppAdmin.GetAppBaseInfoByPrimaryId(ctx, doc.RobotID)
	if err != nil {
		return "", err
	}
	if appBaseInfo == nil {
		return "", errs.ErrRobotNotFound
	}
	thirdModelConfig, err := l.getThirdModelConfig(ctx, doc.CorpID, appBaseInfo)
	if err != nil {
		// 如果获取不到第三方模型，柔性放过
		logx.E(ctx, "SendDocParseWordCount|getThirdModelConfig err:%v", err)
	}
	prefix := strings.TrimSuffix(doc.FileName, filepath.Ext(doc.FileName)) + ": \n"
	splitStrategy, err := config.App().RobotDefault.DocSplit.GetSplitStrategy(ctx, prefix, docEntity.DocSplitTypeDoc)
	if err != nil {
		return "", err
	}
	splitJSON, err := util.MergeJsonString(splitStrategy, doc.SplitRule)
	if err != nil {
		splitJSON = splitStrategy
		logx.W(ctx, "SendDocParseCreateSegment|MergeJsonString err:%v", err)
	}
	req := &fileManagerServer.TaskReq{
		RequestId: requestID,
		AppInfo: &fileManagerServer.AppInfo{
			AppKey: fmt.Sprintf("%d", appBaseInfo.BizId),
			Biz:    fileManagerBiz,
		},
		OpInfo: &fileManagerServer.TaskReq_OpInfo{
			FinalOpType:   docEntity.DocParseOpTypeSplit,
			CurrentOpType: docEntity.DocParseOpTypeWordCount,
		},
		ParseStrategy: fileManagerServer.TaskReq_ParseStrategy(config.App().RobotDefault.ParseStrategy),
		ParseSetting: &fileManagerServer.TaskReq_ParseSetting{
			ParseStrategy:    fileManagerServer.TaskReq_ParseStrategy(config.App().RobotDefault.ParseStrategy),
			ThirdModelConfig: thirdModelConfig,
		},
		SplitStrategy:   splitJSON,
		FCosUrl:         doc.CosURL,
		FMd5:            doc.CosHash,
		BRecallProgress: docEntity.BRecallProgressFalse,
		Priority:        docEntity.DocParseTaskNorMal,
		MetaData: &fileManagerServer.TaskReq_MetaData{
			Version: l.getOfflineFileManagerVersion(),
		},
	}
	if originFileType != "" {
		fileType := docEntity.ConvertFileTypeToFileManagerServerFileType(originFileType)
		req.ParseSetting.OriginFileType = fileType
	}
	if contextx.Metadata(ctx).Uin() == "" {
		// 文档解析需要在context里带上uin
		ctx = contextx.SetServerMetaData(ctx, contextx.MDUin, appBaseInfo.Uin)
	}
	rsp, err := l.rpc.FileManager.AddTask(ctx, req)
	logx.D(ctx, "[SendDocParseWordCount] Submit DocParse Task to count charSize. req:%+v, rsp:%+v", req, rsp)
	if err != nil {
		logx.E(ctx, "[SendDocParseWordCount] Failed to Submit DocParse Task to count charSize. err:%+v, req:%+v, rsp:%+v", err, req, rsp)
		return "", err
	}
	if rsp != nil && rsp.StatusCode != 0 {
		logx.E(ctx, "[SendDocParseWordCount] Failed to Submit DocParse Task to count charSize. rsp.StatusCode:%d, req:%+v, rsp:%+v", rsp.StatusCode, req, rsp)
		return "", errs.ErrCreateDocToIndexTaskFail
	}
	return rsp.TaskId, nil
}

// CreateDocToIndexTask 创建问答生成索引任务
func (l *Logic) CreateDocToIndexTask(ctx context.Context, doc *docEntity.Doc, originDocBizID uint64) error {
	if doc == nil {
		return errs.ErrRobotOrDocNotFound
	}
	if err := scheduler.NewDocToIndexTask(ctx, doc.RobotID, entity.DocToIndexParams{
		CorpID:                  doc.CorpID,
		StaffID:                 doc.StaffID,
		RobotID:                 doc.RobotID,
		DocID:                   doc.ID,
		InterveneOriginDocBizID: originDocBizID,
		ExpireStart:             doc.ExpireStart,
		ExpireEnd:               doc.ExpireEnd,
	}); err != nil {
		logx.E(ctx, "Failed to CreateDocToIndexTask. err:%+v", err)
		return err
	}
	return nil
}

// CreateDocRenameToIndexTask 创建文档重命名后重建向量索引任务
func (l *Logic) CreateDocRenameToIndexTask(ctx context.Context, doc *docEntity.Doc) error {
	if err := async.NewDocRenameToIndexTask(ctx, doc.RobotID, entity.DocRenameToIndexParams{
		CorpID:      doc.CorpID,
		StaffID:     doc.StaffID,
		RobotID:     doc.RobotID,
		DocID:       doc.ID,
		ExpireStart: doc.ExpireStart,
		ExpireEnd:   doc.ExpireEnd,
	}); err != nil {
		logx.E(ctx, "Rebuild index task after rename doc error. err:%+v", err)
		return err
	}
	return nil
}

// createAudit 创建单条送审
func (l *Logic) createAudit(ctx context.Context, p entity.AuditSendParams) error {
	if !config.AuditSwitch() {
		return nil
	}
	now := time.Now()
	audit := releaseEntity.NewParentAudit(p.CorpID, p.RobotID, p.StaffID, p.RelateID, 0, p.Type)
	audit.BusinessID = idgen.GetId()
	audit.UpdateTime = now
	audit.CreateTime = now

	id, err := l.releaseDao.CreateAudit(ctx, nil, audit)

	if err != nil {
		logx.E(ctx, "Failed to create audit data. err:%+v", err)
		return err
	}
	audit.ID = id
	p.ParentAuditBizID = audit.BusinessID
	return async.NewAuditSendTask(ctx, audit.RobotID, p)
}

// RenameDoc 文档重命名
func (l *Logic) RenameDoc(ctx context.Context, staffID uint64, app *entity.App, doc *docEntity.Doc) error {
	if config.FileAuditSwitch() {
		if err := l.createAudit(ctx, entity.AuditSendParams{
			CorpID:   app.CorpPrimaryId,
			RobotID:  app.PrimaryId,
			StaffID:  staffID,
			Type:     releaseEntity.AuditBizTypeDocName,
			RelateID: doc.ID,
			EnvSet:   contextx.Metadata(ctx).EnvSet(),
		}); err != nil {
			logx.E(ctx, "创建文档重命名送审任务失败 err:%+v", err)
			return err
		}
		doc.Status = docEntity.DocStatusDocNameAuditing
		doc.StaffID = staffID
	} else {
		// TODO: refact using docLogic
		if err := l.CreateDocRenameToIndexTask(ctx, doc); err != nil {
			logx.E(ctx, "新增向量重新入库任务失败 err:%+v", err)
			return err
		}
		doc.Status = docEntity.DocStatusCreatingIndex
		doc.StaffID = staffID
	}
	doc.UpdateTime = time.Now()

	/*
		`
			UPDATE
				t_doc
			SET
				file_name_in_audit = :file_name_in_audit,
				status = :status,
				staff_id = :staff_id
			WHERE
				id = :id
		`
	*/

	filter := &docEntity.DocFilter{
		RobotId: app.PrimaryId,
		ID:      doc.ID,
	}
	updateColumns := []string{
		docEntity.DocTblColFileNameInAudit,
		docEntity.DocTblColStatus,
		docEntity.DocTblColStaffId,
		docEntity.DocTblColUpdateTime,
	}

	if _, err := l.docDao.UpdateDoc(ctx, updateColumns, filter, doc); err != nil {
		logx.E(ctx, "Rename doc occur error, err: %+v", err)
		return err
	}
	logx.D(ctx, "Rename doc  success!args: %+v", doc)
	return nil
}

// GetDocDiff 获取相同的需要发起diff任务的doc
func (l *Logic) GetDocDiff(ctx context.Context, doc *docEntity.Doc) ([]*docEntity.Doc,
	error) {
	if doc == nil {
		return nil, nil
	}
	// diff的文档需要是 待发布、发布成功的状态
	status := []uint32{docEntity.DocStatusWaitRelease, docEntity.DocStatusReleaseSuccess}
	diffDocs := make([]*docEntity.Doc, 0)
	var err error
	if doc.Source == docEntity.SourceFromWeb {
		if strings.TrimSpace(doc.OriginalURL) == "" {
			return diffDocs, nil
		}
		filter := &docEntity.DocFilter{
			CorpId:           doc.CorpID,
			RobotId:          doc.RobotID,
			IsDeleted:        ptrx.Bool(false),
			Status:           status,
			OriginalURL:      doc.OriginalURL,
			FileTypes:        []string{doc.FileType},
			NotInBusinessIds: []uint64{doc.BusinessID}, // 查询的时候过滤掉当前文档,因为已经入库
		}
		diffDocs, err = l.docDao.GetDiffDocs(ctx, filter)
		if err != nil {
			logx.E(ctx, "Failed to GetDocDiffURL err: %+v", err)
			return diffDocs, err
		}
	}

	if doc.Source == docEntity.SourceFromFile {
		if strings.TrimSpace(doc.FileName) == "" {
			return diffDocs, nil
		}
		filter := &docEntity.DocFilter{
			CorpId:              doc.CorpID,
			RobotId:             doc.RobotID,
			IsDeleted:           ptrx.Bool(false),
			Status:              status,
			FileNameOrAuditName: doc.FileName,
			FileTypes:           []string{doc.FileType},
			NotInBusinessIds:    []uint64{doc.BusinessID}, // 查询的时候过滤掉当前文档,因为已经入库
		}
		diffDocs, err = l.docDao.GetDiffDocs(ctx, filter)
		if err != nil {
			logx.E(ctx, "Failed to GetDocDiffURL err: %+v", err)
			return diffDocs, err
		}
	}

	return diffDocs, nil

}

// GetDocDiffTaskDocs 获取对比任务的文档列表
func (l *Logic) GetDocDiffTaskDocs(ctx context.Context, filter *docEntity.DocFilter) (map[uint64]*docEntity.Doc, error) {
	beginTime := time.Now()
	offset := 0
	limit := docEntity.DocTableMaxPageSize
	allDocs := make([]*docEntity.Doc, 0)
	for {
		filter.Offset = offset
		filter.Limit = limit

		docs, err := l.docDao.GetDocList(ctx, docEntity.DocTblColList, filter)
		if err != nil {
			logx.E(ctx, "GetAllDocQas failed, err: %+v", err)
			return nil, err
		}
		allDocs = append(allDocs, docs...)
		if len(docs) < limit {
			// 已分页遍历完所有数据
			break
		}
		offset += limit
	}

	if len(allDocs) == 0 {
		return nil, nil
	}

	docMap := make(map[uint64]*docEntity.Doc)
	for _, doc := range allDocs {
		docMap[doc.BusinessID] = doc
	}

	logx.D(ctx, "GetDocDiffTaskDocs count:%d cost:%dms",
		len(allDocs), time.Since(beginTime).Milliseconds())
	return docMap, nil
}

func (l *Logic) GetDocCount(ctx context.Context, selectColumns []string, filter *docEntity.DocFilter) (int64, error) {
	tbl := l.docDao.Query().TDoc
	tableName := tbl.TableName()
	db, err := knowClient.GormClient(ctx, tableName, filter.RobotId, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return 0, err
	}
	return l.docDao.GetDocCountWithFilter(ctx, selectColumns, filter, db)
}

func (l *Logic) GetDocCountAndList(ctx context.Context, selectColumns []string, filter *docEntity.DocFilter) ([]*docEntity.Doc, int64, error) {
	return l.docDao.GetDocCountAndList(ctx, selectColumns, filter)
}

func (l *Logic) GetDocList(ctx context.Context, selectColumns []string, filter *docEntity.DocFilter) ([]*docEntity.Doc, error) {
	return l.docDao.GetDocList(ctx, selectColumns, filter)
}

// GetDocList 获取文档列表
func (l *Logic) GetDocListByListReq(ctx context.Context, req *docEntity.DocListReq) (uint64, []*docEntity.Doc, error) {
	logx.I(ctx, "GetDocList, req: %+v", req)
	docs := make([]*docEntity.Doc, 0)
	docIds := make([]uint64, 0)
	notDocIds := make([]uint64, 0)
	var err error
	fileNameSubStrOrAuditNameSubStr := ""
	if req.FileName != "" {
		if req.QueryType == docEntity.DocQueryTypeAttribute && req.FileName != docEntity.DocQuerySystemTypeUntagged {
			// 属性标签名检索
			docIds, err = l.labelLogic.GetDocIdsByAttrSubStr(ctx, req.RobotID, req.FileName)
			if err != nil {
				logx.E(ctx, "GetDocList failed, err: %+v", err)
				return 0, docs, err
			}
			if len(docIds) == 0 {
				logx.I(ctx, "GetDocIdsByAttrSubStr, no doc found")
				return 0, docs, nil
			}
		}
		if req.QueryType == docEntity.DocQueryTypeFileName {
			// 文件名检索
			fileNameSubStrOrAuditNameSubStr = req.FileName
		}
		if req.FileName == docEntity.DocQuerySystemTypeUntagged {
			// 已有标签的文档id
			notDocIds, err = l.labelLogic.GetDocIdsByAttr(ctx, req.RobotID)
			if err != nil {
				logx.E(ctx, "GetDocIdsByAttr failed, err: %+v", err)
				return 0, docs, err
			}
			logx.I(ctx, "GetDocIdsByAttr|notDocIds:%+v", notDocIds)
			// 查询没有标签的文档,不支持FileName查询
			fileNameSubStrOrAuditNameSubStr = ""
		}
	}

	expandStatus(req)
	offset, limit := utilx.Page(req.Page, req.PageSize)
	docFilter := &docEntity.DocFilter{
		CorpId:                          req.CorpID,
		RobotId:                         req.RobotID,
		IDs:                             docIds,
		FileNameSubStrOrAuditNameSubStr: fileNameSubStrOrAuditNameSubStr,
		FileTypes:                       req.FileTypes,
		FilterFlag:                      req.FilterFlag,
		ValidityStatus:                  req.ValidityStatus,
		Status:                          req.Status,
		Opts:                            req.Opts,
		CategoryIds:                     convx.SliceUint64ToUint32(req.CateIDs),
		IsDeleted:                       ptrx.Bool(false),
		Offset:                          offset,
		Limit:                           limit,
		OrderColumn:                     []string{docEntity.DocTblColCreateTime, docEntity.DocTblColId},
		OrderDirection:                  []string{util.SqlOrderByDesc, util.SqlOrderByDesc},
		NotInIDs:                        notDocIds,
		EnableScope:                     req.EnableScope,
	}
	docs, total, err := l.docDao.GetDocCountAndList(ctx, docEntity.DocTblColList, docFilter)
	if err != nil {
		logx.E(ctx, "GetDocList failed, err: %+v", err)
		return 0, docs, err
	}
	for i := range docs {
		if docs[i].Status == docEntity.DocStatusUpdating {
			docs[i].Status = docEntity.DocStatusCreatingIndex
		} else if docs[i].Status == docEntity.DocStatusUpdateFail {
			docs[i].Status = docEntity.DocStatusCreateIndexFail
		}
	}
	return uint64(total), docs, nil
}

func expandStatus(req *docEntity.DocListReq) {
	if len(req.Status) > 0 {
		status := slicex.Unique(req.Status)
		for _, stat := range status {
			switch stat {
			case docEntity.DocStatusUpdating:
				// 更新中合并到学习中
				req.Status = append(req.Status, docEntity.DocStatusCreatingIndex)
			case docEntity.DocStatusUpdateFail:
				// 更新失败合并到学习失败
				req.Status = append(req.Status, docEntity.DocStatusCreateIndexFail)
			case docEntity.DocStatusAuditFail:
				// 扩展审核失败的过滤条件
				req.Status = append(req.Status, docEntity.DocStatusDocNameAndContentAuditFail,
					docEntity.DocStatusImportDocNameAuditFail)
			}
		}
		req.Status = slicex.Unique(req.Status)
	}
}

// GetDocParseResUrl 获取文档解析md结果地址
func (l *Logic) GetDocParseResUrl(ctx context.Context, docId uint64, robotID uint64) (string, error) {
	docParse, err := l.GetDocParseByDocIDAndTypeAndStatus(ctx, docId, docEntity.DocParseTaskTypeSplitSegment,
		docEntity.DocParseSuccess, robotID)
	if err != nil {
		if errors.Is(err, errs.ErrDocParseTaskNotFound) {
			logx.W(ctx, "GetDocParseResUrl failed, err: %+v", err)
			return "", nil
		}
		return "", err
	}
	result := &pb.FileParserCallbackReq{}
	err = jsonx.UnmarshalFromString(docParse.Result, result)
	if err != nil {
		logx.E(ctx, "getDocParseContent|jsonx.UnmarshalFromString failed, err:%+v", err)
		return "", err
	}
	logx.I(ctx, "getDocParseContent|file parse result:%+v", result)
	resultDataMap := result.GetResults()
	docParseRes := resultDataMap[int32(pb.FileParserSubDataType_FILE_PARSER_SUB_DATA_TYPE_PARSE)]
	// fileData := ""
	parseUrl := ""
	for _, res := range docParseRes.GetResult() {
		// 暂不返回文件内容，防止文件过大
		// data, err := dao.GetFileDataFromCosURL(ctx, res.GetResult())
		// if err != nil {
		// return "", err
		// }
		// fileData += data
		parseUrl, err = l.s3.GetPreSignedURLWithTypeKey(ctx, entity.OfflineStorageTypeKey, res.GetResult(), 0)
		if err != nil {
			return "", err
		}
	}
	return parseUrl, nil
}

// UpdateDocStatus 更新文档状态信息
func (l *Logic) UpdateDocStatus(ctx context.Context, doc *docEntity.Doc) error {
	/*
		`
			UPDATE
			    t_doc
			SET
			    status = :status, update_time = :update_time
			WHERE
			    id = :id
		`
	*/

	db, err := knowClient.GormClient(ctx, l.getDocTableName(), doc.RobotID, 0, []client.Option{}...)

	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}

	doc.UpdateTime = time.Now()

	updatedColumns := []string{
		docEntity.DocTblColStatus,
		docEntity.DocTblColUpdateTime,
	}
	updateDocFilter := &docEntity.DocFilter{
		ID: doc.ID,
	}
	if _, err := l.docDao.UpdateDocByTx(ctx, updatedColumns, updateDocFilter, doc, db); err != nil {
		logx.E(ctx, "Failed to UpdateDocStatus. args:%+v err:%+v", doc, err)
		return err
	}

	return nil
}

// UpdateDocStatusAndUpdateTime 更新文档状态状态,指定更新时间
func (l *Logic) UpdateDocStatusAndUpdateTime(ctx context.Context, doc *docEntity.Doc) error {
	return l.UpdateDocStatus(ctx, doc)
}

// UpdateDocStatusAndCharSize 更新文档状态和字符大小
func (l *Logic) UpdateDocStatusAndCharSize(ctx context.Context, doc *docEntity.Doc) error {
	/*
		`
			UPDATE
			    t_doc
			SET
			    char_size = :char_size, message = :message, audit_flag = :audit_flag, status = :status,
				update_time = :update_time, staff_id = :staff_id
			WHERE
			    id = :id
		`
	*/
	doc.UpdateTime = time.Now()

	db, err := knowClient.GormClient(ctx, l.getDocTableName(), doc.RobotID, 0, []client.Option{}...)

	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}

	doc.UpdateTime = time.Now()

	updatedColumns := []string{
		docEntity.DocTblColStatus,
		docEntity.DocTblColUpdateTime,
		docEntity.DocTblColCharSize,
		docEntity.DocTblColMessage,
		docEntity.DocTblColAuditFlag,
		docEntity.DocTblColStaffId,
	}
	updateDocFilter := &docEntity.DocFilter{
		ID: doc.ID,
	}
	if _, err := l.docDao.UpdateDocByTx(ctx, updatedColumns, updateDocFilter, doc, db); err != nil {
		logx.E(ctx, "Failed to UpdateDocStatusAndCharSize. args:%+v err:%+v", doc, err)
		return err
	}

	return nil
}

// ReferDoc 答案中是否引用
func (l *Logic) ReferDoc(ctx context.Context, doc *docEntity.Doc) error {
	/*
		`
			UPDATE
			    t_doc
			SET
			    is_refer = :is_refer, update_time = :update_time
			WHERE
			    id = :id
		`
	*/
	doc.UpdateTime = time.Now()

	db, err := knowClient.GormClient(ctx, l.getDocTableName(), doc.RobotID, 0, []client.Option{}...)

	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}

	updatedColumns := []string{
		docEntity.DocTblColIsRefer,
		docEntity.DocTblColUpdateTime,
	}

	updateDocFilter := &docEntity.DocFilter{
		ID: doc.ID,
	}
	if _, err := l.docDao.UpdateDocByTx(ctx, updatedColumns, updateDocFilter, doc, db); err != nil {
		logx.E(ctx, "Failed to ReferDoc. args:%+v err:%+v", doc, err)
		return err
	}

	return nil
}

// DeleteDocSuccess 删除文档任务成功
func (l *Logic) DeleteDocSuccess(ctx context.Context, doc *docEntity.Doc) error {
	now := time.Now()

	db, err := knowClient.GormClient(ctx, l.getDocTableName(), doc.RobotID, 0, []client.Option{}...)

	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		/*
				`
				UPDATE
					t_doc
				SET
				    status = :status,
				    next_action = :next_action,
				    update_time = :update_time
				WHERE
				    id = :id
			`
		*/
		updatedColumns := []string{
			docEntity.DocTblColStatus,
			docEntity.DocTblColNextAction,
			docEntity.DocTblColUpdateTime,
		}

		doc.Status = docEntity.DocStatusDeleted
		if !doc.IsNextActionAdd() {
			doc.NextAction = docEntity.DocNextActionDelete
			doc.Status = docEntity.DocStatusWaitRelease
		}
		doc.UpdateTime = now

		updateDocFilter := &docEntity.DocFilter{
			ID: doc.ID,
		}
		if _, err := l.docDao.UpdateDocByTx(ctx, updatedColumns, updateDocFilter, doc, db); err != nil {
			logx.E(ctx, "Failed to UpdateCosInfo. args:%+v err:%+v", doc, err)
			return err
		}

		return nil

	}); err != nil {
		logx.E(ctx, "Failed to DeleteDocSuccess. err:%+v", err)
		return err
	}
	return nil
}

// UpdateCosInfo 更新cos信息
func (l *Logic) UpdateCosInfo(ctx context.Context, doc *docEntity.Doc) error {
	/*
			`
				UPDATE
					t_doc
				SET
				    file_name = :file_name,
				    cos_url = :cos_url,
		            file_type = :file_type,
		            cos_hash = :cos_hash,
				    file_size = :file_size,
				    update_time = :update_time
				WHERE
				    id = :id
			`
	*/
	doc.UpdateTime = time.Now()

	db, err := knowClient.GormClient(ctx, l.getDocTableName(), doc.RobotID, 0, []client.Option{}...)

	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}

	updatedColumns := []string{
		docEntity.DocTblColFileName,
		docEntity.DocTblColCosURL,
		docEntity.DocTblColFileType,
		docEntity.DocTblColCosHash,
		docEntity.DocTblColFileSize,
		docEntity.DocTblColUpdateTime,
	}

	updateDocFilter := &docEntity.DocFilter{
		ID: doc.ID,
	}
	if _, err := l.docDao.UpdateDocByTx(ctx, updatedColumns, updateDocFilter, doc, db); err != nil {
		logx.E(ctx, "Failed to UpdateCosInfo. args:%+v err:%+v", doc, err)
		return err
	}

	return nil
}

// BatchUpdateDoc 批量应用链接，过期时间
func (l *Logic) BatchUpdateDoc(ctx context.Context, staffID uint64, docs []*docEntity.Doc,
	isNeedPublishMap map[uint64]int) error {
	if len(docs) == 0 {
		return nil
	}
	db, err := knowClient.GormClient(ctx, l.getDocTableName(), docs[0].RobotID, 0, []client.Option{}...)

	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		for _, doc := range docs {
			/*
					`
				UPDATE
					t_doc
				SET
					status = :status,
					is_refer = :is_refer,
					next_action = :next_action,
					attr_range = :attr_range,
					refer_url_type = :refer_url_type,
					web_url = :web_url,
					category_id = :category_id,
					expire_start = :expire_start,
				    expire_end = :expire_end,
					customer_knowledge_id = :customer_knowledge_id,
					attribute_flag = :attribute_flag,
					is_downloadable = :is_downloadable,
					staff_id = :staff_id,
					update_period_h = :update_period_h,
					next_update_time = :next_update_time,
					split_rule = :split_rule
				WHERE
					id = :id
			*/
			updatedColumns := []string{
				docEntity.DocTblColStatus,
				docEntity.DocTblColIsRefer,
				docEntity.DocTblColNextAction,
				docEntity.DocTblColAttrRange,
				docEntity.DocTblColReferURLType,
				docEntity.DocTblColWebURL,
				docEntity.DocTblColCategoryId,
				docEntity.DocTblColExpireStart,
				docEntity.DocTblColExpireEnd,
				docEntity.DocTblColCustomerKnowledgeId,
				docEntity.DocTblColIsDownloadable,
				docEntity.DocTblColUpdatePeriodH,
				docEntity.DocTblColNextUpdateTime,
				docEntity.DocTblColSplitRule,
				docEntity.DocTblColAttributeFlag,
				docEntity.DocTblColStaffId,
				docEntity.DocTblColEnableScope,
			}
			updateDocFilter := &docEntity.DocFilter{
				IDs: []uint64{doc.ID},
			}
			if _, err := l.docDao.UpdateDocByTx(ctx, updatedColumns, updateDocFilter, doc, tx); err != nil {
				logx.E(ctx, "Failed to update doc. args:%+v err:%+v", doc, err)
				return err
			}

			_, ok := isNeedPublishMap[doc.ID]
			if !ok {
				continue
			}

			if err := l.sendDocModifyNotice(ctx, staffID, doc, docEntity.DocUpdatingNoticeContent,
				releaseEntity.LevelInfo); err != nil {
				return err
			}
			if err := scheduler.NewDocModifyTask(ctx, doc.RobotID, entity.DocModifyParams{
				CorpID:      doc.CorpID,
				StaffID:     staffID,
				RobotID:     doc.RobotID,
				DocID:       doc.ID,
				EnableScope: doc.EnableScope,
				ExpireStart: doc.ExpireStart,
				ExpireEnd:   doc.ExpireEnd,
			}); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		logx.E(ctx, "更新文档失败 err:%+v", err)
		return err
	}
	return nil
}

// UpdateDoc 更新doc
func (l *Logic) UpdateDoc(ctx context.Context, staffID uint64, doc *docEntity.Doc, isNeedPublish bool,
	attributeLabelReq *label.UpdateDocAttributeLabelReq) error {
	gormClient, err := knowClient.GormClient(ctx, docEntity.DocTableName, doc.RobotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "UpdateDoc get GormClient err:%v,robotID:%v", err, doc.RobotID)
		return err
	}
	err = gormClient.Transaction(func(tx *gorm.DB) error {
		/*
				`
				UPDATE
					t_doc
				SET
					status = :status,
					is_refer = :is_refer,
					next_action = :next_action,
					attr_range = :attr_range,
					refer_url_type = :refer_url_type,
					web_url = :web_url,
					category_id = :category_id,
					expire_start = :expire_start,
				    expire_end = :expire_end,
					customer_knowledge_id = :customer_knowledge_id,
					attribute_flag = :attribute_flag,
					is_downloadable = :is_downloadable,
					staff_id = :staff_id,
					update_period_h = :update_period_h,
					next_update_time = :next_update_time,
					split_rule = :split_rule
				WHERE
					id = :id
			`
		*/
		updatedColumns := []string{
			docEntity.DocTblColStatus,
			docEntity.DocTblColIsRefer,
			docEntity.DocTblColNextAction,
			docEntity.DocTblColAttrRange,
			docEntity.DocTblColReferURLType,
			docEntity.DocTblColWebURL,
			docEntity.DocTblColCategoryId,
			docEntity.DocTblColExpireStart,
			docEntity.DocTblColExpireEnd,
			docEntity.DocTblColCustomerKnowledgeId,
			docEntity.DocTblColIsDownloadable,
			docEntity.DocTblColUpdatePeriodH,
			docEntity.DocTblColNextUpdateTime,
			docEntity.DocTblColSplitRule,
			docEntity.DocTblColAttributeFlag,
			docEntity.DocTblColStaffId,
			docEntity.DocTblColEnableScope,
		}
		updateDocFilter := &docEntity.DocFilter{
			IDs: []uint64{doc.ID},
		}
		if _, err := l.docDao.UpdateDocByTx(ctx, updatedColumns, updateDocFilter, doc, tx); err != nil {
			logx.E(ctx, "Failed to update doc. args:%+v err:%+v", doc, err)
			return err
		}
		if err := l.labelDao.UpdateDocAttributeLabelByTx(ctx, doc.RobotID, doc.ID, attributeLabelReq, tx); err != nil {
			return err
		}
		if !isNeedPublish {
			return nil
		}
		if err := l.sendDocModifyNotice(ctx, staffID, doc, docEntity.DocUpdatingNoticeContent,
			releaseEntity.LevelInfo); err != nil {
			return err
		}
		if err := scheduler.NewDocModifyTask(ctx, doc.RobotID, entity.DocModifyParams{
			CorpID:      doc.CorpID,
			StaffID:     staffID,
			RobotID:     doc.RobotID,
			DocID:       doc.ID,
			EnableScope: doc.EnableScope,
			ExpireStart: doc.ExpireStart,
			ExpireEnd:   doc.ExpireEnd,
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		logx.E(ctx, "Failed to update doc. err:%+v", err)
		return err
	}
	return nil
}

// CreateDocQADone 文档生成QA完成
func (l *Logic) CreateDocQADone(ctx context.Context, staffID uint64, doc *docEntity.Doc, qaCount int, success bool) error {
	logx.I(ctx, "CreateDocQADone doc:%+v qaCount:%d success:%t", doc, qaCount, success)
	db, err := knowClient.GormClient(ctx, l.getDocTableName(), doc.RobotID, 0, []client.Option{}...)

	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		repeatStr := ""
		if doc.IsCreatedQA {
			repeatStr = i18n.Translate(ctx, i18nkey.KeyAgain)
		}
		pageID := releaseEntity.NoticeQAPageID
		level := releaseEntity.LevelError
		subject := i18n.Translate(ctx, i18nkey.KeyQAGenerateFailureWithParam, repeatStr)
		content := i18n.Translate(ctx, i18nkey.KeyQAGenerateFailureWithNameAndQA, doc.GetRealFileName(), repeatStr)
		operations := []releaseEntity.Operation{{Type: releaseEntity.OpTypeViewDetail, Params: releaseEntity.OpParams{}}}
		if qaCount == 0 {
			subject = i18n.Translate(ctx, i18nkey.KeyFileContentTooLittleNoQAGenerate)
			content = i18n.Translate(ctx, i18nkey.KeyFileContentTooLittleNoQAGenerateWithName, doc.GetRealFileName())
			level = releaseEntity.LevelWarning
		}
		if doc.IsBatchImport() && doc.IsExcel() {
			subject = i18n.Translate(ctx, i18nkey.KeyQATemplateImportFailure)
			content = i18n.Translate(ctx, i18nkey.KeyTemplateImportFailureWithName, doc.GetRealFileName())
		}
		if success {
			subject = i18n.Translate(ctx, i18nkey.KeyQAGenerateCompleteWithParam, repeatStr)
			content = i18n.Translate(ctx, i18nkey.KeyQAGenerateCompleteWithNameAndQA, doc.GetRealFileName(), repeatStr)
			if doc.IsBatchImport() && doc.IsExcel() {
				subject = i18n.Translate(ctx, i18nkey.KeyQATemplateImportSuccess)
				content = i18n.Translate(ctx, i18nkey.KeyQATemplateImportSuccessWithName, doc.GetRealFileName())
			} else {
				// 生成QA才需要去校验的按钮，批量导入的不需要
				operations = append(operations, releaseEntity.Operation{Type: releaseEntity.OpTypeVerifyDocQA, Params: releaseEntity.OpParams{
					CosPath:  doc.CosURL,
					DocBizID: strconv.FormatUint(doc.BusinessID, 10),
				}})
			}
			level = releaseEntity.LevelSuccess
			doc.IsCreatedQA = true
		}
		/*
				`
				UPDATE
					t_doc
				SET
				    message = :message,
				    is_deleted = :is_deleted,
				    is_creating_qa = :is_creating_qa,
				    is_created_qa = :is_created_qa,
				    update_time = :update_time
				WHERE
				    id = :id
			`
		*/

		updatedColumns := []string{
			docEntity.DocTblColMessage,
			docEntity.DocTblColIsDeleted,
			docEntity.DocTblColIsCreatingQa,
			docEntity.DocTblColIsCreatedQa,
			docEntity.DocTblColUpdateTime,
			docEntity.DocTblColProcessingFlag,
		}
		updateDocFilter := &docEntity.DocFilter{
			IDs: []uint64{doc.ID},
		}

		if _, err := l.docDao.UpdateDocByTx(ctx, updatedColumns, updateDocFilter, doc, tx); err != nil {
			logx.E(ctx, "Failed to Update doc to CreateDocQADone args:%+v err:%+v", doc, err)
			return err
		}

		noticeOptions := []releaseEntity.NoticeOption{
			releaseEntity.WithGlobalFlag(),
			releaseEntity.WithPageID(pageID),
			releaseEntity.WithLevel(level),
			releaseEntity.WithSubject(subject),
			releaseEntity.WithContent(content),
		}
		notice := releaseEntity.NewNotice(releaseEntity.NoticeTypeDocToQA, doc.ID, doc.CorpID, doc.RobotID, staffID, noticeOptions...)
		if err := notice.SetOperation(operations); err != nil {
			logx.E(ctx, "序列化通知操作参数失败 operations:%+v err:%+v", operations, err)
			return err
		}
		if err := l.rpc.PlatformAdmin.CreateNotice(ctx, notice); err != nil {
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "文档生成问答完成失败 err:%+v", err)
		return err
	}
	return nil
}

// UpdateDocAttrRange 更新doc适用范围
func (l *Logic) UpdateDocAttrRange(ctx context.Context, staffID uint64, docs []*docEntity.Doc,
	attributeLabelReq *labelEntity.UpdateDocAttributeLabelReq) error {
	if len(docs) == 0 {
		return nil
	}
	now := time.Now()
	db, err := knowClient.GormClient(ctx, l.getDocTableName(), docs[0].RobotID, 0, []client.Option{}...)

	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		/*
					`
				UPDATE
					t_doc
				SET
					status = :status,
					is_refer = :is_refer,
					next_action = :next_action,
					attr_range = :attr_range,
					refer_url_type = :refer_url_type,
					web_url = :web_url,
					category_id = :category_id,
					expire_start = :expire_start,
				    expire_end = :expire_end,
					customer_knowledge_id = :customer_knowledge_id,
					attribute_flag = :attribute_flag,
					is_downloadable = :is_downloadable,
					staff_id = :staff_id,
					update_period_h = :update_period_h,
					next_update_time = :next_update_time,
					split_rule = :split_rule
				WHERE
					id = :id
			`
		*/
		for _, doc := range docs {

			doc.UpdateTime = now

			updatedColumns := []string{
				docEntity.DocTblColStatus,
				docEntity.DocTblColIsRefer,
				docEntity.DocTblColNextAction,
				docEntity.DocTblColAttrRange,
				docEntity.DocTblColReferURLType,
				docEntity.DocTblColWebURL,
				docEntity.DocTblColCategoryId,
				docEntity.DocTblColExpireStart,
				docEntity.DocTblColExpireEnd,
				docEntity.DocTblColCustomerKnowledgeId,
				docEntity.DocTblColIsDownloadable,
				docEntity.DocTblColUpdatePeriodH,
				docEntity.DocTblColNextUpdateTime,
				docEntity.DocTblColSplitRule,
				docEntity.DocTblColAttributeFlag,
				docEntity.DocTblColStaffId,
			}
			updateDocFilter := &docEntity.DocFilter{
				IDs: []uint64{doc.ID},
			}
			if _, err := l.docDao.UpdateDocByTx(ctx, updatedColumns, updateDocFilter, doc, tx); err != nil {
				logx.E(ctx, "Failed to Update Doc. args:%+v err:%+v", doc, err)
				return err
			}

			if err := l.labelDao.UpdateDocAttributeLabelByTx(ctx, doc.RobotID, doc.ID, attributeLabelReq, tx); err != nil {
				return err
			}

			if err := l.sendDocModifyNotice(ctx, staffID, doc, docEntity.DocUpdatingNoticeContent,
				releaseEntity.LevelInfo); err != nil {
				return err
			}
			if err := scheduler.NewDocModifyTask(ctx, doc.RobotID, entity.DocModifyParams{
				CorpID:      doc.CorpID,
				StaffID:     staffID,
				RobotID:     doc.RobotID,
				DocID:       doc.ID,
				EnableScope: doc.EnableScope,
				ExpireStart: doc.ExpireStart,
				ExpireEnd:   doc.ExpireEnd,
			}); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		logx.E(ctx, "更新文档失败 err:%+v", err)
		return err
	}
	return nil
}

// UpdateCreatingIndexFlag 更新索引生成中标记
func (l *Logic) UpdateCreatingIndexFlag(ctx context.Context, doc *docEntity.Doc) error {
	/*
		`
				UPDATE
					t_doc
				SET
				    batch_id = :batch_id,
					status = :status,
				    is_creating_index = :is_creating_index,
				    update_time = :update_time
				WHERE
				    id = :id
	*/
	doc.UpdateTime = time.Now()
	db, err := knowClient.GormClient(ctx, l.getDocTableName(), doc.RobotID, 0, []client.Option{}...)

	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}
	docFilter := &docEntity.DocFilter{
		ID: doc.ID,
	}

	updateColumns := []string{
		docEntity.DocTblColBatchId,
		docEntity.DocTblColStatus,
		docEntity.DocTblColIsCreatingIndex,
		docEntity.DocTblColUpdateTime,
	}

	if _, err := l.docDao.UpdateDocByTx(ctx, updateColumns, docFilter, doc, db); err != nil {
		logx.E(ctx, "UpdateCreatingIndexFlag failed. err:%+v", doc, err)
		return err
	}
	return nil
}

// UpdateDocNameAndStatus 更新文档名称,状态以及索引生成中标记
func (l *Logic) UpdateDocNameAndStatus(ctx context.Context, doc *docEntity.Doc) error {
	/*
		`
			UPDATE
				t_doc
			SET
				file_name = :file_name,
				file_name_in_audit = :file_name_in_audit,
				status = :status,
				next_action = :next_action,
			    is_creating_index = :is_creating_index,
			    update_time = :update_time
			WHERE
			    id = :id
		`
	*/
	doc.UpdateTime = time.Now()
	db, err := knowClient.GormClient(ctx, l.getDocTableName(), doc.RobotID, 0, []client.Option{}...)

	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}

	docFilter := &docEntity.DocFilter{
		ID: doc.ID,
	}

	updateColumns := []string{
		docEntity.DocTblColFileName,
		docEntity.DocTblColFileNameInAudit,
		docEntity.DocTblColStatus,
		docEntity.DocTblColNextAction,
		docEntity.DocTblColIsCreatingIndex,
		docEntity.DocTblColUpdateTime,
	}

	if _, err := l.docDao.UpdateDocByTx(ctx, updateColumns, docFilter, doc, db); err != nil {
		logx.E(ctx, "UpdateDocNameAndStatus failed. err:%+v", doc, err)
		return err
	}
	return nil
}

// UpdateCreatingQAFlag 更新问答生成中标记
func (l *Logic) UpdateCreatingQAFlag(ctx context.Context, doc *docEntity.Doc) error {
	/*
		 `
			UPDATE
				t_doc
			SET
			    batch_id = :batch_id,
			    is_creating_qa = :is_creating_qa,
			    update_time = :update_time
			WHERE
			    id = :id
		`
	*/
	doc.UpdateTime = time.Now()
	db, err := knowClient.GormClient(ctx, l.getDocTableName(), doc.RobotID, 0, []client.Option{}...)

	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}

	docFilter := &docEntity.DocFilter{
		ID: doc.ID,
	}

	updateColumns := []string{
		docEntity.DocTblColBatchId,
		docEntity.DocTblColIsCreatingQa,
		docEntity.DocTblColUpdateTime,
	}

	if _, err := l.docDao.UpdateDocByTx(ctx, updateColumns, docFilter, doc, db); err != nil {
		logx.E(ctx, "UpdateCreatingQAFlag failed. err:%+v", doc, err)
		return err
	}
	return nil
}

func (l *Logic) UpdateDocAuditResult(ctx context.Context, doc *docEntity.Doc) error {
	/*
		updateDocAuditResult = `
			UPDATE
				t_doc
			SET
			    status = :status,
			    message = :message,
			    audit_flag = :audit_flag,
			    update_time = :update_time
			WHERE
			    id = :id
		`
	*/

	docFilter := &docEntity.DocFilter{
		IDs: []uint64{doc.ID},
	}
	updateColumns := []string{docEntity.DocTblColStatus, docEntity.DocTblColMessage, docEntity.DocTblColAuditFlag}
	if _, err := l.docDao.UpdateDoc(ctx, updateColumns, docFilter, doc); err != nil {
		logx.E(ctx, "Update doc audit result failed. err:%+v", doc, err)
		return err
	}
	return nil
}

// UpdateDocDisableState 更新文档停用启用状态
func (l *Logic) UpdateDocDisableState(ctx context.Context, staffID uint64, doc *docEntity.Doc, isDisable bool) error {
	doc.StaffID = staffID
	logx.I(ctx, "UpdateDocDisableState doc.AttributeFlag:%v", doc.AttributeFlag)
	err := l.docDao.Query().TDoc.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		updateDocFilter := &docEntity.DocFilter{
			RobotId: doc.RobotID,
			ID:      doc.ID,
		}
		if _, err := l.docDao.UpdateDoc(ctx, []string{docEntity.DocTblColAttributeFlag,
			docEntity.DocTblColStaffId, docEntity.DocTblColStatus, docEntity.DocTblColNextAction}, updateDocFilter, doc); err != nil {
			logx.E(ctx, "Failed to update doc state. args:%+v err:%+v", doc, err)
			return err
		}
		if err := l.sendDocModifyNotice(ctx, staffID, doc, docEntity.DocUpdatingNoticeContent,
			releaseEntity.LevelInfo); err != nil {
			return err
		}
		if err := scheduler.NewDocModifyTask(ctx, doc.RobotID, entity.DocModifyParams{
			CorpID:      doc.CorpID,
			StaffID:     staffID,
			RobotID:     doc.RobotID,
			DocID:       doc.ID,
			EnableScope: doc.EnableScope,
			ExpireStart: doc.ExpireStart,
			ExpireEnd:   doc.ExpireEnd,
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		logx.E(ctx, "更新文档状态失败 err:%+v", err)
		return err
	}
	return nil
}

// UpdateDocStatusMachineByEvent 更新文档，如果涉及到status的变更，会通过有限状态机校验
func (l *Logic) UpdateDocStatusMachineByEvent(ctx context.Context, updateColumns []string, filter *docEntity.DocFilter,
	doc *docEntity.Doc, event string) error {
	logx.I(ctx, "UpdateDocStatusMachineByEvent updateColumns:%v, filter:%v, doc:%v, event:%v",
		updateColumns, filter, doc, event)

	var err error
	updateStatusFlag := false
	for _, col := range updateColumns {
		// 先判断是否需要更新状态
		if col == docEntity.DocTblColStatus {
			updateStatusFlag = true
			break
		}
	}

	// 需要先初始化状态机
	doc.Init()
	fromStatus, err := convx.StringToInt32(doc.FSM.Current())
	if err != nil {
		logx.E(ctx, "UpdateDoc failed, err: %+v", err)
		return err
	}
	if updateStatusFlag {
		// 强制增加当前状态作为过滤条件，乐观锁
		filter.Status = []uint32{uint32(fromStatus)}

		if event == "" {
			err = errors.New("UpdateDoc failed, FSM event is empty")
			logx.E(ctx, "%+v", err)
			return err
		}

		// 状态变更，需要通过有限状态机校验，校验通过后会自动更新doc结构体中的状态
		// 【注意】这里会覆盖doc中传过来的状态，更新成状态机里限定的状态
		err = doc.FSM.Event(ctx, event)
		if err != nil {
			logx.E(ctx, "UpdateDoc failed, FSM err: %+v", err)
			return err
		}

	}

	rowsAffected, err := l.UpdateLogicByDao(ctx, updateColumns, filter, doc)
	if err != nil {
		logx.E(ctx, "UpdateDoc failed, err: %+v", err)
		// 如果更新失败，需要回滚文档结构体状态，状态机每次都会根据文档状态重新初始化，不需要回滚
		doc.Status = uint32(fromStatus)
		return err
	}
	if rowsAffected == 0 {
		err = errors.New("UpdateDoc failed, rowsAffected is 0")
		logx.E(ctx, "%+v", err)
		// 如果更新失败，需要回滚文档结构体状态，状态机每次都会根据文档状态重新初始化，不需要回滚
		doc.Status = uint32(fromStatus)
		return err
	}

	return nil
}

func (l *Logic) UpdateLogicByDao(ctx context.Context, updateColumns []string, filter *docEntity.DocFilter,
	doc *docEntity.Doc) (int64, error) {
	return l.docDao.UpdateDoc(ctx, updateColumns, filter, doc)
}

// ModifyDocSuccess 更新文档任务成功
func (l *Logic) ModifyDocSuccess(ctx context.Context, doc *docEntity.Doc, staffID uint64) error {
	now := time.Now()
	db, err := knowClient.GormClient(ctx, l.getDocTableName(), doc.RobotID, 0, []client.Option{}...)

	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		/*
				`
				UPDATE
					t_doc
				SET
				    status = :status,
				    next_action = :next_action,
				    update_time = :update_time
				WHERE
				    id = :id
			`
		*/

		doc.Status = docEntity.DocStatusWaitRelease
		if !doc.IsNextActionAdd() {
			doc.NextAction = docEntity.DocNextActionUpdate
		}
		doc.UpdateTime = now

		updateColumns := []string{docEntity.DocTblColStatus, docEntity.DocTblColNextAction, docEntity.DocTblColUpdateTime}
		updateDocFilter := &docEntity.DocFilter{
			ID: doc.ID,
		}

		if _, err := l.docDao.UpdateDocByTx(ctx, updateColumns, updateDocFilter, doc, tx); err != nil {
			logx.E(ctx, "Failed to ModifyDocSuccess.  args:%+v err:%+v", doc, err)
			return err

		}

		if err := l.sendDocModifyNotice(ctx, staffID, doc, docEntity.DocUpdateSuccessNoticeContent,
			releaseEntity.LevelSuccess); err != nil {
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "更新文档任务success失败 err:%+v", err)
		return err
	}
	return nil
}

// ModifyDocFail 更新文档任务失败
func (l *Logic) ModifyDocFail(ctx context.Context, doc *docEntity.Doc, staffID uint64) error {
	now := time.Now()
	db, err := knowClient.GormClient(ctx, l.getDocTableName(), doc.RobotID, 0, []client.Option{}...)

	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return err
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		/*
				`
				UPDATE
					t_doc
				SET
				    status = :status,
				    update_time = :update_time
				WHERE
				    id = :id
			`
		*/

		doc.Status = docEntity.DocStatusUpdateFail
		doc.UpdateTime = now
		updateColumns := []string{docEntity.DocTblColStatus, docEntity.DocTblColNextAction, docEntity.DocTblColUpdateTime}
		updateDocFilter := &docEntity.DocFilter{
			ID: doc.ID,
		}

		if _, err := l.docDao.UpdateDocByTx(ctx, updateColumns, updateDocFilter, doc, tx); err != nil {
			logx.E(ctx, "Failed to ModifyDocFail.  args:%+v err:%+v", doc, err)
			return err

		}
		if err := l.sendDocModifyNotice(ctx, staffID, doc, docEntity.DocUpdateFailNoticeContent,
			releaseEntity.LevelError); err != nil {
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "更新文档任务Fails失败 err:%+v", err)
		return err
	}
	return nil
}

func (l *Logic) GetDocAutoRefreshList(ctx context.Context, nextUpdateTime time.Time) ([]*docEntity.Doc, error) {
	return l.docDao.GetDocAutoRefreshList(ctx, nextUpdateTime)
}

// ProcessUnstableStatusDoc 处理文档非稳定状态
func (l *Logic) ProcessUnstableStatusDoc(ctx context.Context, doc *docEntity.Doc) {
	docUnstableTimeoutMinutes := config.GetMainConfig().DefaultDocUnstableTimeoutMinutes
	if value, ok := config.GetMainConfig().DocUnstableTimeoutMinutes[doc.Status]; ok {
		// 定制配置优先级更高
		docUnstableTimeoutMinutes = value
	}
	if docUnstableTimeoutMinutes == 0 {
		// 如果没配置就不做处理，兼容旧逻辑
		return
	}
	if doc.UpdateTime.Add(time.Duration(docUnstableTimeoutMinutes) * time.Minute).After(time.Now()) {
		// 非稳定状态时间未过期，不做处理
		return
	}
	logx.E(ctx, "ProcessUnstableStatusDoc unstable status docID:%d status:%d updateTime:%s",
		doc.ID, doc.Status, doc.UpdateTime.Format("2006-01-02 15:04:05"))
	// 将状态更新为失败，方便客户删除或者重试
	event := docEntity.EventProcessFailed
	docFilter := &docEntity.DocFilter{
		RobotId: doc.RobotID,
		IDs:     []uint64{doc.ID},
	}
	doc.Message = i18nkey.KeyProcessingTimeout
	updateCols := []string{docEntity.DocTblColStatus, docEntity.DocTblColMessage}
	err := l.UpdateDocStatusMachineByEvent(ctx, updateCols, docFilter, doc, event)
	if err != nil {
		logx.E(ctx, "ProcessDocUnstable UpdateDoc failed, doc:%+v err:%+v", doc, err)
		return
	}
}

// CountDocWithTimeAndStatus 通过时间，获取指定状态的文档总数
// 新增文档：✅
// 修改文档：✅
// 删除文档：✅
// 修改后删除：✅
// 新增后删除：❌
func (l *Logic) CountDocWithTimeAndStatus(ctx context.Context,
	corpID, robotID uint64,
	status []uint32,
	startTime time.Time,
) (uint64, error) {
	db, err := knowClient.GormClient(ctx, l.getDocTableName(), robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return 0, err
	}
	return l.docDao.CountDocWithTimeAndStatus(ctx, corpID, robotID, status, startTime, db)
}

// GetDeletingDoc 获取删除中的文档
func (l *Logic) GetDeletingDoc(ctx context.Context, corpID, robotID uint64) (map[uint64]*docEntity.Doc, error) {
	/*
		`
			SELECT
				%s
			FROM
			    t_doc
			WHERE
			    corp_id = ?
			    AND robot_id = ?
			    AND is_deleted = ?
			    AND status = ?
		`
	*/
	filter := &docEntity.DocFilter{
		CorpId:    corpID,
		RobotId:   robotID,
		Status:    []uint32{docEntity.DocStatusDeleting},
		IsDeleted: ptrx.Bool(true),
	}

	db, err := knowClient.GormClient(ctx, l.getDocTableName(), filter.RobotId, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return nil, err
	}

	list, err := l.docDao.GetDocListWithFilter(ctx, docEntity.DocTblColList, filter, db)
	if err != nil {
		logx.E(ctx, "GetDocByIDs failed, err: %+v", err)
		return nil, err
	}

	docs := make(map[uint64]*docEntity.Doc, 0)
	for _, doc := range list {
		docs[doc.ID] = doc
	}
	return docs, nil
}

// GetResumeDocCount 获取恢复中的文档数量
func (l *Logic) GetResumeDocCount(ctx context.Context, corpID, robotID uint64) (uint64, error) {
	/*
		`
			SELECT COUNT(*) FROM t_doc
			WHERE corp_id = ? AND robot_id = ? AND is_deleted = ? AND status in (?)
		`
	*/

	db, err := knowClient.GormClient(ctx, l.getDocTableName(), robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return 0, err
	}

	docFilter := &docEntity.DocFilter{
		CorpId:    corpID,
		RobotId:   robotID,
		Status:    docEntity.DocResumingStatusList,
		IsDeleted: ptrx.Bool(false),
	}

	count, err := l.docDao.GetDocCountWithFilter(ctx, nil, docFilter, db)

	if err != nil {
		logx.E(ctx, "GetResumeDocCount failed, err: %+v", err)
		return 0, err
	}
	return uint64(count), nil
}

// GetDocByID 通过ID获取文档
func (l *Logic) GetDocByID(ctx context.Context, id uint64, robotID uint64) (*docEntity.Doc, error) {
	docs, err := l.GetDocByIDs(ctx, []uint64{id}, robotID)
	if err != nil {
		return nil, err
	}
	doc, ok := docs[id]
	if !ok {
		return nil, nil
	}
	return doc, nil
}

// GetDocByID 通过ID获取文档
// func (l *Logic) GetDocByID(ctx context.Context, id uint64, robotID uint64) (*docEntity.Doc, error) {
//	docs, err := l.docDao.GetDocByIDs(ctx, []uint64{id}, robotID)
//	if err != nil {
//		return nil, err
//	}
//	doc, ok := docs[id]
//	if !ok {
//		return nil, nil
//	}
//	return doc, nil
// }

// GetDocByIDs 通过ID获取文档，不区分是否标记为删除
func (l *Logic) GetDocByIDs(ctx context.Context, ids []uint64, robotID uint64) (map[uint64]*docEntity.Doc, error) {
	/*
		`
			SELECT
				%s
			FROM
			    t_doc
			WHERE
			    id IN (%s)
		`
	*/
	docs := make(map[uint64]*docEntity.Doc, 0)
	if len(ids) == 0 {
		return docs, nil
	}

	// robotID只用来路由数据库，不参与查询条件，否则会导致某些接口传应用ID和共享知识库文档ID时，查询不到数据
	filter := &docEntity.DocFilter{
		IDs: ids,
	}

	logx.D(ctx, "GetDocByIDs filter:%+v", filter)
	tbl := l.docDao.Query().TDoc
	tableName := tbl.TableName()

	db, err := knowClient.GormClient(ctx, tableName, robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return nil, err
	}

	list, err := l.docDao.GetDocListWithFilter(ctx, docEntity.DocTblColList, filter, db)
	if err != nil {
		logx.E(ctx, "GetDocByIDs failed, err: %+v", err)
		return nil, err
	}

	for _, doc := range list {
		docs[doc.ID] = doc
	}
	return docs, nil
}

// GetDocByBizIDs 通过业务ID获取文档
func (l *Logic) GetDocByBizIDs(ctx context.Context, bizIDs []uint64, robotID uint64) (map[uint64]*docEntity.Doc, error) {
	logx.D(ctx, "GetDocByBizIDs bizIDs:%+v, robotID:%d", bizIDs, robotID)
	docs := make(map[uint64]*docEntity.Doc, 0)
	if len(bizIDs) == 0 {
		return docs, nil
	}

	tbl := l.docDao.Query().TDoc
	tableName := tbl.TableName()

	dbClients := make([]*gorm.DB, 0)

	if robotID == knowClient.NotVIP {
		dbClients = knowClient.GetAllGormClients(ctx, tableName)
	} else {
		db, err := knowClient.GormClient(ctx, tableName, robotID, 0, []client.Option{}...)
		if err != nil {
			logx.E(ctx, "get GormClient failed, err: %+v", err)
			return nil, err
		}
		dbClients = append(dbClients, db)
	}

	filter := &docEntity.DocFilter{
		BusinessIds: bizIDs,
	}

	var err error
	list := make([]*docEntity.Doc, 0)
	for _, db := range dbClients {
		list, err = l.docDao.GetDocListWithFilter(ctx, docEntity.DocTblColList, filter, db)
		if err != nil {
			logx.E(ctx, "GetDocByBizIDs failed, err: %+v", err)
			continue
		}
		break
	}

	if err != nil {
		return nil, err
	}
	for _, doc := range list {
		docs[doc.BusinessID] = doc
	}
	return docs, nil
}

// GetDocByBizID 通过BusinessID获取文档
func (l *Logic) GetDocByBizID(ctx context.Context, docBizID uint64, robotID uint64) (*docEntity.Doc, error) {
	logx.D(ctx, "GetDocByBizID docBizID:%d, robotID:%d", docBizID, robotID)
	docs, err := l.GetDocByBizIDs(ctx, []uint64{docBizID}, robotID)
	if err != nil {
		return nil, err
	}
	doc, ok := docs[docBizID]
	if !ok {
		return nil, errs.ErrDocNotFound
	}
	return doc, nil
}

// GetDocByBizIDAndAppID 通过文档BusinessID获取文档详情
func (l *Logic) GetDocByBizIDAndAppID(ctx context.Context, corpID, robotID, businessID uint64,
	selectColumns []string) (*docEntity.Doc, error) {
	if corpID == 0 || robotID == 0 || businessID == 0 || len(selectColumns) == 0 {
		return nil, errs.ErrParams
	}
	filter := &docEntity.DocFilter{
		CorpId:  corpID,
		RobotId: robotID,
	}
	if businessID > 0 {
		filter.BusinessIds = []uint64{businessID}
	}
	logx.D(ctx, "GetDocByBizID filter:%+v", filter)
	tbl := l.docDao.Query().TDoc
	tableName := tbl.TableName()

	db, err := knowClient.GormClient(ctx, tableName, filter.RobotId, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return nil, err
	}

	return l.docDao.GetDocByDocFilter(ctx, selectColumns, filter, db)
}

// GetDocByIDAndFileName 通过ID和文档名称获取文档
func (l *Logic) GetDocByIDAndFileName(ctx context.Context, ids []uint64, fileName string) ([]*docEntity.Doc, error) {
	/*
		`
			SELECT
				%s
			FROM
			    t_doc
			WHERE
			    id IN (%s) %s AND file_name LIKE ?
		`

	*/
	docs := make([]*docEntity.Doc, 0)
	if len(ids) == 0 {
		return docs, nil
	}

	filter := &docEntity.DocFilter{
		IDs:            ids,
		FileNameSubStr: fileName,
	}
	logx.D(ctx, "GetDocByIDAndFileName filter:%+v", filter)

	docs, err := l.docDao.GetDocListWithFilter(ctx, docEntity.DocTblColList, filter, nil)
	if err != nil {
		logx.E(ctx, "GetDocByIDAndFileName failed, err: %+v", err)
		return nil, err
	}
	return docs, nil
}

// GetDocIDByBusinessID 通过BusinessID获取文档ID
func (l *Logic) GetDocIDByBusinessID(ctx context.Context, businessID uint64, robotID uint64) (uint64, error) {
	/*
		`
			SELECT
				id
			FROM
			    t_doc
			WHERE
			    business_id = ?
		`
	*/

	filter := &docEntity.DocFilter{
		BusinessIds: []uint64{businessID},
	}

	tbl := l.docDao.Query().TDoc
	tableName := tbl.TableName()

	db, err := knowClient.GormClient(ctx, tableName, robotID, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return 0, err
	}

	doc, err := l.docDao.GetDocByDocFilter(ctx, []string{docEntity.DocTblColId}, filter, db)

	if err != nil {
		logx.E(ctx, "GetDocByDocFilter failed, err: %+v", err)
		return 0, err
	}
	if doc == nil {
		return 0, errs.ErrDocNotFound

	}
	return doc.ID, nil
}

// IsDocInEditState 判断文档是否正在生成QA或者正在删除
func (l *Logic) IsDocInEditState(ctx context.Context, corpID, robotID uint64) (bool, error) {
	/*
		`
			SELECT
				%s
			FROM
			    t_doc
			WHERE
			    corp_id = ? AND robot_id = ? AND status IN (2,5) LIMIT 1
		`
	*/
	filter := &docEntity.DocFilter{
		CorpId:  corpID,
		RobotId: robotID,
	}

	logx.D(ctx, "IsDocInEditState filter:%+v", filter)
	tbl := l.docDao.Query().TDoc
	tableName := tbl.TableName()

	db, err := knowClient.GormClient(ctx, tableName, robotID, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return false, err
	}

	docs, err := l.docDao.GetDocListWithFilter(ctx, docEntity.DocTblColList, filter, db)
	if err != nil {
		logx.E(ctx, "GetDocByDocFilter failed, err: %+v", err)
		return false, err
	}

	return len(docs) > 0, nil
}

// GetDocByCosHash 通过cos_hash获取文档
func (l *Logic) GetDocByCosHash(ctx context.Context, corpID, robotID uint64, cosHash string) (*docEntity.Doc, error) {
	/*

		getDocByCosHash = `
			SELECT
				%s
			FROM
			    t_doc
			WHERE
			    corp_id = ? AND robot_id = ? AND cos_hash = ? AND is_deleted = ?
		`
	*/

	filter := &docEntity.DocFilter{
		CorpId:    corpID,
		RobotId:   robotID,
		CosHash:   cosHash,
		IsDeleted: ptrx.Bool(false),
	}

	logx.D(ctx, "GetDocByCosHash filter:%+v", filter)
	tbl := l.docDao.Query().TDoc
	tableName := tbl.TableName()

	db, err := knowClient.GormClient(ctx, tableName, robotID, filter.RouterAppBizID, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return nil, err
	}

	doc, err := l.docDao.GetDocByDocFilter(ctx, docEntity.DocTblColList, filter, db)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errs.ErrDocNotFound
		}
		logx.E(ctx, "GetDocByCosHash failed, err: %+v", err)
		return nil, err
	}

	return doc, nil
}

// DbDoc2PbDoc 将文档结构体转换为pb结构体
func DbDoc2PbDoc(ctx context.Context, releasingDocIdMap map[uint64]struct{}, doc *docEntity.Doc,
	latestRelease *releaseEntity.Release, qaNums map[uint64]map[uint32]uint32,
	mapDocID2AttrLabels map[uint64][]*labelEntity.AttrLabel, docParsesFailMap map[uint64]docEntity.DocParse,
	docAuditFailMap map[uint64]releaseEntity.AuditStatus, cateMap map[uint64]*category.CateInfo,
	isSharedKnowledge bool) *pb.ListDocRsp_Doc {
	_, ok := releasingDocIdMap[doc.ID]
	pbDoc := &pb.ListDocRsp_Doc{
		DocBizId:            doc.BusinessID,
		FileName:            doc.FileName,
		NewName:             doc.FileNameInAudit,
		CosUrl:              doc.CosURL,
		Reason:              i18n.Translate(ctx, doc.Message),
		UpdateTime:          doc.UpdateTime.Unix(),
		Status:              doc.StatusCorrect(),
		StatusDesc:          i18n.Translate(ctx, doc.StatusDesc(latestRelease.IsPublishPause())),
		FileType:            doc.FileType,
		IsRefer:             doc.IsRefer,
		QaNum:               qaNums[doc.ID][qaEntity.QAIsNotDeleted],
		IsDeleted:           doc.HasDeleted(),
		Source:              doc.Source,
		SourceDesc:          doc.DocSourceDesc(),
		IsAllowRestart:      !ok && doc.IsAllowCreateQA(),
		IsDeletedQa:         qaNums[doc.ID][qaEntity.QAIsNotDeleted] == 0 && qaNums[doc.ID][qaEntity.QAIsDeleted] != 0,
		IsCreatingQa:        doc.IsCreatingQaV1(),
		IsAllowDelete:       !ok && doc.IsAllowDelete(),
		IsAllowRefer:        doc.IsAllowRefer(),
		IsCreatedQa:         doc.IsCreatedQA,
		DocCharSize:         doc.CharSize,
		IsAllowEdit:         !ok && doc.IsAllowEdit(),
		AttrRange:           doc.AttrRange,
		AttrLabels:          fillPBAttrLabels(mapDocID2AttrLabels[doc.ID]),
		ReferUrlType:        doc.ReferURLType,
		WebUrl:              doc.WebURL,
		ExpireStart:         uint64(doc.ExpireStart.Unix()),
		ExpireEnd:           uint64(doc.ExpireEnd.Unix()),
		IsAllowRetry:        isAllowRetry(ctx, doc.ID, doc.Status, docParsesFailMap, docAuditFailMap),
		CreateTime:          doc.CreateTime.Unix(),
		CustomerKnowledgeId: doc.CustomerKnowledgeId,
		IsDisabled:          false, // 知识库概念统一后该字段已废弃
		EnableScope:         pb.RetrievalEnableScope(doc.EnableScope),
		DocSize:             doc.FileSize,
	}
	if isSharedKnowledge {
		if pbDoc.Status == docEntity.DocStatusReleaseSuccess {
			// 共享知识库，需要兼容从应用知识库人工转换成共享知识库的情况
			pbDoc.Status = docEntity.DocStatusWaitRelease
		}
	}
	if pbDoc.Status == docEntity.DocStatusWaitRelease || pbDoc.Status == docEntity.DocStatusReleaseSuccess {
		// 所有的待发布和已发布状态都变为导入成功
		pbDoc.StatusDesc = i18n.Translate(ctx, i18nkey.KeyImportComplete)
	}
	if doc.CategoryID != 0 {
		if cate, ok := cateMap[uint64(doc.CategoryID)]; ok {
			pbDoc.CateBizId = cate.BusinessID
		}
	}
	for k, v := range docEntity.IsProcessingMap {
		if doc.IsProcessing([]uint64{k}) {
			pbDoc.Processing = append(pbDoc.Processing, v)
		}
	}
	for k, v := range docEntity.AttributeFlagMap {
		if doc.HasAttributeFlag(k) {
			pbDoc.AttributeFlags = append(pbDoc.AttributeFlags, v)
		}
	}
	return pbDoc
}

func isAllowRetry(ctx context.Context, docID uint64, docStatus uint32,
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
	return getIsAllowRetry(result.ErrorCode)
}

func getIsAllowRetry(errorCode string) bool {
	if conf, ok := config.App().DocParseError[errorCode]; ok {
		return conf.IsAllowRetry
	}
	return config.App().DocParseErrorDefault.IsAllowRetry
}

// fillPBAttrLabels 转成成PB的属性标签
func fillPBAttrLabels(attrLabels []*labelEntity.AttrLabel) []*pb.AttrLabel {
	list := make([]*pb.AttrLabel, 0)
	for _, v := range attrLabels {
		attrLabel := &pb.AttrLabel{
			Source:    v.Source,
			AttrBizId: v.BusinessID,
			AttrKey:   v.AttrKey,
			AttrName:  v.AttrName,
		}
		for _, label := range v.Labels {
			labelName := label.LabelName
			if label.LabelID == 0 {
				labelName = config.App().AttributeLabel.FullLabelDesc
			}
			attrLabel.Labels = append(attrLabel.Labels, &pb.AttrLabel_Label{
				LabelBizId: label.BusinessID,
				LabelName:  labelName,
			})
		}
		list = append(list, attrLabel)
	}
	return list
}

// GetWorkflowListByDoc 获取文档被引用的工作流列表
func (l *Logic) GetWorkflowListByDoc(ctx context.Context, req *pb.CheckDocReferWorkFlowReq) ([]*pb.DocRefByWorkflow, error) {
	rsp, err := l.rpc.TaskFlow.GetWorkflowListByDoc(ctx, req.BotBizId, req.GetDocBizIds())
	if err != nil {
		logx.E(ctx, "GetWorkflowListByDoc failed, err: %+v", err)
		return nil, err
	}
	var ret []*pb.DocRefByWorkflow
	if rsp == nil || len(rsp.GetList()) == 0 {
		return ret, nil
	}
	for _, item := range rsp.GetList() {
		var list []*pb.WorkflowRef
		for _, wf := range item.GetWorkflowList() {
			list = append(list, &pb.WorkflowRef{
				WorkflowId:   wf.GetWorkflowId(),
				WorkflowName: wf.GetWorkflowName(),
				WorkflowDesc: wf.GetWorkflowDesc(),
				AppBizId:     wf.GetAppBizId(),
				UpdateTime:   wf.GetUpdateTime(),
			})
		}
		ret = append(ret, &pb.DocRefByWorkflow{DocBizId: item.GetDocBizId(), WorkflowList: list})
	}
	return ret, nil
}

// BatchDownloadDoc 批量下载文档
func (l *Logic) BatchDownloadDoc(ctx context.Context, robotID uint64, docIDs []uint64, d dao.Dao) (
	*pb.BatchDownloadDocRsp, error) {
	docFilter := &docEntity.DocFilter{
		RobotId:     robotID,
		BusinessIds: docIDs,
	}
	selectColumns := []string{docEntity.DocTblColId, docEntity.DocTblColBusinessId, docEntity.DocTblColFileName,
		docEntity.DocTblColFileNameInAudit, docEntity.DocTblColFileType, docEntity.DocTblColCosURL}
	docs, err := l.docDao.GetDocList(ctx, selectColumns, docFilter)
	if err != nil {
		return nil, err
	}
	if len(docs) != len(docFilter.BusinessIds) {
		logx.E(ctx, "BatchDownloadDoc Check docs not found, len(docs):%d docIDs:%+v",
			len(docs), docFilter.BusinessIds)
		return nil, errs.ErrDocNotFound
	}
	rsp := &pb.BatchDownloadDocRsp{}
	for _, doc := range docs {
		signURL, err := l.s3.GetPreSignedURLWithTypeKey(ctx, entity.OfflineStorageTypeKey, doc.CosURL, 0)
		if err != nil {
			logx.E(ctx, "BatchDownloadDoc GetPresignedURLWithTypeKey failed, err:%+v", err)
			return nil, errs.ErrSystem
		}
		rsp.DocList = append(rsp.DocList, &pb.BatchDownloadDocRsp_DocList{
			FileName: doc.GetRealFileName(),
			FileType: doc.FileType,
			CosUrl:   doc.CosURL,
			Url:      signURL,
			DocBizId: doc.BusinessID,
		})
	}
	return rsp, nil
}

// GetDocNextUpdateTime 获取文档下次更新时间
func GetDocNextUpdateTime(ctx context.Context, updatePeriodH uint32) time.Time {
	// 如果updatePeriodH为0，返回1970-01-01 08:00:00
	if updatePeriodH == 0 {
		return time.Unix(0, 0).Add(8 * time.Hour) // 1970-01-01 08:00:00
	}
	now := time.Now()
	var daysToAdd int
	// 根据updatePeriodH的值决定增加的天数
	switch {
	case updatePeriodH > 72: // 大于72小时加7天
		daysToAdd = 7
	case updatePeriodH > 24: // 大于24小时加3天
		daysToAdd = 3
	default: // 其他情况(>0)加1天
		daysToAdd = 1
	}
	// 计算增加天数后的日期  取整到当天的0点
	nextUpdateTimeDay := now.AddDate(0, 0, daysToAdd) // 先加天数
	nextUpdateTimeDay = time.Date(                    // 再重置时间部分
		nextUpdateTimeDay.Year(),
		nextUpdateTimeDay.Month(),
		nextUpdateTimeDay.Day(),
		0, 0, 0, 0,
		time.Local,
	)
	logx.D(ctx, "GetDocNextUpdateTime: nextUpdateTimeDay: %v", nextUpdateTimeDay)
	return nextUpdateTimeDay
}

// RefreshCorpCOSDoc 刷新客户cos文档
func (l *Logic) RefreshCorpCOSDoc(ctx context.Context, isAuto bool, docs []*docEntity.Doc) error {
	for _, doc := range docs {
		if doc.Status != docEntity.DocStatusWaitRelease && doc.Status != docEntity.DocStatusReleaseSuccess {
			if isAuto {
				logx.W(ctx, "RefreshCorpCOSDoc doc status is not wait release or release success, doc: %+v", doc)
				continue
			}
			return errs.ErrRefreshCorpCOSDocStatusFail
		}
		if time.Unix(0, 0).Before(doc.ExpireEnd) && time.Now().After(doc.ExpireEnd) {
			logx.W(ctx, "RefreshCorpCOSDoc status Expire, doc: %+v", doc)
			continue
		}
		uin := contextx.Metadata(ctx).Uin()
		if uin == "" {
			appDB, err := l.rpc.AppAdmin.DescribeAppByPrimaryId(ctx, doc.RobotID)
			if err != nil {
				return err
			}
			uin = appDB.Uin
		}
		// 校验授权信息
		_, status, err := l.rpc.Cloud.AssumeServiceRole(ctx, uin,
			config.App().COSDocumentConfig.ServiceRole, 0, nil)
		if err != nil {
			return err
		}
		if status != pb.RoleStatusType_RoleStatusAvailable {
			return errs.ErrRefreshCorpCOSDocUserAuthFail
		}
	}

	taskID, err := scheduler.NewCorpCOSDocRefreshTask(ctx, docs)
	if err != nil {
		logx.E(ctx, "RefreshCorpCOSDoc NewTxDocRefreshTask err: %+v", err)
		return err
	}
	logx.D(ctx, "RefreshCorpCOSDoc NewTxDocRefreshTask taskID: %v", taskID)
	return nil
}

// RefreshTxDoc 刷新腾讯文档
func (l *Logic) RefreshTxDoc(ctx context.Context, isAuto bool, docs []*docEntity.Doc) error {
	var tFileInfo []entity.TxDocRefreshTFileInfo
	for _, doc := range docs {
		if doc.Status != docEntity.DocStatusWaitRelease && doc.Status != docEntity.DocStatusReleaseSuccess {
			if isAuto {
				logx.W(ctx, "RefreshTxDoc doc status is not wait release or release success, doc: %+v", doc)
				continue
			}
			return errs.ErrRefreshTxDocStatusFail
		}
		if time.Unix(0, 0).Before(doc.ExpireEnd) && time.Now().After(doc.ExpireEnd) {
			logx.W(ctx, "RefreshTxDoc status Expire, doc: %+v", doc)
			continue
		}
		uin := contextx.Metadata(ctx).Uin()
		if uin == "" {
			appDB, err := l.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, doc.RobotID)
			if err != nil {
				return err
			}
			appInfo, err := l.rpc.AppAdmin.DescribeAppInfoUsingScenesById(ctx, appDB.BizId, entity.AppTestScenes)
			if err != nil {
				logx.E(ctx, "RefreshTxDoc GetAppInfo err: %+v", err)
				return err
			}
			uin = appInfo.Uin
		}
		rsp, err := l.rpc.TDocLinker.CheckUserAuth(ctx, uin, uin)
		if err != nil {
			if isAuto {
				// 定时任务自动刷新,未授权跳过
				logx.D(ctx, "RefreshTxDoc CheckUserAuth rsp.Response.Code != 200, isAuto: %v uin: %s",
					isAuto, uin)
				continue
			}
			logx.E(ctx, "RefreshTxDoc ImportTFile err: %+v", err)
			return err
		}
		logx.D(ctx, "RefreshTxDoc CheckUserAuth rsp: %+v", rsp)
		if rsp.Response.Code != 200 {
			if isAuto {
				// 定时任务自动刷新,未授权跳过
				logx.D(ctx, "RefreshTxDoc CheckUserAuth rsp.Response.Code != 200, isAuto: %v uin: %s",
					isAuto, uin)
				continue
			}
			return errs.ErrRefreshTxDocUserAuthFail
		}

		operationID, err := l.rpc.TDocLinker.ImportTFile(ctx, uin, uin, doc.CustomerKnowledgeId)
		if err != nil {
			logx.E(ctx, "RefreshTxDoc ImportTFile err: %+v", err)
			return err
		}
		if operationID == "" {
			return errors.New("ImportTFile operationID is empty")
		}

		tFileInfo = append(tFileInfo, entity.TxDocRefreshTFileInfo{
			DocID:       doc.ID,
			CorpID:      doc.CorpID,
			StaffID:     doc.StaffID,
			RobotID:     doc.RobotID,
			FileID:      doc.CustomerKnowledgeId,
			OperationID: operationID,
		})
	}

	// isAuto 如果是自动刷新任务，需要更新所有文档下次执行时间
	if isAuto {
		logx.D(ctx, "RefreshTxDoc isAuto: %v", isAuto)
	}

	taskID, err := scheduler.NewTxDocRefreshTask(ctx, tFileInfo)
	if err != nil {
		logx.E(ctx, "RefreshTxDoc NewTxDocRefreshTask err: %+v", err)
		return err
	}
	logx.D(ctx, "RefreshTxDoc NewTxDocRefreshTask taskID: %v", taskID)
	return nil
}

// ModifyItemsActionUpdateDoc 更新文档指定db字段内容
func (l *Logic) ModifyItemsActionUpdateDoc(ctx context.Context, doc *docEntity.Doc, app *entity.App,
	updateDocColumns []string, update *docEntity.Doc, isReloadDoc, isModifySplitRule bool) error {
	updateDocFilter := &docEntity.DocFilter{
		IDs: []uint64{doc.ID}, CorpId: doc.CorpID, RobotId: doc.RobotID,
	}
	_, err := l.docDao.UpdateDoc(ctx, updateDocColumns, updateDocFilter, update)
	if err != nil {
		logx.E(ctx, "ReloadUpdateDoc|UpdateDocStatus|err:%+v", err)
		return err
	}
	if isModifySplitRule {
		corp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, app.CorpPrimaryId)
		if err != nil || corp == nil {
			logx.E(ctx, "ModifyItemsActionUpdateDoc GetCorpByID err:%+v", err)
			return errs.ErrCorpNotFound
		}
		err = scheduler.NewDocDocSplitRuleModifyTask(ctx, entity.DocSplitRuleModifyParams{
			CorpBizID: corp.GetCorpId(),
			AppBizID:  app.BizId,
			AppID:     app.PrimaryId,
			DocBizID:  doc.BusinessID,
		})
		if err != nil {
			logx.E(ctx, "ModifyItemsActionUpdateDoc NewDocDocSplitRuleModifyTask err:%+v", err)
			return err
		}
		logx.D(ctx, "ModifyItemsActionUpdateDoc newtask success")
	}
	// 需要重新解析
	if isReloadDoc {
		doc.CosURL = update.CosURL
		doc.CosHash = update.CosHash
		requestID := contextx.TraceID(ctx)
		taskID, err := l.SendDocParseWordCount(ctx, doc, requestID, "")
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
		err = l.docDao.CreateDocParseTask(ctx, docParse)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *Logic) UpdateOldDocStatus(ctx context.Context, auditFlag uint32, doc *docEntity.Doc) error {
	// 重置文档的状态
	updateDocFilter := &docEntity.DocFilter{
		IDs:     []uint64{doc.ID},
		CorpId:  doc.CorpID,
		RobotId: doc.RobotID,
	}
	update := &docEntity.Doc{
		StaffID:    contextx.Metadata(ctx).StaffID(),
		Status:     docEntity.DocStatusParseIng,
		Message:    "",
		AuditFlag:  auditFlag,
		BatchID:    0,
		NextAction: docEntity.DocNextActionAdd,
		CharSize:   0,
		UpdateTime: time.Now(),
	}
	update.AddProcessingFlag([]uint64{docEntity.DocProcessingFlagSegmentIntervene})
	updateDocColumns := []string{
		docEntity.DocTblColStaffId,
		docEntity.DocTblColStatus,
		docEntity.DocTblColMessage,
		docEntity.DocTblColAuditFlag,
		docEntity.DocTblColBatchId,
		docEntity.DocTblColNextAction,
		docEntity.DocTblColCharSize,
		docEntity.DocTblColUpdateTime,
		docEntity.DocTblColProcessingFlag}
	_, err := l.docDao.UpdateDoc(ctx, updateDocColumns, updateDocFilter, update)
	if err != nil {
		logx.E(ctx, "CreateDocParsingIntervention|UpdateDoc|err:%+v", err)
		return err
	}
	return nil
}

// UpdateDocToQACreatingQa 更新文档是否生成问答状态
func (l *Logic) UpdateDocToQACreatingQa(ctx context.Context, tx *gorm.DB, doc *docEntity.Doc) error {
	/*
		`
			UPDATE
				t_doc
			SET
			    message = :message,
			    is_deleted = :is_deleted,
			    is_creating_qa = :is_creating_qa,
			    is_created_qa = :is_created_qa,
			    update_time = :update_time
			WHERE
			    id = :id
		`

	*/
	filter := &docEntity.DocFilter{
		ID: doc.ID,
	}
	updateColumns := []string{docEntity.DocTblColMessage,
		docEntity.DocTblColIsDeleted,
		docEntity.DocTblColIsCreatingQa,
		docEntity.DocTblColIsCreatedQa,
		docEntity.DocTblColUpdateTime}
	if _, err := l.docDao.UpdateDocByTx(ctx, updateColumns, filter, doc, tx); err != nil {
		logx.E(ctx, "updateDocToQACreatingQa error. err:%+v", err)
		return err
	}
	return nil
}

// CheckReloadUpdateDocFileType 校验更新文件类型
func CheckReloadUpdateDocFileType(ctx context.Context, doc *docEntity.Doc, url string) (err error) {
	ext := util.GetFileExt(url)
	logx.D(ctx, "checkReloadUpdateDocFileType|GetFileExt|ext:%+v", ext)
	if strings.ToLower(doc.FileType) != strings.ToLower(ext) {
		logx.W(ctx, "checkReloadUpdateDocFileType|FileType not match|doc.FileType:%s fileType:%s",
			doc.FileType, ext)
		return errs.ErrFileExtNotMatch
	}
	return nil
}

// ModifyItemsAction 更新文档指定内容
func (l *Logic) ModifyItemsAction(ctx context.Context, app *entity.App, doc *docEntity.Doc,
	req *pb.ModifyDocReq) error {
	// 校验修改类型是否合法
	validModifyTypes := map[pb.ModifyDocReq_ModifyType]bool{
		pb.ModifyDocReq_COS_INFO:                     true,
		pb.ModifyDocReq_REFER_INFO:                   true,
		pb.ModifyDocReq_UPDATE_PERIOD:                true,
		pb.ModifyDocReq_UPDATE_TX_DOC_REFRESH:        true,
		pb.ModifyDocReq_UPDATE_SPLIT_RULE:            true,
		pb.ModifyDocReq_UPDATE_CORP_COS_INFO:         true,
		pb.ModifyDocReq_UPDATE_ONE_DRIVE_DOC_REFRESH: true,
	}
	for _, modifyType := range req.GetModifyTypes() {
		if !validModifyTypes[modifyType] {
			logx.W(ctx, "ModifyItemsAction| invalid modify type:%v", req.GetModifyTypes())
			return errs.ErrParams
		}
	}
	isReloadDoc := false
	isModifySplitRule := false
	updateDocColumns := []string{docEntity.DocTblColStaffId}
	update := &docEntity.Doc{
		StaffID: contextx.Metadata(ctx).StaffID(),
	}

	// 腾讯文档刷新
	if slices.Contains(req.GetModifyTypes(), pb.ModifyDocReq_UPDATE_TX_DOC_REFRESH) {
		if doc.Source != docEntity.SourceFromTxDoc {
			logx.W(ctx, "ModifyItemsAction|RefreshTxDoc invalid modify type:%v doc %v",
				req.GetModifyTypes(), doc)
			return errs.ErrParams
		}
		err := l.RefreshTxDoc(ctx, false, []*docEntity.Doc{doc})
		if err != nil {
			logx.E(ctx, "ModifyItemsAction|RefreshTxDoc|failed, err:%v", err)
			return errs.ErrRefreshTxDocFail
		}
		return nil
	}

	// OneDrive文档刷新
	if slices.Contains(req.GetModifyTypes(), pb.ModifyDocReq_UPDATE_ONE_DRIVE_DOC_REFRESH) {
		if doc.Source != docEntity.SourceFromOnedrive {
			logx.W(ctx, "ModifyItemsAction|RefreshOneDriveDoc invalid modify type:%v doc %v",
				req.GetModifyTypes(), doc)
			return errs.ErrParams
		}
		err := l.thirdDocLogic.GetThirdDocLogic(common.SourceFromType(doc.Source)).RefreshDoc(ctx, false, []*docEntity.Doc{doc})
		if err != nil {
			logx.E(ctx, "ModifyItemsAction|RefreshOneDriveDoc|failed, err:%v", err)
			return errs.ErrRefreshDocFail
		}
		return nil
	}

	if slices.Contains(req.GetModifyTypes(), pb.ModifyDocReq_UPDATE_PERIOD) {
		if doc.Source != docEntity.SourceFromTxDoc {
			logx.W(ctx, "ModifyItemsAction|UpdatePeriodInfo invalid modify type:%v doc:%v",
				req.GetModifyTypes(), doc)
			return errs.ErrParams
		}
		if req.GetUpdatePeriodInfo() == nil {
			return errs.ErrParams
		}
		nextUpdateTime := GetDocNextUpdateTime(ctx, req.GetUpdatePeriodInfo().GetUpdatePeriodH())

		update.UpdatePeriodH = req.GetUpdatePeriodInfo().GetUpdatePeriodH()
		update.NextUpdateTime = nextUpdateTime
		updateDocColumns = append(updateDocColumns, docEntity.DocTblColUpdatePeriodH, docEntity.DocTblColNextUpdateTime)
	}

	// 刷新需要解析的操作
	if slices.Contains(req.GetModifyTypes(), pb.ModifyDocReq_COS_INFO) {
		if req.GetCosInfo() == nil {
			return errs.ErrParams
		}
		fileSize, err := util.CheckReqBotBizIDUint64(ctx, req.GetCosInfo().GetSize())
		if err != nil {
			return err
		}
		corp, err := l.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, app.CorpPrimaryId)
		// corp, err := l.rawSqlDao.GetCorpByID(ctx, app.CorpPrimaryId)
		if err != nil || corp == nil {
			return errs.ErrCorpNotFound
		}
		if err = l.s3.CheckURLFile(ctx, app.CorpPrimaryId, corp.GetCorpId(), app.BizId, req.GetCosInfo().GetCosUrl(), req.GetCosInfo().GetETag()); err != nil {
			logx.E(ctx, "ModifyDoc|CheckURLFile failed, err:%+v", err)
			return errs.ErrInvalidURL
		}
		if err = CheckReloadUpdateDocFileType(ctx, doc, req.GetCosInfo().GetCosUrl()); err != nil {
			logx.E(ctx, "ModifyDoc|CheckReloadUpdateDocFileType failed, err:%+v", err)
			return err
		}
		isReloadDoc = true
		update.Status = docEntity.DocStatusParseIng
		update.UpdateTime = time.Now()
		update.CosURL = req.GetCosInfo().GetCosUrl()
		update.CosHash = req.GetCosInfo().GetETag()
		update.FileSize = fileSize
		updateDocColumns = append(updateDocColumns, docEntity.DocTblColStatus, docEntity.DocTblColUpdateTime,
			docEntity.DocTblColCosURL, docEntity.DocTblColCosHash, docEntity.DocTblColFileSize)
	}

	if slices.Contains(req.GetModifyTypes(), pb.ModifyDocReq_REFER_INFO) {
		if req.GetReferInfo() == nil {
			return errs.ErrParams
		}
		update.IsRefer = req.GetReferInfo().GetIsRefer()
		update.ReferURLType = req.GetReferInfo().GetReferUrlType()
		update.WebURL = req.GetReferInfo().GetWebUrl()
		updateDocColumns = append(updateDocColumns, docEntity.DocTblColIsRefer, docEntity.DocTblColReferURLType,
			docEntity.DocTblColWebURL)
	}

	// 自定义拆分规则更新
	if slices.Contains(req.GetModifyTypes(), pb.ModifyDocReq_UPDATE_SPLIT_RULE) {
		if len(req.GetSplitRule()) == 0 {
			logx.E(ctx, "ModifyItemsAction invalid modify type:%+v doc %+v",
				req.GetModifyTypes(), doc)
			return errs.ErrParams
		}
		update.StaffID = contextx.Metadata(ctx).StaffID()
		update.Status = docEntity.DocStatusParseIng
		update.SplitRule = req.GetSplitRule()
		update.UpdateTime = time.Now()
		update.CharSize = 0
		update.BatchID = 0
		update.NextAction = docEntity.DocNextActionAdd
		update.Message = ""
		updateDocColumns = append(updateDocColumns, docEntity.DocTblColStatus, docEntity.DocTblColUpdateTime, docEntity.DocTblColSplitRule,
			docEntity.DocTblColStaffId, docEntity.DocTblColCharSize, docEntity.DocTblColBatchId, docEntity.DocTblColNextAction, docEntity.DocTblColMessage)
		isModifySplitRule = true
		// 减少已使用容量
		err := l.financeLogic.UpdateAppCapacityUsage(ctx, entity.CapacityUsage{
			CharSize:          -int64(doc.CharSize),
			StorageCapacity:   gox.IfElse(doc.Source == docEntity.SourceFromCorpCOSDoc, 0, -int64(doc.FileSize)),
			ComputeCapacity:   -int64(doc.FileSize),
			KnowledgeCapacity: -int64(doc.FileSize),
		}, doc.RobotID, doc.CorpID)
		if err != nil {
			logx.E(ctx, "CreateDocParsingIntervention|UpdateAppUsedCharSizeTx|err:%+v", err)
			return err
		}
	}

	// 客户cos文件刷新
	if slices.Contains(req.GetModifyTypes(), pb.ModifyDocReq_UPDATE_CORP_COS_INFO) {
		if doc.Source != docEntity.SourceFromCorpCOSDoc {
			logx.W(ctx, "ModifyItemsAction|RefreshCorpCOS invalid modify type:%v doc %v",
				req.GetModifyTypes(), doc)
			return errs.ErrParams
		}
		err := l.RefreshCorpCOSDoc(ctx, false, []*docEntity.Doc{doc})
		if err != nil {
			logx.E(ctx, "ModifyItemsAction|RefreshCorpCOS|failed, err:%v", err)
			return errs.ErrRefreshCorpCOSDocFail
		}
		return nil
	}

	// 执行文档更新
	if err := l.ModifyItemsActionUpdateDoc(ctx, doc, app, updateDocColumns, update,
		isReloadDoc, isModifySplitRule); err != nil {
		logx.E(ctx, "ModifyItemsActionUpdateDoc failed, err:%v", err)
		return err
	}
	return nil
}

// CheckDuplicateFile 处理文档重复的情况
func (l *Logic) CheckDuplicateFile(ctx context.Context, req *pb.SaveDocReq, corpID uint64, appID uint64) (bool,
	*pb.SaveDocRsp, error) {
	duplicateFileHandles := req.GetDuplicateFileHandles()
	if len(duplicateFileHandles) == 0 {
		// 默认策略
		duplicateFileHandles = make([]*pb.DuplicateFileHandle, 0)
		duplicateFileHandles = append(duplicateFileHandles, &pb.DuplicateFileHandle{
			CheckType:  pb.DuplicateFileCheckType_CHECK_TYPE_COS_HASH,
			HandleType: pb.DuplicateFileHandleType_HANDLE_TYPE_RETURN_ERR,
		})
	}
	var err error
	for i, fileHandle := range duplicateFileHandles {
		// 依次按照重复判断方式检查是否有重复文档
		// 只要有一个重复判断方式检查出重复文档，即可按照重复处理方式处理完，返回结果，不再继续检查
		var doc *docEntity.Doc
		switch fileHandle.GetCheckType() {
		case pb.DuplicateFileCheckType_CHECK_TYPE_INVALID:
			return false, nil, errs.ErrWrapf(errs.ErrParameterInvalid,
				i18n.Translate(ctx, i18nkey.KeyInvalidParamDuplicateFileHandleCheckType), i, fileHandle.GetCheckType())
		case pb.DuplicateFileCheckType_CHECK_TYPE_COS_HASH:
			doc, err = l.GetDocByCosHash(ctx, corpID, appID, req.GetCosHash())
			if err != nil {
				if errors.Is(err, errs.ErrDocNotFound) {
					continue
				}
				logx.W(ctx, "CheckDuplicateFile GetDocByCosHash failed, err:%+v", err)
				return false, nil, errs.ErrWrapf(errs.ErrSystem, "err:%+v", err)
			}
			if doc == nil {
				// 未找到重复文档，继续下一个检查类型
				continue
			}
		default:
			return false, nil, errs.ErrWrapf(errs.ErrParameterInvalid,
				i18n.Translate(ctx, i18nkey.KeyInvalidParamDuplicateFileHandleCheckType), i, fileHandle.GetCheckType())
		}
		rsp, err := handleDuplicateFile(ctx, fileHandle.GetHandleType(), doc)
		if rsp != nil {
			// 返回重复判断方式
			rsp.DuplicateFileCheckType = fileHandle.GetCheckType()
		}
		return true, rsp, err
	}
	rsp := &pb.SaveDocRsp{}
	return false, rsp, nil
}

// handleDuplicateFile 处理文档重复的情况
func handleDuplicateFile(ctx context.Context, handleType pb.DuplicateFileHandleType,
	doc *docEntity.Doc) (*pb.SaveDocRsp, error) {
	if doc == nil {
		logx.E(ctx, "handleDuplicateFile doc is nil")
		return nil, errs.ErrWrapf(errs.ErrSystem, "handleDuplicateFile doc is nil")
	}
	switch handleType {
	case pb.DuplicateFileHandleType_HANDLE_TYPE_INVALID:
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid,
			i18n.Translate(ctx, i18nkey.KeyInvalidParamHandleType), handleType)
	case pb.DuplicateFileHandleType_HANDLE_TYPE_RETURN_ERR:
		logx.W(ctx, "handleDuplicateFile doc is duplicate, doc:%+v", doc)
		return nil, errs.ErrWrapf(errs.ErrDocExist,
			i18n.Translate(ctx, i18nkey.KeyDocumentAlreadyExistsWithNameAndID), doc.FileName, doc.BusinessID)
	case pb.DuplicateFileHandleType_HANDLE_TYPE_SKIP:
		return &pb.SaveDocRsp{
			DocBizId: doc.BusinessID,
		}, nil
	default:
		return nil, errs.ErrWrapf(errs.ErrParameterInvalid,
			i18n.Translate(ctx, i18nkey.KeyInvalidParamHandleType), handleType)
	}
}

func GetDataSource(ctx context.Context, splitRuleJSON string) int {
	// 如果没有拆分规则，则使用COS数据源
	if splitRuleJSON == "" {
		return entity.DataSourceCOS
	}

	splitRule := docEntity.SplitRule{}
	err := jsonx.Unmarshal([]byte(splitRuleJSON), &splitRule)
	if err != nil {
		logx.W(ctx, "getDataSource|Unmarshal oldSplitRule failed, err:%v", err)
		return entity.DataSourceCOS
	}
	// 如果为默认拆分规则，则使用COS数据源
	if splitRule.SplitConfigNew.XlsxSplitter.SplitRow == 0 {
		return entity.DataSourceCOS
	}
	// 如果为自定义拆分规则，则使用DB数据源
	if splitRule.SplitConfigNew.XlsxSplitter.SplitRow > 0 {
		return entity.DataSourceDB
	}

	return entity.DataSourceCOS
}

// DeleteDocsCharSizeExceeded 删除超量失效超时文档
func (l *Logic) DeleteDocsCharSizeExceeded(ctx context.Context, corpID uint64, robotID uint64,
	reserveTime time.Duration) error {
	req := &docEntity.DocListReq{
		CorpID:   corpID,
		RobotID:  robotID,
		Page:     1,
		PageSize: 100,
	}
	_, docs, err := l.GetDocsCharSizeExceededAndExpire(ctx, req, reserveTime)
	if err != nil {
		return err
	}
	if len(docs) == 0 {
		return nil
	}
	return l.DeleteDocs(ctx, 0, robotID, 0, docs)
}

// DeleteDocs 删除文档
func (l *Logic) DeleteDocs(ctx context.Context, staffID, appID, appBizID uint64, docs []*docEntity.Doc) error {
	now := time.Now()
	if len(docs) == 0 {
		return nil
	}

	db, err := knowClient.GormClient(ctx, docEntity.DocTableName, appID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "UpdateDoc get GormClient err:%v,robotID:%v", err, appID)
		return err
	}

	// 累加容量变化，用于循环后统一更新
	var totalCapacity entity.CapacityUsage
	var robotID uint64
	var corpID uint64
	capacityMutex := sync.Mutex{} // 保护容量累加的并发安全

	// 并行执行所有操作
	wg, wgCtx := errgroupx.WithContext(ctx)
	wg.SetLimit(10) // 限制并发数
	for _, v := range docs {
		doc := v // 避免闭包
		wg.Go(func() (err error) {
			if err := db.Transaction(func(tx *gorm.DB) error {
				updateColumns := []string{docEntity.DocTblColIsDeleted, docEntity.DocTblColStatus, docEntity.DocTblColUpdateTime}
				docFilter := &docEntity.DocFilter{
					IDs: []uint64{doc.ID},
				}
				doc.IsDeleted = true
				doc.Status = docEntity.DocStatusDeleting
				doc.UpdateTime = now
				if _, err := l.docDao.UpdateDocByTx(wgCtx, updateColumns, docFilter, doc, tx); err != nil {
					logx.E(wgCtx, "Update Document deleted flag error. doc:%+v err:%+v", doc, err)
					return err
				}
				// 累加容量变化
				capacityMutex.Lock()
				totalCapacity.CharSize += -int64(doc.CharSize)
				totalCapacity.StorageCapacity += gox.IfElse(doc.Source == docEntity.SourceFromCorpCOSDoc, int64(0), -int64(doc.FileSize))
				totalCapacity.ComputeCapacity += -int64(doc.FileSize)
				totalCapacity.KnowledgeCapacity += -int64(doc.FileSize)
				robotID = doc.RobotID
				corpID = doc.CorpID
				capacityMutex.Unlock()
				if err := l.labelDao.DeleteDocAttributeLabel(wgCtx, doc.RobotID, doc.ID, tx); err != nil {
					logx.E(wgCtx, "Failed to DeleteDocAttributeLabel doc:%+v err:%+v", doc, err)
					return err
				}
				if err := scheduler.NewDocDeleteTask(wgCtx, doc.RobotID, entity.DocDeleteParams{
					CorpID:  doc.CorpID,
					StaffID: staffID,
					RobotID: doc.RobotID,
					DocID:   doc.ID,
				}); err != nil {
					logx.E(wgCtx, "New DocDeleteTask err:%+v", err)
					return err
				}
				docParse, err := l.GetDocParseByDocIDAndTypeAndStatus(wgCtx, doc.ID, docEntity.DocParseTaskTypeWordCount,
					docEntity.DocParseIng, doc.RobotID)
				if err != nil {
					logx.I(wgCtx, "DeleteDocs 文档解析任务未找到 docID:%+d", doc.ID)
					return nil // 如果没有正在进行的解析任务，则不用发送停止解析信号
				}
				requestID := contextx.TraceID(wgCtx)
				err = l.StopDocParseTask(wgCtx, docParse.TaskID, requestID, appBizID)
				if err != nil {
					logx.W(wgCtx, "StopDocParseTask err:%+v,docParse:%+v,requestID:%s",
						err, docParse, requestID)
					return nil // 如果发送停止解析信号失败，不阻塞流程继续
				}
				docParse.Status = docEntity.DocParseCallBackCancel
				docParse.RequestID = requestID
				err = l.docDao.UpdateDocParseTaskByTx(wgCtx, []string{docEntity.DocParseTblColStatus, docEntity.DocParseTblColRequestID}, docParse, tx)
				if err != nil {
					logx.W(wgCtx, "UpdateDocParseTask err:%+v,docParse:%+v,requestID:%s",
						err, docParse, requestID)
					return nil // 如果更新解析任务状态失败，不阻塞流程继续
				}
				return nil
			}); err != nil {
				logx.E(wgCtx, "Failed to delete document err:%+v", err)
				return err
			}
			return nil
		})
	}

	// 等待所有goroutine完成
	if err := wg.Wait(); err != nil {
		return err
	}

	// 统一更新应用容量
	if totalCapacity.CharSize != 0 || totalCapacity.KnowledgeCapacity != 0 {
		err := l.financeLogic.UpdateAppCapacityUsage(ctx, totalCapacity, robotID, corpID)
		if err != nil {
			logx.E(ctx, "Failed to UpdateAppCapacityUsage totalCapacity:%+v robotID:%d corpID:%d err:%+v",
				totalCapacity, robotID, corpID, err)
			return err
		}
	}

	return nil
}

// GetDocsCharSizeExceededAndExpire 获取超量失效而且已经超过超时状态保留时间的文档列表
func (l *Logic) GetDocsCharSizeExceededAndExpire(ctx context.Context, req *docEntity.DocListReq, reserveTime time.Duration) (
	uint64, []*docEntity.Doc, error) {

	// 文档字数服超量相关的状态
	docCharSizeExceededStatus := []uint32{
		docEntity.DocStatusCharExceeded,
		docEntity.DocStatusParseImportFailCharExceeded,
		docEntity.DocStatusAuditFailCharExceeded,
		docEntity.DocStatusUpdateFailCharExceeded,
		docEntity.DocStatusCreateIndexFailCharExceeded,
		docEntity.DocStatusAppealFailedCharExceeded,
	}

	lastUpdateTime := time.Now().Add(-reserveTime)

	filter := &docEntity.DocFilter{
		CorpId:        req.CorpID,
		RobotId:       req.RobotID,
		IsDeleted:     ptrx.Bool(true),
		Status:        docCharSizeExceededStatus,
		MaxUpdateTime: lastUpdateTime,
	}

	offset, limit := utilx.Page(req.Page, req.PageSize)
	filter.Offset = offset
	filter.Limit = limit

	count, err := l.docDao.GetDocCountByDistinctID(ctx, filter)
	if err != nil {
		logx.E(ctx, "failed to get count of DocsCharSizeExceededAndExpire, err: %+v", err)
		return 0, nil, err
	}

	docList, err := l.docDao.GetDocList(ctx, docEntity.DocTblColList, filter)
	if err != nil {
		logx.E(ctx, "failed to get list of DocsCharSizeExceededAndExpire: err:%+v", err)
		return 0, nil, err
	}

	return uint64(count), docList, nil
}

// CreateDocToQATask 创建问答生成索引任务
func (l *Logic) CreateDocToQATask(ctx context.Context, doc *docEntity.Doc, qaTask *qaEntity.DocQATask) (uint64, error) {
	var qaTaskID uint64
	qaTaskType := qaEntity.DocQATaskStatusGenerating
	if qaTask != nil {
		qaTaskID = qaTask.ID
		qaTaskType = qaTask.Status
	}
	taskID, err := scheduler.NewDocToQATask(ctx, doc.RobotID, entity.DocToQAParams{
		CorpID:     doc.CorpID,
		CorpBizID:  contextx.Metadata(ctx).CorpBizID(),
		StaffID:    doc.StaffID,
		RobotID:    doc.RobotID,
		DocID:      doc.ID,
		QaTaskID:   qaTaskID,
		QaTaskType: qaTaskType,
		Uin:        contextx.Metadata(ctx).Uin(),
		Sid:        contextx.Metadata(ctx).SID(),
	})
	if err != nil {
		logx.E(ctx, "创建文档生成问答任务失败 err:%+v", err)
		return taskID, err
	}
	logx.D(ctx, "CreateDocToQATask taskID:%v", taskID)
	return taskID, nil
}

// GetWaitReleaseDocCount 获取待发布的文档数量
func (l *Logic) GetWaitReleaseDocCount(ctx context.Context, corpID, robotID uint64, fileName string, startTime,
	endTime time.Time, actions []uint32) (uint64, error) {
	/*
			`
			SELECT
				count(*)
			FROM
			    t_doc
			WHERE
			    corp_id = ? AND robot_id = ? AND status = ? %s
		`
	*/

	docFilter := &docEntity.DocFilter{
		CorpId:         corpID,
		RobotId:        robotID,
		Status:         []uint32{docEntity.DocStatusWaitRelease},
		FileNameSubStr: fileName,
		MinUpdateTime:  startTime,
		MaxUpdateTime:  endTime,
		NextActions:    actions,
	}

	db, err := knowClient.GormClient(ctx, l.getDocTableName(), robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return 0, err
	}

	count, err := l.docDao.GetDocCountWithFilter(ctx, nil, docFilter, db)

	if err != nil {
		logx.E(ctx, "get wait release doc count failed, err: %+v", err)
		return 0, err

	}
	return uint64(count), nil
}

// GetWaitReleaseDoc 获取待发布的文档
func (l *Logic) GetWaitReleaseDoc(ctx context.Context, corpID, robotID uint64, fileName string, startTime,
	endTime time.Time, actions []uint32, page, pageSize uint32) ([]*docEntity.Doc, error) {
	/*
			`
			SELECT
				%s
			FROM
			    t_doc
			WHERE
			    corp_id = ? AND robot_id = ? AND status = ? %s
			LIMIT
				?,?
		`
	*/

	offset, limit := utilx.Page(page, pageSize)

	docFilter := &docEntity.DocFilter{
		CorpId:         corpID,
		RobotId:        robotID,
		Status:         []uint32{docEntity.DocStatusWaitRelease},
		FileNameSubStr: fileName,
		MinUpdateTime:  startTime,
		MaxUpdateTime:  endTime,
		NextActions:    actions,
		Offset:         offset,
		Limit:          limit,
	}

	db, err := knowClient.GormClient(ctx, l.getDocTableName(), robotID, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return nil, err
	}

	list, err := l.docDao.GetDocListWithFilter(ctx, docEntity.DocTblColList, docFilter, db)
	if err != nil {
		logx.E(ctx, "get wait release doc failed, err: %+v", err)
		return nil, err
	}
	return list, nil
}

// GetRobotDocUsage 获取机器人总文档大小
func (l *Logic) GetRobotDocUsage(ctx context.Context, robotID uint64, corpID uint64) (entity.CapacityUsage, error) {
	return l.docDao.GetRobotDocUsage(ctx, robotID, corpID)
}

// GetRobotDocExceedUsage 获取机器人超量总文档大小
func (l *Logic) GetRobotDocExceedUsage(ctx context.Context, corpID uint64, robotIDs []uint64) (
	map[uint64]entity.CapacityUsage, error) {
	if len(robotIDs) == 0 {
		// 当 robotIDs 为空时，自动获取企业下所有的 robotID
		apps, _, err := l.rpc.AppAdmin.ListAllAppBaseInfo(ctx, &appconfig.ListAppBaseInfoReq{
			CorpPrimaryId: corpID,
		})
		if err != nil {
			return nil, err
		}
		if len(apps) == 0 {
			// 企业下没有任何应用，直接返回空 map
			return make(map[uint64]entity.CapacityUsage), nil
		}
		robotIDs = make([]uint64, 0, len(apps))
		for _, app := range apps {
			robotIDs = append(robotIDs, app.PrimaryId)
		}
	}
	return l.docDao.GetRobotDocExceedUsage(ctx, corpID, robotIDs)
}

func (l *Logic) GetRefersByBusinessIDs(ctx context.Context, robotID uint64, businessIDs []uint64,
) ([]*entity.Refer, error) {
	/*
		 `
			SELECT
				%s
			FROM
			    t_refer
			WHERE
			    robot_id = ? AND business_id IN (%s)
		`
	*/
	filter := &entity.ReferFilter{
		RobotID:     robotID,
		BusinessIDs: businessIDs,
	}
	return l.docDao.GetReferListByFilter(ctx, []string{}, filter)
}

func (l *Logic) UpdateDocStatusByRobotId(ctx context.Context, id, robotId uint64, status uint32) error {
	err := l.docDao.UpdateDocStatus(ctx, id, robotId, status)
	if err != nil {
		logx.E(ctx, "UpdateAppealDoc failed , err:%v", err)
		return err
	}
	return err
}

func (l *Logic) GetLatestDocUpdateTime(ctx context.Context, corpPrimaryId, robotPrimaryId uint64) (int64, error) {
	filter := &docEntity.DocFilter{
		RobotId:        robotPrimaryId,
		CorpId:         corpPrimaryId,
		OrderColumn:    []string{"update_time"},
		OrderDirection: []string{util.SqlOrderByDesc},
		Limit:          1,
		Offset:         0,
	}
	docs, err := l.docDao.GetDocListWithFilter(ctx, []string{"update_time"}, filter, nil)
	if err != nil {
		logx.E(ctx, "GetLatestDocUpdateTime failed, robotId: %d, err: %v", robotPrimaryId, err)
		return 0, err
	}

	if len(docs) == 0 {
		logx.W(ctx, "GetLatestDocUpdateTime no docs found, robotId: %d", robotPrimaryId)
		return 0, nil
	}
	return docs[0].UpdateTime.Unix(), nil
}
