package service

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	jsoniter "github.com/json-iterator/go"

	terrs "git.code.oa.com/trpc-go/trpc-go/errs"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// CreateSynonyms 创建同义词
func (s *Service) CreateSynonyms(ctx context.Context, req *pb.CreateSynonymsReq) (*pb.CreateSynonymsRsp, error) {
	log.InfoContextf(ctx, "CreateSynonyms Req: %+v", req)
	rsp := new(pb.CreateSynonymsRsp)
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	if err = s.isInTestMode(ctx, corpID, app.ID, nil); err != nil {
		return rsp, err
	}

	var cateID uint64
	if req.GetCateBizId() != "" {
		cateBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
		if err != nil {
			return nil, err
		}
		cateID, err = s.dao.CheckCateBiz(ctx, model.SynonymsCate, corpID, cateBizId, app.ID)
	} else {
		cateID, err = s.dao.GetRobotUncategorizedCateID(ctx, model.SynonymsCate, corpID, app.ID)
	}

	var synonymsList []string
	if synonymsList, err = checkCreateSynonymsReq(req.GetStandardWord(), req.GetSynonyms()); err != nil {
		return rsp, err
	}
	createReq := &model.SynonymsCreateReq{
		RobotID:      app.ID,
		CorpID:       corpID,
		CateID:       cateID,
		StandardWord: req.GetStandardWord(),
		Synonyms:     synonymsList,
	}
	createRsp, err := s.dao.CreateSynonyms(ctx, createReq)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	rsp.ConflictType = createRsp.ConflictType
	rsp.ConflictContent = createRsp.ConflictContent
	rsp.SynonymBizId = strconv.FormatUint(createRsp.SynonymsID, 10)

	log.InfoContextf(ctx, "CreateSynonyms Rsp: %+v", rsp)
	_ = s.dao.AddOperationLog(ctx, model.SynonymsEventAdd, corpID, app.ID, req, rsp, nil, nil)
	return rsp, nil
}

// ListSynonyms 同义词列表
func (s *Service) ListSynonyms(ctx context.Context, req *pb.ListSynonymsReq) (*pb.ListSynonymsRsp, error) {
	log.InfoContextf(ctx, "ListSynonyms Req:%+v", req)
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	synonymsListReq, err := s.dao.GetSynonymsListReq(ctx, req, app.ID, corpID)
	if err != nil {
		return nil, err
	}
	total, err := s.dao.GetSynonymsListCount(ctx, synonymsListReq)
	if err != nil {
		return nil, errs.ErrSystem
	}
	listRsp, err := s.dao.GetSynonymsList(ctx, synonymsListReq)
	if err != nil {
		return nil, errs.ErrSystem
	}

	return &pb.ListSynonymsRsp{
		Total:      uint64(total),
		PageNumber: req.GetPageNumber(),
		List:       getSynonymsFromItem(ctx, listRsp.Synonyms),
	}, nil
}

func getSynonymsFromItem(ctx context.Context, synonyms []*model.SynonymsItem) []*pb.Synonym {
	synonymsList := make([]*pb.Synonym, 0)
	for _, synonym := range synonyms {
		synonymsList = append(synonymsList, &pb.Synonym{
			SynonymBizId: strconv.FormatUint(synonym.SynonymsID, 10),
			StandardWord: synonym.StandardWord,
			Synonyms:     synonym.Synonyms,
			Status:       synonym.Status,
			StatusDesc:   i18n.Translate(ctx, synonym.StatusDesc),
			CreateTime:   synonym.CreateTime.Unix(),
			UpdateTime:   synonym.UpdateTime.Unix(),
		})
	}
	return synonymsList
}

// checkCreateSynonymsReq 检查创建同义词请求参数
func checkCreateSynonymsReq(standardWord string, synonyms []string) ([]string, error) {
	if standardWord == "" || utf8.RuneCountInString(standardWord) > int(config.App().Synonyms.SynonymsWordMaxLength) {
		return nil, errs.ErrSynonymsInvalidStandard
	}
	dedupSynonyms := slicex.Unique(synonyms)
	if len(dedupSynonyms) > int(config.App().Synonyms.MaxSynonymsCountPerWord) {
		return nil, errs.ErrSynonymsTooMany
	}

	for i := range dedupSynonyms {
		if dedupSynonyms[i] == "" ||
			utf8.RuneCountInString(synonyms[i]) > int(config.App().Synonyms.SynonymsWordMaxLength) ||
			dedupSynonyms[i] == standardWord {
			return nil, errs.ErrSynonymsInvalidWord
		}
	}
	return dedupSynonyms, nil
}

// ModifySynonyms 更新同义词
func (s *Service) ModifySynonyms(ctx context.Context, req *pb.ModifySynonymsReq) (*pb.ModifySynonymsRsp, error) {
	log.InfoContextf(ctx, "UpdateSynonyms Req: %+v", req)
	rsp := new(pb.ModifySynonymsRsp)
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	if err = s.isInTestMode(ctx, corpID, app.ID, nil); err != nil {
		return rsp, err
	}
	if _, err = checkCreateSynonymsReq(req.GetStandardWord(), req.GetSynonyms()); err != nil {
		return rsp, err
	}
	synonymBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetSynonymBizId())
	if err != nil {
		return nil, err
	}
	oldSynonym, err := s.dao.GetSynonymDetailsByBizID(ctx, corpID, app.ID, synonymBizID)
	if err != nil {
		return nil, errs.ErrSynonymsNotFound
	}
	if oldSynonym.IsDelete() {
		return rsp, errs.ErrSynonymsIsDeleted
	}

	var cateID uint64
	if req.GetCateBizId() != "" {
		cateBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
		if err != nil {
			return rsp, err
		}
		cateID, err = s.dao.CheckCateBiz(ctx, model.SynonymsCate, corpID, cateBizID, app.ID)
	} else {
		cateID, err = s.dao.GetRobotUncategorizedCateID(ctx, model.SynonymsCate, corpID, app.ID)
	}
	if err != nil {
		return rsp, err
	}
	synonymModifyReq := &model.SynonymsModifyReq{
		RobotID:      app.ID,
		CorpID:       corpID,
		SynonymID:    synonymBizID,
		CateID:       cateID,
		StandardWord: req.GetStandardWord(),
		Synonyms:     req.GetSynonyms(),
	}

	var conflictType uint32
	var conflictContent string
	conflictType, conflictContent, err = s.dao.UpdateSynonyms(ctx, oldSynonym, synonymModifyReq)
	if err != nil {
		return rsp, errs.ErrSystem
	}
	rsp.ConflictType = conflictType
	rsp.ConflictContent = conflictContent

	_ = s.dao.AddOperationLog(ctx, model.SynonymsEventEdit, corpID, app.ID, req, rsp, oldSynonym, synonymModifyReq)
	return rsp, nil
}

// DeleteSynonyms 删除同义词
func (s *Service) DeleteSynonyms(ctx context.Context, req *pb.DeleteSynonymsReq) (*pb.DeleteSynonymsRsp, error) {
	log.InfoContextf(ctx, "DeleteSynonyms Req: %+v", req)
	corpID := pkg.CorpID(ctx)
	staffID := pkg.StaffID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
	}
	if err = s.isInTestMode(ctx, corpID, app.ID, nil); err != nil {
		return nil, err
	}
	synonymsBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetSynonymBizIds())
	if err != nil {
		return nil, err
	}
	bizIds := slicex.Unique(synonymsBizIDs)
	details, err := s.dao.GetSynonymsDetailsByBizIDs(ctx, corpID, app.ID, bizIds)
	if err != nil || len(details) == 0 {
		return nil, errs.ErrSynonymsNotFound
	}
	notDeletedSynonyms := make([]*model.Synonyms, 0)
	for _, detail := range details {
		if !detail.IsDelete() {
			notDeletedSynonyms = append(notDeletedSynonyms, detail)
		}
	}
	if len(notDeletedSynonyms) == 0 {
		return nil, errs.ErrSynonymsForbidDelete
	}
	if err = s.dao.DeleteSynonyms(ctx, corpID, app.ID, staffID, notDeletedSynonyms); err != nil {
		return nil, errs.ErrSystem
	}
	_ = s.dao.AddOperationLog(ctx, model.SynonymsEventDel, corpID, app.ID, req, nil, nil, nil)
	return &pb.DeleteSynonymsRsp{}, nil
}

// UploadSynonymsList 上传同义词列表
func (s *Service) UploadSynonymsList(ctx context.Context, req *pb.UploadSynonymsListReq) (*pb.UploadSynonymsListRsp,
	error) {
	log.InfoContextf(ctx, "UploadSynonymsList Req: %+v", req)
	rsp := new(pb.UploadSynonymsListRsp)
	key := fmt.Sprintf(dao.LockForUplodSynonymsList, req.GetCosHash())
	if err := s.dao.Lock(ctx, key, 10*time.Second); err != nil {
		log.ErrorContextf(ctx, "UploadSynonymsList file lock req:%+v,err :%v", req, err)
		return rsp, errs.ErrSynonymsListUploading
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()
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
	corp, err := s.dao.GetCorpByID(ctx, app.CorpID)
	if err != nil || corp == nil {
		return rsp, errs.ErrCorpNotFound
	}
	if err = s.dao.CheckURLPrefix(ctx, app.CorpID, corp.BusinessID, app.BusinessID, req.CosUrl); err != nil {
		log.ErrorContextf(ctx, "UploadSynonymsList|CheckURLPrefix failed, err:%+v", err)
		return rsp, errs.ErrInvalidURL
	}
	// 文件大小限制
	fileSize, err := util.CheckReqParamsIsUint64(ctx, req.GetSize())
	if err != nil {
		return nil, err
	}
	if fileSize > config.App().Synonyms.ImportMaxFileSize {
		return nil, errs.ErrFileSizeTooBig
	}
	// 检查表结构
	if rsp, err := s.checkSynonymsXlsx(ctx, app.CorpID, app.ID, req.CosUrl); err != nil || rsp != nil {
		return rsp, err
	}

	if _, err = s.dao.CreateSynonymsImportTask(ctx, req, app.CorpID, app.StaffID, app.ID); err != nil {
		return nil, err
	}

	_ = s.dao.AddOperationLog(ctx, model.SynonymsListUpload, app.CorpID, app.ID, req, rsp, nil, nil)
	return rsp, nil
}

// ExportSynonymsList 导出同义词列表
func (s *Service) ExportSynonymsList(ctx context.Context, req *pb.ExportSynonymsListReq) (*pb.ExportSynonymsListRsp,
	error) {
	log.InfoContextf(ctx, "ExportSynonymsList Req: %+v", req)
	rsp := new(pb.ExportSynonymsListRsp)
	corpID := pkg.CorpID(ctx)
	staffID := pkg.StaffID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrExportSynonyms
	}
	_, err = util.CheckReqSliceUint64(ctx, req.GetSynonymsBizIds())
	if err != nil {
		return rsp, err
	}
	// 检查导出数量
	if len(req.GetSynonymsBizIds()) > 0 { // 按 ID 导出
		if len(req.GetSynonymsBizIds()) > int(config.App().CronTask.SynonymsTask.MaxSynonymsCount) {
			return rsp, errs.ErrExportSynonymsTooMany
		}
	} else { // 按筛选器导出
		l, err := s.ListSynonyms(ctx, req.GetFilters())
		if err != nil {
			return rsp, errs.ErrExportSynonyms
		}
		if l.GetTotal() > uint64(config.App().CronTask.SynonymsTask.MaxSynonymsCount) {
			return rsp, errs.ErrExportSynonymsTooMany
		}
	}

	paramStr, err := jsoniter.MarshalToString(req)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	export := model.Export{
		CorpID:        corpID,
		RobotID:       app.ID,
		CreateStaffID: staffID,
		TaskType:      model.ExportSynonymsTaskType,
		Name:          model.ExportSynonymsTaskName,
		Params:        paramStr,
		Status:        model.TaskExportStatusInit,
		UpdateTime:    now,
		CreateTime:    now,
	}

	params := model.ExportParams{
		CorpID:           corpID,
		RobotID:          app.ID,
		CreateStaffID:    staffID,
		TaskType:         model.ExportSynonymsTaskType,
		TaskName:         model.ExportSynonymsTaskName,
		Params:           paramStr,
		NoticeContent:    i18n.Translate(ctx, model.ExportSynonymsNoticeContent),
		NoticePageID:     model.NoticeSynonymsPageID,
		NoticeTypeExport: model.NoticeTypeSynonymsExport,
		NoticeContentIng: i18n.Translate(ctx, model.ExportSynonymsNoticeContentIng),
	}

	if _, err = s.dao.CreateExportTask(ctx, corpID, staffID, app.ID, export, params); err != nil {
		return rsp, err
	}

	return rsp, nil
}

// GroupSynonyms 分类批量操作
func (s *Service) GroupSynonyms(ctx context.Context, req *pb.GroupObjectReq) (*pb.GroupObjectRsp, error) {
	log.InfoContextf(ctx, "GroupSynonyms Req:%+v", req)
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
	var cateID uint64
	cateBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
	if err != nil {
		return nil, err
	}
	if cateID, err = s.dao.CheckCateBiz(ctx, model.SynonymsCate, corpID, cateBizID, app.ID); err != nil {
		return rsp, errs.ErrCateNotFound
	}
	ids := slicex.Unique(req.GetBizIds())
	if err = dao.GetCateDao(model.SynonymsCate).GroupCateObject(ctx, s.dao, model.SynonymsCate, ids, cateID, app); err != nil {
		return rsp, errs.ErrSystem
	}

	return rsp, nil
}

// checkSynonymsXlsx 检查同义词模板文件是否符合要求
func (s *Service) checkSynonymsXlsx(ctx context.Context, corpID, robotID uint64,
	cosURL string) (*pb.UploadSynonymsListRsp, error) {
	body, err := s.dao.GetObject(ctx, cosURL)
	if err != nil {
		return nil, errs.ErrSystem
	}
	// 将配置中文件头翻译成ctx中语言
	var checkHead []string
	for _, v := range model.SynonymsExcelTplHead {
		checkHead = append(checkHead, i18n.Translate(ctx, v))
	}
	log.InfoContextf(ctx, "checkSynonymsXlsx checkHead:%v", checkHead)
	// 复用QA的配置
	rows, bs, err := util.CheckContent(ctx, cosURL, 0, int(config.App().Synonyms.ImportMaxCount),
		checkHead, body, checkSynonymsRow)
	if err != nil {
		if !errors.Is(err, errs.ErrExcelContent) {
			return nil, err
		}
		key := cosURL + ".check.xlsx"
		if err = s.dao.PutObject(ctx, bs, key); err != nil {
			return nil, errs.ErrSystem
		}
		url, err := s.dao.GetPresignedURL(ctx, key)
		if err != nil {
			return nil, errs.ErrSystem
		}
		return &pb.UploadSynonymsListRsp{
			ErrorMsg:      i18n.Translate(ctx, i18nkey.KeyFileDataErrorPleaseDownloadErrorFile),
			ErrorLink:     url,
			ErrorLinkText: i18n.Translate(ctx, i18nkey.KeyDownload),
		}, nil
	}

	allCates, err := s.dao.GetCateList(ctx, model.SynonymsCate, corpID, robotID)
	if err != nil {
		return nil, errs.ErrSystem
	}

	tree := model.BuildCateTree(allCates)
	for _, row := range rows {
		_, cate := model.GetCatePath(row)
		tree.Create(cate)
	}
	limit := config.App().DocQA.CateNodeLimit
	if tree.NodeCount()-1 > limit {
		return nil, errs.ErrWrapf(errs.ErrCodeCateCountExceed, i18n.Translate(ctx, i18nkey.KeySynonymCategoryCountExceeded), limit)
	}

	return nil, nil
}

// checkSynonymsRow 检查同义词每一行的内容
func checkSynonymsRow(ctx context.Context, i int, row []string, allSynonyms map[string]int) string {
	ok, cates := model.GetCatePath(row)
	if !ok {
		return i18n.Translate(ctx, i18nkey.KeyCategoryErrorPleaseRefill)
	}

	for _, cate := range cates {
		if err := checkCateName(ctx, cate); err != nil {
			return terrs.Msg(err)
		}
	}

	if len(row) < model.SynonymsExcelTplHeadLen {
		return i18n.Translate(ctx, i18nkey.KeyStandardWordOrSynonymEmptyPleaseFill)
	}

	standard := strings.TrimSpace(row[model.SynonymsExcelTplStandardIndex])
	synonyms := strings.TrimSpace(row[model.SynonymsExcelTplSynonymsIndex])
	if standard == "" || synonyms == "" {
		return i18n.Translate(ctx, i18nkey.KeyStandardWordOrSynonymEmptyPleaseFill)
	}
	if _, ok := allSynonyms[standard]; ok {
		return i18n.Translate(ctx, i18nkey.KeyStandardWordDuplicateWithSynonym, standard)
	}
	synonymsList := pkg.SplitAndTrimString(synonyms, "\n")
	if len(synonymsList) > 0 {
		// 仅在文件内进行重复校验
		distinctSyns, err := checkCreateSynonymsReq(standard, synonymsList)
		if err != nil {
			return i18n.Translate(ctx, i18nkey.KeyStandardWordInfo, standard, terrs.Msg(err))
		}
		for _, w := range distinctSyns {
			if _, ok := allSynonyms[w]; ok {
				return i18n.Translate(ctx, i18nkey.KeyStandardWordSimilarWordDuplicate, standard, w)
			}
		}
	} else {
		return i18n.Translate(ctx, i18nkey.KeySynonymEmptyPleaseFill)
	}

	// add
	allSynonyms[standard] = i
	for idx := range synonymsList {
		allSynonyms[synonymsList[idx]] = i
	}

	return ""
}
