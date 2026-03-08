package dao

import (
	"context"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"strings"

	jsoniter "github.com/json-iterator/go"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// ExportQaTask 导出 QA 任务定义
type ExportQaTask struct {
	Dao Dao
}

// GetExportTotal 获取导出 QA 总数
func (e ExportQaTask) GetExportTotal(ctx context.Context, corpID, robotID uint64, params string) (uint64, error) {
	return 0, nil
}

// GetExportData Deprecate(部分代码) 获取导出 QA 数据
func (e ExportQaTask) GetExportData(ctx context.Context, corpID, robotID uint64, params string, page, pageSize uint32) (
	[][]string, error) {
	req := &pb.ExportQAListReq{}
	if err := jsoniter.UnmarshalFromString(params, req); err != nil {
		log.ErrorContextf(ctx, "任务参数解析失败 req:%+v err:%+v", req, err)
		return nil, err
	}
	var qas []*model.DocQA
	var err error
	if len(req.GetQaBizIds()) > 0 {
		limit := uint64(pageSize)
		offset := uint64(page-1) * limit
		qaBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetQaBizIds())
		if err != nil {
			return nil, err
		}
		qas, err = e.Dao.GetQAsByBizIDs(ctx, corpID, robotID, qaBizIDs, offset, limit)
		if err != nil {
			log.ErrorContextf(ctx, "根据QAID导出qa失败, 获取qa失败, err:%+v", err)
			return nil, err
		}
	} else {
		qaListReq := req.GetFilters()
		qaListReq.PageSize = pageSize
		qaListReq.PageNumber = page
		log.DebugContextf(ctx, "根据筛选器导出qa, qaListReq: %+v", qaListReq)
		getQaListReq, err := e.getQaListReq(ctx, qaListReq, robotID, corpID)
		if err != nil {
			return nil, err
		}
		qas, err = e.Dao.GetQAList(ctx, getQaListReq)
		if err != nil {
			log.ErrorContextf(ctx, "根据筛选器导出qa失败, 获取qa失败, err:%+v, qas:%+v", err, qas)
			return nil, err
		}
	}
	// 获取相似问
	similarQuestionMap := make(map[uint64][]*model.SimilarQuestionSimple)
	qaIDs := make([]uint64, 0, len(qas))
	for _, qa := range qas {
		qaIDs = append(qaIDs, qa.ID)
	}
	if similarQuestionMap, err = e.Dao.GetSimilarQuestionsSimpleByQAIDs(ctx, corpID, robotID, qaIDs); err != nil {
		log.ErrorContextf(ctx, "批量获取主问相似问失败, err:%+v", err)
	}
	categories, err := e.Dao.GetCateList(ctx, model.QACate, corpID, robotID)
	if err != nil {
		return nil, err
	}
	tree := model.BuildCateTree(categories)
	rows := e.getQAList(ctx, qas, similarQuestionMap, tree)
	return rows, nil
}

// // GetExportData Deprecate(部分代码) 获取导出 QA 数据
// func (e ExportQaTask) GetExportData(ctx context.Context, corpID, robotID uint64, params string, page, pageSize uint32) (
//	[][]string, error) {
//	req := &pb.ExportQAListReq{}
//	if err := jsoniter.UnmarshalFromString(params, req); err != nil {
//		log.ErrorContextf(ctx, "任务参数解析失败 req:%+v err:%+v", req, err)
//		return nil, err
//	}
//	var qas []*model.DocQA
//	var err error
//	if len(req.GetQaBizIds()) > 0 {
//		limit := uint64(pageSize)
//		offset := uint64(page-1) * limit
//		qas, err = e.Dao.GetQAsByBizIDs(ctx, corpID, robotID, req.GetQaBizIds(), offset, limit)
//		if err != nil {
//			log.ErrorContextf(ctx, "根据QAID导出qa失败, 获取qa失败, err:%+v", err)
//			return nil, err
//		}
//	} else {
//		qaListReq := req.GetFilters()
//		qaListReq.PageSize = pageSize
//		qaListReq.PageNumber = page
//		log.DebugContextf(ctx, "根据筛选器导出qa, qaListReq: %+v", qaListReq)
//		getAaListReq, err := e.getQaListReq(ctx, qaListReq, robotID, corpID)
//		if err != nil {
//			return nil, err
//		}
//		qas, err = e.Dao.GetQAList(ctx, getAaListReq)
//		if err != nil {
//			log.ErrorContextf(ctx, "根据筛选器导出qa失败, 获取qa失败, err:%+v, qas:%+v", err, qas)
//			return nil, err
//		}
//	}
//	docIDMap := make(map[uint64]struct{})
//	if len(qas) == 0 {
//		return nil, nil
//	}
//	for _, v := range qas {
//		docIDMap[v.DocID] = struct{}{}
//	}
//	docIDs := make([]uint64, 0, len(docIDMap))
//	for v := range docIDMap {
//		docIDs = append(docIDs, v)
//	}
//	docs, err := e.Dao.GetDocByIDs(ctx, docIDs)
//	if err != nil {
//		log.ErrorContextf(ctx, "导出qa失败, 获取doc失败, docIDs:%+v err:%+v", docIDs, err)
//		return nil, err
//	}
//	categories, err := e.Dao.GetQACateList(ctx, corpID, robotID)
//	if err != nil {
//		return nil, err
//	}
//	tree := model.BuildCateTree(categories)
//	rows := e.getQAList(qas, docs, tree)
//	return rows, nil
// }

// getQAList 支持相似问
func (e ExportQaTask) getQAList(ctx context.Context, qas []*model.DocQA, similarQuestionMap map[uint64][]*model.
	SimilarQuestionSimple,
	tree *model.CateNode) [][]string {
	var rows [][]string
	for _, qa := range qas {
		// 分类
		cateTree := tree.Path(ctx, qa.CategoryID)
		head := make([]string, 0, len(cateTree))
		head = append(head, cateTree...)
		headLen := len(cateTree)
		if headLen < model.ExcelTplCateLen {
			start := headLen + 1
			for i := start; i <= model.ExcelTplCateLen; i++ {
				head = append(head, "")
			}
		}
		// 增加相似问内容
		qaSim := ""
		if simList, exists := similarQuestionMap[qa.ID]; exists {
			s := make([]string, 0, len(simList))
			for _, simItem := range simList {
				s = append(s, simItem.Question)
			}
			qaSim = strings.Join(s, "\n")
		}
		rows = append(rows, append(head, qa.Question, qa.Answer, qa.QuestionDesc, qaSim,
			util.GetStringFromTime(ctx, model.ExcelTplTimeLayout, qa.ExpireEnd), qa.CustomParam,
			utils.When(qa.AttributeFlag > 0, i18n.Translate(ctx, model.ExcelTplQaStatusDisable), i18n.Translate(ctx, model.ExcelTplQaStatusEnable))))
	}
	return rows
}

// func (e ExportQaTask) getQAList(qas []*model.DocQA, docs map[uint64]*model.Doc, tree *model.CateNode) [][]string {
//	var rows [][]string
//	for _, qa := range qas {
//		docName := ""
//		if doc, ok := docs[qa.DocID]; ok {
//			docName = doc.FileName
//		}
//		// 分类
//		cateTree := tree.Path(qa.CategoryID)
//		head := make([]string, 0, len(cateTree))
//		head = append(head, cateTree...)
//		headLen := len(cateTree)
//		if headLen < model.ExcelTplCateLen {
//			start := headLen + 1
//			for i := start; i <= model.ExcelTplCateLen; i++ {
//				head = append(head, "")
//			}
//		}
//		rows = append(rows, append(head, qa.Question, qa.Answer, qa.SourceName(), docName))
//	}
//	return rows
// }

// GetExportHeader 获取 QA 导出表头信息
func (e ExportQaTask) GetExportHeader(ctx context.Context) []string {
	var headers []string
	for _, v := range model.ExcelTplHead {
		headers = append(headers, i18n.Translate(ctx, v))
	}
	return headers
}

// // GetExportHeader 获取 QA 导出表头信息
// func (e ExportQaTask) GetExportHeader() []string {
//	head := make([]string, 0)
//	for i := uint(1); i <= uint(model.ExcelTplCateLen); i++ {
//		head = append(head, fmt.Sprintf("%d级分类", i))
//	}
//	head = append(head, []string{i18nkey.KeyQuestion, i18nkey.KeyAnswer, "来源", "关联文档"}...)
//	return head
// }

// getQaListReq 获取 QaListReq 请求参数
func (e ExportQaTask) getQaListReq(ctx context.Context, req *pb.ListQAReq, robotID,
	corpID uint64) (*model.QAListReq,
	error) {
	deletingDocID, err := e.getDeletingDocID(ctx, corpID, robotID)
	if err != nil {
		return nil, errs.ErrSystem
	}

	var cateIDs []uint64
	if req.GetCateBizId() != model.AllCateID {
		cates, err := e.Dao.GetCateList(ctx, model.QACate, corpID, robotID)
		if err != nil {
			return nil, errs.ErrSystem
		}
		node := model.BuildCateTree(cates).FindNode(uint64(req.GetCateBizId()))
		if node == nil {
			return nil, errs.ErrCateNotFound
		}
		cateIDs = append(node.ChildrenIDs(), node.ID)
	}
	docID, err := e.validateDocAndRetrieveID(ctx, req.GetDocBizId(), robotID)
	if err != nil {
		return nil, err
	}
	validityStatus, releaseStatus, err := e.getQaExpireStatus(req)
	if err != nil {
		return nil, err
	}
	err = e.checkQueryType(req.GetQueryType())
	if err != nil {
		return nil, err
	}
	return &model.QAListReq{
		CorpID:         corpID,
		RobotID:        robotID,
		IsDeleted:      model.QAIsNotDeleted,
		Query:          req.GetQuery(),
		Source:         req.GetSource(),
		ExcludeDocID:   deletingDocID,
		AcceptStatus:   req.GetAcceptStatus(),
		ReleaseStatus:  slicex.Unique(releaseStatus),
		Page:           req.GetPageNumber(),
		PageSize:       req.GetPageSize(),
		DocID:          utils.When(docID > 0, []uint64{docID}, nil),
		CateIDs:        cateIDs,
		QueryType:      req.GetQueryType(),
		QueryAnswer:    req.GetQueryAnswer(),
		ValidityStatus: validityStatus,
	}, nil
}

// checkQueryType 校验查询类型
func (e ExportQaTask) checkQueryType(fileType string) error {
	if fileType != model.DocQueryTypeFileName && fileType != model.DocQueryTypeAttribute {
		return errs.ErrParamsNotExpected
	}
	return nil
}

func (e ExportQaTask) getQaExpireStatus(req *pb.ListQAReq) (uint32, []uint32, error) {
	if req == nil {
		return 0, nil, errs.ErrParameterInvalid
	}
	var validityStatus uint32
	if len(req.GetReleaseStatus()) == 0 {
		return validityStatus, req.GetReleaseStatus(), nil
	}
	var releaseStatus []uint32
	for i := range req.GetReleaseStatus() {
		switch req.GetReleaseStatus()[i] { // 预留后续会有未生效、生效中状态
		case model.QAReleaseStatusExpired:
			validityStatus = model.QaExpiredStatus
		case model.QAReleaseStatusCharExceeded:
			releaseStatus = append(releaseStatus, model.QAReleaseStatusCharExceeded,
				model.QAReleaseStatusAppealFailCharExceeded, model.QAReleaseStatusAuditNotPassCharExceeded,
				model.QAReleaseStatusLearnFailCharExceeded)
		case model.QAReleaseStatusResuming:
			releaseStatus = append(releaseStatus, model.QAReleaseStatusResuming,
				model.QAReleaseStatusAppealFailResuming, model.QAReleaseStatusAuditNotPassResuming,
				model.QAReleaseStatusLearnFailResuming)
		default:
			releaseStatus = append(releaseStatus, req.GetReleaseStatus()[i])
		}
	}
	// 如果选择了状态，但是没有选择已过期，那就是未过期
	if validityStatus != model.QaExpiredStatus && len(releaseStatus) > 0 {
		validityStatus = model.QaUnExpiredStatus
	}
	return validityStatus, releaseStatus, nil
}

func (e ExportQaTask) validateDocAndRetrieveID(ctx context.Context, docBizID uint64, robotID uint64) (uint64, error) {
	if docBizID == 0 {
		return 0, nil
	}
	doc, err := e.Dao.GetDocByBizID(ctx, docBizID, robotID)
	if err != nil {
		return 0, errs.ErrSystem
	}
	if doc == nil {
		return 0, errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		return 0, errs.ErrDocHasDeleted
	}
	return doc.ID, nil
}

func (e ExportQaTask) getDeletingDocID(ctx context.Context, corpID, robotID uint64) ([]uint64, error) {
	docs, err := e.Dao.GetDeletingDoc(ctx, corpID, robotID)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(docs))
	for _, doc := range docs {
		ids = append(ids, doc.ID)
	}
	return ids, nil
}
