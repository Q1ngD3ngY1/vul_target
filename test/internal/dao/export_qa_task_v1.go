package dao

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"

	jsoniter "github.com/json-iterator/go"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// ExportQaTaskV1 导出 QA 任务定义
type ExportQaTaskV1 struct {
	Dao Dao
}

// GetExportTotal 获取导出 QA 总数
func (e ExportQaTaskV1) GetExportTotal(ctx context.Context, corpID, robotID uint64, params string) (uint64, error) {
	return 0, nil
}

// GetExportData 获取导出 QA 数据
func (e ExportQaTaskV1) GetExportData(ctx context.Context, corpID, robotID uint64, params string, page,
	pageSize uint32) (
	[][]string, error) {
	req := &pb.ExportQAListReqV1{}
	if err := jsoniter.UnmarshalFromString(params, req); err != nil {
		log.ErrorContextf(ctx, "任务参数解析失败 req:%+v err:%+v", req, err)
		return nil, err
	}
	qaIDs := req.QaIds
	var qas []*model.DocQA
	var err error
	if len(qaIDs) > 0 {
		limit := uint64(pageSize)
		offset := uint64(page-1) * limit
		qas, err = e.Dao.GetQAsByIDs(ctx, corpID, robotID, qaIDs, offset, limit)
		if err != nil {
			log.ErrorContextf(ctx, "根据QAID导出qa失败, 获取qa失败, err:%+v", err)
			return nil, err
		}
	} else {
		qaListReq := req.GetFilters()
		qaListReq.PageSize = pageSize
		qaListReq.Page = page
		log.DebugContextf(ctx, "根据筛选器导出qa, qaListReq: %+v", qaListReq)
		getAaListReq, err := e.getQaListReq(ctx, qaListReq, robotID, corpID)
		if err != nil {
			return nil, err
		}
		qas, err = e.Dao.GetQAList(ctx, getAaListReq)
		if err != nil {
			log.ErrorContextf(ctx, "根据筛选器导出qa失败, 获取qa失败, err:%+v, qas:%+v", err, qas)
			return nil, err
		}
	}
	categories, err := e.Dao.GetCateList(ctx, model.QACate, corpID, robotID)
	if err != nil {
		return nil, err
	}
	tree := model.BuildCateTree(categories)
	rows := e.getQAList(ctx, qas, tree)
	return rows, nil
}

// // GetExportData 获取导出 QA 数据
// func (e ExportQaTaskV1) GetExportData(ctx context.Context, corpID, robotID uint64, params string, page, pageSize uint32) (
//	[][]string, error) {
//	req := &pb.ExportQAListReqV1{}
//	if err := jsoniter.UnmarshalFromString(params, req); err != nil {
//		log.ErrorContextf(ctx, "任务参数解析失败 req:%+v err:%+v", req, err)
//		return nil, err
//	}
//	qaIDs := req.QaIds
//	var qas []*model.DocQA
//	var err error
//	if len(qaIDs) > 0 {
//		limit := uint64(pageSize)
//		offset := uint64(page-1) * limit
//		qas, err = e.Dao.GetQAsByIDs(ctx, corpID, robotID, qaIDs, offset, limit)
//		if err != nil {
//			log.ErrorContextf(ctx, "根据QAID导出qa失败, 获取qa失败, err:%+v", err)
//			return nil, err
//		}
//	} else {
//		qaListReq := req.GetFilters()
//		qaListReq.PageSize = pageSize
//		qaListReq.Page = page
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

func (e ExportQaTaskV1) getQAList(ctx context.Context, qas []*model.DocQA, tree *model.CateNode) [][]string {
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
		rows = append(rows, append(head, qa.Question, qa.Answer, qa.QuestionDesc,
			util.GetStringFromTime(ctx, model.ExcelTplTimeLayout, qa.ExpireEnd), qa.CustomParam))
	}
	return rows
}

// func (e ExportQaTaskV1) getQAList(qas []*model.DocQA, docs map[uint64]*model.Doc, tree *model.CateNode) [][]string {
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
func (e ExportQaTaskV1) GetExportHeader(ctx context.Context) []string {
	return model.ExcelTplHead
}

// // GetExportHeader 获取 QA 导出表头信息
// func (e ExportQaTaskV1) GetExportHeader() []string {
//	head := make([]string, 0)
//	for i := uint(1); i <= uint(model.ExcelTplCateLen); i++ {
//		head = append(head, fmt.Sprintf("%d级分类", i))
//	}
//	head = append(head, []string{i18nkey.KeyQuestion, i18nkey.KeyAnswer, "来源", "关联文档"}...)
//	return head
// }

// getQaListReq 获取 QaListReq 请求参数
func (e *ExportQaTaskV1) getQaListReq(ctx context.Context, req *pb.GetQAListReq, robotID,
	corpID uint64) (*model.QAListReq,
	error) {
	deletingDocID, err := e.getDeletingDocID(ctx, corpID, robotID)
	if err != nil {
		return nil, errs.ErrSystem
	}

	var cateIDs []uint64
	if req.GetCateId() != model.AllCateID {
		cates, err := e.Dao.GetCateList(ctx, model.QACate, corpID, robotID)
		if err != nil {
			return nil, errs.ErrSystem
		}
		node := model.BuildCateTree(cates).FindNode(uint64(req.GetCateId()))
		if node == nil {
			return nil, errs.ErrCateNotFound
		}
		cateIDs = append(node.ChildrenIDs(), node.ID)
	}

	return &model.QAListReq{
		CorpID:        corpID,
		RobotID:       robotID,
		IsDeleted:     model.QAIsNotDeleted,
		Query:         req.GetQuery(),
		Source:        req.GetSource(),
		ExcludeDocID:  deletingDocID,
		AcceptStatus:  req.GetAcceptStatus(),
		ReleaseStatus: req.GetReleaseStatus(),
		Page:          req.GetPage(),
		PageSize:      req.GetPageSize(),
		DocID:         utils.When(req.GetDocId() > 0, []uint64{req.GetDocId()}, nil),
		CateIDs:       cateIDs,
	}, nil
}

func (e ExportQaTaskV1) getDeletingDocID(ctx context.Context, corpID, robotID uint64) ([]uint64, error) {
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
