package export

import (
	"context"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	"strconv"
	"strings"

	"github.com/spf13/cast"

	"git.woa.com/adp/kb/kb-config/internal/rpc"
	pm "git.woa.com/adp/pb-go/platform/platform_manager"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/common/x/utilx"
	"git.woa.com/adp/kb/kb-config/internal/dao/qa"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	cateLogic "git.woa.com/adp/kb/kb-config/internal/logic/category"
	docLogic "git.woa.com/adp/kb/kb-config/internal/logic/document"
	qa2 "git.woa.com/adp/kb/kb-config/internal/logic/qa"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

type QaExportLogic struct {
	qaDao     qa.Dao
	qaLogic   *qa2.Logic
	docLogic  *docLogic.Logic
	cateLogic *cateLogic.Logic
	rpc       *rpc.RPC
}

func NewQaExportLogic(qaDao qa.Dao, docLogic *docLogic.Logic, qaLogic *qa2.Logic, cateLogic *cateLogic.Logic,
	rpc *rpc.RPC) *QaExportLogic {
	return &QaExportLogic{
		qaDao:     qaDao,
		qaLogic:   qaLogic,
		cateLogic: cateLogic,
		docLogic:  docLogic,
		rpc:       rpc,
	}
}

func (e QaExportLogic) GetExportData(ctx context.Context,
	corpID, robotID uint64, params string, page, pageSize uint32) (
	[][]string, error) {
	req := &pb.ExportQAListReq{}
	if err := jsonx.UnmarshalFromString(params, req); err != nil {
		logx.E(ctx, "任务参数解析失败 req:%+v err:%+v", req, err)
		return nil, err
	}
	var qas []*qaEntity.DocQA
	var err error
	if len(req.GetQaBizIds()) > 0 {
		offset, limit := utilx.Page(page, pageSize)
		qaBizIDs, err := util.CheckReqSliceUint64(ctx, req.GetQaBizIds())
		if err != nil {
			return nil, err
		}
		qas, err = e.qaLogic.GetQAsByBizIDs(ctx, corpID, robotID, qaBizIDs, offset, limit)
		if err != nil {
			logx.E(ctx, "根据QAID导出qa失败, 获取qa失败, err:%+v", err)
			return nil, err
		}
	} else {
		qaListReq := req.GetFilters()
		qaListReq.PageSize = pageSize
		qaListReq.PageNumber = page
		logx.D(ctx, "根据筛选器导出qa, qaListReq: %+v", qaListReq)
		getQaListReq, err := e.getQaListReq(ctx, qaListReq, robotID, corpID)
		if err != nil {
			return nil, err
		}
		qas, err = e.qaLogic.GetQAList(ctx, getQaListReq)
		if err != nil {
			logx.E(ctx, "根据筛选器导出qa失败, 获取qa失败, err:%+v, qas:%+v", err, qas)
			return nil, err
		}
	}
	// 获取相似问
	qaIDs := make([]uint64, 0, len(qas))
	// 获取关联文档
	qaDocIDs := make([]uint64, 0)
	// 获取操作人id
	staffIDMap := make(map[uint64]bool)
	for _, qa := range qas {
		qaIDs = append(qaIDs, qa.ID)
		if qa.DocID != 0 {
			qaDocIDs = append(qaDocIDs, qa.DocID)
		}
		staffIDMap[qa.StaffID] = true
	}
	staffIDs := make([]uint64, 0, len(staffIDMap)) // 获取操作人id
	for staffID := range staffIDMap {
		staffIDs = append(staffIDs, staffID)
	}
	// 获取员工名称
	staffs, err := e.rpc.PlatformAdmin.DescribeStaffList(ctx, &pm.DescribeStaffListReq{
		StaffIds: staffIDs,
	})
	if err != nil { // 失败降级为返回员工ID
		logx.E(ctx, "GetExportData DescribeStaffList staffIDs:%v, error:%v", staffIDs, err)
	}

	docs, err := e.docLogic.GetDocByIDs(ctx, slicex.Unique(qaDocIDs), robotID)
	if err != nil {
		logx.E(ctx, "GetExportData GetDocByIDs, err:%+v", err)
		return nil, errs.ErrSystem
	}

	listReq := &qaEntity.SimilarityQuestionReq{
		CorpId:       corpID,
		RobotId:      robotID,
		IsDeleted:    qaEntity.QAIsNotDeleted,
		RelatedQAIDs: qaIDs,
	}
	simQAs, err := e.qaDao.BatchListSimilarQuestions(ctx, listReq)

	if err != nil {
		logx.E(ctx, "批量获取主问相似问失败, err:%+v", err)
		return nil, err
	}

	sqsMap := make(map[uint64][]*qaEntity.SimilarQuestionSimple)

	for _, sq := range simQAs {
		if _, ok := sqsMap[sq.RelatedQAID]; !ok {
			sqsMap[sq.RelatedQAID] = make([]*qaEntity.SimilarQuestionSimple, 0)
		}
		sqsMap[sq.RelatedQAID] = append(sqsMap[sq.RelatedQAID], &qaEntity.SimilarQuestionSimple{
			SimilarID:   sq.SimilarID,
			Question:    sq.Question,
			RelatedQAID: sq.RelatedQAID,
		})
	}
	categories, err := e.cateLogic.DescribeCateList(ctx, cateEntity.QACate, corpID, robotID)
	if err != nil {
		return nil, err
	}
	tree := cateEntity.BuildCateTree(categories)
	rows := e.getQAList(ctx, qas, sqsMap, tree, staffs, docs)
	return rows, nil
}

func (e QaExportLogic) getQAList(ctx context.Context, qas []*qaEntity.DocQA,
	similarQuestionMap map[uint64][]*qaEntity.SimilarQuestionSimple,
	tree *cateEntity.CateNode, staffIdMap map[uint64]*pm.StaffInfo, docs map[uint64]*docEntity.Doc) [][]string {
	logx.I(ctx, "getQAList: from %d qas, %d similarQuestionMap", len(qas), len(similarQuestionMap))
	var rows [][]string
	for _, qa := range qas {
		// 分类
		cateTree, _ := tree.Path(ctx, qa.CategoryID)
		head := make([]string, 0, len(cateTree))
		head = append(head, cateTree...)
		headLen := len(cateTree)
		if headLen < docEntity.ExcelTplCateLen {
			start := headLen + 1
			for i := start; i <= docEntity.ExcelTplCateLen; i++ {
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

		source := ""
		switch qa.Source {
		case docEntity.SourceFromDoc:
			source = "文档生成"
		case docEntity.SourceFromBatch:
			source = "批量导入"
		case docEntity.SourceFromManual:
			source = "手动录入"
		default:
			source = ""
		}

		staffName := cast.ToString(qa.StaffID)
		if staff, ok := staffIdMap[qa.StaffID]; ok { // 赋值员工名称
			staffName = staff.GetNickName()
		}
		fileName := ""
		if doc, ok := docs[qa.DocID]; ok {
			fileName = doc.GetFileNameByStatus()
		}
		rows = append(rows, append(head, qa.Question, qa.Answer, qa.QuestionDesc, qaSim,
			util.GetStringFromTime(ctx, docEntity.ExcelTplTimeLayout, qa.ExpireEnd), qa.CustomParam,
			getEnableScope(ctx, qa), source, fileName,
			strconv.FormatUint(qa.CharSize, 10),
			util.GetStringFromTime(ctx, docEntity.ExcelTplTimeLayout, qa.UpdateTime),
			util.GetStringFromTime(ctx, docEntity.ExcelTplTimeLayout, qa.CreateTime), staffName))
	}
	logx.D(ctx, "export qa %d rows", len(rows))
	return rows
}

func getEnableScope(ctx context.Context, qa *qaEntity.DocQA) string {
	if qa == nil {
		return ""
	}
	switch qa.EnableScope {
	case entity.EnableScopeAll:
		return i18n.Translate(ctx, i18nkey.KeyEnableScopeAll)
	case entity.EnableScopeDev:
		return i18n.Translate(ctx, i18nkey.KeyEnableScopeDev)
	case entity.EnableScopePublish:
		return i18n.Translate(ctx, i18nkey.KeyEnableScopePublish)
	case entity.EnableScopeDisable:
		return i18n.Translate(ctx, i18nkey.KeyEnableScopeDisable)
	default:
		return ""
	}
}

func (e QaExportLogic) getQaListReq(ctx context.Context, req *pb.ListQAReq, robotID,
	corpID uint64) (*qaEntity.QAListReq,
	error) {
	deletingDocID, err := e.getDeletingDocID(ctx, corpID, robotID)
	if err != nil {
		return nil, errs.ErrSystem
	}

	var cateIDs []uint64
	if req.GetCateBizId() != cateEntity.AllCateID {
		cates, err := e.cateLogic.DescribeCateList(ctx, cateEntity.QACate, corpID, robotID)
		if err != nil {
			return nil, errs.ErrSystem
		}
		node := cateEntity.BuildCateTree(cates).FindNode(uint64(req.GetCateBizId()))
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
	qaListReq := &qaEntity.QAListReq{
		CorpID:         corpID,
		RobotID:        robotID,
		IsDeleted:      qaEntity.QAIsNotDeleted,
		Query:          req.GetQuery(),
		Source:         req.GetSource(),
		ExcludeDocID:   deletingDocID,
		AcceptStatus:   req.GetAcceptStatus(),
		ReleaseStatus:  slicex.Unique(releaseStatus),
		Page:           req.GetPageNumber(),
		PageSize:       req.GetPageSize(),
		DocID:          gox.IfElse(docID > 0, []uint64{docID}, nil),
		CateIDs:        cateIDs,
		QueryType:      req.GetQueryType(),
		QueryAnswer:    req.GetQueryAnswer(),
		ValidityStatus: validityStatus,
	}
	if req.GetEnableScope() != pb.RetrievalEnableScope_ENABLE_SCOPE_TYPE_UNKNOWN {
		qaListReq.EnableScope = ptrx.Uint32(uint32(req.GetEnableScope()))
	}
	return qaListReq, nil
}

// checkQueryType 校验查询类型
func (e QaExportLogic) checkQueryType(fileType string) error {
	if fileType != docEntity.DocQueryTypeFileName && fileType != docEntity.DocQueryTypeAttribute {
		return errs.ErrParamsNotExpected
	}
	return nil
}

func (e QaExportLogic) getQaExpireStatus(req *pb.ListQAReq) (uint32, []uint32, error) {
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
		case qaEntity.QAReleaseStatusExpired:
			validityStatus = qaEntity.QaExpiredStatus
		case qaEntity.QAReleaseStatusCharExceeded:
			releaseStatus = append(releaseStatus, qaEntity.QAReleaseStatusCharExceeded,
				qaEntity.QAReleaseStatusAppealFailCharExceeded, qaEntity.QAReleaseStatusAuditNotPassCharExceeded,
				qaEntity.QAReleaseStatusLearnFailCharExceeded)
		case qaEntity.QAReleaseStatusResuming:
			releaseStatus = append(releaseStatus, qaEntity.QAReleaseStatusResuming,
				qaEntity.QAReleaseStatusAppealFailResuming, qaEntity.QAReleaseStatusAuditNotPassResuming,
				qaEntity.QAReleaseStatusLearnFailResuming)
		default:
			releaseStatus = append(releaseStatus, req.GetReleaseStatus()[i])
		}
	}
	// 如果选择了状态，但是没有选择已过期，那就是未过期
	if validityStatus != qaEntity.QaExpiredStatus && len(releaseStatus) > 0 {
		validityStatus = qaEntity.QaUnExpiredStatus
	}
	return validityStatus, releaseStatus, nil
}

func (e QaExportLogic) validateDocAndRetrieveID(ctx context.Context, docBizID uint64, robotID uint64) (uint64, error) {
	if docBizID == 0 {
		return 0, nil
	}
	doc, err := e.docLogic.GetDocByBizID(ctx, docBizID, robotID)
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

func (e QaExportLogic) getDeletingDocID(ctx context.Context, corpID, robotID uint64) ([]uint64, error) {
	docs, err := e.docLogic.GetDeletingDoc(ctx, corpID, robotID)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(docs))
	for _, doc := range docs {
		ids = append(ids, doc.ID)
	}
	return ids, nil
}

// GetExportHeader 获取 QA 导出表头信息
func (e QaExportLogic) GetExportHeader(ctx context.Context) []string {
	var headers []string
	for _, v := range docEntity.ExcelTplHead {
		headers = append(headers, i18n.Translate(ctx, v))
	}
	return headers
}
