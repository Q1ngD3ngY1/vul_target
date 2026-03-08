package qa

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	async "git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/internal/entity/finance"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	knowClient "git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/app/app_config"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
	"gorm.io/gorm"
)

// CreateQA 创建QA(支持相似问)
func (l *Logic) CreateQA(ctx context.Context, app *entity.App, qa *qaEntity.DocQA, cateBizId, docBizId string,
	businessSource uint32, businessID uint64, needAudit bool,
	attributeLabelReq *labelEntity.UpdateQAAttributeLabelReq, simQuestions []string) error {

	// 前端按base64传代码内容，需要做decode
	decodeQuestion, isBase64 := util.StrictBase64DecodeToValidString(qa.Question)
	logx.D(ctx, "base64 decode:%v original:%s decoded:%s ", isBase64, qa.Question, decodeQuestion)

	decodeAnswer, isBase64 := util.StrictBase64DecodeToValidString(qa.Answer)
	logx.D(ctx, "base64 decode:%v original:%s decoded:%s ", isBase64, qa.Answer, decodeAnswer)

	qa.Answer = strings.TrimSpace(decodeAnswer)
	qa.Question = strings.TrimSpace(decodeQuestion)

	var err error
	if err = l.CheckQAAndDescAndParam(ctx, qa.Question, qa.Answer,
		qa.QuestionDesc, qa.CustomParam); err != nil {
		return err
	}
	logx.I(ctx, "CreateQA checkQAAndDescAndParam ok")
	if _, err = l.CheckSimilarQuestionNumLimit(ctx, len(simQuestions), 0, 0); err != nil {
		return err
	}
	logx.I(ctx, "CreateQA checkSimilarQuestionNumLimit ok")
	var simTotalCharSize, simTotalBytes = 0, 0 // 相似问总字符数
	if simTotalCharSize, simTotalBytes, err = l.CheckSimilarQuestionContent(ctx, qa.Question, simQuestions); err != nil {
		return err
	}
	logx.I(ctx, "CreateQA checkSimilarQuestionContent ok (simTotalCharSize:%d, simTotalBytes:%d)", simTotalCharSize, simTotalBytes)
	if cateBizId != "" && cateBizId != "0" {
		var cateBizID uint64
		cateBizID, err = util.CheckReqParamsIsUint64(ctx, cateBizId)
		if err != nil {
			return err
		}
		qa.CategoryID, err = l.cateDao.VerifyCateBiz(ctx, cateEntity.QACate, app.CorpPrimaryId, cateBizID, app.PrimaryId)
	} else {
		qa.CategoryID, err = l.cateDao.DescribeRobotUncategorizedCateID(ctx, cateEntity.QACate, app.CorpPrimaryId, app.PrimaryId)
	}
	if err != nil {
		return errs.ErrCateNotFound
	}
	uiDocBizId, err := util.CheckReqParamsIsUint64(ctx, docBizId)
	if err != nil {
		return err
	}
	qa.DocID, err = l.ValidateDocAndRetrieveID(ctx, uiDocBizId, app.PrimaryId)
	if err != nil {
		return errs.ErrDocNotFound
	}

	err = util.CheckMarkdownImageURL(ctx, qa.Answer, contextx.Metadata(ctx).Uin(), app.BizId, nil)
	if err != nil {
		logx.W(ctx, "ModifyQA Answer CheckQaImgURLSafeToMD err:%d", err)
		return err
	}
	videoCharSize, videoBytes, err := l.GetVideoURLsCharSize(ctx, qa.Answer)
	if err != nil {
		return err
	}
	logx.I(ctx, "CreateQA Answer videoCharSize|%d", videoCharSize)
	// 检查字符限制(含相似问、答案中视频转换的字符数)
	diff := utf8.RuneCountInString(qa.Question+qa.Answer) + simTotalCharSize + videoCharSize
	qaSize := len(qa.Question) + len(qa.Answer) + simTotalBytes + int(videoBytes)
	err = l.financeLogic.CheckKnowledgeBaseQuota(ctx, finance.CheckQuotaReq{
		App:                  app,
		NewCharSize:          uint64(diff),
		NewKnowledgeCapacity: uint64(qaSize),
	})
	if err != nil {
		return err
	}
	qa.CharSize = uint64(diff) // 总字符数(含相似问)
	qa.QaSize = uint64(qaSize) // 总字节数(含相似问)

	var releaseStatus, isAuditFree = qaEntity.QAReleaseStatusAuditing, qaEntity.QAIsAuditNotFree
	if !needAudit {
		releaseStatus = qaEntity.QAReleaseStatusLearning
		isAuditFree = qaEntity.QAIsAuditFree
	}
	qa.ReleaseStatus = releaseStatus
	qa.IsAuditFree = isAuditFree
	qa.IsDeleted = qaEntity.QAIsNotDeleted
	qa.NextAction = qaEntity.NextActionAdd
	qa.RobotID = app.PrimaryId
	qa.CorpID = app.CorpPrimaryId

	err = l.qaDao.Query().Transaction(func(qtx *mysqlquery.Query) error {
		now := time.Now()
		qa.UpdateTime = now
		qa.CreateTime = now
		qa.NextAction = qaEntity.NextActionAdd
		qa.ReleaseStatus = qaEntity.QAReleaseStatusAuditing
		if !needAudit {
			qa.ReleaseStatus = qaEntity.QAReleaseStatusLearning
		}
		if err := l.qaDao.CreateDocQa(ctx, qa); err != nil {
			return err
		}

		if err := l.updateQABusinessSource(ctx, qa, businessSource, businessID); err != nil {
			logx.E(ctx, "Failed to updateQABusinessSource. err:%+v", err)
			return err
		}
		if err := l.UpdateQAAttributeLabel(ctx, qa.RobotID, qa.ID, attributeLabelReq); err != nil {
			return err
		}
		// 处理相似问
		var syncSimilarQuestionsIDs []uint64
		var err error
		if len(simQuestions) > 0 {
			sqs := qaEntity.NewSimilarQuestions(ctx, qa, simQuestions)
			if err := l.AddSimilarQuestions(ctx, sqs); err != nil {
				logx.E(ctx, "Failed to add similar questions. err:%+v", err)
				return err
			}
			if !needAudit {
				if syncSimilarQuestionsIDs, err = l.vectorSyncLogic.AddSimilarQuestionSyncBatch(ctx, sqs); err != nil {
					logx.E(ctx, "Failed to add similar question sync batch. err:%+v", err)
					return err
				}
			}
		}
		if qa.AcceptStatus != qaEntity.AcceptYes {
			// 如果是问答未采纳（例如应用包导入了待校验问答），则不进行审核。
			logx.D(ctx, "CreateQA acceptStatus:%d, no need to do audit", qa.AcceptStatus)
			return nil
		}
		if needAudit {
			err := l.CreateQaAudit(ctx, qa)
			if err != nil {
				return errs.ErrCreateAuditFail
			}
		} else {
			syncID, err := l.vectorSyncLogic.AddQASync(ctx, qa)
			if err != nil {
				logx.E(ctx, "Failed to CreateQA caused by addQASync error. qa:%+v err:%+v", qa, err)
				return err
			}
			l.vectorSyncLogic.Push(ctx, syncID)
			l.vectorSyncLogic.BatchPush(ctx, syncSimilarQuestionsIDs)
		}
		return l.financeLogic.UpdateAppCapacityUsage(ctx, entity.CapacityUsage{
			CharSize:          int64(qa.CharSize),
			ComputeCapacity:   int64(qa.QaSize),
			KnowledgeCapacity: int64(qa.QaSize),
		}, qa.RobotID, qa.CorpID)
	})
	if err != nil {
		return err
	}

	return nil
}

// updateQABusinessSource TODO
func (l *Logic) updateQABusinessSource(ctx context.Context, qa *qaEntity.DocQA, businessSource uint32,
	businessID uint64) error {
	switch businessSource {
	case qaEntity.QABusinessSourceUnsatisfiedReply:
		// return d.updateUnsatisfiedReplyStatus(ctx, tx, qa.CorpPrimaryId, qa.AppPrimaryId, []uint64{businessID},
		// 	entity.UnsatisfiedReplyStatusWait, entity.UnsatisfiedReplyStatusPass)
		updateReq := &pb.ModifyUnsatisfiedReplyReq{
			CorpId:     qa.CorpID,
			AppId:      qa.RobotID,
			ReplyBizId: []uint64{businessID},
			OldStatus:  entity.UnsatisfiedReplyStatusWait,
			NewStatus:  entity.UnsatisfiedReplyStatusPass,
		}

		_, err := l.rpc.AppAdmin.ModifyUnsatisfiedReply(ctx, updateReq)
		if err != nil {
			logx.E(ctx, "Failed to modify unsatisfied reply err:%+v", err)
			return err
		}

		return nil
	}
	return nil
}

// BatchCreateQA 批量创建QA
func (l *Logic) BatchCreateQA(
	ctx context.Context,
	seg *segEntity.DocSegmentExtend,
	doc *docEntity.Doc, qas []*qaEntity.QA, tree *cateEntity.CateNode, isNeedAudit bool,
) error {
	logx.D(ctx, "BatchCreateQA... (from doc:%+v, seg:%+v)", doc, seg)
	// 文档生成问答时不需要审核，在后续采纳问答时走审核;
	// 批量导入问答时需要审核
	err := l.segDao.Query().Transaction(func(tx *mysqlquery.Query) error {
		if err := l.updateSegmentStatus(ctx, seg); err != nil {
			return err
		}

		if err := l.createCates(ctx, cateEntity.QACate, seg.CorpID, seg.RobotID, tree); err != nil {
			return err
		}

		_, err := l.createQA(ctx, seg, doc, qas, tree, isNeedAudit)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}
	return nil
}

func (l *Logic) createCates(ctx context.Context, t cateEntity.CateObjectType, corpID, robotID uint64,
	tree *cateEntity.CateNode) error {
	if tree == nil {
		return nil
	}
	for _, child := range tree.Children {
		if child.ID == 0 {
			cate := &cateEntity.CateInfo{
				BusinessID: idgen.GetId(),
				CorpID:     corpID,
				RobotID:    robotID,
				Name:       child.Name,
				IsDeleted:  false,
				ParentID:   tree.ID,
				CreateTime: time.Now(),
				UpdateTime: time.Now(),
			}
			id, err := l.cateDao.CreateCate(ctx, t, cate)
			if err != nil {
				return fmt.Errorf("CreateCateChild error, err: %v", err)
			}
			child.ID = id
		}
		if err := l.createCates(ctx, t, corpID, robotID, child); err != nil {
			return err
		}
	}
	return nil
}

// createQA 创建问答,这里主要是excel生成的问答,保存文档的时候对字符数已经做过更新,故不在此接口中做机器人字符数的更新
func (l *Logic) createQA(
	ctx context.Context,
	seg *segEntity.DocSegmentExtend, doc *docEntity.Doc, qas []*qaEntity.QA, tree *cateEntity.CateNode, isNeedAudit bool,
) ([]uint64, error) {
	var syncIDs []uint64
	var err error
	var attributeLabelReq *labelEntity.UpdateQAAttributeLabelReq
	if doc.IsExcel() {
		// 文档未设置attr_range时，使用默认值
		doc.AttrRange = gox.IfElse(doc.AttrRange == docEntity.AttrRangeDefault, docEntity.AttrRangeAll, doc.AttrRange)
		attributeLabelReq, err = l.fillQAAttributeLabelsFromDoc(ctx, doc)
		if err != nil {
			return nil, err
		}
	}
	for _, v := range qas {
		qa := qaEntity.NewDocQA(doc, seg, v, uint64(tree.Find(v.Path)), isNeedAudit)
		qa.BusinessID = idgen.GetId()
		videoCharSize, videoBytes, err := l.GetVideoURLsCharSize(ctx, qa.Answer)
		if err != nil {
			logx.W(ctx, "createQA|GetVideoCharSize err,  qa: %+v, err: %+v", qa, err)
			return nil, err
		}
		qa.CharSize = qaEntity.CalcQACharSize(v) + uint64(videoCharSize)
		qa.QaSize = qaEntity.CalcQABytes(v) + uint64(videoBytes)
		if err := l.qaDao.CreateDocQa(ctx, qa); err != nil {
			logx.E(ctx, "Failed to create QA, err: %+v", err)
		}

		if doc.IsExcel() {
			if err := l.UpdateQAAttributeLabel(ctx, qa.RobotID, qa.ID, attributeLabelReq); err != nil {
				return nil, err
			}
		}
		// 处理相似问
		if len(v.SimilarQuestions) > 0 {
			sqs := qaEntity.NewSimilarQuestions(ctx, qa, v.SimilarQuestions)
			if err = l.AddSimilarQuestions(ctx, sqs); err != nil {
				logx.E(ctx, "添加相似问失败 err:%+v", err)
				return nil, err
			}
		}
		// if isNeedAudit { // 批量导入问答需要审核，不过审核的代码挪到了excel_to_qa.go文件中
		// 	err = d.CreateQaAudit(ctx, tx, qa)
		// 	if err != nil {
		// 		return nil, pkg.ErrCreateAuditFail
		// 	}
		// }
	}
	return syncIDs, nil
}

// fillQAAttributeLabelsFromDoc
func (l *Logic) fillQAAttributeLabelsFromDoc(ctx context.Context, doc *docEntity.Doc) (*labelEntity.UpdateQAAttributeLabelReq,
	error) {
	attrLabel, err := l.labelDao.GetDocAttributeLabel(ctx, doc.RobotID, []uint64{doc.ID})
	if err != nil {
		return nil, err
	}
	req := &labelEntity.UpdateQAAttributeLabelReq{
		IsNeedChange:    true,
		AttributeLabels: make([]*labelEntity.QAAttributeLabel, 0, len(attrLabel)),
	}
	for _, v := range attrLabel {
		req.AttributeLabels = append(req.AttributeLabels, &labelEntity.QAAttributeLabel{
			Source:  v.Source,
			AttrID:  v.AttrID,
			LabelID: v.LabelID,
		})
	}
	return req, nil
}

// UpdateQA 更新问答对
func (l *Logic) UpdateQA(ctx context.Context, qa *qaEntity.DocQA, sqm *qaEntity.SimilarQuestionModifyInfo, isNeedPublish,
	isNeedAudit bool, diffCharSize, diffBytes int64, attributeLabelReq *labelEntity.UpdateQAAttributeLabelReq) error {
	now := time.Now()
	var syncID uint64
	similarSyncIDs := make([]uint64, 0)
	err := l.qaDao.Query().Transaction(func(tx *mysqlquery.Query) error {
		/*
			`
			UPDATE
				t_doc_qa
			SET
			    question = :question,
			    answer = :answer,
			    custom_param = :custom_param,
				question_desc = :question_desc,
			    category_id = :category_id,
			    update_time = :update_time,
			    release_status = :release_status,
				is_audit_free = :is_audit_free,
			    similar_status = :similar_status,
			    doc_id = :doc_id,
			    next_action = :next_action,
			    char_size = :char_size,
			    attr_range = :attr_range,
			    expire_start = :expire_start,
			    expire_end = :expire_end,
				staff_id = :staff_id,
				qa_size = :qa_size
			WHERE
			    id = :id
		*/
		updateColumns := []string{
			qaEntity.DocQaTblColQuestion, qaEntity.DocQaTblColAnswer, qaEntity.DocQaTblColCustomParam, qaEntity.DocQaTblColQuestionDesc,
			qaEntity.DocQaTblColCategoryId, qaEntity.DocQaTblColUpdateTime, qaEntity.DocQaTblColReleaseStatus, qaEntity.DocQaTblColIsAuditFree,
			qaEntity.DocQaTblColSimilarStatus, qaEntity.DocQaTblColDocId, qaEntity.DocQaTblColNextAction, qaEntity.DocQaTblColCharSize,
			qaEntity.DocQaTblColAttrRange, qaEntity.DocQaTblColExpireStart, qaEntity.DocQaTblColExpireEnd, qaEntity.DocQaTblColStaffId,
			qaEntity.DocQaTblEnableScope, qaEntity.DocQaSimTblColQASize,
		}
		filter := &qaEntity.DocQaFilter{
			QAId: qa.ID,
		}
		qa.UpdateTime = now
		if err := l.qaDao.UpdateDocQas(ctx, updateColumns, filter, qa); err != nil {
			logx.E(ctx, "Failed to update qa args:%+v err:%+v", qa, err)
			return err
		}

		if sqm != nil {
			for i := range sqm.UpdateQuestions {
				sqm.UpdateQuestions[i].ReleaseStatus = qa.ReleaseStatus
				sqm.UpdateQuestions[i].IsAuditFree = qa.IsAuditFree
			}
			for i := range sqm.AddQuestions {
				sqm.AddQuestions[i].ReleaseStatus = qa.ReleaseStatus
				sqm.AddQuestions[i].IsAuditFree = qa.IsAuditFree
			}
		}

		if err := l.ModifySimilarQuestions(ctx, qa, sqm); err != nil {
			return err
		}
		if attributeLabelReq.IsNeedChange {
			// 只在属性标签发生变化时，才需要更新属性标签
			if err := l.UpdateQAAttributeLabel(ctx, qa.RobotID, qa.ID, attributeLabelReq); err != nil {
				return err
			}
		}
		if isNeedAudit {
			if err := l.CreateQaAudit(ctx, qa); err != nil {
				return errs.ErrCreateAuditFail
			}
		}
		if err := l.financeLogic.UpdateAppCapacityUsage(ctx, entity.CapacityUsage{
			CharSize:          diffCharSize,
			ComputeCapacity:   diffBytes,
			KnowledgeCapacity: diffBytes,
		}, qa.RobotID, qa.CorpID); err != nil {
			return err
		}

		if !isNeedPublish {
			return nil
		}
		if err := l.DeleteQASimilarByQA(ctx, qa); err != nil {
			return err
		}
		if !isNeedAudit { // 如果需要审核，就在审核之后再sync; 如果不需要审核就直接sync
			// 用于同步主问
			id, err := l.vectorSyncLogic.AddQASync(ctx, qa)
			if err != nil {
				return err
			}
			syncID = id

			// 用于同步相似问
			if sqm != nil {
				sqs := make([]*qaEntity.SimilarQuestion, 0)
				sqs = append(sqs, sqm.AddQuestions...)
				sqs = append(sqs, sqm.DeleteQuestions...)
				sqs = append(sqs, sqm.UpdateQuestions...)
				similarSyncIDs, err = l.vectorSyncLogic.AddSimilarQuestionSyncBatch(ctx, sqs)
				if err != nil {
					return err
				}
			}
		} else {
			// 需要审核的场景下，先sync删除的相似问，因为审核回调是异步逻辑，没法知道本次删除了哪些相似问
			if sqm != nil {
				sqs := make([]*qaEntity.SimilarQuestion, 0)
				sqs = append(sqs, sqm.DeleteQuestions...)
				var err error
				similarSyncIDs, err = l.vectorSyncLogic.AddSimilarQuestionSyncBatch(ctx, sqs)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !isNeedAudit { // 如果需要审核，就在审核之后再sync; 如果不需要审核就直接sync
		l.vectorSyncLogic.Push(ctx, syncID)
	}
	l.vectorSyncLogic.BatchPush(ctx, similarSyncIDs)
	return nil
}

func (l *Logic) updateSegmentStatus(ctx context.Context, seg *segEntity.DocSegmentExtend) error {
	/*
			`
			UPDATE
			    t_doc_segment
			SET
			    status = :status,
			    update_time = :update_time
			WHERE
			    id = :id
		`
	*/

	filter := &segEntity.DocSegmentFilter{
		ID: seg.ID,
	}
	updateColumns := map[string]any{
		segEntity.DocSegmentTblColStatus:     segEntity.SegmentStatusDone,
		segEntity.DocSegmentTblColUpdateTime: time.Now(),
	}

	if err := l.segDao.BatchUpdateDocSegmentByFilter(ctx, filter, updateColumns, nil); err != nil {
		return err
	}
	return nil
}

// VerifyQA 校验QA
func (l *Logic) VerifyQA(ctx context.Context, qas []*qaEntity.DocQA, robotID uint64) error {
	if err := l.qaDao.Query().Transaction(func(tx *mysqlquery.Query) error {
		return l.verifyQAs(ctx, qas, robotID)
	}); err != nil {
		logx.E(ctx, "校验问答对失败 err:%+v", err)
		return err
	}
	return nil
}

func (l *Logic) verifyQAs(ctx context.Context, qas []*qaEntity.DocQA, robotID uint64) error {
	length := len(qas)
	pageSize := 100
	pages := int(math.Ceil(float64(length) / float64(pageSize)))
	now := time.Now()
	charSize, qaSize := int64(0), int64(0)
	corpPrimaryID := uint64(0)
	for i := 0; i < pages; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > length {
			end = length
		}
		tmpQAs := qas[start:end]
		for _, qa := range tmpQAs {
			/*
						`
					UPDATE
						t_doc_qa
					SET
					    accept_status = :accept_status,
					    category_id = :category_id ,
					    question = :question ,
						answer = :answer ,
						next_action = :next_action,
						similar_status = :similar_status,
						update_time = :update_time,
						char_size = :char_size,
						release_status = :release_status,
						staff_id = :staff_id,
						qa_size = :qa_size
					WHERE
					    id = :id
				`
			*/
			charSize += int64(qa.CharSize)
			qaSize += int64(qa.QaSize)
			corpPrimaryID = qa.CorpID
			qa.UpdateTime = now
			qa.SimilarStatus = docEntity.SimilarStatusInit
			if !qa.IsNextActionAdd() {
				qa.NextAction = qaEntity.NextActionUpdate
			}
			if qa.IsAccepted() {
				qa.ReleaseStatus = qaEntity.QAReleaseStatusAuditing
				if !config.AuditSwitch() {
					qa.ReleaseStatus = qaEntity.QAReleaseStatusLearning
				}
			}
			updateColumns := []string{
				qaEntity.DocQaTblColAcceptStatus,
				qaEntity.DocQaTblColCategoryId,
				qaEntity.DocQaTblColQuestion,
				qaEntity.DocQaTblColAnswer,
				qaEntity.DocQaTblColNextAction,
				qaEntity.DocQaTblColSimilarStatus,
				qaEntity.DocQaTblColUpdateTime,
				qaEntity.DocQaTblColCharSize,
				qaEntity.DocQaTblColReleaseStatus,
				qaEntity.DocQaTblColStaffId,
				qaEntity.DocQaTblColQaSize,
			}
			filter := &qaEntity.DocQaFilter{
				QAId: qa.ID,
			}
			if err := l.qaDao.UpdateDocQas(ctx, updateColumns, filter, qa); err != nil {
				logx.E(ctx, "Failed to verify qa db record.  args:%+v err:%+v", qa, err)
				return err
			}
			if qa.IsAccepted() && config.AuditSwitch() { // 采纳的问答才需要送审，送审的问答在审核通过后，才会写入向量
				err := l.CreateQaAudit(ctx, qa)
				if err != nil {
					logx.E(ctx, "Failed to verify qa when createQaAudit qa:%+v err:%+v", qa, err)
					return err
				}
			} else {
				id, err := l.vectorSyncLogic.AddQASync(ctx, qa)
				if err != nil {
					logx.E(ctx, "Failed to verify qa when addQASync qa:%+v err:%+v", qa, err)
					return err
				}
				syncID := id
				l.vectorSyncLogic.Push(ctx, syncID)
			}
		}
	}
	return l.financeLogic.UpdateAppCapacityUsage(ctx, entity.CapacityUsage{
		CharSize:          charSize,
		ComputeCapacity:   qaSize,
		KnowledgeCapacity: qaSize,
	}, robotID, corpPrimaryID)
}

func (l *Logic) UpdateQAEnableScope(ctx context.Context, qas []*qaEntity.DocQA) error {
	if len(qas) == 0 {
		return nil
	}
	now := time.Now()
	var syncIDs []uint64
	var syncSimilarQuestionsIDs = make([]uint64, 0)
	err := l.qaDao.Query().TDocQa.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		/*
				`
				UPDATE
					t_doc_qa
				SET
				    question = :question,
				    answer = :answer,
				    custom_param = :custom_param,
					question_desc = :question_desc,
				    category_id = :category_id,
				    update_time = :update_time,
				    release_status = :release_status,
					is_audit_free = :is_audit_free,
				    similar_status = :similar_status,
				    doc_id = :doc_id,
				    next_action = :next_action,
				    char_size = :char_size,
				    attr_range = :attr_range,
				    expire_start = :expire_start,
				    expire_end = :expire_end,
					staff_id = :staff_id
				WHERE
				    id = :id
			`
		*/
		updateColumns := []string{
			qaEntity.DocQaTblColUpdateTime,
			qaEntity.DocQaTblColReleaseStatus,
			qaEntity.DocQaTblColSimilarStatus,
			qaEntity.DocQaTblColNextAction,
			qaEntity.DocQaTblColStaffId,
			qaEntity.DocQaTblEnableScope,
		}
		for _, qa := range qas {
			qa.UpdateTime = now
			isAuditOrAppealFail := false // 原先处于审核失败或者人工申诉失败，不修改qa状态，也不入库
			if qa.ReleaseStatus == qaEntity.QAReleaseStatusAuditNotPass ||
				qa.ReleaseStatus == qaEntity.QAReleaseStatusAppealFail {
				isAuditOrAppealFail = true
			}
			filter := &qaEntity.DocQaFilter{
				QAId: qa.ID,
			}
			if err := l.qaDao.UpdateDocQas(ctx, updateColumns, filter, qa); err != nil {
				logx.E(ctx, "Failed to update qa db record.  args:%+v err:%+v", qa, err)
				return err
			}

			if !isAuditOrAppealFail {
				id, err := l.vectorSyncLogic.AddQASync(ctx, qa)
				if err != nil {
					return err
				}
				syncIDs = append(syncIDs, id)
			}

			// 相似问处理
			sqs, err := l.GetSimilarQuestionsByQA(ctx, qa)
			if err != nil {
				// 伽利略error日志告警
				logx.E(ctx, "UpdateQAAttrRange qa_id: %d, GetSimilarQuestionsByQA err: %+v", qa.ID, err)
				// 柔性放过
			}
			if len(sqs) > 0 {
				if err = l.UpdateSimilarQuestions(ctx, qa, sqs); err != nil {
					logx.E(ctx, "Failed to UpdateSimilarQuestions. err:%+v", err)
					return err
				}
				if !isAuditOrAppealFail {
					if syncSimilarQuestionsIDs, err = l.vectorSyncLogic.AddSimilarQuestionSyncBatch(ctx, sqs); err != nil {
						logx.E(ctx, "Failed to AddSimilarQuestionSyncBatch(update) err:%+v", err)
						return err
					}
					syncIDs = append(syncIDs, syncSimilarQuestionsIDs...)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, syncID := range syncIDs {
		l.vectorSyncLogic.Push(ctx, syncID)
	}
	return nil
}

func (l *Logic) UpdateQAAttrRange(ctx context.Context, qas []*qaEntity.DocQA,
	attributeLabelReq *labelEntity.UpdateQAAttributeLabelReq) error {
	if len(qas) == 0 {
		return nil
	}
	now := time.Now()
	var syncIDs []uint64
	var syncSimilarQuestionsIDs = make([]uint64, 0)
	err := l.qaDao.Query().TDocQa.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		/*
				`
				UPDATE
					t_doc_qa
				SET
				    question = :question,
				    answer = :answer,
				    custom_param = :custom_param,
					question_desc = :question_desc,
				    category_id = :category_id,
				    update_time = :update_time,
				    release_status = :release_status,
					is_audit_free = :is_audit_free,
				    similar_status = :similar_status,
				    doc_id = :doc_id,
				    next_action = :next_action,
				    char_size = :char_size,
				    attr_range = :attr_range,
				    expire_start = :expire_start,
				    expire_end = :expire_end,
					staff_id = :staff_id
				WHERE
				    id = :id
			`
		*/
		updateColumns := []string{
			qaEntity.DocQaTblColQuestion,
			qaEntity.DocQaTblColAnswer,
			qaEntity.DocQaTblColCustomParam,
			qaEntity.DocQaTblColQuestionDesc,
			qaEntity.DocQaTblColCategoryId,
			qaEntity.DocQaTblColUpdateTime,
			qaEntity.DocQaTblColReleaseStatus,
			qaEntity.DocQaTblColIsAuditFree,
			qaEntity.DocQaTblColSimilarStatus,
			qaEntity.DocQaTblColDocId,
			qaEntity.DocQaTblColNextAction,
			qaEntity.DocQaTblColCharSize,
			qaEntity.DocQaTblColAttrRange,
			qaEntity.DocQaTblColExpireStart,
			qaEntity.DocQaTblColExpireEnd,
			qaEntity.DocQaTblColStaffId,
		}
		for _, qa := range qas {
			qa.UpdateTime = now
			qa.IsAuditFree = qaEntity.QAIsAuditNotFree
			isAuditOrAppealFail := false // 原先处于审核失败或者人工申诉失败，不修改qa状态，也不入库
			if qa.ReleaseStatus == qaEntity.QAReleaseStatusAuditNotPass ||
				qa.ReleaseStatus == qaEntity.QAReleaseStatusAppealFail {
				isAuditOrAppealFail = true
			}
			filter := &qaEntity.DocQaFilter{
				QAId: qa.ID,
			}
			if err := l.qaDao.UpdateDocQas(ctx, updateColumns, filter, qa); err != nil {
				logx.E(ctx, "Failed to update qa db record.  args:%+v err:%+v", qa, err)
				return err
			}

			if err := l.UpdateQAAttributeLabel(ctx, qa.RobotID, qa.ID, attributeLabelReq); err != nil {
				return err
			}
			if !isAuditOrAppealFail {
				id, err := l.vectorSyncLogic.AddQASync(ctx, qa)
				if err != nil {
					return err
				}
				syncIDs = append(syncIDs, id)
			}

			// 相似问处理
			sqs, err := l.GetSimilarQuestionsByQA(ctx, qa)
			if err != nil {
				// 伽利略error日志告警
				logx.E(ctx, "UpdateQAAttrRange qa_id: %d, GetSimilarQuestionsByQA err: %+v", qa.ID, err)
				// 柔性放过
			}
			if len(sqs) > 0 {
				if err = l.UpdateSimilarQuestions(ctx, qa, sqs); err != nil {
					logx.E(ctx, "Failed to UpdateSimilarQuestions. err:%+v", err)
					return err
				}
				if !isAuditOrAppealFail {
					if syncSimilarQuestionsIDs, err = l.vectorSyncLogic.AddSimilarQuestionSyncBatch(ctx, sqs); err != nil {
						logx.E(ctx, "Failed to AddSimilarQuestionSyncBatch(update) err:%+v", err)
						return err
					}
					syncIDs = append(syncIDs, syncSimilarQuestionsIDs...)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, syncID := range syncIDs {
		l.vectorSyncLogic.Push(ctx, syncID)
	}
	return nil
}

// UpdateQAsExpire 批量更新问答过期时间(支持相似问联动)
func (l *Logic) UpdateQAsExpire(ctx context.Context, qas []*qaEntity.DocQA) error {
	if len(qas) == 0 {
		return nil
	}
	now := time.Now()
	var syncIDs []uint64
	var syncSimilarQuestionsIDs = make([]uint64, 0)
	err := l.qaDao.Query().Transaction(func(query *mysqlquery.Query) error {
		/*
				`
				UPDATE
					t_doc_qa
				SET
				    update_time = :update_time,
					is_audit_free = :is_audit_free,
				    release_status = :release_status,
				    next_action = :next_action,
				    expire_end = :expire_end,
					staff_id = :staff_id
				WHERE
				    id = :id
			`
		*/

		updateColumns := []string{
			qaEntity.DocQaTblColUpdateTime,
			qaEntity.DocQaTblColReleaseStatus,
			qaEntity.DocQaTblColIsAuditFree,
			qaEntity.DocQaTblColNextAction,
			qaEntity.DocQaTblColExpireEnd,
			qaEntity.DocQaTblColStaffId,
		}

		for _, qa := range qas {
			qa.UpdateTime = now
			qa.IsAuditFree = qaEntity.QAIsAuditNotFree
			qa.NextAction = qaEntity.NextActionUpdate
			isAuditOrAppealFail := false // 原先处于审核失败或者人工申诉失败，不修改qa状态，也不入库
			if qa.ReleaseStatus == qaEntity.QAReleaseStatusAuditNotPass ||
				qa.ReleaseStatus == qaEntity.QAReleaseStatusAppealFail {
				isAuditOrAppealFail = true
			}
			if !isAuditOrAppealFail {
				qa.ReleaseStatus = qaEntity.QAReleaseStatusLearning
			}

			filter := &qaEntity.DocQaFilter{
				QAId: qa.ID,
			}
			if err := l.UpdateDocQas(ctx, updateColumns, filter, qa); err != nil {
				logx.E(ctx, "Failed to update qa expire end db record.  args:%+v err:%+v", qa, err)
				return err
			}

			if err := l.DeleteQASimilarByQA(ctx, qa); err != nil {
				logx.E(ctx, "Failed to DeleteQASimilarByQA when updateQAsExpire. args:%+v err:%+v", qa, err)
				return err
			}
			if !isAuditOrAppealFail {
				id, err := l.vectorSyncLogic.AddQASync(ctx, qa)
				if err != nil {
					logx.E(ctx, "Failed to AddQASync when updateQAsExpire.gs:%+v err:%+v", qa, err)
					return err
				}
				syncIDs = append(syncIDs, id)
			}
			// 相似问处理
			sqs, err := l.GetSimilarQuestionsByQA(ctx, qa)
			if err != nil {
				// 伽利略error日志告警
				logx.E(ctx, "UpdateQAsExpire qa_id: %d, GetSimilarQuestionsByQA err: %+v", qa.ID, err)
				// 柔性放过
			}
			if len(sqs) > 0 {
				if err = l.UpdateSimilarQuestions(ctx, qa, sqs); err != nil {
					logx.E(ctx, "Failed to UpdateSimilarQuestions. err:%+v", err)
					return err
				}
				if !isAuditOrAppealFail {
					if syncSimilarQuestionsIDs, err = l.vectorSyncLogic.AddSimilarQuestionSyncBatch(ctx, sqs); err != nil {
						logx.E(ctx, "Failed to AddSimilarQuestionSyncBatch(update) err:%+v", err)
						return err
					}
					syncIDs = append(syncIDs, syncSimilarQuestionsIDs...)
				}
			}
		}

		return nil
	})
	if err != nil {
		return err
	}
	for _, syncID := range syncIDs {
		l.vectorSyncLogic.Push(ctx, syncID)
	}
	return nil
}

// UpdateQAsDoc 更新问答关联文档
func (l *Logic) UpdateQAsDoc(ctx context.Context, qas []*qaEntity.DocQA) error {
	if len(qas) == 0 {
		return nil
	}
	now := time.Now()
	var syncIDs []uint64
	err := l.qaDao.Query().Transaction(func(query *mysqlquery.Query) error {

		/*
				 `
				UPDATE
					t_doc_qa
				SET
				    update_time = :update_time,
					is_audit_free = :is_audit_free,
				    release_status = :release_status,
				    next_action = :next_action,
				    doc_id = :doc_id,
					staff_id = :staff_id
				WHERE
				    id = :id
			`
		*/

		updateColumns := []string{
			qaEntity.DocQaTblColUpdateTime,
			qaEntity.DocQaTblColReleaseStatus,
			qaEntity.DocQaTblColIsAuditFree,
			qaEntity.DocQaTblColNextAction,
			qaEntity.DocQaTblColDocId,
			qaEntity.DocQaTblColStaffId,
		}

		for _, qa := range qas {
			qa.UpdateTime = now
			qa.IsAuditFree = qaEntity.QAIsAuditNotFree
			qa.NextAction = qaEntity.NextActionUpdate
			isAuditOrAppealFail := false // 原先处于审核失败或者人工申诉失败，不修改qa状态，也不入库
			if qa.ReleaseStatus == qaEntity.QAReleaseStatusAuditNotPass ||
				qa.ReleaseStatus == qaEntity.QAReleaseStatusAppealFail {
				isAuditOrAppealFail = true
			}
			if !isAuditOrAppealFail {
				qa.ReleaseStatus = qaEntity.QAReleaseStatusLearning
			}
			filter := &qaEntity.DocQaFilter{
				QAId: qa.ID,
			}
			if err := l.UpdateDocQas(ctx, updateColumns, filter, qa); err != nil {
				logx.E(ctx, "Failed to update qasDoc db record.  args:%+v err:%+v", qa, err)
				return err
			}

			if err := l.DeleteQASimilarByQA(ctx, qa); err != nil {
				logx.E(ctx, "Failed to DeleteQASimilarByQA when updateQAsDoc. args:%+v err:%+v", qa, err)
				return err
			}
			if !isAuditOrAppealFail {
				id, err := l.vectorSyncLogic.AddQASync(ctx, qa)
				if err != nil {
					logx.E(ctx, "Failed to AddQASync when updateQAsDoc. args:%+v err:%+v", qa, err)
					return err
				}
				syncIDs = append(syncIDs, id)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, syncID := range syncIDs {
		l.vectorSyncLogic.Push(ctx, syncID)
	}
	return nil
}

// DeleteDocToQA 删除文档只取消文档下问答对文档的引用
func (l *Logic) DeleteDocToQA(ctx context.Context, qa *qaEntity.DocQA) error {
	var syncID uint64
	tbl := l.qaDao.Query().TDocQa
	if err := tbl.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		/*
				`
				UPDATE
					t_doc_qa
				SET
				    update_time = :update_time,
				    doc_id = :doc_id
				WHERE
				    id = :id
			`
		*/

		updateColumns := []string{
			qaEntity.DocQaTblColUpdateTime,
			qaEntity.DocQaTblColDocId,
		}

		filter := &qaEntity.DocQaFilter{
			QAId: qa.ID,
		}

		qa.UpdateTime = time.Now()
		qa.DocID = 0

		if _, err := l.qaDao.UpdateDocQasWithTx(ctx, updateColumns, filter, qa, tx); err != nil {
			logx.E(ctx, "Failed to update qa doc id to 0 db record. err:%+v", err)
			return err
		}

		id, err := l.vectorSyncLogic.AddQASync(ctx, qa)
		if err != nil {
			return err
		}
		syncID = id
		return nil
	}); err != nil {
		return err
	}
	l.vectorSyncLogic.Push(ctx, syncID)
	return nil
}

// func (l *Logic) GetDocQas(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaFilter) ([]*qaEntity.DocQA, error) {
// 	return l.qaDao.GetDocQas(ctx, selectColumns, filter)
// }

func (l *Logic) GetAllDocQas(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaFilter) ([]*qaEntity.DocQA, error) {
	return l.qaDao.GetAllDocQas(ctx, selectColumns, filter)
}

func (l *Logic) GetQasBySegmentIDs(ctx context.Context, corpID, docID uint64, segmentIDs []uint64) ([]*qaEntity.DocQA, error) {
	filter := &qaEntity.DocQaFilter{
		CorpId:     corpID,
		DocID:      docID,
		SegmentIDs: segmentIDs,
	}
	return l.qaDao.GetAllDocQas(ctx, qaEntity.DocQaTblColList, filter)
}

func (l *Logic) UpdateDocQas(ctx context.Context, updateColumns []string, filter *qaEntity.DocQaFilter, docQa *qaEntity.DocQA) error {
	return l.qaDao.UpdateDocQas(ctx, updateColumns, filter, docQa)
}

func (l *Logic) GetDocQaCount(ctx context.Context, selectColumns []string, filter *qaEntity.DocQaFilter) (int64, error) {
	rawDB := l.qaDao.Query().TDocQa
	docQaTableName := rawDB.TableName()

	db, err := knowClient.GormClient(ctx, docQaTableName, filter.RobotId, 0, []client.Option{}...)
	if err != nil {
		logx.E(ctx, "get GormClient failed, err: %+v", err)
		return 0, err
	}
	return l.qaDao.GetDocQaCountWithTx(ctx, selectColumns, filter, db)
}

// GetSimilarChunkCount 获取相似问总数
func (l *Logic) GetSimilarChunkCount(ctx context.Context, corpID, appID uint64) (int, error) {

	/*
		`
			SELECT COUNT(*) FROM t_doc_qa
			WHERE corp_id = ? AND robot_id = ? AND is_deleted = ?
		`
	*/

	deletedFlag := qaEntity.QAIsNotDeleted
	filter := &qaEntity.DocQaFilter{

		CorpId:    corpID,
		RobotId:   appID,
		IsDeleted: &deletedFlag,
	}

	count, err := l.qaDao.GetDocQaCountWithTx(ctx, nil, filter, nil)

	if err != nil {
		logx.E(ctx, "GetSimilarChunkCount fail, err: %+v", err)
		return 0, err
	}
	return int(count), nil
}

func (l *Logic) GetQAIDsByOriginDocID(ctx context.Context, robotID, originDocID uint64) ([]uint64, error) {
	logx.I(ctx, "GetQAIDsByOriginDocID robotID: %+v, originDocID: %+v", robotID, originDocID)
	selectColumns := []string{qaEntity.DocQaTblColId}
	filter := &qaEntity.DocQaFilter{
		RobotId:     robotID,
		OriginDocID: originDocID,
	}
	ids := make([]uint64, 0)
	qas, err := l.qaDao.GetDocQasByPagenation(ctx, selectColumns, filter, false)

	if err != nil {
		return ids, err
	}

	for _, qa := range qas {
		ids = append(ids, qa.ID)
	}
	return ids, nil
}

// GetQADetail 获取QA详情
func (l *Logic) GetQADetail(ctx context.Context, corpID, robotID uint64, id uint64) (*qaEntity.DocQA, error) {
	qas, err := l.GetQADetails(ctx, corpID, robotID, []uint64{id})
	if err != nil {
		return nil, err
	}
	qa, ok := qas[id]
	if !ok {
		return nil, errs.ErrQANotFound
	}
	return qa, nil
}

// GetQADetails 批量获取QA详情
func (l *Logic) GetQADetails(ctx context.Context, corpID, robotID uint64, ids []uint64) (map[uint64]*qaEntity.DocQA, error) {
	/*
		`
			SELECT
				%s
			FROM
			    t_doc_qa
			WHERE
			    corp_id = ? AND robot_id = ? AND id IN (%s)
		`
	*/
	if len(ids) == 0 {
		return nil, nil
	}

	selectColumns := qaEntity.DocQaTblColList
	filter := &qaEntity.DocQaFilter{
		CorpId:  corpID,
		RobotId: robotID,
		QAIds:   ids,
	}
	list, err := l.qaDao.GetDocQasByPagenation(ctx, selectColumns, filter, false)
	if err != nil {
		logx.E(ctx, "Failed to GetQADetails. err:%+v", err)
		return nil, err
	}

	qas := make(map[uint64]*qaEntity.DocQA, 0)
	for _, item := range list {
		qas[item.ID] = item
	}
	return qas, nil

}

// GetQADetailsByBizIDs 批量获取QA详情
func (l *Logic) GetQADetailsByBizIDs(ctx context.Context, corpID, robotID uint64,
	bizIDs []uint64) (map[uint64]*qaEntity.DocQA, error) {
	/*
			`
			SELECT
				%s
			FROM
			    t_doc_qa
			WHERE
				corp_id = ? AND robot_id = ? AND business_id IN (%s)
		`
	*/
	if len(bizIDs) == 0 {
		return nil, nil
	}

	selectColumns := qaEntity.DocQaTblColList
	filter := &qaEntity.DocQaFilter{
		CorpId:      corpID,
		RobotId:     robotID,
		BusinessIds: bizIDs,
	}
	list, err := l.qaDao.GetDocQasByPagenation(ctx, selectColumns, filter, false)
	if err != nil {
		logx.E(ctx, "Failed to GetQADetailsByBizIDs. err:%+v", err)
		return nil, err
	}

	qas := make(map[uint64]*qaEntity.DocQA, 0)
	for _, item := range list {
		qas[item.BusinessID] = item
	}
	return qas, nil

}

// GetQADetailsByBizID 获取QA详情
func (l *Logic) GetQADetailsByBizID(ctx context.Context, corpID, robotID uint64, bizID uint64) (*qaEntity.DocQA, error) {
	qas, err := l.GetQADetailsByBizIDs(ctx, corpID, robotID, []uint64{bizID})
	if err != nil {
		return nil, err
	}
	qa, ok := qas[bizID]
	if !ok {
		return nil, errs.ErrQANotFound
	}
	return qa, nil
}

// GetDocQANum 统计文档有效问答对
func (l *Logic) GetDocQANum(ctx context.Context, corpID, robotID uint64, docIDs []uint64) (map[uint64]map[uint32]uint32,
	error) {
	/*
			`
			SELECT
				doc_id,is_deleted,count(*) as total
			FROM
			    t_doc_qa
			WHERE
			    corp_id = ? AND robot_id = ? AND doc_id IN (%s)
			GROUP BY doc_id,is_deleted
		`
	*/
	statMap := make(map[uint64]map[uint32]uint32, 0)
	if len(docIDs) == 0 {
		return statMap, nil
	}

	filter := &qaEntity.DocQaFilter{
		CorpId:  corpID,
		RobotId: robotID,
		DocIDs:  docIDs,
	}

	selectColumns := []string{qaEntity.DocQaTblColDocId, qaEntity.DocQaTblColIsDeleted}

	stat, err := l.qaDao.GetDocQANum(ctx, selectColumns, filter, nil)
	if err != nil {
		return nil, err
	}

	for _, v := range stat {
		_, ok := statMap[v.DocID]
		if !ok {
			statMap[v.DocID] = make(map[uint32]uint32)
		}
		statMap[v.DocID][v.IsDeleted] = v.Total
	}
	return statMap, nil
}

// CheckUnconfirmedQa 检查是否有未确认的QA
func (l *Logic) CheckUnconfirmedQa(ctx context.Context, robotID uint64) (bool, error) {
	/*
			`
			SELECT
				count(*)
			FROM
			    t_doc_qa
			WHERE
			    robot_id = ? AND accept_status = ?	AND is_deleted = ?
		`
	*/

	delFlag := qaEntity.QAIsDeleted
	filter := &qaEntity.DocQaFilter{
		AcceptStatus: qaEntity.AcceptInit,
		RobotId:      robotID,
		IsDeleted:    &delFlag,
	}
	total, err := l.qaDao.GetDocQaCountWithTx(ctx, nil, filter, nil)
	if err != nil {
		logx.E(ctx, "Failed to CheckUnconfirmedQa.  robotID:%+v err:%+v", robotID, err)
		return false, err
	}
	if total > 0 {
		return true, nil
	}
	return false, nil
}

// PollQaToSimilar 获取要匹配相似度的问答对列表
func (l *Logic) PollQaToSimilar(ctx context.Context) ([]*qaEntity.DocQA, error) {
	/*
		`
			SELECT
				%s
			FROM
			    t_doc_qa
			WHERE
			    similar_status = ? AND is_deleted = ? AND update_time >= ? AND update_time <= ?
			LIMIT
				?`
	*/

	delFlag := qaEntity.QAIsNotDeleted
	endTime := time.Now().Add(-config.App().CronTask.QASimilarTask.WaitAMoment)
	startTime := endTime.Add(-10 * time.Minute)

	selectColumns := qaEntity.DocQaTblColList
	filter := &qaEntity.DocQaFilter{
		SimilarStatus: ptrx.Uint32(docEntity.SimilarStatusInit),
		IsDeleted:     &delFlag,
		MinUpdateTime: startTime,
		MaxUpdateTime: endTime,
		Limit:         config.App().CronTask.QASimilarTask.PageSize,
	}
	qaList, err := l.qaDao.GetDocQasByPagenation(ctx, selectColumns, filter, true)
	if err != nil {
		logx.E(ctx, "Failed to PollQaToSimilar. err:%+v", err)
		return nil, err
	}
	return qaList, nil
}

// GetReleaseQACount 获取发布QA总数
func (l *Logic) GetReleaseQACount(ctx context.Context, corpID, robotID uint64, question string, startTime,
	endTime time.Time, actions []uint32) (uint64, error) {
	return l.qaDao.GetReleaseQACount(ctx, corpID, robotID, question, startTime, endTime, actions)
}

// GetReleaseQAList 获取发布问答对列表
func (l *Logic) GetReleaseQAList(ctx context.Context, corpID, robotID uint64, question string, startTime,
	endTime time.Time, actions []uint32, page, pageSize uint32) (
	[]*qaEntity.DocQA, error) {
	logx.D(ctx, "GetReleaseQAList corpID:%+v robotID:%+v question:%+v startTime:%+v endTime:%+v actions:%+v page:%+v pageSize:%+v",
		corpID, robotID, question, startTime, endTime, actions, page, pageSize)
	return l.qaDao.GetReleaseQAList(ctx, corpID, robotID, question, startTime, endTime, actions, page, pageSize)
}

// GetQAByID 通过ID获取QA详情
func (l *Logic) GetQAByID(ctx context.Context, id uint64) (*qaEntity.DocQA, error) {
	/*
		`
			SELECT
				%s
			FROM
			    t_doc_qa
			WHERE
			    id = ?
		`
	*/
	return l.qaDao.GetQAByID(ctx, id)
	// 	args := make([]any, 0, 1)
	// 	args = append(args, id)
	// 	querySQL := fmt.Sprintf(getQAByID, qaFields)
	// 	list := make([]*qaEntity.DocQA, 0)
	// 	if err := d.db.QueryToStructs(ctx, &list, querySQL, args...); err != nil {
	// 		logx.E(ctx, "通过ID获取QA详情失败 sql:%s args:%+v err:%+v", querySQL, args, err)
	// 		return nil, err
	// 	}
	// 	if len(list) == 0 {
	// 		return nil, nil
	// 	}
	// 	return list[0], nil
	// }
}

// GetQAsByIDs 根据ID获取问答
func (l *Logic) GetQAsByIDs(
	ctx context.Context, corpID, robotID uint64, qaIDs []uint64, offset, limit int,
) ([]*qaEntity.DocQA, error) {
	/*
		`
			SELECT
				%s
			FROM
			    t_doc_qa
			WHERE
			     corp_id = ? AND robot_id = ? AND accept_status = ? AND is_deleted = ? AND id IN(?)
			ORDER BY
			    id ASC
			LIMIT ?,?
		`
	*/
	if len(qaIDs) == 0 {
		return nil, nil
	}

	delFlag := qaEntity.QAIsNotDeleted

	filter := &qaEntity.DocQaFilter{
		CorpId:       corpID,
		RobotId:      robotID,
		AcceptStatus: qaEntity.AcceptYes,
		IsDeleted:    &delFlag,
		QAIds:        qaIDs,
		Offset:       offset,
		Limit:        limit,
	}

	selectColumns := qaEntity.DocQaTblColList

	qaList, err := l.qaDao.GetDocQasByPagenation(ctx, selectColumns, filter, true)
	if err != nil {
		logx.E(ctx, "Failed to GetQAsByIDs. err: %v", err)
		return nil, err
	}
	return qaList, nil
}

// GetQAByBizID 通过bizID获取QA详情
func (l *Logic) GetQAByBizID(ctx context.Context, bizID uint64) (*qaEntity.DocQA, error) {
	/*
		`
			SELECT
				%s
			FROM
			    t_doc_qa
			WHERE
			    business_id = ?
		`

	*/
	filter := &qaEntity.DocQaFilter{
		BusinessId: bizID,
	}

	selectColumns := qaEntity.DocQaTblColList
	qa, err := l.qaDao.GetQaByFilterWithTx(ctx, selectColumns, filter, nil)
	if err != nil {
		logx.E(ctx, "Failed to GetQAByBizID. err:%+v", err)
		return nil, err
	}
	return qa, nil
}

func (l *Logic) GetQAsByBizIDs(ctx context.Context, corpID, robotID uint64, qaBizIDs []uint64, offset, limit int,
) ([]*qaEntity.DocQA, error) {
	return l.qaDao.GetQAsByBizIDs(ctx, corpID, robotID, qaBizIDs, offset, limit)
}

// LockOneQa 锁定一条问答对
func (l *Logic) LockOneQa(ctx context.Context, task *qaEntity.DocQA) error {
	/*
		`
			UPDATE
				t_doc_qa
			SET
			    similar_status = :similar_status
			WHERE
			    id = :id
		`
	*/

	task.SimilarStatus = docEntity.SimilarStatusIng

	// updateColumns := map[string]any{qaEntity.DocQaTblColSimilarStatus: task.SimilarStatus}
	updateColumns := []string{qaEntity.DocQaTblColSimilarStatus}

	filter := &qaEntity.DocQaFilter{
		QAId: task.ID,
	}

	if rowAffected, err := l.qaDao.UpdateDocQasWithTx(ctx, updateColumns, filter, task, nil); err != nil {
		logx.E(ctx, "Failed to LockOneQa.rr:%+v", err)
		return err
	} else if rowAffected == 0 {
		return errs.ErrLockTaskFail
	}

	return nil
}

// UnLockOneQa 解锁一条问答对
func (l *Logic) UnLockOneQa(ctx context.Context, task *qaEntity.DocQA) error {
	task.SimilarStatus = docEntity.SimilarStatusEnd
	// updateColumns := map[string]any{qaEntity.DocQaTblColSimilarStatus: task.SimilarStatus}
	updateColumns := []string{qaEntity.DocQaTblColSimilarStatus}

	filter := &qaEntity.DocQaFilter{
		QAId: task.ID,
	}

	if rowAffected, err := l.qaDao.UpdateDocQasWithTx(ctx, updateColumns, filter, task, nil); err != nil {
		logx.E(ctx, "Failed to LockOneQa.rr:%+v", err)
		return err
	} else if rowAffected == 0 {
		return errs.ErrLockTaskFail
	}

	return nil
}

// GetRobotQAUsage 获取单个机器人问答使用量（字符和容量）
func (l *Logic) GetRobotQAUsage(ctx context.Context, robotID uint64, corpID uint64) (entity.CapacityUsage, error) {
	return l.qaDao.GetRobotQAUsage(ctx, robotID, corpID)
}

// GetRobotQAExceedUsage 获取机器人超量问答字符总数
func (l *Logic) GetRobotQAExceedUsage(ctx context.Context, corpID uint64, robotIDs []uint64) (
	map[uint64]entity.CapacityUsage, error) {

	appQAExceedCapacity, err := l.qaDao.GetRobotQAExceedUsage(ctx, corpID, robotIDs)
	if err != nil {
		logx.E(ctx, "Failed to GetRobotQAExceedUsage. err:%+v", err)
		return nil, err
	}
	return appQAExceedCapacity, nil
}

// UpdateAuditQA 更新QA审核状态
func (l *Logic) UpdateAuditQA(ctx context.Context, qa *qaEntity.DocQA) error {
	/*
		`
			UPDATE
				t_doc_qa
			SET
			    release_status = :release_status, message = :message, update_time = :update_time
			WHERE
			    id = :id
		`
	*/

	qa.UpdateTime = time.Now()
	updateColumns := []string{qaEntity.DocQaTblColReleaseStatus, qaEntity.DocQaTblColMessage, qaEntity.DocQaTblColUpdateTime}
	filter := &qaEntity.DocQaFilter{
		QAId: qa.ID,
	}

	// if err := d.db.Transactionx(ctx, func(tx *sqlx.Tx) error {
	_, err := l.qaDao.UpdateDocQasWithTx(ctx, updateColumns, filter, qa, nil)
	if err != nil {
		logx.E(ctx, "Failed to UpdateAuditQA. err:%+v", err)
		return err
	}
	return nil
}

// UpdateQADisableState 更新问答对停用启用状态
func (l *Logic) UpdateQADisableState(ctx context.Context, qa *qaEntity.DocQA, sqm *qaEntity.SimilarQuestionModifyInfo,
	isDisable bool) error {
	now := time.Now()
	var syncID uint64
	similarSyncIDs := make([]uint64, 0)
	logx.I(ctx, "UpdateQADisableState qa.AttributeFlag:%v", qa.AttributeFlag)
	err := l.qaDao.Query().TDocQa.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		qa.UpdateTime = now
		updateQAFilter := &qaEntity.DocQaFilter{
			QAId:    qa.ID,
			RobotId: qa.RobotID,
		}
		// 考虑后续迁移tdsql,问答更新非事务
		if err := l.qaDao.UpdateDocQas(ctx, []string{qaEntity.DocQaTblColAttributeFlag, qaEntity.DocQaTblColStaffId,
			qaEntity.DocQaTblColReleaseStatus, qaEntity.DocQaTblColNextAction}, updateQAFilter, qa); err != nil {
			logx.E(ctx, "更新问答对失败 args:%+v err:%+v", qa, err)
			return err
		}
		// 用于同步主问
		id, err := l.vectorSyncLogic.AddQASync(ctx, qa)
		// id, err := l.vectorSyncLogic.AddQASync(ctx, tx, qa)
		if err != nil {
			return err
		}
		syncID = id
		// 用于同步相似问
		if sqm != nil {
			sqs := make([]*qaEntity.SimilarQuestion, 0)
			sqs = append(sqs, sqm.AddQuestions...)
			sqs = append(sqs, sqm.DeleteQuestions...)
			sqs = append(sqs, sqm.UpdateQuestions...)
			similarSyncIDs, err = l.vectorSyncLogic.AddSimilarQuestionSyncBatch(ctx, sqs)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		logx.E(ctx, "UpdateQADisableState err:%+v", err)
		return err
	}
	l.vectorSyncLogic.Push(ctx, syncID)
	l.vectorSyncLogic.BatchPush(ctx, similarSyncIDs)
	return nil
}

// UpdateQAAuditStatusAndUpdateTimeTx 事务的方式更新问答对审核状态和更新时间
func (l *Logic) UpdateQAAuditStatusAndUpdateTimeTx(ctx context.Context, qa *qaEntity.DocQA) error {
	/*
		`
				UPDATE
					t_doc_qa
				SET
				    update_time = :update_time,
				    release_status = :release_status,
		            is_audit_free = :is_audit_free
				WHERE
				    id = :id
			`
	*/
	updateQAFilter := &qaEntity.DocQaFilter{
		QAId: qa.ID,
	}
	if err := l.qaDao.UpdateDocQas(ctx, []string{qaEntity.DocQaTblColUpdateTime, qaEntity.DocQaTblColReleaseStatus, qaEntity.DocQaTblColIsAuditFree},
		updateQAFilter, qa); err != nil {
		logx.E(ctx, "Failed to update qa audit status args:%+v err:%+v", qa, err)
		return err
	}
	return nil
}

func (l *Logic) UpdateQAReleaseStatus(ctx context.Context, qa *qaEntity.DocQA) error {
	updateQAFilter := &qaEntity.DocQaFilter{
		QAId: qa.ID,
	}
	if err := l.qaDao.UpdateDocQas(ctx, []string{qaEntity.DocQaTblColUpdateTime, qaEntity.DocQaTblColReleaseStatus},
		updateQAFilter, qa); err != nil {
		logx.E(ctx, "Failed to update qa status args:%+v err:%+v", qa, err)
		return err
	}
	return nil
}

// DeleteQAsCharSizeExceeded 删除超量失效过期的问答
func (l *Logic) DeleteQAsCharSizeExceeded(ctx context.Context, corpID uint64, robotID uint64,
	reserveTime time.Duration) error {
	req := &qaEntity.QAListReq{
		CorpID:  corpID,
		RobotID: robotID,
		ReleaseStatus: []uint32{
			qaEntity.QAReleaseStatusCharExceeded,
		},
		Page:     1,
		PageSize: 1000,
	}
	qas, err := l.GetQAList(ctx, req)
	if err != nil {
		return err
	}
	exTimeoutQAs := make([]*qaEntity.DocQA, 0, len(qas))
	for _, qa := range qas {
		if time.Now().Before(qa.UpdateTime.Add(reserveTime)) {
			continue
		}
		exTimeoutQAs = append(exTimeoutQAs, qa)
	}
	if len(exTimeoutQAs) == 0 {
		return nil
	}
	return l.DeleteQAs(ctx, corpID, robotID, 0, exTimeoutQAs)
}

// DeleteQAs 删除QA(支持相似问联动)
func (l *Logic) DeleteQAs(ctx context.Context, corpID, robotID, staffID uint64, qas []*qaEntity.DocQA) error {
	if err := l.qaDao.Query().Transaction(func(tx *mysqlquery.Query) error {
		return l.deleteQAs(ctx, corpID, robotID, staffID, qas)
	}); err != nil {
		logx.E(ctx, "Fialed to delete QAs. err:%+v", err)
		return err
	}
	return nil
}

func (l *Logic) deleteQAs(ctx context.Context, corpID, robotID, staffID uint64, qas []*qaEntity.DocQA) error {
	length := len(qas)
	pageSize := 100
	pages := int(math.Ceil(float64(length) / float64(pageSize)))
	now := time.Now()
	logx.I(ctx, "deleteQAs is divided pages:%d, total:%d", pages, length)
	for i := 0; i < pages; i++ {
		start := pageSize * i
		end := pageSize * (i + 1)
		if end > length {
			end = length
		}
		tmpQAs := qas[start:end]
		qaIDs := make([]uint64, 0, len(tmpQAs))
		var charSize, qaSize int64
		corpPrimaryID := uint64(0)
		for _, qa := range tmpQAs {
			corpPrimaryID = qa.CorpID
			qaIDs = append(qaIDs, qa.ID)
			if qa.AcceptStatus == qaEntity.AcceptYes && qa.IsDeleted != qaEntity.QAIsDeleted {
				charSize += int64(qa.CharSize)
				qaSize += int64(qa.QaSize)
			}
			qa.IsDeleted = qaEntity.QAIsDeleted
			qa.UpdateTime = now
			if !qa.IsNextActionAdd() {
				qa.NextAction = qaEntity.NextActionDelete
				qa.ReleaseStatus = qaEntity.QAReleaseStatusInit
				qa.IsAuditFree = qaEntity.QAIsAuditNotFree
			}
			/*
					`
					UPDATE
						t_doc_qa
					SET
					    is_deleted = :is_deleted,
					    update_time = :update_time,
					    release_status = :release_status,
						is_audit_free = :is_audit_free,
					    next_action = :next_action
					WHERE
					    id = :id
				`
			*/

			updateQAFilter := &qaEntity.DocQaFilter{
				QAId: qa.ID,
			}
			if err := l.qaDao.UpdateDocQas(ctx, []string{qaEntity.DocQaTblColIsDeleted, qaEntity.DocQaTblColUpdateTime,
				qaEntity.DocQaTblColReleaseStatus, qaEntity.DocQaTblColIsAuditFree, qaEntity.DocQaTblColNextAction}, updateQAFilter, qa); err != nil {
				logx.E(ctx, "Failed to update qa delete flag when delete QA. args:%+v err:%+v", qa, err)
				return err
			}

			if err := l.labelDao.DeleteQAAttributeLabel(ctx, qa.RobotID, qa.ID); err != nil {
				logx.E(ctx, "Faile to delete QA attribute label.err:%+v", err)
				return err
			}
			if err := l.DeleteSimilarQuestionsByQA(ctx, qa); err != nil {
				logx.E(ctx, "Failed to delete similar questions.rr:%+v", err)
				return err
			}
		}
		if err := l.financeLogic.UpdateAppCapacityUsage(ctx, entity.CapacityUsage{
			CharSize:          -charSize,
			ComputeCapacity:   -qaSize,
			KnowledgeCapacity: -qaSize,
		}, robotID, corpPrimaryID); err != nil {
			return err
		}
		// 这里没有sync同步操作, task处理内有sync
		if err := scheduler.NewQADeleteTask(ctx, robotID, entity.QADeleteParams{
			CorpID:  corpID,
			StaffID: staffID,
			RobotID: robotID,
			QAIDs:   qaIDs,
		}); err != nil {
			logx.E(ctx, "创建删除问答对任务失败 err:%+v", err)
			return err
		}
	}
	return nil
}

// DeleteQA 删除一条问答(支持相似问联动)
func (l *Logic) DeleteQA(ctx context.Context, qa *qaEntity.DocQA) error {
	var syncID uint64
	var syncSimilarQuestionsIDs = make([]uint64, 0)
	var deleteCharSize, deleteQaSize int64
	if err := l.qaDao.Query().TDocQa.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		if qa.AcceptStatus == qaEntity.AcceptYes && qa.IsDeleted != qaEntity.QAIsDeleted {
			deleteCharSize = int64(qa.CharSize)
			deleteQaSize = int64(qa.QaSize)
		}
		if err := l.financeLogic.UpdateAppCapacityUsage(ctx, entity.CapacityUsage{
			CharSize:          -deleteCharSize,
			ComputeCapacity:   -deleteQaSize,
			KnowledgeCapacity: -deleteQaSize,
		}, qa.RobotID, qa.CorpID); err != nil {
			return err
		}
		qa.IsDeleted = qaEntity.QAIsDeleted
		qa.UpdateTime = time.Now()
		if !qa.IsNextActionAdd() {
			qa.NextAction = qaEntity.NextActionDelete
			qa.ReleaseStatus = qaEntity.QAReleaseStatusInit
			qa.IsAuditFree = qaEntity.QAIsAuditNotFree
		}
		/*
				`
				UPDATE
					t_doc_qa
				SET
				    is_deleted = :is_deleted,
				    update_time = :update_time,
				    release_status = :release_status,
					is_audit_free = :is_audit_free,
				    next_action = :next_action
				WHERE
				    id = :id
			`
		*/

		updateQAFilter := &qaEntity.DocQaFilter{
			QAId: qa.ID,
		}
		if err := l.qaDao.UpdateDocQas(ctx, []string{qaEntity.DocQaTblColIsDeleted, qaEntity.DocQaTblColUpdateTime,
			qaEntity.DocQaTblColReleaseStatus, qaEntity.DocQaTblColIsAuditFree, qaEntity.DocQaTblColNextAction}, updateQAFilter, qa); err != nil {
			logx.E(ctx, "Failed to update qa delete flag when delete QA. args:%+v err:%+v", qa, err)
			return err
		}

		if err := l.labelDao.DeleteQAAttributeLabel(ctx, qa.RobotID, qa.ID); err != nil {
			logx.E(ctx, "Faile to delete QA attribute label.err:%+v", err)
			return err
		}
		id, err := l.vectorSyncLogic.AddQASync(ctx, qa)
		if err != nil {
			return err
		}
		syncID = id
		// 相似问处理
		sqs, err := l.GetSimilarQuestionsByQA(ctx, qa)
		if err != nil {
			// 伽利略error日志告警
			logx.E(ctx, "DeleteQA qa_id: %d, GetSimilarQuestionsByQA err: %+v", qa.ID, err)
			// 柔性放过
		}
		if len(sqs) > 0 {
			if err = l.DeleteSimilarQuestions(ctx, sqs); err != nil {
				logx.E(ctx, "Failed to delete similar questions. err:%+v", err)
				return err
			}
			if syncSimilarQuestionsIDs, err = l.vectorSyncLogic.AddSimilarQuestionSyncBatch(ctx, sqs); err != nil {
				logx.E(ctx, "Failed to add similar question sync (delete) err:%+v", err)
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	l.vectorSyncLogic.Push(ctx, syncID)
	l.vectorSyncLogic.BatchPush(ctx, syncSimilarQuestionsIDs)

	return nil
}

// CreateQaAudit 创建问答送审任务
func (l *Logic) CreateQaAudit(ctx context.Context, qa *qaEntity.DocQA) error {
	logx.I(ctx, "Prepare to CreateQaAudit qa:%+v", qa)
	if !config.AuditSwitch() {
		logx.I(ctx, "AuditSwitch is off, skip to CreateQaAudit")
		return nil
	}
	sendParams := entity.AuditSendParams{
		CorpID: qa.CorpID, StaffID: qa.StaffID, RobotID: qa.RobotID, Type: releaseEntity.AuditBizTypeQa,
		RelateID: qa.ID, EnvSet: contextx.Metadata(ctx).EnvSet(),
	}
	if err := l.releaseDao.MysqlQuery().TAudit.WithContext(ctx).UnderlyingDB().Transaction(func(tx *gorm.DB) error {
		audit, err := l.releaseDao.CreateAuditByAuditSendParams(ctx, sendParams, tx)

		if err != nil {
			logx.E(ctx, "Failed to create audit record error. err:%+v", err)
			return err
		}

		sendParams.ParentAuditBizID = audit.BusinessID

		return async.NewAuditSendTask(ctx, audit.RobotID, sendParams)

	}); err != nil {
		logx.E(ctx, "Failed to create audit error. err:%+v", err)
		return err
	}
	return nil
}

// GetQaCountWithDocID 获取某个文档ID对应的问答个数
func (l *Logic) GetQaCountWithDocID(ctx context.Context, req *qaEntity.QAListReq) (uint32, error) {
	/*
		`
			SELECT
				count(*) as total
			FROM
			    t_doc_qa
			WHERE
			     corp_id = ? AND doc_id = ?  AND robot_id = ?;
		`
	*/
	if req == nil || len(req.DocID) == 0 {
		logx.E(ctx, "参数错误，获取文档对应的问答个数失败")
		return 0, errs.ErrParameterInvalid
	}

	filter := &qaEntity.DocQaFilter{
		CorpId:  req.CorpID,
		DocID:   req.DocID[0],
		RobotId: req.RobotID,
	}
	count, err := l.qaDao.GetDocQaCountWithTx(ctx, nil, filter, nil)

	if err != nil {
		logx.E(ctx, "Failed to GetQaCountWithDocID. err:%+v", err)

	}
	return uint32(count), nil
}

// GetQAAndRelateDocs 获取QA和QA关联的文档
func (l *Logic) GetQAAndRelateDocs(ctx context.Context, ids []uint64, robotID uint64) (
	map[uint64]*qaEntity.DocQA, map[uint64]*docEntity.Doc, error) {
	/*
			 `
			SELECT
				%s
			FROM
			    t_doc_qa
			WHERE
			    id IN (%s)
		`
	*/

	qaMap := make(map[uint64]*qaEntity.DocQA)
	qaDocMap := make(map[uint64]*docEntity.Doc)

	if len(ids) == 0 {
		return qaMap, qaDocMap, nil
	}

	filter := &qaEntity.DocQaFilter{
		QAIds: ids,
	}
	selectColumns := qaEntity.DocQaTblColList

	qaList, err := l.qaDao.GetDocQasByPagenation(ctx, selectColumns, filter, false)
	if err != nil {
		return nil, nil, err
	}

	qaRelateDocIDMaps := make(map[uint64]uint64)
	qaRelateDocIds := make([]uint64, 0)
	for _, item := range qaList {
		qaMap[item.ID] = item
		if item.DocID > 0 {
			qaRelateDocIDMaps[item.ID] = item.DocID
			qaRelateDocIds = append(qaRelateDocIds, item.DocID)
		}
	}

	docs, err := l.docDao.GetDocByIDs(ctx, slicex.Unique(qaRelateDocIds), robotID)
	if err != nil {
		return nil, nil, err
	}
	for key, value := range qaRelateDocIDMaps {
		qaDocMap[key] = docs[value]
	}
	return qaMap, qaDocMap, nil
}

// HasUnconfirmedQa 是否有未确认的QA
func (l *Logic) HasUnconfirmedQa(ctx context.Context, corpID, staffID, robotID uint64) (bool, error) {
	key := fmt.Sprintf("qbot:admin:qa:%s:%s", cast.ToString(robotID), cast.ToString(staffID))
	val, err := l.qaDao.RedisCli().Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			val = ""
		} else {
			logx.E(ctx, "HasUnconfirmedQa 获取用户访问问答时间错误: %+v, key: %s", err, key)
			return false, err
		}
	}
	logx.D(ctx, "HasUnconfirmedQa corpID:%d,staffId:%d,robotId:%d,key:%s,val:%s", corpID, robotID, staffID, key, val)
	accessTime := time.UnixMilli(cast.ToInt64(val))
	/*
		 `
				SELECT
					count(*)
				FROM
				    t_doc_qa
				WHERE
				   corp_id = ? AND  robot_id = ? AND is_deleted = ? AND accept_status = ? AND create_time >= ?
			`
	*/
	filter := &qaEntity.DocQaFilter{
		CorpId:          corpID,
		RobotId:         robotID,
		IsDeleted:       ptrx.Uint32(qaEntity.QAIsNotDeleted),
		AcceptStatus:    qaEntity.AcceptInit,
		MinEqCreateTime: accessTime,
	}
	total, err := l.qaDao.GetDocQaCountWithTx(ctx, nil, filter, nil)
	if err != nil {
		logx.E(ctx, "HasUnconfirmedQa 获取未确认的QA数量错误: %+v, corpID: %d, staffID: %d, robotID: %d", err, corpID, staffID, robotID)
		return false, err
	}
	return total > 0, nil
}

// GetUnconfirmedQaNum 未确认的QA数量
func (l *Logic) GetUnconfirmedQaNum(ctx context.Context, corpID, robotID uint64) (uint64, error) {
	/*
		`
				SELECT
					count(*)
				FROM
				    t_doc_qa
				WHERE
				   corp_id = ? AND  robot_id = ? AND is_deleted = ? AND accept_status = ?
			`
	*/

	filter := &qaEntity.DocQaFilter{
		CorpId:       corpID,
		RobotId:      robotID,
		IsDeleted:    ptrx.Uint32(qaEntity.QAIsNotDeleted),
		AcceptStatus: qaEntity.AcceptInit,
	}

	total, err := l.qaDao.GetDocQaCountWithTx(ctx, nil, filter, nil)
	if err != nil {
		logx.E(ctx, "GetUnconfirmedQaNum 获取未确认的QA数量错误: %+v, corpID: %d, robotID: %d", err, corpID, robotID)
		return 0, err
	}
	return uint64(total), nil
}

func (l *Logic) GetQADetailsByReleaseStatus(ctx context.Context, corpID, robotID uint64, ids []uint64,
	releaseStatus uint32) (map[uint64]*qaEntity.DocQA, error) {
	/*
		SELECT
			%s
		FROM
		    t_doc_qa
		WHERE
		    corp_id = ? AND robot_id = ? AND release_status = ? AND id IN (%s)
	*/
	rsp, err := l.qaDao.GetQADetailsByReleaseStatus(ctx, corpID, robotID, ids, releaseStatus)
	if err != nil {
		logx.E(ctx, "GetQADetailsByReleaseStatus  failed: %+v, corpID: %d, robotID: %d", err, corpID, robotID)
		return nil, err
	}
	return rsp, nil
}

// ModifyUnsatisfiedReplyStatus 修改不满意回复状态
func (l *Logic) ModifyUnsatisfiedReplyStatus(ctx context.Context, corpID, robotID uint64, replyBizID uint64, oldStatus, newStatus uint32) error {
	updateReq := &pb.ModifyUnsatisfiedReplyReq{
		CorpId:     corpID,
		AppId:      robotID,
		ReplyBizId: []uint64{replyBizID},
		OldStatus:  oldStatus,
		NewStatus:  newStatus,
	}

	_, err := l.rpc.AppAdmin.ModifyUnsatisfiedReply(ctx, updateReq)
	if err != nil {
		logx.E(ctx, "Failed to modify unsatisfied reply status, corpID:%d, robotID:%d, replyBizID:%d, oldStatus:%d, newStatus:%d, err:%+v",
			corpID, robotID, replyBizID, oldStatus, newStatus, err)
		return err
	}

	logx.I(ctx, "Modify unsatisfied reply status success, corpID:%d, robotID:%d, replyBizID:%d, oldStatus:%d, newStatus:%d",
		corpID, robotID, replyBizID, oldStatus, newStatus)
	return nil
}

func (l *Logic) GetLatestDocQaUpdateTime(ctx context.Context, corpPrimaryId, robotPrimaryId uint64) (int64, error) {
	filter := &qaEntity.DocQaFilter{
		RobotId:        robotPrimaryId,
		CorpId:         corpPrimaryId,
		OrderColumn:    []string{"update_time"},
		OrderDirection: []string{util.SqlOrderByDesc},
		Limit:          1,
		Offset:         0,
	}
	docQas, err := l.qaDao.GetDocQaList(ctx, []string{"update_time"}, filter)
	if err != nil {
		logx.E(ctx, "GetLatestDocQaUpdateTime failed, robotId: %d, err: %v", robotPrimaryId, err)
		return 0, err
	}

	if len(docQas) == 0 {
		logx.W(ctx, "GetLatestDocQaUpdateTime no doc qas found, robotId: %d", robotPrimaryId)
		return 0, nil
	}
	return docQas[0].UpdateTime.Unix(), nil
}
