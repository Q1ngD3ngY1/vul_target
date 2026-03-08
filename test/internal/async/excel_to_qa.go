package async

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/logx/auditx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/spf13/cast"
)

// ExcelToQATaskHandler 文档生成问答任务
type ExcelToQATaskHandler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity.ExcelToQAParams
}

func registerExcelToQATaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.ExcelToQATask,
		func(t task_scheduler.Task, params entity.ExcelToQAParams) task_scheduler.TaskHandler {
			return &ExcelToQATaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		},
	)
}

// Prepare 数据准备
func (d *ExcelToQATaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	logx.D(ctx, "task(ExcelToQA) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return kv, err
	}
	if doc == nil {
		return kv, errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		return kv, nil
	}
	if !doc.IsExcel() {
		return kv, errs.ErrDocIsNotExcel
	}
	doc.BatchID += 1
	doc.IsCreatingQA = true
	doc.AddProcessingFlag([]uint64{docEntity.DocProcessingFlagCreatingQA})
	if err = d.docLogic.UpdateCreatingQAFlag(ctx, doc); err != nil {
		return kv, err
	}
	logx.I(ctx, "task(ExcelToQA) |getDocNotDeleteQA|  Prepare doc: %+v", doc)
	qas, err := getDocNotDeleteQA(ctx, doc, d.qaLogic)
	if err != nil {
		logx.E(ctx, "task(ExcelToQA) |getDocNotDeleteQA|  Prepare doc: %+v, err: %+v", doc, err)
		return kv, err
	}
	for _, qa := range qas {
		kv[fmt.Sprintf("%s%d", qaDeletePrefix, qa.ID)] = fmt.Sprintf("%d", qa.ID)
	}

	logx.I(ctx, "task(ExcelToQA) |getDocNotDeleteQA|  Prepare doc: %+v", doc)
	if err = d.segLogic.DeleteSegmentsForQA(ctx, doc); err != nil {
		return kv, err
	}
	if err = d.createSegment(ctx, doc); err != nil {
		return kv, err
	}
	ids, err := d.segLogic.GetSegmentIDByDocIDAndBatchID(ctx, doc.ID, doc.BatchID, d.p.RobotID)
	if err != nil {
		return kv, err
	}
	for _, id := range ids {
		logx.D(ctx, "task(ExcelToQA) CreateSegment seg.ID: %d", id)
		kv[fmt.Sprintf("%s%d", segGenQAPrefix, id)] = fmt.Sprintf("%d", id)
	}
	logx.I(ctx, "task(ExcelToQA) Prepare kv: %+v", kv)
	return kv, nil
}

// Init 初始化
func (d *ExcelToQATaskHandler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *ExcelToQATaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	// if len(d.p.EnvSet) > 0 { // 生成问答会创建审核任务，审核回调需要env-set
	// 	ctx = pkg.WithEnvSet(ctx, d.p.EnvSet)
	// }
	logx.D(ctx, "task(ExcelToQA) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		logx.D(ctx, "task(ExcelToQA) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, d.p.RobotID)
		if err != nil {
			logx.E(ctx, "task(ExcelToQA) DescribeAppByPrimaryIdWithoutNotFoundError err:%+v", err)
			return err
		}
		if appDB.HasDeleted() {
			logx.D(ctx, "task(ExcelToQA) appDB.HasDeleted()|appID:%d", d.p.RobotID)
			if err = progress.Finish(ctx, key); err != nil {
				logx.E(ctx, "task(ExcelToQA) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		id := cast.ToUint64(v)
		if strings.HasPrefix(key, segGenQAPrefix) {
			segment, err := d.segLogic.GetSegmentByID(ctx, id, appDB.PrimaryId)
			if err != nil {
				return err
			}
			if segment == nil {
				return errs.ErrSegmentNotFound
			}
			doc, err := d.docLogic.GetDocByID(ctx, segment.DocID, appDB.PrimaryId)
			if err != nil {
				return err
			}
			if doc.HasDeleted() {
				if err = progress.Finish(ctx, key); err != nil {
					logx.E(ctx, "task(ExcelToQA) Finish kv:%s err:%+v", key, err)
					return err
				}
				return nil
			}
			logx.I(ctx, "task(ExcelToQA) |getQAAndCateNode| docId: %d, segment: %+v", doc, segment)
			tree, qas, err := d.getQAAndCateNode(ctx, doc, segment, appDB)
			logx.I(ctx, "task(ExcelToQA) |getQAAndCateNode| tree: %+v, qas: %+v", tree, qas)
			if err != nil {
				return err
			}
			qas = slicex.Filter(qas, func(qa *qaEntity.QA) bool {
				return checkQuestionAndAnswer(ctx, qa.Question, qa.Answer, qa.SimilarQuestions) == nil
			})
			logx.I(ctx, "task(ExcelToQA) |BatchCreateQA... from doc: %+v", doc)
			if err = d.qaLogic.BatchCreateQA(ctx, segment, doc, qas, tree, true); err != nil {
				return err
			}
			// excel导入的问答默认采纳，这里需要更新机器人字符使用量
			charSize, qaBytes := int64(0), int64(0)
			for _, qa := range qas {
				charSize += int64(qaEntity.CalcQACharSize(qa))
				qaBytes += int64(qaEntity.CalcQABytes(qa))
				videoCharSize, videoBytes, err := d.qaLogic.GetVideoURLsCharSize(ctx, qa.Answer)
				if err != nil {
					logx.E(ctx, "task(ExcelToQA) GetVideoURLsCharSize qaAnswer:%s kv:%s err:%+v",
						qa.Answer, key, err)
					return err
				}
				charSize += int64(videoCharSize)
				qaBytes += int64(videoBytes)
				logx.I(ctx, "ExcelToQA Answer videoCharSize|%d", videoCharSize)
			}
			logx.I(ctx, "ExcelToQA Answer charSize|%d qaBytes|%d", charSize, qaBytes)
			if err := d.financeLogic.UpdateAppCapacityUsage(ctx, entity.CapacityUsage{
				CharSize:          charSize,
				ComputeCapacity:   qaBytes,
				KnowledgeCapacity: qaBytes,
			}, appDB.PrimaryId, appDB.CorpPrimaryId); err != nil {
				return errs.ErrUpdateRobotUsedCharSizeFail
			}

			// 这里用代表 问答从文档生成
			auditx.Create(auditx.BizQA).App(appDB.BizId).Space(appDB.SpaceId).Log(ctx, doc.BusinessID, doc.FileName)
			// 未调用大模型，不消耗token
		}
		if strings.HasPrefix(key, qaDeletePrefix) {
			qa, err := d.qaLogic.GetQAByID(ctx, id)
			if err != nil {
				return err
			}
			if err = d.qaLogic.DeleteQA(ctx, qa); err != nil {
				return err
			}
			if err = d.qaLogic.DeleteQASimilarByQA(ctx, qa); err != nil {
				return err
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			logx.E(ctx, "task(ExcelToQA) Finish kv:%s err:%+v", key, err)
			return err
		}

		logx.D(ctx, "task(ExcelToQA) Finish kv:%s", k)
	}
	return nil
}

// Fail 任务失败
func (d *ExcelToQATaskHandler) Fail(ctx context.Context) error {
	logx.D(ctx, "task(ExcelToQA) Fail")
	doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		return nil
	}
	doc.Message = i18nkey.KeyGenerateQAFailed
	doc.IsCreatingQA = false
	doc.RemoveProcessingFlag([]uint64{docEntity.DocProcessingFlagCreatingQA})
	doc.UpdateTime = time.Now()
	doc.IsDeleted = true
	if err = d.docLogic.CreateDocQADone(ctx, d.p.StaffID, doc, -1, false); err != nil {
		return err
	}
	return nil
}

// Stop 任务停止
func (d *ExcelToQATaskHandler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *ExcelToQATaskHandler) Done(ctx context.Context) error {
	if len(d.p.EnvSet) > 0 { // 生成问答会创建审核任务，审核回调需要env-set
		contextx.Metadata(ctx).WithEnvSet(d.p.EnvSet)
	}
	logx.D(ctx, "task(ExcelToQA) Done")
	doc, err := d.docLogic.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
	if err != nil {
		return err
	}
	if doc == nil {
		return errs.ErrDocNotFound
	}
	if doc.HasDeleted() {
		return nil
	}
	doc.Message = i18nkey.KeyGenerateQASuccess
	doc.IsCreatingQA = false
	doc.RemoveProcessingFlag([]uint64{docEntity.DocProcessingFlagCreatingQA})
	doc.UpdateTime = time.Now()
	doc.IsDeleted = true
	if err = d.docLogic.CreateDocQADone(ctx, d.p.StaffID, doc, 1, true); err != nil {
		logx.E(ctx, "task(ExcelToQA) 更新文档状态失败, doc: %+v, err: %+v", doc, err)
		return err
	}
	if err = d.auditLogic.CreateQaAuditForExcel2Qa(ctx, doc); err != nil {
		logx.E(ctx, "task(ExcelToQA) 创建问答审核失败, doc: %+v, err: %+v", doc, err)
		return err
	}
	return nil
}

func (d *ExcelToQATaskHandler) createSegment(ctx context.Context, doc *docEntity.Doc) error {
	prefix := strings.TrimSuffix(doc.FileName, filepath.Ext(doc.FileName)) + ": \n"
	var segments []*segEntity.DocSegmentExtend
	pageContents, err := d.docLogic.ParseExcelQA(ctx, doc.CosURL, doc.FileName)
	if err != nil {
		logx.E(ctx, "task(ExcelToQA) 解析excelQA失败, doc: %+v, err: %+v", doc, err)
		return err
	}
	logx.I(ctx, "task(ExcelToQA) 解析excelQA成功, doc: %+v, pageContents: %+v (%d个)", doc, pageContents, len(pageContents))
	segments = d.newDocSegmentFromQA(ctx, doc, prefix, pageContents)
	logx.I(ctx, "task(ExcelToQA) 创建segment, doc: %+v, segments: %+v (%d 个切分)", doc, segments, len(segments))
	if err = d.segLogic.CreateSegment(ctx, segments, doc.RobotID); err != nil {
		logx.E(ctx, "task(ExcelToQA) 创建segment失败, doc: %+v, err: %+v", doc, err)
		return err
	}
	return nil
}

func (d *ExcelToQATaskHandler) newDocSegmentFromQA(
	_ context.Context, doc *docEntity.Doc, title string, pageContents []string,
) []*segEntity.DocSegmentExtend {
	docSegments := make([]*segEntity.DocSegmentExtend, 0, len(pageContents))
	for _, pageContent := range pageContents {
		docSegments = append(docSegments, &segEntity.DocSegmentExtend{
			DocSegment: segEntity.DocSegment{
				RobotID:         doc.RobotID,
				CorpID:          doc.CorpID,
				StaffID:         doc.StaffID,
				DocID:           doc.ID,
				Outputs:         "",
				FileType:        doc.FileType,
				Title:           title,
				PageContent:     pageContent,
				SplitModel:      "",
				Status:          segEntity.SegmentStatusInit,
				ReleaseStatus:   segEntity.SegmentReleaseStatusNotRequired,
				IsDeleted:       segEntity.SegmentIsNotDeleted,
				Type:            segEntity.SegmentTypeQA,
				NextAction:      segEntity.SegNextActionAdd,
				RichTextIndex:   0,
				UpdateTime:      time.Now(),
				StartChunkIndex: 0,
				EndChunkIndex:   0,
				CreateTime:      time.Now(),
				BatchID:         doc.BatchID,
			},
			ExpireStart: doc.ExpireStart,
			ExpireEnd:   doc.ExpireEnd,
		})
	}
	return docSegments
}

func (d *ExcelToQATaskHandler) getQAAndCateNode(ctx context.Context,
	doc *docEntity.Doc,
	segment *segEntity.DocSegmentExtend,
	appDB *entity.App) (*cateEntity.CateNode, []*qaEntity.QA, error) {
	cates, err := d.cateLogic.DescribeCateList(ctx, cateEntity.QACate, doc.CorpID, appDB.PrimaryId)
	if err != nil {
		return nil, nil, err
	}
	tree := cateEntity.BuildCateTree(cates)
	qas := make([]*qaEntity.QA, 0, 5000)
	if err := jsonx.UnmarshalFromString(segment.PageContent, &qas); err != nil {
		logx.E(ctx, "解析段落内容失败 segment:%+v err:%+v", segment, err)
		return nil, nil, err
	}
	for _, qa := range qas {
		// answerMD, err := util.ConvertDocQaHtmlToMD(ctx, qa.Answer)
		// if err != nil {
		//	return nil, nil, err
		// }
		// qa.Answer = answerMD
		if len(qa.Path) == 0 {
			qa.Path = []string{cateEntity.UncategorizedCateName}
		}
		tree.Create(qa.Path)
	}
	return tree, qas, nil
}
