package task

import (
	"context"
	"fmt"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"path/filepath"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
)

// ExcelToQAScheduler 文档生成问答任务
type ExcelToQAScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    model.ExcelToQAParams
}

func initExcelToQAScheduler() {
	task_scheduler.Register(
		model.ExcelToQATask,
		func(t task_scheduler.Task, params model.ExcelToQAParams) task_scheduler.TaskHandler {
			return &ExcelToQAScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		},
	)
}

// Prepare 数据准备
func (d *ExcelToQAScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	i18n.SetUserLangString(ctx, d.p.Language)
	log.DebugContextf(ctx, "task(ExcelToQA) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
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
	doc.AddProcessingFlag([]uint64{model.DocProcessingFlagCreatingQA})
	if err = d.dao.UpdateCreatingQAFlag(ctx, doc); err != nil {
		return kv, err
	}
	qas, err := getDocNotDeleteQA(ctx, doc, d.dao)
	if err != nil {
		return kv, err
	}
	for _, qa := range qas {
		kv[fmt.Sprintf("%s%d", qaDeletePrefix, qa.ID)] = fmt.Sprintf("%d", qa.ID)
	}
	if err = d.dao.DeleteSegmentsForQA(ctx, doc); err != nil {
		return kv, err
	}
	if err = d.createSegment(ctx, doc); err != nil {
		return kv, err
	}
	ids, err := d.dao.GetSegmentIDByDocIDAndBatchID(ctx, doc.ID, doc.BatchID, d.p.RobotID)
	if err != nil {
		return kv, err
	}
	for _, id := range ids {
		log.DebugContextf(ctx, "task(ExcelToQA) CreateSegment seg.ID: %d", id)
		kv[fmt.Sprintf("%s%d", segGenQAPrefix, id)] = fmt.Sprintf("%d", id)
	}
	return kv, nil
}

// Init 初始化
func (d *ExcelToQAScheduler) Init(ctx context.Context, _ task_scheduler.TaskKV) error {
	i18n.SetUserLangString(ctx, d.p.Language)
	return nil
}

// Process 任务处理
func (d *ExcelToQAScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	// if len(d.p.EnvSet) > 0 { // 生成问答会创建审核任务，审核回调需要env-set
	// 	ctx = pkg.WithEnvSet(ctx, d.p.EnvSet)
	// }
	log.DebugContextf(ctx, "task(ExcelToQA) Process, task: %+v, params: %+v", d.task, d.p)
	for k, v := range progress.TaskKV(ctx) {
		log.DebugContextf(ctx, "task(ExcelToQA) Start k:%s, v:%s", k, v)
		key := k
		appDB, err := d.dao.GetAppByID(ctx, d.p.RobotID)
		if err != nil {
			return err
		}
		if appDB.HasDeleted() {
			log.DebugContextf(ctx, "task(ExcelToQA) appDB.HasDeleted()|appID:%d", appDB.GetAppID())
			if err = progress.Finish(ctx, key); err != nil {
				log.ErrorContextf(ctx, "task(ExcelToQA) Finish kv:%s err:%+v", key, err)
				return err
			}
			continue
		}
		id := cast.ToUint64(v)
		if strings.HasPrefix(key, segGenQAPrefix) {
			segment, err := d.dao.GetSegmentByID(ctx, id, appDB.ID)
			if err != nil {
				return err
			}
			if segment == nil {
				return errs.ErrSegmentNotFound
			}
			doc, err := d.dao.GetDocByID(ctx, segment.DocID, appDB.ID)
			if err != nil {
				return err
			}
			if doc.HasDeleted() {
				if err = progress.Finish(ctx, key); err != nil {
					log.ErrorContextf(ctx, "task(ExcelToQA) Finish kv:%s err:%+v", key, err)
					return err
				}
				return nil
			}
			tree, qas, err := d.getQAAndCateNode(ctx, doc, segment, appDB)
			if err != nil {
				return err
			}
			qas = slicex.Filter(qas, func(qa *model.QA) bool {
				return checkQuestionAndAnswer(ctx, qa.Question, qa.Answer, qa.SimilarQuestions) == nil
			})
			if err = d.dao.BatchCreateQA(ctx, segment, doc, qas, tree, true); err != nil {
				return err
			}
			// excel导入的问答默认采纳，这里需要更新机器人字符使用量
			charSize := int64(0)
			for _, qa := range qas {
				charSize += int64(d.dao.CalcQACharSize(ctx, qa))
				videoCharSize, err := d.dao.GetVideoURLsCharSize(ctx, qa.Answer)
				if err != nil {
					log.ErrorContextf(ctx, "task(ExcelToQA) GetVideoURLsCharSize qaAnswer:%s kv:%s err:%+v",
						qa.Answer, key, err)
					return err
				}
				charSize += int64(videoCharSize)
				log.InfoContextf(ctx, "ExcelToQA Answer videoCharSize|%d", videoCharSize)
			}
			if err = d.dao.UpdateAppUsedCharSizeTx(ctx, charSize, appDB.ID); err != nil {
				return errs.ErrUpdateRobotUsedCharSizeFail
			}

			// 未调用大模型，不消耗token
		}
		if strings.HasPrefix(key, qaDeletePrefix) {
			qa, err := d.dao.GetQAByID(ctx, id)
			if err != nil {
				return err
			}
			if err = d.dao.DeleteQA(ctx, qa); err != nil {
				return err
			}
			if err = d.dao.DeleteQASimilar(ctx, qa); err != nil {
				return err
			}
		}
		if err := progress.Finish(ctx, key); err != nil {
			log.ErrorContextf(ctx, "task(ExcelToQA) Finish kv:%s err:%+v", key, err)
			return err
		}
		log.DebugContextf(ctx, "task(ExcelToQA) Finish kv:%s", k)
	}
	return nil
}

// Fail 任务失败
func (d *ExcelToQAScheduler) Fail(ctx context.Context) error {
	log.DebugContextf(ctx, "task(ExcelToQA) Fail")
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
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
	doc.RemoveProcessingFlag([]uint64{model.DocProcessingFlagCreatingQA})
	doc.UpdateTime = time.Now()
	doc.IsDeleted = model.DocIsDeleted
	if err = d.dao.CreateDocQADone(ctx, d.p.StaffID, doc, -1, false); err != nil {
		return err
	}
	return nil
}

// Stop 任务停止
func (d *ExcelToQAScheduler) Stop(_ context.Context) error {
	return nil
}

// Done 任务完成回调
func (d *ExcelToQAScheduler) Done(ctx context.Context) error {
	if len(d.p.EnvSet) > 0 { // 生成问答会创建审核任务，审核回调需要env-set
		ctx = pkg.WithEnvSet(ctx, d.p.EnvSet)
	}
	log.DebugContextf(ctx, "task(ExcelToQA) Done")
	doc, err := d.dao.GetDocByID(ctx, d.p.DocID, d.p.RobotID)
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
	doc.RemoveProcessingFlag([]uint64{model.DocProcessingFlagCreatingQA})
	doc.UpdateTime = time.Now()
	doc.IsDeleted = model.DocIsDeleted
	if err = d.dao.CreateDocQADone(ctx, d.p.StaffID, doc, 1, true); err != nil {
		log.ErrorContextf(ctx, "task(ExcelToQA) 更新文档状态失败, doc: %+v, err: %+v", doc, err)
		return err
	}
	if err = d.dao.CreateQaAuditForExcel2Qa(ctx, doc); err != nil {
		log.ErrorContextf(ctx, "task(ExcelToQA) 创建问答审核失败, doc: %+v, err: %+v", doc, err)
		return err
	}
	return nil
}

func (d *ExcelToQAScheduler) createSegment(ctx context.Context, doc *model.Doc) error {
	prefix := strings.TrimSuffix(doc.FileName, filepath.Ext(doc.FileName)) + ": \n"
	var segments []*model.DocSegmentExtend
	pageContents, err := d.dao.ParseExcelQA(ctx, doc.CosURL, doc.FileName)
	if err != nil {
		log.ErrorContextf(ctx, "task(ExcelToQA) 解析excelQA失败, doc: %+v, err: %+v", doc, err)
		return err
	}
	segments = d.newDocSegmentFromQA(ctx, doc, prefix, pageContents)
	if err = d.dao.CreateSegment(ctx, segments, doc.RobotID); err != nil {
		return err
	}
	return nil
}

func (d *ExcelToQAScheduler) newDocSegmentFromQA(
	_ context.Context, doc *model.Doc, title string, pageContents []string,
) []*model.DocSegmentExtend {
	docSegments := make([]*model.DocSegmentExtend, 0, len(pageContents))
	for _, pageContent := range pageContents {
		docSegments = append(docSegments, &model.DocSegmentExtend{
			DocSegment: model.DocSegment{
				RobotID:         doc.RobotID,
				CorpID:          doc.CorpID,
				StaffID:         doc.StaffID,
				DocID:           doc.ID,
				Outputs:         "",
				FileType:        doc.FileType,
				Title:           title,
				PageContent:     pageContent,
				SplitModel:      "",
				Status:          model.SegmentStatusInit,
				ReleaseStatus:   model.SegmentReleaseStatusNotRequired,
				IsDeleted:       model.SegmentIsNotDeleted,
				Type:            model.SegmentTypeQA,
				NextAction:      model.SegNextActionAdd,
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

func (d *ExcelToQAScheduler) getQAAndCateNode(ctx context.Context, doc *model.Doc, segment *model.DocSegmentExtend,
	appDB *model.AppDB) (*model.CateNode, []*model.QA, error) {
	cates, err := d.dao.GetCateList(ctx, model.QACate, doc.CorpID, appDB.ID)
	if err != nil {
		return nil, nil, err
	}
	tree := model.BuildCateTree(cates)
	qas := make([]*model.QA, 0, 5000)
	if err := jsoniter.UnmarshalFromString(segment.PageContent, &qas); err != nil {
		log.ErrorContextf(ctx, "解析段落内容失败 segment:%+v err:%+v", segment, err)
		return nil, nil, err
	}
	for _, qa := range qas {
		//answerMD, err := util.ConvertDocQaHtmlToMD(ctx, qa.Answer)
		//if err != nil {
		//	return nil, nil, err
		//}
		//qa.Answer = answerMD
		if len(qa.Path) == 0 {
			qa.Path = []string{model.UncategorizedCateName}
		}
		tree.Create(qa.Path)
	}
	return tree, qas, nil
}
