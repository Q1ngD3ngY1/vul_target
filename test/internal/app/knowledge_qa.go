package app

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/common/v3/errors"
	"math"
	"sync"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	jsoniter "github.com/json-iterator/go"
)

// knowledgeQA 知识问答流程处理
type knowledgeQA struct{}

var (
	// KQaModelTypes 知识库问答应用模型策略
	KQaModelTypes = []string{model.AppModelNormal, model.AppModelNormalNonGeneralKnowledge, model.AppModelQueryRewrite}

	knowledgeQANeedAuditFiled = map[string]int{model.ConfigItemName: 1, model.ConfigItemRoleDescription: 2,
		model.ConfigItemGreeting: 3, model.ConfigItemBareAnswer: 4, model.ConfigItemDescription: 5,
		model.ConfigItemAvatar: 6}
)

func init() {
	handler[model.KnowledgeQaAppType] = &knowledgeQA{}
}

// Collect 发布采集
func (k *knowledgeQA) Collect(ctx context.Context, release *model.Release) error {
	appModel, err := b.dao.GetAppByID(ctx, release.RobotID)
	if err != nil {
		return errs.ErrRobotNotFound
	}
	if !release.IsInit() {
		return errs.ErrReleaseIsNotInit
	}
	releaseQA, err := k.getReleaseQA(ctx, release)
	if err != nil {
		log.ErrorContextf(ctx, "getReleaseQA release:%+v err:%+v", release, err)
		return err
	}
	releaseDoc, err := k.getReleaseDocs(ctx, release)
	if err != nil {
		log.ErrorContextf(ctx, "getReleaseDocs release:%+v err:%+v", release, err)
		return err
	}
	releaseSegments, err := k.getReleaseSegment(ctx, release.ID, releaseDoc)
	if err != nil {
		log.ErrorContextf(ctx, "getReleaseSegment release:%+v err:%+v", release, err)
		return err
	}
	releaseRejectedQuestions, err := k.getReleaseRejectedQuestion(ctx, release.CorpID, release.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "getReleaseRejectedQuestion err:%+v", err)
		return err
	}
	taskQACount, err := b.dao.GetUnreleasedTaskQACount(ctx, appModel.BusinessID, release.CorpID, release.StaffID)
	if err != nil {
		return err
	}
	robotConfigHistory, err := b.dao.GetConfigHistoryByVersionID(ctx, appModel.ID, release.ID)
	if err != nil {
		return err
	}
	if taskQACount != 0 {
		_, err = b.dao.SendDataSyncTask(ctx, release.RobotID, release.BusinessID, release.CorpID,
			release.StaffID, model.TaskConfigEventCollect)
		if err != nil {
			log.ErrorContextf(ctx, "sendNotifyToTaskConfigServerError err:%+v", err)
			return err
		}
	}
	if err = k.createReleaseDetail(ctx, release, releaseDoc, releaseQA, releaseSegments,
		releaseRejectedQuestions, taskQACount, robotConfigHistory.ReleaseJSON, appModel.ReleaseJSON); err != nil {
		log.ErrorContextf(ctx, "doRelease err:%+v", err)
		return err
	}
	return nil
}

// AuditCollect 发布审核数据采集
func (k *knowledgeQA) AuditCollect(ctx context.Context, parent *model.Audit,
	release *model.Release, p model.AuditSendParams) (
	[]*model.Audit, error) {
	auditItems := make([]*model.AuditItem, 0)
	qas, err := b.dao.GetAuditQAByVersion(ctx, release.ID)
	if err != nil {
		return nil, err
	}
	cfg, err := b.dao.GetAuditConfigItemByVersion(ctx, release.ID)
	if err != nil {
		return nil, err
	}
	// 采集qa
	for _, qa := range qas {
		content := fmt.Sprintf("%s\n%s", qa.Question, qa.Answer)
		auditItems = append(
			auditItems,
			model.NewPlainTextAuditItem(qa.ID, model.AuditSourceReleaseQA, content, p.EnvSet),
		)
		for _, image := range util.ExtractImagesFromMarkdown(qa.Answer) {
			url := getRedirectedURL(image)
			auditItems = append(
				auditItems, model.NewPictureAuditItem(qa.ID, model.AuditSourceReleaseQA, url, p.EnvSet,
					b.dao.GetObjectETag(ctx, url)),
			)
		}
		videos, err := util.ExtractVideoURLs(ctx, qa.Answer)
		if err != nil {
			return nil, err
		}
		if videos == nil || len(videos) == 0 {
			continue
		}
		for k, video := range videos {
			objectInfo, err := b.dao.GetCosFileInfoByUrl(ctx, video.CosURL)
			if err != nil {
				return nil, err
			}
			videos[k].ETag = objectInfo.ETag
			videos[k].Size = objectInfo.Size
			videoAudit, err := b.dao.GetAuditByEtag(ctx, p.RobotID, p.CorpID, qa.ID, objectInfo.ETag)
			if err != nil {
				return nil, err
			}
			if len(videoAudit) > 0 {
				log.InfoContextf(ctx, "AuditCollect videoAudit 已经审核 video:%v qa:%v", videos[k], qa)
				continue
			}
			auditItems = append(
				auditItems, model.NewVideoAuditItem(qa.ID, model.AuditSourceReleaseQA, video.CosURL, p.EnvSet,
					video.ETag),
			)
		}
	}
	// 采集配置
	for _, v := range cfg {
		auditItems = append(auditItems, k.getAuditConfig(ctx, v, p)...)
	}
	audits := model.NewAudits(ctx, parent, auditItems)
	now := time.Now()
	for _, audit := range audits {
		audit.BusinessID = b.dao.GenerateSeqID()
		audit.UpdateTime = now
		audit.CreateTime = now
	}
	releaseAudit, err := b.dao.BatchCreateReleaseAudit(ctx, parent, audits, p)
	if err != nil {
		return nil, err
	}
	return releaseAudit, nil
}

// BeforeAudit 处理发布审核前状态
func (k *knowledgeQA) BeforeAudit(ctx context.Context, audit *model.Audit) error {
	if err := k.updateDocQAAuditing(ctx, audit); err != nil {
		return err
	}
	return nil
}

// AfterAudit 发布审核后继续发布流程
func (k *knowledgeQA) AfterAudit(ctx context.Context, audit *model.Audit, isAuditPass bool) error {
	if err := b.dao.AuditRelease(ctx, audit, isAuditPass); err != nil {
		return err
	}
	return nil
}

// ExecRelease 执行发布
func (k *knowledgeQA) ExecRelease(ctx context.Context, release *model.Release) error {
	if err := b.dao.ExecRelease(ctx, true, true, false,
		release); err != nil {
		return err
	}
	return nil
}

// Success 发布成功
func (k *knowledgeQA) Success(ctx context.Context, release *model.Release) error {
	appModel, err := b.dao.GetAppByID(ctx, release.RobotID)
	if err != nil {
		return errs.ErrRobotNotFound
	}
	if appModel == nil {
		return errs.ErrRobotNotFound
	}
	// 删除qa后，在次发布，vector不会通过ReleaseDetailNotify通知，但是会通过ReleaseNotify通知，需要admin自己处理删除的数据
	qaIDs, err := k.getReleaseDeleteQA(ctx, appModel.ID, release.ID)
	if err != nil {
		return err
	}
	forbidReleaseQAIDs, err := k.getForbidReleaseQA(ctx, release.ID)
	if err != nil {
		return err
	}
	segmentIDs, err := k.getReleaseDeleteSegment(ctx, appModel.ID, release.ID)
	if err != nil {
		return err
	}
	releaseRejectedQuestion, err := b.dao.GetReleaseRejectedQuestionByVersion(ctx, release.CorpID, appModel.ID,
		release.ID)
	if err != nil {
		return err
	}
	rejectedQuestionIDs := make([]uint64, 0)
	var configAuditPass, configAuditFail []*model.ReleaseConfig
	for _, rejectedQuestion := range releaseRejectedQuestion {
		rejectedQuestionIDs = append(rejectedQuestionIDs, rejectedQuestion.RejectedQuestionID)
	}
	total, err := b.dao.GetModifyDocCount(ctx, appModel.ID, release.ID, "", nil, nil)
	if err != nil {
		return err
	}
	releaseDoc, err := b.dao.GetModifyDocList(ctx, appModel.ID, release.ID, "",
		nil, 1, uint32(total))
	if err != nil {
		return err
	}
	docs, err := k.getReleaseDoc(ctx, releaseDoc, release.RobotID)
	if err != nil {
		return err
	}
	configs, err := b.dao.GetConfigItemByVersionID(ctx, release.ID)
	if err != nil {
		return err
	}
	for _, v := range configs {
		if v.AuditStatus == model.ConfigReleaseStatusAuditNotPass {
			configAuditFail = append(configAuditFail, v)
			continue
		}
		configAuditPass = append(configAuditPass, v)
	}
	if err = b.dao.ReleaseSuccess(ctx, appModel, release, qaIDs, segmentIDs, rejectedQuestionIDs, forbidReleaseQAIDs,
		configAuditPass, configAuditFail, releaseDoc, docs); err != nil {
		return err
	}
	return nil
}

// AppDetailDiff 应用详情差异项
func (k *knowledgeQA) AppDetailDiff(ctx context.Context, previewJSON string,
	releaseJSON string) []model.AppConfigDiff {
	var preview model.AppDetailsConfig
	var release model.AppDetailsConfig
	var diff []model.AppConfigDiff
	if err := jsoniter.Unmarshal([]byte(previewJSON), &preview); err != nil {
		log.WarnContextf(ctx, "unmarshal app config json err:%+v", err)
		return nil
	}
	if len(releaseJSON) > 0 {
		if err := jsoniter.Unmarshal([]byte(releaseJSON), &release); err != nil {
			log.WarnContextf(ctx, "unmarshal app config json err:%+v", err)
			return nil
		}
	}
	if release.AppConfig.KnowledgeQaConfig == nil {
		release.AppConfig.KnowledgeQaConfig = &model.KnowledgeQaConfig{}
	}
	diff = append(diff, release.Equals(&preview)...)
	diff = append(diff, release.AppConfig.KnowledgeQaConfig.Equals(preview.AppConfig.KnowledgeQaConfig)...)
	return diff
}

// getReleaseQA 获取发布的QA
func (k *knowledgeQA) getReleaseQA(ctx context.Context, release *model.Release) (
	[]*model.ReleaseQA, error) {
	var (
		zeroTime      time.Time
		emptyStatus   []uint32
		emptyQuestion string
	)
	corpID := release.CorpID
	robotID := release.RobotID
	docs, err := b.dao.GetDeletingDoc(ctx, corpID, robotID)
	if err != nil {
		return nil, err
	}
	total, err := b.dao.GetReleaseQACount(ctx, corpID, robotID, emptyQuestion, zeroTime, zeroTime, emptyStatus)
	if err != nil {
		return nil, err
	}
	mapQAID2AttrLabels := new(sync.Map)
	releaseQA := make([]*model.ReleaseQA, 0, total)
	releaseQAChan := make(chan *model.DocQA, 5000)
	existReleaseQA := make(map[uint64]struct{})
	finish := make(chan any)
	go func() {
		defer errors.PanicHandler()
		for qa := range releaseQAChan {
			if _, ok := docs[qa.DocID]; ok && qa.IsNextActionAdd() {
				continue
			}
			if _, ok := existReleaseQA[qa.ID]; ok {
				continue
			}
			existReleaseQA[qa.ID] = struct{}{}
			attrLabelJSON := parseAttrLabels2Json(mapQAID2AttrLabels, qa.ID)
			releaseQA = append(releaseQA, k.transDocQAToReleaseQA(qa, release.ID, attrLabelJSON))
		}
		finish <- nil
	}()
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 1; i <= pages; i++ {
		page := uint32(i)
		list, err := b.dao.GetReleaseQAList(ctx, corpID, robotID, emptyQuestion, zeroTime, zeroTime, emptyStatus,
			page, uint32(pageSize))
		if err != nil {
			return nil, err
		}
		if err := k.getReleaseQAAttrLabels(ctx, robotID, mapQAID2AttrLabels, list); err != nil {
			return nil, err
		}
		for _, item := range list {
			if !item.IsAllowRelease() {
				continue
			}
			releaseQAChan <- item
		}
	}
	close(releaseQAChan)
	<-finish
	return releaseQA, nil
}

func (k *knowledgeQA) transDocQAToReleaseQA(qa *model.DocQA, versionID uint64,
	attrLabelsJSON string) *model.ReleaseQA {
	auditStatus := model.ReleaseQAAuditStatusDoing
	isAllowRelease := model.ForbidRelease
	auditResult := ""
	if !config.AuditSwitch() || !qa.IsReleaseNeedAudit() || qa.IsAuditFree {
		auditStatus = model.ReleaseQAAuditStatusSuccess
		isAllowRelease = model.AllowRelease
		auditResult = "无需审核"
	}
	now := time.Now()
	return &model.ReleaseQA{
		RobotID:        qa.RobotID,
		CorpID:         qa.CorpID,
		StaffID:        qa.StaffID,
		VersionID:      versionID,
		QAID:           qa.ID,
		DocID:          qa.DocID,
		OriginDocID:    qa.OriginDocID,
		SegmentID:      qa.SegmentID,
		CategoryID:     qa.CategoryID,
		Source:         qa.Source,
		Question:       qa.Question,
		Answer:         qa.Answer,
		CustomParam:    qa.CustomParam,
		QuestionDesc:   qa.QuestionDesc,
		ReleaseStatus:  qa.ReleaseStatus,
		IsDeleted:      qa.IsDeleted,
		Message:        qa.Message,
		AcceptStatus:   qa.AcceptStatus,
		SimilarStatus:  qa.SimilarStatus,
		Action:         qa.ReleaseAction(),
		AuditStatus:    auditStatus,
		AuditResult:    auditResult,
		CreateTime:     now,
		UpdateTime:     now,
		IsAllowRelease: isAllowRelease,
		AttrLabels:     attrLabelsJSON,
		ExpireTime:     qa.ExpireEnd,
	}
}

func (k *knowledgeQA) getReleaseQAAttrLabels(ctx context.Context, robotID uint64,
	mapQAID2AttrLabels *sync.Map, list []*model.DocQA) error {
	if len(list) == 0 {
		return nil
	}
	var originDocIDs, qaIDs []uint64
	for _, v := range list {
		if v.Source == model.SourceFromDoc {
			originDocIDs = append(originDocIDs, v.OriginDocID)
			continue
		}
		if v.AttrRange == model.AttrRangeAll {
			continue
		}
		qaIDs = append(qaIDs, v.ID)
	}
	mapDocID2AttrLabels := new(sync.Map)
	if err := k.getReleaseDocAttrLabels(ctx, mapDocID2AttrLabels, robotID, originDocIDs); err != nil {
		return err
	}
	mapQAID2AttrLabelsDetail, err := b.dao.GetQAAttributeLabelDetail(ctx, robotID, qaIDs)
	if err != nil {
		return err
	}
	for _, v := range list {
		if v.Source == model.SourceFromDoc {
			storeAttrLabels(mapQAID2AttrLabels, v.ID, loadAttrLabels(mapDocID2AttrLabels, v.OriginDocID)...)
			continue
		}
		if v.AttrRange == model.AttrRangeAll {
			storeAttrLabels(mapQAID2AttrLabels, v.ID, &model.ReleaseAttrLabel{
				Name:  config.App().AttributeLabel.GeneralVectorAttrKey,
				Value: config.App().AttributeLabel.FullLabelValue,
			})
			continue
		}
		storeAttrLabels(mapQAID2AttrLabels, v.ID, fillReleaseAttrLabel(mapQAID2AttrLabelsDetail[v.ID])...)
	}
	return nil

}

// parseAttrLabels2Json TODO
func parseAttrLabels2Json(mapAttrLabels *sync.Map, key any) string {
	if mapAttrLabels == nil {
		return ""
	}
	value, ok := mapAttrLabels.Load(key)
	if !ok {
		return ""
	}
	attrLabelJSON, _ := jsoniter.MarshalToString(value)
	return attrLabelJSON
}

// loadAttrLabels TODO
func loadAttrLabels(mapAttrLabels *sync.Map, key any) []*model.ReleaseAttrLabel {
	if mapAttrLabels == nil {
		return nil
	}
	values, ok := mapAttrLabels.Load(key)
	if !ok {
		return nil
	}
	attrLabels, ok := values.([]*model.ReleaseAttrLabel)
	if !ok {
		return nil
	}
	return attrLabels
}

// storeAttrLabels TODO
func storeAttrLabels(mapAttrLabels *sync.Map, key any, attrLabels ...*model.ReleaseAttrLabel) {
	if mapAttrLabels == nil || len(attrLabels) == 0 {
		return
	}
	mapAttrLabels.Store(key, attrLabels)
}

// fillReleaseAttrLabel TODO
func fillReleaseAttrLabel(attrLabels []*model.AttrLabel) []*model.ReleaseAttrLabel {
	releaseAttrLabels := make([]*model.ReleaseAttrLabel, 0)
	for _, v := range attrLabels {
		for _, label := range v.Labels {
			releaseAttrLabels = append(releaseAttrLabels, &model.ReleaseAttrLabel{
				Name:  v.AttrKey,
				Value: label.LabelName,
			})
		}
	}
	return releaseAttrLabels
}

func (k *knowledgeQA) getReleaseDocAttrLabels(ctx context.Context, mapDocID2AttrLabels *sync.Map,
	robotID uint64, docIDs []uint64) error {
	if len(docIDs) == 0 {
		return nil
	}
	docs, err := b.dao.GetDocByIDs(ctx, docIDs, robotID)
	if err != nil {
		return err
	}
	referIDs := make([]uint64, 0)
	for _, v := range docs {
		if v.AttrRange == model.AttrRangeAll {
			storeAttrLabels(mapDocID2AttrLabels, v.ID, &model.ReleaseAttrLabel{
				Name:  config.App().AttributeLabel.GeneralVectorAttrKey,
				Value: config.App().AttributeLabel.FullLabelValue,
			})
			continue
		}
		referIDs = append(referIDs, v.ID)
	}
	mapDocID2AttrLabelDetail, err := b.dao.GetDocAttributeLabelDetail(ctx, robotID, referIDs)
	if err != nil {
		return err
	}
	for docID, attrLabels := range mapDocID2AttrLabelDetail {
		storeAttrLabels(mapDocID2AttrLabels, docID, fillReleaseAttrLabel(attrLabels)...)
	}
	return nil
}

// getReleaseDocs 获取发布的文档
func (k *knowledgeQA) getReleaseDocs(ctx context.Context, release *model.Release) (
	[]*model.ReleaseDoc, error) {
	corpID := release.CorpID
	robotID := release.RobotID
	zeroTime := time.Time{}
	total, err := b.dao.GetWaitReleaseDocCount(ctx, corpID, robotID, "",
		zeroTime, zeroTime, nil)
	if err != nil {
		return nil, err
	}
	docs, err := b.dao.GetWaitReleaseDoc(ctx, corpID, robotID, "", zeroTime, zeroTime,
		nil, 1, uint32(total))
	if err != nil {
		return nil, err
	}
	now := time.Now()
	releaseDocs := make([]*model.ReleaseDoc, 0, len(docs))
	for _, doc := range docs {
		releaseDocs = append(releaseDocs, &model.ReleaseDoc{
			VersionID:       release.ID,
			DocID:           doc.ID,
			BusinessID:      doc.BusinessID,
			RobotID:         doc.RobotID,
			CorpID:          doc.CorpID,
			StaffID:         doc.StaffID,
			FileName:        doc.FileName,
			FileType:        doc.FileType,
			FileSize:        doc.FileSize,
			Bucket:          doc.Bucket,
			CosURL:          doc.CosURL,
			CosHash:         doc.CosHash,
			Message:         doc.Message,
			Status:          doc.Status,
			IsDeleted:       doc.IsDeleted,
			IsRefer:         doc.IsRefer,
			Source:          doc.Source,
			WebURL:          doc.WebURL,
			BatchID:         doc.BatchID,
			AuditFlag:       doc.AuditFlag,
			IsCreatingQA:    doc.IsCreatingQaV1(),
			IsCreatingIndex: doc.IsCreatingIndexV1(),
			Action:          doc.NextAction,
			AttrRange:       doc.AttrRange,
			CreateTime:      now,
			UpdateTime:      now,
			ExpireTime:      doc.ExpireEnd,
		})
	}
	return releaseDocs, nil
}

// getReleaseSegment 获取发布的Segment
func (k *knowledgeQA) getReleaseSegment(
	ctx context.Context, versionID uint64, releaseDocs []*model.ReleaseDoc) ([]*model.ReleaseSegment, error) {
	releaseSegments := make([]*model.ReleaseSegment, 0, 50000)
	releaseSegmentChan := make(chan *model.DocSegmentExtend, 5000)
	existReleaseSegments := make(map[uint64]struct{})
	mapDocID2AttrLabels := new(sync.Map)
	finish := make(chan any)
	now := time.Now()
	go func() {
		defer errors.PanicHandler()
		for seg := range releaseSegmentChan {
			if _, ok := existReleaseSegments[seg.ID]; ok {
				continue
			}
			existReleaseSegments[seg.ID] = struct{}{}
			releaseSegments = append(releaseSegments, &model.ReleaseSegment{
				RobotID:         seg.RobotID,
				CorpID:          seg.CorpID,
				StaffID:         seg.StaffID,
				DocID:           seg.DocID,
				SegmentID:       seg.ID,
				VersionID:       versionID,
				FileType:        seg.FileType,
				Title:           seg.Title,
				PageContent:     seg.PageContent,
				OrgData:         seg.OrgData,
				SplitModel:      seg.SplitModel,
				Status:          seg.Status,
				ReleaseStatus:   seg.ReleaseStatus,
				Message:         seg.Message,
				IsDeleted:       seg.IsDeleted,
				Action:          seg.NextAction,
				BatchID:         seg.BatchID,
				RichTextIndex:   seg.RichTextIndex,
				StartChunkIndex: seg.StartChunkIndex,
				EndChunkIndex:   seg.EndChunkIndex,
				UpdateTime:      now,
				CreateTime:      now,
				IsAllowRelease:  model.AllowRelease,
				AttrLabels:      parseAttrLabels2Json(mapDocID2AttrLabels, seg.DocID),
				ExpireTime:      seg.ExpireEnd,
			})
		}
		finish <- nil
	}()
	for _, doc := range releaseDocs {
		if err := k.getDocReleaseSegments(ctx, doc, releaseSegmentChan, mapDocID2AttrLabels); err != nil {
			return nil, err
		}
	}
	close(releaseSegmentChan)
	<-finish
	return releaseSegments, nil
}

func (k *knowledgeQA) getDocReleaseSegments(ctx context.Context, doc *model.ReleaseDoc,
	releaseSegmentChan chan *model.DocSegmentExtend, mapDocID2AttrLabels *sync.Map) error {
	if err := k.getReleaseDocAttrLabels(ctx, mapDocID2AttrLabels, doc.RobotID, []uint64{doc.DocID}); err != nil {
		return err
	}
	total, err := b.dao.GetReleaseSegmentCount(ctx, doc.DocID, doc.RobotID)
	if err != nil {
		return err
	}
	pageSize := 5000
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 1; i <= pages; i++ {
		page := uint32(i)
		list, err := b.dao.GetReleaseSegmentList(ctx, doc.DocID, page, uint32(pageSize), doc.RobotID)
		if err != nil {
			return err
		}
		for _, item := range list {
			// TODO 目前暂时只需要管结束时间，等以后扩展生效时间，还需要再复制
			item.ExpireEnd = doc.ExpireTime
			releaseSegmentChan <- item
		}
	}
	return nil
}

func (k *knowledgeQA) createReleaseDetail(ctx context.Context, release *model.Release,
	releaseDoc []*model.ReleaseDoc, releaseQA []*model.ReleaseQA, releaseSegments []*model.ReleaseSegment,
	releaseRejectedQuestions []*model.ReleaseRejectedQuestion, taskQACount uint32, previewJSON string,
	releaseJSON string) error {
	diff := k.AppDetailDiff(ctx, previewJSON, releaseJSON)
	var releaseConfig []*model.ReleaseConfig
	if len(releaseDoc) == 0 && len(releaseQA) == 0 && len(releaseSegments) == 0 &&
		len(releaseRejectedQuestions) == 0 && taskQACount == 0 && len(diff) == 0 {
		return errs.ErrNoReleaseQA
	}
	release.Status = model.ReleaseStatusPending
	vectorNoNeedRelease := len(releaseDoc) == 0 && len(releaseQA) == 0 && len(releaseSegments) == 0 &&
		len(releaseRejectedQuestions) == 0
	taskFlowNoNeedRelease := taskQACount == 0
	if vectorNoNeedRelease {
		release.CallbackStatus = model.ReleaseVectorSuccessCallbackFlag
	} else if taskFlowNoNeedRelease {
		release.CallbackStatus = model.ReleaseTaskFlowSuccessCallbackFlag
	}
	if vectorNoNeedRelease && taskFlowNoNeedRelease && len(diff) > 0 {
		release.CallbackStatus = model.ReleaseAllServeCallbackFlag
	}
	for _, qa := range releaseQA {
		if qa.IsAuditDoing() {
			release.Status = model.ReleaseStatusAudit
			break
		}
	}
	if len(diff) > 0 {
		auditDiffConfig, noAuditDiffConfig := getNeedAuditDiffConfig(knowledgeQANeedAuditFiled, diff)
		if len(auditDiffConfig) > 0 {
			release.Status = model.ReleaseStatusAudit
			releaseConfig = append(releaseConfig, getReleaseConfig(auditDiffConfig, true, release)...)
		}
		releaseConfig = append(releaseConfig, getReleaseConfig(noAuditDiffConfig, false, release)...)
	}
	if err := b.dao.CreateReleaseDetail(ctx, release, releaseDoc, releaseQA, releaseSegments,
		releaseRejectedQuestions, releaseConfig); err != nil {
		return err
	}
	return nil
}

// getReleaseRejectedQuestion 获取待发布拒答问题
func (k *knowledgeQA) getReleaseRejectedQuestion(ctx context.Context, corpID,
	robotID uint64) ([]*model.ReleaseRejectedQuestion, error) {
	var (
		query     string
		startTime time.Time
		endTime   time.Time
		status    []uint32
	)
	total, err := b.dao.GetReleaseRejectedQuestionCount(ctx, corpID, robotID, query, startTime, endTime, status)
	if err != nil {
		return nil, err
	}
	releaseRejectedQuestion := make([]*model.ReleaseRejectedQuestion, 0, total)
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for page := 1; page <= pages; page++ {
		list, err := b.dao.GetReleaseRejectedQuestionList(ctx, corpID, robotID, uint32(page), uint32(pageSize),
			query, startTime, endTime, status)
		if err != nil {
			return nil, err
		}
		for _, v := range list {
			releaseRejectedQuestion = append(releaseRejectedQuestion, &model.ReleaseRejectedQuestion{
				ID:                 v.ID,
				CorpID:             v.CorpID,
				RobotID:            v.RobotID,
				CreateStaffID:      v.CreateStaffID,
				VersionID:          0,
				RejectedQuestionID: v.ID,
				Message:            "",
				Question:           v.Question,
				ReleaseStatus:      v.ReleaseStatus,
				IsDeleted:          v.IsDeleted,
				Action:             v.Action,
				IsAllowRelease:     uint32(1),
			})
		}
	}
	return releaseRejectedQuestion, nil
}

// updateDocQAAuditing 送审前将QA文档更新成审核中
func (k *knowledgeQA) updateDocQAAuditing(ctx context.Context, audit *model.Audit) error {
	qa, err := b.dao.GetReleaseQAByID(ctx, audit.RelateID)
	if err != nil {
		return err
	}
	if qa == nil {
		log.DebugContextf(ctx, "get releaseQA but not found,id:%+v", audit.RelateID)
		return nil
	}
	docQA, err := b.dao.GetQAByID(ctx, qa.QAID)
	if err != nil {
		return err
	}
	if docQA == nil {
		return errs.ErrDocNotFound
	}
	docQA.ReleaseStatus = model.QAReleaseStatusAuditing
	if err = b.dao.UpdateAuditQA(ctx, docQA); err != nil {
		return err
	}
	return nil
}

func (k *knowledgeQA) getReleaseDeleteQA(ctx context.Context, robotID, versionID uint64) (
	[]uint64, error) {
	total, err := b.dao.GetModifyQACount(ctx, robotID, versionID, "", []uint32{model.NextActionDelete}, []uint32{})
	if err != nil {
		return nil, err
	}
	idChan := make(chan uint64, 5000)
	finishChan := make(chan any)
	ids := make([]uint64, 0, total)
	go func() {
		defer errors.PanicHandler()
		for id := range idChan {
			ids = append(ids, id)
		}
		finishChan <- nil
	}()
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 1; i <= pages; i++ {
		page := uint32(i)
		qas, err := b.dao.GetModifyQAList(ctx, robotID, versionID, "", []uint32{model.NextActionDelete},
			page, uint32(pageSize), "", nil)
		if err != nil {
			return nil, err
		}
		for _, qa := range qas {
			idChan <- qa.QAID
		}
	}
	close(idChan)
	<-finishChan
	return ids, nil
}

func (k *knowledgeQA) getForbidReleaseQA(ctx context.Context, versionID uint64) ([]uint64, error) {
	return b.dao.GetForbidReleaseQA(ctx, versionID)
}

func (k *knowledgeQA) getReleaseDeleteSegment(ctx context.Context, robotID, versionID uint64) (
	[]uint64, error) {
	total, err := b.dao.GetModifySegmentCount(ctx, robotID, versionID, model.NextActionDelete)
	if err != nil {
		return nil, err
	}
	idChan := make(chan uint64, 5000)
	finishChan := make(chan any)
	ids := make([]uint64, 0, total)
	go func() {
		defer errors.PanicHandler()
		for id := range idChan {
			ids = append(ids, id)
		}
		finishChan <- nil
	}()
	pageSize := 500
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	for i := 1; i <= pages; i++ {
		page := uint32(i)
		segments, err := b.dao.GetModifySegmentList(ctx, robotID, versionID, []uint32{model.NextActionDelete}, page,
			uint32(pageSize))
		if err != nil {
			return nil, err
		}
		for _, segment := range segments {
			idChan <- segment.SegmentID
		}
	}
	close(idChan)
	<-finishChan
	return ids, nil
}

func (k *knowledgeQA) getReleaseDoc(ctx context.Context, releaseDoc []*model.ReleaseDoc, robotID uint64) (
	[]*model.Doc, error) {
	if len(releaseDoc) == 0 {
		return nil, nil
	}
	docIDs := make([]uint64, 0, len(releaseDoc))
	for _, doc := range releaseDoc {
		docIDs = append(docIDs, doc.DocID)
	}
	docs, err := b.dao.GetDocByIDs(ctx, docIDs, robotID)
	if err != nil {
		return nil, err
	}
	list := make([]*model.Doc, 0, len(docs))
	for _, doc := range docs {
		list = append(list, doc)
	}
	return list, nil
}

func (k *knowledgeQA) getAuditConfig(ctx context.Context, cfg *model.AuditReleaseConfig,
	p model.AuditSendParams) []*model.AuditItem {
	content := cfg.Value
	var auditItems []*model.AuditItem
	switch cfg.ConfigItem {
	case model.ConfigItemName:
		auditItems = append(auditItems,
			model.NewUserDataAuditItem(cfg.ID, model.AuditSourceRobotName, content, p.EnvSet))
	case model.ConfigItemAvatar:
		auditItems = append(auditItems,
			model.NewUserHeadURLAuditItem(cfg.ID, model.AuditSourceRobotAvatar, content, p.EnvSet,
				b.dao.GetObjectETag(ctx, content)))
	case model.ConfigItemGreeting:
		if len(content) == 0 {
			return auditItems
		}
		auditItems = append(auditItems,
			model.NewUserGreetingAuditItem(cfg.ID, model.AuditSourceRobotGreeting, content, p.EnvSet))
	case model.ConfigItemBareAnswer:
		auditItems = append(auditItems,
			model.NewPlainTextAuditItem(cfg.ID, model.AuditSourceBareAnswer, content, p.EnvSet))
		for _, image := range util.ExtractImagesFromMarkdown(content) {
			auditItems = append(
				auditItems,
				model.NewPictureAuditItem(cfg.ID, model.AuditSourceBareAnswer, image, p.EnvSet,
					b.dao.GetObjectETag(ctx, image)),
			)
		}
	case model.ConfigItemRoleDescription:
		if len(content) == 0 {
			return auditItems
		}
		auditItems = append(auditItems,
			model.NewPlainTextAuditItem(cfg.ID, model.AuditSourceRobotRoleDescription, content, p.EnvSet))
	}
	return auditItems
}

// AnalysisDescribeApp 解析APP数据
func (k *knowledgeQA) AnalysisDescribeApp(ctx context.Context, appDB *model.AppDB) (*model.App, error) {
	app, err := appDB.ToApp()
	if err != nil {
		log.ErrorContextf(ctx, "Analysis describe app, to app err:%v", err)
		return nil, err
	}
	return app, nil
}
