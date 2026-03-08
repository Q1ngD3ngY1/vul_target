package service

import (
	"context"
	"errors"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"net/url"
	"strings"
	"sync"

	terrs "git.code.oa.com/trpc-go/trpc-go/errs"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// checkXlsx 检查问答模板文件是否符合要求
func (s *Service) checkXlsx(ctx context.Context, corpID, robotID uint64, cosURL string, uin string, appBizID uint64) (*pb.SaveDocRsp, error) {
	body, err := s.dao.GetObject(ctx, cosURL)
	if err != nil {
		return nil, errs.ErrSystem
	}
	// 将配置中文件头翻译成ctx中语言
	var checkHead []string
	for _, v := range model.ExcelTplHead {
		checkHead = append(checkHead, i18n.Translate(ctx, v))
	}
	log.InfoContextf(ctx, "checkXlsx checkHead:%v", checkHead)
	rows, bs, err := util.CheckXlsxContent(ctx, cosURL, 0, config.App().DocQA.ImportMaxLength,
		checkHead, body, s.checkRow, uin, appBizID)
	if err != nil {
		if !errors.Is(err, errs.ErrExcelContent) {
			return nil, err
		}
		key := cosURL + ".check.xlsx"
		if err := s.dao.PutObject(ctx, bs, key); err != nil {
			return nil, errs.ErrSystem
		}
		url, err := s.dao.GetPresignedURL(ctx, key)
		if err != nil {
			return nil, errs.ErrSystem
		}
		return &pb.SaveDocRsp{
			ErrorMsg:      i18n.Translate(ctx, i18nkey.KeyFileDataErrorPleaseDownloadErrorFile),
			ErrorLink:     url,
			ErrorLinkText: i18n.Translate(ctx, i18nkey.KeyDownload),
		}, nil
	}

	allCates, err := s.dao.GetCateList(ctx, model.QACate, corpID, robotID)
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
		return nil, errs.ErrWrapf(errs.ErrCodeCateCountExceed, i18n.Translate(ctx, i18nkey.KeyQACategoryCountExceeded), limit)
	}

	return nil, nil
}

// checkRow check每一行的内容
func (s *Service) checkRow(ctx context.Context, i int, row []string, questions *sync.Map, uin string, appBizID uint64,
	uniqueImgHost *sync.Map) string {
	ok, cates := model.GetCatePath(row)
	if !ok {
		return i18n.Translate(ctx, i18nkey.KeyCategoryErrorPleaseRefill)
	}

	for _, cate := range cates {
		if err := checkCateName(ctx, cate); err != nil {
			return terrs.Msg(err)
		}
	}

	if len(row) < model.ExcelTplHeadLen-model.ExcelTpOptionalLen {
		return i18n.Translate(ctx, i18nkey.KeyQuestionOrAnswerEmptyPleaseFill)
	}

	answer := strings.TrimSpace(row[model.ExcelTplAnswerIndex])
	question := strings.TrimSpace(row[model.ExcelTplQuestionIndex])
	if question == "" || answer == "" {
		return i18n.Translate(ctx, i18nkey.KeyQuestionOrAnswerEmptyPleaseFill)
	}

	if _, ok := questions.Load(question); ok {
		log.InfoContextf(context.Background(), "checkRow|question:%s", question)
		return i18n.Translate(ctx, i18nkey.KeyQAKnowledgeBaseDuplicateCorpus, question)
	}
	questions.Store(question, i)
	// 检查问题描述
	if len(row) >= model.ExcelTplQuestionDescIndex+1 {
		questionDesc := strings.TrimSpace(row[model.ExcelTplQuestionDescIndex])
		if err := checkQuestionDesc(ctx, questionDesc); err != nil {
			return terrs.Msg(err)
		}
	}
	// 检查相似问
	if len(row) >= model.ExcelTplSimilarQuestionIndex+1 {
		simQuestions := strings.TrimSpace(row[model.ExcelTplSimilarQuestionIndex])
		sqs := pkg.SplitAndTrimString(simQuestions, "\n")
		if len(sqs) > 0 {
			if _, err := checkSimilarQuestionNumLimit(ctx, len(sqs), 0, 0); err != nil {
				return i18n.Translate(ctx, i18nkey.KeyQAKnowledgeBaseInfo, question, terrs.Msg(err))
			}
			if _, err := checkSimilarQuestionContent(ctx, question, sqs); err != nil {
				return i18n.Translate(ctx, i18nkey.KeyQAKnowledgeBaseInfo, question, terrs.Msg(err))
			}
			/* 增加相似问,一起判断是否重复, 不判断, 和api接口行为保持一致
			   for _, sq := range sqs {
			      questions[sq] = i
			   }
			*/
		}
	}
	mdAnswer, err := util.CheckQaImgURLSafeToMD(ctx, answer, uin, appBizID, uniqueImgHost)
	if err != nil {
		log.WarnContextf(ctx, "ModifyQA Answer ConvertDocQaHtmlToMD err:%d", err)
		return i18n.Translate(ctx, i18nkey.KeyQAKnowledgeBaseInfo, question, terrs.Msg(err))
	}
	videoUrls, err := util.CheckVideoUrls(mdAnswer)
	if err != nil {
		return i18n.Translate(ctx, i18nkey.KeyQAKnowledgeBaseInfo, answer, terrs.Msg(err))
	}
	for _, videoUrl := range videoUrls {
		u, err := url.Parse(videoUrl)
		if err != nil {
			return i18n.Translate(ctx, i18nkey.KeyQAKnowledgeBaseInfo, answer, terrs.Msg(err))
		}
		if u.Host != config.App().Storage.VideoDomain {
			return i18n.Translate(ctx, i18nkey.KeyQAKnowledgeBaseInfo, answer,
				i18nkey.KeyExternalVideoLinksNotSupported)
		}
		// 去掉前面的斜线
		path := strings.TrimPrefix(u.Path, "/")
		objectInfo, err := s.dao.StatObject(context.Background(), path)
		if err != nil || objectInfo == nil {
			log.WarnContextf(context.Background(), "checkRow|StatObject:%+v err:%v", objectInfo, err)
			return i18n.Translate(ctx, i18nkey.KeyQAKnowledgeBaseInfo, answer,
				i18nkey.KeyInvalidOrUnreachableVideoUrl)
		}
	}

	// 检查时间有效期格式是否满足
	if err := checkInDataValidity(ctx, row); err != nil {
		return terrs.Msg(err)
	}
	// 检查自定义参数是否满足
	if model.ExcelTplCustomParamIndex+1 > len(row) {
		if err := checkQuestionAndAnswer(ctx, question, answer); err != nil {
			return terrs.Msg(err)
		}
		return ""
	}
	customParam := strings.TrimSpace(row[model.ExcelTplCustomParamIndex])
	questionDesc := strings.TrimSpace(row[model.ExcelTplQuestionDescIndex])
	if err := checkQAAndDescAndParam(ctx, question, answer, questionDesc, customParam); err != nil {
		return terrs.Msg(err)
	}
	return ""
}
