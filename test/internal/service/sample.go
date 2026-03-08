package service

import (
	"context"
	"errors"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"strings"
	"time"
	"unicode/utf8"

	jsoniter "github.com/json-iterator/go"

	"golang.org/x/exp/slices"

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

// UploadSampleFile 上传样本集合
func (s *Service) UploadSampleFile(ctx context.Context, req *pb.UploadSampleReq) (*pb.UploadSampleRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := &pb.UploadSampleRsp{}
	return rsp, nil
}

// UploadSampleSet 上传样本集合
func (s *Service) UploadSampleSet(ctx context.Context, req *pb.UploadSampleSetReq) (*pb.UploadSampleSetRsp, error) {
	key := fmt.Sprintf(dao.LockForUploadSampleFiles, req.GetCosHash())
	if err := s.dao.Lock(ctx, key, 10*time.Second); err != nil {
		log.ErrorContextf(ctx, "UploadSampleFile file lock err :%v", err)
		return nil, errs.ErrSameDocUploading
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()
	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrSystem
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
	}
	corp, err := s.dao.GetCorpByID(ctx, app.CorpID)
	if err != nil || corp == nil {
		return nil, errs.ErrCorpNotFound
	}
	if err = s.dao.CheckURLFile(ctx, app.CorpID, corp.BusinessID, app.BusinessID,
		req.CosUrl, req.ETag); err != nil {
		log.ErrorContextf(ctx, "UploadSampleSet|CheckURLFile failed, err:%+v", err)
		return nil, errs.ErrInvalidURL
	}
	rows, rsp, err, _, _ := s.checkSampleXlsxFile(ctx, req)
	if rsp != nil || err != nil {
		return rsp, err
	}
	sampleSet, _ := s.dao.GetSampleSetByCosHash(ctx, corpID, app.ID, req.GetCosHash())
	if sampleSet != nil {
		return rsp, errs.ErrSystem
	}

	roleDescription := app.PreviewDetails.AppConfig.KnowledgeQaConfig.RoleDescription
	log.DebugContextf(ctx, "UploadSampleSetWithCheck roleDescription:%s", roleDescription)

	sampleRecord := getSampleSetContents(rows, roleDescription)
	if len(sampleRecord) <= 0 {
		return rsp, errs.ErrWrapf(errs.ErrCodeExcelNumTooFew, i18n.Translate(ctx, i18nkey.KeyTableValidCorpusLessThanOne))
	}
	sampleSet = &model.SampleSet{
		BusinessID:    s.dao.GenerateSeqID(),
		CorpID:        corpID,
		RobotID:       app.ID,
		Name:          req.GetFileName(),
		Num:           uint32(len(sampleRecord)),
		CosBucket:     s.dao.GetBucket(ctx),
		CosURL:        req.GetCosUrl(),
		CosHash:       req.GetCosHash(),
		CreateStaffID: staffID,
	}
	if err = s.dao.CreateSampleSet(ctx, sampleSet, sampleRecord); err != nil {
		log.ErrorContextf(ctx, "CreateSampleSet insert db err :%v", err)
		return rsp, errs.ErrSystem
	}
	rsp = &pb.UploadSampleSetRsp{
		SetBizId: sampleSet.BusinessID,
		Total:    sampleSet.Num,
	}
	_ = s.dao.AddOperationLog(ctx, model.SampleEventUpload, corpID, app.ID, req, rsp, nil, sampleSet)
	return rsp, nil
}

// UploadSampleSetWithCheck 带校验上传样本集合
func (s *Service) UploadSampleSetWithCheck(ctx context.Context, req *pb.UploadSampleSetWithCheckReq) (
	*pb.UploadSampleSetWithCheckRsp, error) {
	key := fmt.Sprintf(dao.LockForUploadSampleFiles, req.GetCosHash())
	if err := s.dao.Lock(ctx, key, 10*time.Second); err != nil {
		log.ErrorContextf(ctx, "UploadSampleSetWithCheck file lock err :%v", err)
		return nil, errs.ErrSameDocUploading
	}
	defer func() { _ = s.dao.UnLock(ctx, key) }()
	staffID, corpID := pkg.StaffID(ctx), pkg.CorpID(ctx)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrSystem
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
	}
	corp, err := s.dao.GetCorpByID(ctx, app.CorpID)
	if err != nil || corp == nil {
		return nil, errs.ErrCorpNotFound
	}
	if err = s.dao.CheckURLFile(ctx, app.CorpID, corp.BusinessID, app.BusinessID,
		req.CosUrl, req.ETag); err != nil {
		log.ErrorContextf(ctx, "UploadSampleSetWithCheck|CheckURLFile failed, err:%+v", err)
		return nil, errs.ErrInvalidURL
	}
	roleDescription := app.PreviewDetails.AppConfig.KnowledgeQaConfig.RoleDescription
	log.DebugContextf(ctx, "UploadSampleSetWithCheck roleDescription:%s", roleDescription)
	rows, uploadSampleSetRsp, err, maxTips, contentTips := s.checkSampleXlsxFile(ctx,
		convertUploadSampleSetReqToCheckReq(req))
	rsp := convertUploadSampleSetRspToCheckRsp(uploadSampleSetRsp)
	if uploadSampleSetRsp != nil || err != nil {
		rsp.IsAllow = false
		return rsp, err
	}
	sampleSet, _ := s.dao.GetSampleSetByCosHash(ctx, corpID, app.ID, req.GetCosHash())
	if sampleSet != nil {
		rsp.IsAllow = false
		return rsp, errs.ErrDocExist
	}
	contents := getSampleSetContents(rows, roleDescription)
	log.DebugContextf(ctx, "getSampleSetContents contents:%s", contents)
	if len(contents) <= 0 {
		rsp.IsAllow = false
		rsp.ErrorMsg = i18n.Translate(ctx, i18nkey.KeyTableValidCorpusLessThanOne)
		return rsp, nil
	}
	checkMsg := s.getContentsLengthCheckMsg(ctx, rows, contents, maxTips, contentTips)
	if len(checkMsg) > 0 {
		// 校验信息不为空，此时不能导入，返回提示信息，通过UploadSampleSet接口二次导入
		rsp.IsAllow = true
		rsp.ErrorMsg = checkMsg
		return rsp, nil
	}
	sampleSet = &model.SampleSet{
		BusinessID:    s.dao.GenerateSeqID(),
		CorpID:        corpID,
		RobotID:       app.ID,
		Name:          req.GetFileName(),
		Num:           uint32(len(contents)),
		CosBucket:     s.dao.GetBucket(ctx),
		CosURL:        req.GetCosUrl(),
		CosHash:       req.GetCosHash(),
		CreateStaffID: staffID,
	}
	if err = s.dao.CreateSampleSet(ctx, sampleSet, contents); err != nil {
		log.ErrorContextf(ctx, "UploadSampleSetWithCheck dao.CreateSampleSet insert db err :%v", err)
		return rsp, errs.ErrSystem
	}
	rsp.SetBizId = sampleSet.BusinessID
	rsp.Total = sampleSet.Num
	rsp.IsAllow = true
	_ = s.dao.AddOperationLog(ctx, model.SampleEventUpload, corpID, app.ID, req, rsp, nil, sampleSet)
	return rsp, nil
}

// QuerySampleSetList 查询样本集列表
func (s *Service) QuerySampleSetList(ctx context.Context, req *pb.QuerySampleReq) (*pb.QuerySampleRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := &pb.QuerySampleRsp{}
	return rsp, nil
}

// ListSampleSet 查询样本集列表
func (s *Service) ListSampleSet(ctx context.Context, req *pb.ListSampleSetReq) (*pb.ListSampleSetRsp, error) {
	rsp := new(pb.ListSampleSetRsp)
	corpID := pkg.CorpID(ctx)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	total, sampleSets, err := s.dao.GetSampleSets(ctx, corpID, app.ID, req.GetSetName(), req.GetPageNumber(),
		req.GetPageSize())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	rsp.Total = uint32(total)
	rsp.List = slicex.Map(sampleSets, (*model.SampleSet).ToRspList)
	return rsp, nil
}

// DeleteSampleFiles 批量删除样本集
func (s *Service) DeleteSampleFiles(ctx context.Context, req *pb.DeleteSampleReq) (*pb.DeleteSampleRsp, error) {
	log.ErrorContextf(ctx, "准备删除的接口收到了请求 deprecated interface req:%+v", req)
	rsp := new(pb.DeleteSampleRsp)
	return rsp, nil
}

// DeleteSampleSet 批量删除样本集
func (s *Service) DeleteSampleSet(ctx context.Context, req *pb.DeleteSampleSetReq) (*pb.DeleteSampleSetRsp, error) {
	rsp := new(pb.DeleteSampleSetRsp)
	corpID := pkg.CorpID(ctx)
	app, err := s.getAppByAppBizID(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	var ids []uint64
	sets, err := s.dao.GetSampleSetsByBizIDs(ctx, corpID, app.ID, req.GetSetBizIds())
	if err != nil {
		return rsp, errs.ErrSystem
	}
	for _, set := range sets {
		ids = append(ids, set.ID)
	}
	if err := s.isInTestMode(ctx, corpID, app.ID, ids); err != nil {
		return rsp, err
	}
	if err = s.dao.DeleteSampleSets(ctx, corpID, app.ID, ids); err != nil {
		return rsp, errs.ErrSystem
	}
	_ = s.dao.AddOperationLog(ctx, model.SampleEventDelete, corpID, app.ID, req, rsp, nil, nil)
	return rsp, nil
}

// checkSampleXlsx 检查sample样本文件是否符合要求
func (s *Service) checkSampleXlsx(ctx context.Context, req *pb.UploadSampleReq) ([][]string, *pb.UploadSampleRsp,
	error) {
	body, err := s.dao.GetObject(ctx, req.GetCosUrl())
	if err != nil {
		log.ErrorContextf(ctx, "checkSampleXlsx file get file by url err :%v", err)
		return nil, nil, errs.ErrSystem
	}
	setName := strings.TrimSuffix(req.GetFileName(), ".xlsx")
	if len(setName) == 0 {
		return nil, nil, errs.ErrInvalidFileName
	}
	cfg := config.App().SampleRule
	// 将配置中文件头翻译成ctx中语言
	var checkHead []string
	for _, v := range cfg.ExcelHead {
		checkHead = append(checkHead, i18n.Translate(ctx, v))
	}
	log.InfoContextf(ctx, "checkSampleXlsx checkHead:%v", checkHead)
	rows, bs, err := util.CheckContent(ctx, setName, cfg.MinRow, cfg.MaxRow, checkHead, body, checkSampleRow)
	if err != nil {
		if err != errs.ErrExcelContent {
			log.WarnContextf(ctx, "checkSampleXlsx file check excel err :%v", err)
			return nil, nil, err
		}
		key := req.GetCosUrl() + ".check.xlsx"
		if err := s.dao.PutObject(ctx, bs, key); err != nil {
			return nil, nil, errs.ErrSystem
		}
		url, err := s.dao.GetPresignedURL(ctx, key)
		if err != nil {
			log.ErrorContextf(ctx, "UploadSampleFile file write excl err :%v", err)
			return nil, nil, errs.ErrSystem
		}
		return nil, &pb.UploadSampleRsp{
			ErrorMsg:      i18n.Translate(ctx, i18nkey.KeyFileDataErrorPleaseDownloadErrorFile),
			ErrorLink:     url,
			ErrorLinkText: i18n.Translate(ctx, i18nkey.KeyDownload),
		}, nil
	}
	return rows, nil, nil
}

// checkSampleXlsxFile 检查sample样本文件是否符合要求
func (s *Service) checkSampleXlsxFile(ctx context.Context, req *pb.UploadSampleSetReq) (
	[][]string, *pb.UploadSampleSetRsp, error, bool, bool) {
	body, err := s.dao.GetObject(ctx, req.GetCosUrl())
	if err != nil {
		log.ErrorContextf(ctx, "checkSampleXlsxFile file get file by url err :%v", err)
		return nil, nil, errs.ErrSystem, false, false
	}
	setName := strings.TrimSuffix(req.GetFileName(), ".xlsx")
	if len(setName) == 0 {
		return nil, nil, errs.ErrInvalidFileName, false, false
	}
	cfg := config.App().SampleRule
	// 将配置中文件头翻译成ctx中语言
	var checkHead []string
	for _, v := range cfg.ExcelHead {
		checkHead = append(checkHead, i18n.Translate(ctx, v))
	}
	log.InfoContextf(ctx, "checkSampleXlsxFile checkHead:%v", checkHead)
	rows, bs, err, maxTips, contentTips := util.CheckSampleContent(ctx, setName, cfg.MinRow, cfg.MaxRow, checkHead,
		body, checkSampleRowFileError)
	if err != nil {
		log.WarnContextf(ctx, "CheckSampleContent file err :%v", err)
		if !errors.Is(err, errs.ErrExcelContent) {
			log.WarnContextf(ctx, "checkSampleXlsxFile file check excel err :%v", err)
			return nil, nil, err, maxTips, contentTips
		}
		key := req.GetCosUrl() + ".check.xlsx"
		if err := s.dao.PutObject(ctx, bs, key); err != nil {
			return nil, nil, errs.ErrSystem, maxTips, contentTips
		}
		url, err := s.dao.GetPresignedURL(ctx, key)
		if err != nil {
			log.ErrorContextf(ctx, "UploadSampleFile file write excl err :%v", err)
			return nil, nil, errs.ErrSystem, maxTips, contentTips
		}
		return nil, &pb.UploadSampleSetRsp{
			ErrorMsg:      i18n.Translate(ctx, i18nkey.KeyFileDataErrorPleaseDownloadErrorFile),
			ErrorLink:     url,
			ErrorLinkText: i18n.Translate(ctx, i18nkey.KeyDownload),
		}, nil, maxTips, contentTips
	}
	return rows, nil, nil, maxTips, contentTips
}

// getContentsLengthCheckMsg 获取sample样本文件是否数量要求信息
func (s *Service) getContentsLengthCheckMsg(ctx context.Context, beforeDeduplicateRows [][]string,
	afterDeduplicateContents []model.SampleRecord, maxTips, contentTips bool) string {
	cfg := config.App().SampleRule
	if maxTips {
		return i18n.Translate(ctx, i18nkey.KeyFileExceedsEntryLimit, cfg.MaxRow)
	}
	if contentTips {
		return i18n.Translate(ctx, i18nkey.KeyTestSampleOrRoleSettingCharExceedAutoTruncate)
	}
	if len(beforeDeduplicateRows) < cfg.CheckRow {
		return i18n.Translate(ctx, i18nkey.KeyDatasetInsufficient, cfg.CheckRow)
	}
	if len(beforeDeduplicateRows) >= cfg.CheckRow && len(afterDeduplicateContents) < cfg.CheckRow {
		return i18n.Translate(ctx, i18nkey.KeyCurrentDatasetDeduplicatedInsufficient, cfg.CheckRow)
	}
	return ""
}

func checkSampleRow(ctx context.Context, i int, row []string, questions map[string]int) string {
	cfg := config.App().SampleRule
	if len(row) < len(cfg.ExcelHead) {
		return i18n.Translate(ctx, i18nkey.KeySampleRowContentEmptyPleaseFill)
	}
	question := strings.TrimSpace(row[0])
	if question == "" {
		return i18n.Translate(ctx, i18nkey.KeySampleCorpusEmpty)
	}
	if len([]rune(question)) < cfg.Question.MinLength {
		return i18n.Translate(ctx, i18nkey.KeySampleCorpusLessThanMinChars, cfg.Question.MinLength)
	}
	if len([]rune(question)) > cfg.Question.MaxLength {
		return i18n.Translate(ctx, i18nkey.KeySampleCorpusExceedMaxChars, cfg.Question.MaxLength)
	}
	questions[question] = i
	return ""
}

// checkSampleRowFileError 检查样本文件是否有需要返回文件内容错误
func checkSampleRowFileError(ctx context.Context, i int, row []string, questions map[string]int) string {
	if len(row) < 3 || strings.TrimSpace(row[2]) == "" {
		return ""
	}
	customVariables := row[2]
	// 第一步：验证是否为有效JSON
	if !jsoniter.Valid([]byte(customVariables)) {
		return fmt.Sprintf("自定义参数不是有效json格式,请按api文档示例填写")
	}

	var raw jsoniter.RawMessage
	if err := jsoniter.Unmarshal([]byte(row[2]), &raw); err != nil {
		return fmt.Sprintf("自定义参数不是有效json格式,请按api文档示例填写")
	}
	// 第二步：解析为 interface{} 进行类型检查
	var v interface{}
	if err := jsoniter.Unmarshal(raw, &v); err != nil {
		return fmt.Sprintf("自定义参数不是有效json格式,请按api文档示例填写")
	}
	// 第三步：递归检查所有值类型
	if err := checkJSONStrings(v); err != nil {
		return fmt.Sprintf("自定义参数json值类型错误,请按api文档示例填写")
	}
	return ""
}

// 检查所有json值的类型是否为 string
func checkJSONStrings(v interface{}) error {
	switch val := v.(type) {
	case map[string]interface{}:
		// 遍历所有键值对
		for k, v := range val {
			// 递归检查值
			if err := checkJSONStrings(v); err != nil {
				return fmt.Errorf(".%s%s", k, err)
			}
		}
		return nil
	case []interface{}:
		// 遍历数组元素
		for i, elem := range val {
			if err := checkJSONStrings(elem); err != nil {
				return fmt.Errorf("[%d]%s", i, err)
			}
		}
		return nil
	case string:
		return nil // 字符串类型通过检查
	default:
		// 非字符串类型返回错误
		return fmt.Errorf(": json值非字符串类型 (%T)", val)
	}
}

// getSampleSetContents 忽略 重复、字符/角色 超限数据
func getSampleSetContents(contents [][]string, roleDescription string) []model.SampleRecord {
	if len(contents) == 0 {
		return nil
	}
	unique := make([]string, 0)
	sampleRecord := make([]model.SampleRecord, 0)
	for _, content := range contents {
		if len(content) > 0 && strings.TrimSpace(content[0]) == "" {
			continue
		}
		// 导入样本集时，每条语料超过12000字符的自动截断，https://tapd.woa.com/tapd_fe/70080800/story/detail/1070080800118892670
		maxLen := config.App().SampleRule.Question.MaxLength
		if utf8.RuneCountInString(content[0]) > maxLen {
			var newContent = string([]rune(content[0])[:maxLen])
			content[0] = newContent
		}
		if len(content) > 1 && content[1] != "" {
			if utf8.RuneCountInString(content[1]) > config.App().SampleRule.RoleLength {
				continue
			}
		}
		if len(content) > 0 && !slices.Contains(unique, content[0]) {
			unique = append(unique, content[0])
			record := model.SampleRecord{
				Content: content[0],
			}
			if len(content) > 1 && content[1] != "" {
				record.RoleDescription = content[1]
			} else {
				record.RoleDescription = roleDescription
			}
			if len(content) > 2 && content[2] != "" { // 添加自定义参数
				//if utf8.RuneCountInString(content[2]) > config.App().SampleRule.RoleLength {
				//	continue
				//}
				record.CustomVariables = content[2]
			}

			sampleRecord = append(sampleRecord, record)
		}
	}
	return sampleRecord
}

func convertUploadSampleSetReqToCheckReq(req *pb.UploadSampleSetWithCheckReq) *pb.UploadSampleSetReq {
	if req == nil {
		return &pb.UploadSampleSetReq{}
	}
	return &pb.UploadSampleSetReq{
		FileName: req.GetFileName(),
		CosUrl:   req.GetCosUrl(),
		ETag:     req.GetETag(),
		CosHash:  req.GetCosHash(),
		Size:     req.GetSize(),
		BotBizId: req.GetBotBizId(),
	}
}

func convertUploadSampleSetRspToCheckRsp(rsp *pb.UploadSampleSetRsp) *pb.UploadSampleSetWithCheckRsp {
	if rsp == nil {
		return &pb.UploadSampleSetWithCheckRsp{}
	}
	return &pb.UploadSampleSetWithCheckRsp{
		SetBizId:      rsp.GetSetBizId(),
		Total:         rsp.GetTotal(),
		ErrorMsg:      rsp.GetErrorMsg(),
		ErrorLink:     rsp.GetErrorLink(),
		ErrorLinkText: rsp.GetErrorLinkText(),
	}
}
