package service

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"
	"golang.org/x/sync/errgroup"
	"hash/fnv"
	"io"
	"net/http"
	stdhttp "net/http"
	"strings"
)

// AppNotice 应用通知
type AppNotice struct {
	Config Config `json:"config"` // 配置
	Notice Notice `json:"notice"` // 通知
}

// Config 配置
type Config struct {
	BatchSize   int `json:"batch_size"`  // 处理批次大小
	Concurrency int `json:"concurrency"` // 处理并发数量
}

// Notice 通知
type Notice struct {
	BotBizId     string            `json:"bot_biz_id"`     // 应用ID（*：表示全部应用）
	PageId       uint32            `json:"page_id"`        // 页面ID 标记页面
	Type         uint32            `json:"type"`           // 通知业务类型
	Level        string            `json:"level"`          // 通知级别
	RelateId     uint64            `json:"relate_id"`      // 业务ID
	Subject      string            `json:"subject"`        // 通知主题
	Content      string            `json:"content"`        // 通知内容
	IsGlobal     bool              `json:"is_global"`      // 是否全局通知
	IsAllowClose bool              `json:"is_allow_close"` // 是否允许被关闭
	CorpId       string            `json:"corp_id"`        // 企业ID （*：表示全部企业）
	StaffId      string            `json:"staff_id"`       // 员工ID （*：表示全部员工）
	Operations   []model.Operation `json:"operations"`     // 操作列表
}

// levelMap 通知级别类型
var levelMap = map[string]struct{}{
	model.LevelSuccess: {},
	model.LevelWarning: {},
	model.LevelInfo:    {},
	model.LevelError:   {},
}

// defaultBatchSize 批次大小 每次1000个应用
var defaultBatchSize = 1000

// defaultConcurrency 并发大小 每次10并发
var defaultConcurrency = 10

// SendAppNotice 应用通知
// 内部通用接口，应用相关的通知，需要后台手动发送给应用触达用户的可以复用
func (s *Service) SendAppNotice(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	log.ErrorContextf(r.Context(), "准备删除的接口收到了请求 deprecated interface req:%+v", r)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if strings.ToUpper(r.Method) != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("\nonly POST is allowed\n"))
		return
	}

	ctx := r.Context()
	reqBody, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("\nBody Read Failed, err:%+v\n", err)))
		return
	}
	appNotice := &AppNotice{}
	if err = jsoniter.Unmarshal(reqBody, appNotice); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(fmt.Sprintf("\nBody Unmarshal Failed, err:%+v\n", err)))
		return
	}

	requestID := uuid.NewString()
	ctx = log.WithContextFields(ctx, "RequestID", requestID)
	log.InfoContextf(ctx, "SendAppNotice appNotice:%+v", appNotice)

	// 校验通知参数
	if err = s.checkAppNotice(ctx, appNotice); err != nil {
		log.ErrorContextf(ctx, "SendAppNotice checkAppNotice failed, err:%+v", err)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(fmt.Sprintf("\nBody Check Failed, err:%+v, RequestID: %s\n", err, requestID)))
		return
	}

	// 通知应用消息
	// carryAppNotice
	if err = s.carryAppNotice(ctx, appNotice); err != nil {
		log.ErrorContextf(ctx, "SendAppNotice carryAppNotice failed, err:%+v", err)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(fmt.Sprintf("\nBody Check Failed, err:%+v, RequestID: %s\n", err, requestID)))
		return
	}

	log.InfoContextf(ctx, "SendAppNotice success")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(fmt.Sprintf("\nOK, RequestID: %s\n", requestID)))
	return
}

// checkAppNotice 校验通知参数
func (s *Service) checkAppNotice(ctx context.Context, appNotice *AppNotice) error {
	if appNotice == nil {
		return fmt.Errorf("appNotice is nil")
	}
	if appNotice.Config.BatchSize <= 0 {
		appNotice.Config.BatchSize = defaultBatchSize
	}
	if appNotice.Config.Concurrency <= 0 {
		appNotice.Config.Concurrency = defaultConcurrency
	}
	if len(appNotice.Notice.BotBizId) == 0 {
		return fmt.Errorf("appNotice.Notice.BotBizId is empty")
	}
	if _, err := util.CheckReqParamsIsUint64(ctx, appNotice.Notice.BotBizId); err != nil &&
		appNotice.Notice.BotBizId != "*" {
		return fmt.Errorf("appNotice.Notice.BotBizId is invalid")
	}
	if _, err := cast.ToUint32E(appNotice.Notice.PageId); err != nil {
		return fmt.Errorf("appNotice.Notice.PageId is invalid")
	}
	if _, err := cast.ToUint32E(appNotice.Notice.Type); err != nil {
		return fmt.Errorf("appNotice.Notice.Type is invalid")
	}
	if _, ok := levelMap[appNotice.Notice.Level]; !ok {
		return fmt.Errorf("appNotice.Notice.Level is invalid")
	}
	if _, err := cast.ToUint64E(appNotice.Notice.RelateId); err != nil {
		return fmt.Errorf("appNotice.Notice.RelateId is invalid")
	}
	// 业务ID如果为空，默认填充一个
	if cast.ToUint64(appNotice.Notice.RelateId) == 0 {
		h := fnv.New64a()
		_, err := h.Write([]byte(fmt.Sprintf("%d_%d", appNotice.Notice.Type, appNotice.Notice.PageId)))
		if err != nil {
			return err
		}
		v := int64(h.Sum64())
		if v < 0 {
			appNotice.Notice.RelateId = uint64(-v)
		} else {
			appNotice.Notice.RelateId = uint64(v)
		}
	}
	if len(appNotice.Notice.Subject) == 0 {
		return fmt.Errorf("appNotice.Notice.Subject is empty")
	}
	if len(appNotice.Notice.Content) == 0 {
		return fmt.Errorf("appNotice.Notice.Content is empty")
	}
	if len(appNotice.Notice.CorpId) == 0 {
		return fmt.Errorf("appNotice.Notice.CorpId is empty")
	}
	if _, err := util.CheckReqParamsIsUint64(ctx, appNotice.Notice.CorpId); err != nil &&
		appNotice.Notice.CorpId != "*" {
		return fmt.Errorf("appNotice.Notice.CorpId is invalid")
	}
	if len(appNotice.Notice.StaffId) == 0 {
		return fmt.Errorf("appNotice.Notice.StaffId is empty")
	}
	if _, err := util.CheckReqParamsIsUint64(ctx, appNotice.Notice.StaffId); err != nil &&
		appNotice.Notice.StaffId != "*" {
		return fmt.Errorf("appNotice.Notice.StaffId is invalid")
	}
	return nil
}

// carryAppNotice 执行消息通知
func (s *Service) carryAppNotice(ctx context.Context, appNotice *AppNotice) error {
	log.InfoContextf(ctx, "carryAppNotice appNotice:%+v", appNotice)
	corpID, staffIDs, appBizIDs := uint64(0), make([]uint64, 0), make([]uint64, 0)
	if appNotice.Notice.CorpId != "*" {
		corpID = cast.ToUint64(appNotice.Notice.CorpId)
	}
	if appNotice.Notice.StaffId != "*" {
		staffIDs = []uint64{cast.ToUint64(appNotice.Notice.StaffId)}
	}
	if appNotice.Notice.BotBizId != "*" {
		appBizIDs = []uint64{cast.ToUint64(appNotice.Notice.BotBizId)}
	}

	// 查询应用
	// 目前数据量不大全量查询，后续考虑分页
	total, err := s.dao.GetAppCount(ctx, corpID, staffIDs, appBizIDs, []string{model.KnowledgeQaAppType},
		[]uint32{model.AppIsNotDeleted}, "", "")
	if err != nil {
		return err
	}
	apps, err := s.dao.GetAppList(ctx, corpID, staffIDs, appBizIDs, []string{model.KnowledgeQaAppType},
		[]uint32{model.AppIsNotDeleted}, "", "", 1, uint32(total))
	if err != nil {
		return err
	}
	log.InfoContextf(ctx, "carryAppNotice total:%d, len(apps):%d", total, len(apps))
	if total != uint64(len(apps)) {
		return fmt.Errorf("carryAppNotice total:%d, len(apps):%d not eqaul", total, len(apps))
	}

	// 分批处理
	noticedCount := uint64(0)
	appGroups := slicex.Chunk(apps, appNotice.Config.BatchSize)
	for i, appGroup := range appGroups {
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(appNotice.Config.Concurrency)
		log.InfoContextf(ctx, "carryAppNotice batchIndex:%d, len(appGroup):%d", i, len(appGroup))
		for _, app := range appGroup {
			noticedCount++
			gApp := app
			g.Go(func() error {
				if err = s.createAppNotice(gCtx, gApp, appNotice.Notice); err != nil {
					log.ErrorContextf(ctx, "carryAppNotice createAppNotice err:%+v|appBizID:%d",
						err, gApp.BusinessID)
					return err
				}
				return nil
			})
		}
		if err = g.Wait(); err != nil {
			return err
		}
		log.InfoContextf(ctx, "carryAppNotice batchIndex:%d, noticedCount:%d", i, noticedCount)
	}

	if noticedCount != total {
		return fmt.Errorf("carryAppNotice noticedCount:%d, total:%d not eqaul", noticedCount, total)
	}
	return nil
}

// createAppNotice 创建消息通知
func (s *Service) createAppNotice(ctx context.Context, app *model.AppDB, notice Notice) error {
	log.InfoContextf(ctx, "createAppNotice appBizID:%d", app.BusinessID)
	staffIDs := make([]uint64, 0)
	if notice.StaffId == "*" { // 全部的用户
		staffs, err := s.dao.GetStaffByCorpID(ctx, app.CorpID, "", []uint64{}, 1, 10000) // 企业下用户 写一个大的值 1w
		if err != nil {
			return err
		}
		for _, staff := range staffs {
			staffIDs = append(staffIDs, staff.ID)
		}
	} else { // 指定用户
		staffIDs = append(staffIDs, cast.ToUint64(notice.StaffId))
	}
	modelNotices := make([]*model.Notice, 0)
	for _, staffID := range staffIDs {
		modelNotice, err := generateModelNotice(ctx, app, staffID, notice)
		if err != nil {
			return err
		}
		modelNotices = append(modelNotices, modelNotice)
	}
	log.InfoContextf(ctx, "createAppNotice appBizID:%d, len(modelNotices):%d",
		app.BusinessID, len(modelNotices))
	for _, modelNotice := range modelNotices {
		if err := s.dao.CreateNotice(ctx, modelNotice); err != nil {
			log.ErrorContextf(ctx, "createAppNotice CreateNotice failed, err:%+v", err)
			return err
		}
	}
	return nil
}

// generateModelNotice 生成应用通知
func generateModelNotice(ctx context.Context, app *model.AppDB, staffID uint64, notice Notice) (
	*model.Notice, error) {
	noticeOptions := []model.NoticeOption{
		model.WithPageID(notice.PageId),
		model.WithLevel(notice.Level),
		model.WithContent(notice.Content),
		model.WithSubject(notice.Subject),
	}
	if notice.IsGlobal {
		noticeOptions = append(noticeOptions, model.WithGlobalFlag())
	}
	if !notice.IsAllowClose {
		noticeOptions = append(noticeOptions, model.WithForbidCloseFlag())
	}
	modelNotice := model.NewNotice(notice.Type, notice.RelateId, app.CorpID, app.ID, staffID, noticeOptions...)
	if len(notice.Operations) > 0 {
		if err := modelNotice.SetOperation(notice.Operations); err != nil {
			log.ErrorContextf(ctx, "generateModelNotice SetOperation failed, err:%+v", err)
			return nil, err
		}
	} else {
		if err := modelNotice.SetOperation(make([]model.Operation, 0)); err != nil {
			log.ErrorContextf(ctx, "generateModelNotice SetOperation failed, err:%+v", err)
			return nil, err
		}
	}
	return modelNotice, nil
}
