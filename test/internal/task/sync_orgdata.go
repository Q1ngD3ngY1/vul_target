package task

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	terrs "git.woa.com/baicaoyuan/moss/errs"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/app"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/cast"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"git.code.oa.com/trpc-go/trpc-database/redis"
	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"golang.org/x/sync/errgroup"
)

const (
	syncOrgDataPrefix                  = "sync:orgdata:"
	defaultSleepMsEachRobotSyncOrgData = 5
	getOrgDataChunkSize                = 2000
)

var syncOrgDataUpgradeCache app.UpgradeCache

type SyncOrgDataScheduler struct {
	dao  dao.Dao
	task task_scheduler.Task
	p    model.SyncOrgDataParams
}

func initSyncOrgDataScheduler() {
	task_scheduler.Register(
		model.SyncOrgDataTask,
		func(t task_scheduler.Task, params model.SyncOrgDataParams) task_scheduler.TaskHandler {
			return &SyncOrgDataScheduler{
				dao:  dao.New(),
				task: t,
				p:    params,
			}
		})
	syncOrgDataUpgradeCache.UpgradeType = app.SyncOrgDataUpgrade
	syncOrgDataUpgradeCache.ExpiredTimeS = app.DefaultUpgradeCacheExpiredS
}

func (d *SyncOrgDataScheduler) orgDataKey(appID uint64) string {
	return fmt.Sprintf("task_org_data_sync_app:%d", appID)
}

func (d *SyncOrgDataScheduler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	log.InfoContextf(ctx, "task(SyncOrgData) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	ids := d.p.AppIDs
	if len(d.p.AppIDs) == 0 {
		// 为空则查找所有的未删除、未升级的robot id
		allRobotIDs, err := dao.GetRobotDao().GetAllValidAppIDs(ctx, 0)
		if err != nil {
			return kv, nil
		}
		ids = allRobotIDs
	}
	pendingIDs, err := syncOrgDataUpgradeCache.GetNotUpgradedApps(ctx, ids)
	if err != nil {
		return kv, err
	}
	for _, id := range pendingIDs {
		kv[syncOrgDataPrefix+cast.ToString(id)] = cast.ToString(id)
	}
	log.InfoContextf(ctx, "task(SyncOrgData) prepare finish, robot id count %v", len(pendingIDs))
	return kv, nil
}

func (d *SyncOrgDataScheduler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	return nil
}

func (d *SyncOrgDataScheduler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	for k, v := range progress.TaskKV(ctx) {
		if !utilConfig.GetMainConfig().SegmentIntervene.SyncOrgDataSwitch {
			return terrs.New(1000, "停止任务")
		}
		t0 := time.Now()
		appID := cast.ToUint64(v)
		log.InfoContextf(
			ctx, "待同步应用数: %d, 已完成百分比: %d/%d(%s)",
			progress.Remain(), progress.Total()-progress.Remain(), progress.Total(), progress.PercentS(),
		)
		log.InfoContextf(ctx, "sync OrgData for app %v, ", appID)
		if err := d.sync(ctx, appID); err != nil {
			log.ErrorContextf(ctx, "同步应用失败 appID:%d, err: %v", appID, err)
			// 存在恢复中文档/执行任务 该应用先跳过
			continue
		}
		// 清除应用的切片redis缓存
		_ = d.cleanOrgData(ctx, appID)
		if err := progress.Finish(ctx, k); err != nil {
			log.ErrorContextf(ctx, "finish %v error", k)
			return err
		}
		_ = syncOrgDataUpgradeCache.SetAppFinish(ctx, appID)
		log.InfoContextf(ctx, "appID %v upgrade success, cost: %vms",
			appID, time.Now().Sub(t0).Milliseconds())
		// 每个robot之间增加延时，防止对数据库、redis压力过大
		if d.p.DelayMs == 0 {
			d.p.DelayMs = defaultSleepMsEachRobotSyncOrgData
		}
		time.Sleep(time.Duration(d.p.DelayMs) * time.Millisecond)
	}
	if progress.Remain() != 0 {
		var ids []string
		for k := range progress.TaskKV(ctx) {
			ids = append(ids, k)
		}
		log.ErrorContextf(
			ctx, "待同步应用数: %d, 已完成百分比: %d/%d(%s), 剩余未完成应用ID: %+v",
			progress.Remain(), progress.Total()-progress.Remain(), progress.Total(), progress.PercentS(), ids,
		)
		return fmt.Errorf(
			"待同步应用数: %d, 已完成百分比: %d/%d(%s)",
			progress.Remain(), progress.Total()-progress.Remain(), progress.Total(), progress.PercentS(),
		)
	} else {
		log.InfoContextf(ctx, "同步应用完成(all)")
		return nil
	}
}

// sync 单应用同步
func (d *SyncOrgDataScheduler) sync(ctx context.Context, appID uint64) error {
	// 获取应用
	app, err := d.getApp(ctx, appID)
	if err != nil {
		log.ErrorContextf(ctx, "getApp fail, appID: %d, err: %v", appID, err)
		return err
	}
	if app.HasDeleted() { // 应用已被删除, 不做处理
		return nil
	}
	// 检查当前是否允许升级，判断是否有在执行任务，是否有恢复中文档
	upgradeNow, err := d.syncNow(ctx, app)
	if err != nil {
		log.ErrorContextf(ctx, "syncNow fail, appID: %d, err: %v", appID, err)
		return err
	}
	if !upgradeNow {
		return fmt.Errorf("应用当前状态无法刷新")
	}
	if d.p.ChunkSize == 0 {
		d.p.ChunkSize = getOrgDataChunkSize
	}
	for {
		var segments []*model.DocSegment
		if segments, err = d.dao.GetSegmentSyncChunk(ctx, app.CorpID, app.ID, 0, d.p.ChunkSize); err != nil {
			log.ErrorContextf(ctx, "GetSegmentChunk fail, appID: %d, err: %v", app.ID, err)
			return err
		}
		if len(segments) == 0 {
			log.InfoContextf(ctx, "process success, appID: %d", app.ID)
			break
		}
		batch, x := d.batch(), int64(0)
		segmentsGroup := slicex.Chunk(segments, batch)
		for _, segmentGroup := range segmentsGroup {
			g, gCtx := errgroup.WithContext(ctx)
			g.SetLimit(batch)
			for _, segGroup := range groupSegments(segmentGroup) {
				localSegGroup := segGroup
				localSegGroupLen := int64(len(localSegGroup))
				g.Go(func() error {
					// 取第一个切片 查询t_doc_segment_org_data是否已经存在 不存在则创建
					var orgDataBizID uint64
					orgDataBizID, err = d.getSegmentOrgDataInfo(gCtx, localSegGroup[0])
					if err != nil {
						log.ErrorContextf(ctx, "getSegmentOrgDataInfo fail, appID: %d, err: %v",
							app.ID, err)
						return err
					}
					// 拿到orgDataBizID 更新t_doc_segment表
					segIDs := make([]uint64, 0, localSegGroupLen)
					for _, seg := range localSegGroup {
						segIDs = append(segIDs, seg.ID)
					}
					err = d.dao.UpdateSegmentSyncOrgDataBizID(gCtx,
						localSegGroup[0].RobotID, localSegGroup[0].DocID, localSegGroup[0].CorpID,
						localSegGroup[0].StaffID, segIDs, orgDataBizID)
					if err != nil {
						log.ErrorContextf(ctx, "UpdateSegmentOrgDataBizID fail, appID: %d, err: %v",
							app.ID, err)
						return err
					}
					atomic.AddInt64(&x, localSegGroupLen)
					return nil
				})
			}
			if err = g.Wait(); err != nil {
				return err
			}
			log.InfoContextf(ctx, "process segment, appID: %d, %d/%d(%.2f%%)",
				app.ID, atomic.LoadInt64(&x), len(segments), float64(atomic.LoadInt64(&x))*100/float64(len(segments)))
		}
	}
	// 标记完成
	return nil
}

// syncNow 检查当前是否允许升级，判断是否有在执行任务，是否有恢复中文档
func (d *SyncOrgDataScheduler) syncNow(ctx context.Context, app model.AppDB) (bool, error) {
	log.InfoContextf(ctx, "check if syncNow, appID: %d", app.ID)
	// 检查是否有执行中的任务, 如果有执行中的任务不允许同步
	tasks, err := dao.GetTasksByAppID(ctx, app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "GetTasksByAppID fail, appID: %d, err: %v", app.ID, err)
		return false, err
	}
	if len(tasks) > 0 {
		for _, task := range tasks {
			// 排除已经终止的任务
			if task.Runner == "terminated" {
				continue
			} else {
				log.WarnContextf(
					ctx,
					"syncNow(no), appID: %d, taskID:%d, %d tasks still running",
					app.ID, task.ID, len(tasks),
				)
				return false, nil
			}
		}
	}
	// 检查是否有恢复中的文档，有恢复中的文档则先不升级，等恢复完成后再升级，避免恢复过程中文档只升级部分切片
	resumeDocCount, err := d.dao.GetResumeDocCount(ctx, app.CorpID, app.ID)
	if err != nil {
		log.ErrorContextf(ctx, "syncNow(no) GetResumeDocCount fail, appID: %d, err: %v", app.ID, err)
		return false, err
	}
	if resumeDocCount > 0 {
		log.InfoContextf(ctx, "syncNow(no), doc is resuming, appID: %d, resumeDocCount:%d", app.ID, resumeDocCount)
		return false, nil
	}
	log.InfoContextf(ctx, "syncNow(yes), appID: %d", app.ID)
	return true, nil
}

func (d *SyncOrgDataScheduler) batch() int {
	return d.p.Batch
}

// getSegmentOrgDataInfo 获取切片 org_data 信息 不存在则创建
func (d *SyncOrgDataScheduler) getSegmentOrgDataInfo(ctx context.Context, seg *model.DocSegment) (
	orgDataBizID uint64, err error) {
	// redis中查询org_data是否存在
	orgDataBizID, err = d.getOrgData(ctx, seg)
	if err != nil {
		log.ErrorContextf(ctx, "getOrgData fail, segID: %d, err: %v", seg.ID, err)
		return 0, err
	}
	if orgDataBizID > 0 {
		return orgDataBizID, nil
	}

	// 查询org_data所需要的信息并创建
	appDB, err := d.getApp(ctx, seg.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "getApp fail, segID: %d, err: %v", seg.ID, err)
		return 0, err
	}
	appBizID := appDB.BusinessID

	doc, err := d.dao.GetDocByID(ctx, seg.DocID, seg.RobotID)
	if err != nil {
		log.ErrorContextf(ctx, "GetDocByID fail, segID: %d, err: %v", seg.ID, err)
		return 0, err
	}
	docBizID := doc.BusinessID

	corp, err := d.dao.GetCorpByID(ctx, seg.CorpID)
	if err != nil {
		log.ErrorContextf(ctx, "GetCorp fail, segID: %d, err: %v", seg.ID, err)
		return 0, err
	}
	corpBizID := corp.BusinessID

	staff, err := d.dao.GetStaffByID(ctx, seg.StaffID)
	if err != nil {
		log.ErrorContextf(ctx, "GetStaffByID fail, segID: %d, err: %v", seg.ID, err)
		return 0, err
	}
	staffBizID := staff.BusinessID

	info, err := d.dao.GetSegmentPageInfosBySegIDs(ctx, seg.RobotID, []uint64{seg.ID})
	if err != nil {
		log.ErrorContextf(ctx, "GetSegmentPageInfosBySegIDs fail, segID: %d, err: %v", seg.ID, err)
		return 0, err
	}
	pageNumber := ""
	sheetData := ""
	docSegmentPageInfo, ok := info[seg.ID]
	if !ok || docSegmentPageInfo == nil {
		log.WarnContextf(ctx, "GetSegmentPageInfosBySegIDs info not exist or is nil, segID: %d", seg.ID)
	} else {
		pageNumber = docSegmentPageInfo.OrgPageNumbers
		sheetData = docSegmentPageInfo.SheetData
	}

	sheetName := ""
	// 解析SheetData，获取sheet名，如有多个只取第一个（按行拆分时使用）
	var sheetDatas []model.SheetData
	jsoniter.Unmarshal([]byte(sheetData), &sheetDatas)
	if len(sheetDatas) > 0 {
		sheetName = sheetDatas[0].SheetName
	}

	orgDataBizID = d.dao.GenerateSeqID()
	orgData := &model.DocSegmentOrgData{
		BusinessID:         orgDataBizID,
		AppBizID:           appBizID,
		DocBizID:           docBizID,
		CorpBizID:          corpBizID,
		StaffBizID:         staffBizID,
		OrgData:            seg.OrgData,
		OrgPageNumbers:     pageNumber,
		SheetData:          sheetData,
		SegmentType:        seg.SegmentType,
		AddMethod:          0,
		IsTemporaryDeleted: 0,
		IsDeleted:          0,
		IsDisabled:         0,
		CreateTime:         seg.CreateTime,
		UpdateTime:         seg.CreateTime,
		SheetName:          sheetName,
	}
	err = dao.GetDocSegmentOrgDataDao().CreateDocSegmentOrgData(ctx, orgData)
	if err != nil {
		log.ErrorContextf(ctx, "CreateDocSegmentOrgData fail, segID: %d, err: %v", seg.ID, err)
		return 0, err
	}
	// redis中写入org_data创建信息
	err = d.setOrgData(ctx, seg, orgDataBizID)
	if err != nil {
		log.ErrorContextf(ctx, "setOrgData fail|segID:%d|orgDataBizID:%d|err:%v", seg.ID, orgDataBizID, err)
		// 回滚，删除新增的OrgData
		err1 := dao.GetDocSegmentOrgDataDao().RealityDeleteDocSegmentOrgDataByOrgDataBizID(ctx,
			nil, corpBizID, appBizID, docBizID, orgDataBizID)
		if err1 != nil {
			log.ErrorContextf(ctx, "redis set fail and delete OrgData fail, 请人工处理!|segID:%d|orgDataBizID:%d|err1:%v",
				seg.ID, orgDataBizID, err1)
			return 0, err1
		}
		return 0, err
	}
	return orgDataBizID, nil
}

// 生成唯一字符串用于哈希
func generateKeyString(seg *model.DocSegment) string {
	// 这里用 "|" 分隔，避免字段拼接歧义
	return fmt.Sprintf("%d|%d|%d|%d|%s|%s",
		seg.RobotID, seg.DocID, seg.CorpID, seg.StaffID, seg.SegmentType, seg.OrgData)
}

// 计算sha256哈希，返回16进制字符串
func hashKey(keyStr string) string {
	hash := sha256.Sum256([]byte(keyStr))
	return hex.EncodeToString(hash[:])
}

func groupSegments(segments []*model.DocSegment) [][]*model.DocSegment {
	groupMap := make(map[string][]*model.DocSegment)
	for _, seg := range segments {
		keyStr := generateKeyString(seg)
		hashKeyStr := hashKey(keyStr)
		if _, ok := groupMap[hashKeyStr]; ok {
			groupMap[hashKeyStr] = append(groupMap[hashKeyStr], seg)
		} else {
			groupMap[hashKeyStr] = []*model.DocSegment{seg}
		}
	}

	groups := make([][]*model.DocSegment, 0, len(groupMap))
	for _, segGroup := range groupMap {
		groups = append(groups, segGroup)
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i][0].ID < groups[j][0].ID
	})
	return groups
}

func (d *SyncOrgDataScheduler) setOrgData(ctx context.Context, segment *model.DocSegment, orgDataBizID uint64) error {
	hashKeyStr := hashKey(generateKeyString(segment))
	if _, err := d.dao.RedisCli().Do(ctx, "HSET",
		d.orgDataKey(segment.RobotID), hashKeyStr, fmt.Sprintf("%d", orgDataBizID)); err != nil {
		log.ErrorContextf(ctx, "set orgData fail, key: %s, field: %s, orgDataBizID: %d, err: %v",
			d.orgDataKey(segment.RobotID), hashKeyStr, orgDataBizID, err,
		)
		return err
	}
	if _, err := d.dao.RedisCli().Do(ctx, "EXPIRE", d.orgDataKey(segment.RobotID), 3600*24*7); err != nil {
		log.WarnContextf(ctx, "set orgData EXPIRE fail|err:%v", err)
		return nil
	}
	return nil
}

func (d *SyncOrgDataScheduler) getOrgData(ctx context.Context, segment *model.DocSegment) (
	orgDataBizID uint64, err error) {
	hashKeyStr := hashKey(generateKeyString(segment))
	rr, err := redis.String(d.dao.RedisCli().Do(ctx, "HGET",
		d.orgDataKey(segment.RobotID), hashKeyStr))
	if err == nil {
		if orgDataBizID, err = strconv.ParseUint(rr, 10, 64); err != nil {
			log.ErrorContextf(
				ctx, "get orgData fail, key: %s, field: %s, redis reply: %s, err: %v",
				d.orgDataKey(segment.RobotID), hashKeyStr, rr, err,
			)
			return 0, err
		}
		return orgDataBizID, nil
	} else if errors.Is(err, redis.ErrNil) {
		return 0, nil
	} else {
		log.ErrorContextf(ctx, "get orgData fail, key: %s, field: %s, err: %v",
			d.orgDataKey(segment.RobotID), hashKeyStr, err)
		return 0, err
	}
}

func (d *SyncOrgDataScheduler) cleanOrgData(ctx context.Context, appID uint64) error {
	if _, err := d.dao.RedisCli().Do(ctx, "DEL", d.orgDataKey(appID)); err != nil {
		log.ErrorContextf(ctx, "clean orgData fail, key: %s, err: %v", d.orgDataKey(appID), err)
		return err
	}
	return nil
}

func (d *SyncOrgDataScheduler) getApp(ctx context.Context, appID uint64) (model.AppDB, error) {
	app, err := d.dao.GetAppByID(ctx, appID)
	if err != nil {
		log.ErrorContextf(ctx, "GetRobotByID fail, appID: %d, err: %v", appID, err)
		return model.AppDB{}, err
	}
	if app == nil {
		log.ErrorContextf(ctx, "GetRobotByID fail, app not found, appID: %d", appID)
		return model.AppDB{}, errs.ErrRobotNotFound
	}
	return *app, nil
}

func (d *SyncOrgDataScheduler) retry(
	ctx context.Context, name string, timeout time.Duration, fn func(context.Context) error,
) (err error) {
	if d.p.RetryTimes > 0 {
		timeout = time.Duration(d.p.RetryInterval) * timeout
	}
	rCtx, cancel := context.WithTimeout(trpc.CloneContext(ctx), timeout)
	defer cancel()
	if err = fn(rCtx); err == nil {
		return nil
	}
	log.InfoContextf(rCtx, "run %s fail, err: %v", name, err)
	for i := 0; i < d.p.RetryTimes; i++ {
		if rCtx.Err() != nil {
			return rCtx.Err()
		}
		time.Sleep(time.Duration(d.p.RetryInterval) * time.Millisecond)
		log.InfoContextf(rCtx, "retry %s, %d time(s)", name, i)
		if err = fn(rCtx); err == nil {
			log.InfoContextf(rCtx, "retry %s, %d time(s), success", name, i)
			return nil
		}
		log.InfoContextf(rCtx, "retry %s, %d time(s), err: %v", name, i, err)
	}
	return err
}

func (d *SyncOrgDataScheduler) Done(ctx context.Context) error {
	return nil
}

func (d *SyncOrgDataScheduler) Fail(ctx context.Context) error {
	return nil
}

func (d *SyncOrgDataScheduler) Stop(ctx context.Context) error {
	return nil
}
