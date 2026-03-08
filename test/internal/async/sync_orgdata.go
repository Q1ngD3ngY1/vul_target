package async

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	segEntity "git.woa.com/adp/kb/kb-config/internal/entity/segment"
	"git.woa.com/adp/kb/kb-config/internal/logic/app"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/task_scheduler"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
	"golang.org/x/sync/errgroup"
)

const (
	syncOrgDataPrefix                  = "sync:orgdata:"
	defaultSleepMsEachRobotSyncOrgData = 5
	getOrgDataChunkSize                = 2000
)

var syncOrgDataUpgradeCache app.UpgradeCache

type SyncOrgDataTaskHandler struct {
	*taskCommon

	task task_scheduler.Task
	p    entity.SyncOrgDataParams
}

func registerSyncOrgDataTaskHandler(tc *taskCommon) {
	task_scheduler.Register(
		entity.SyncOrgDataTask,
		func(t task_scheduler.Task, params entity.SyncOrgDataParams) task_scheduler.TaskHandler {
			return &SyncOrgDataTaskHandler{
				taskCommon: tc,
				task:       t,
				p:          params,
			}
		})
	syncOrgDataUpgradeCache.UpgradeType = app.SyncOrgDataUpgrade
	syncOrgDataUpgradeCache.ExpiredTimeS = app.DefaultUpgradeCacheExpiredS
	syncOrgDataUpgradeCache.Rdb = tc.adminRdb
}

func orgDataKey(appID uint64) string {
	return fmt.Sprintf("task_org_data_sync_app:%d", appID)
}

func (d *SyncOrgDataTaskHandler) Prepare(ctx context.Context) (task_scheduler.TaskKV, error) {
	logx.I(ctx, "task(SyncOrgData) Prepare, task: %+v, params: %+v", d.task, d.p)
	kv := make(task_scheduler.TaskKV)
	ids := d.p.AppIDs
	if len(d.p.AppIDs) == 0 {
		// 为空则查找所有的未删除、未升级的robot id
		apps, _, err := d.rpc.AppAdmin.ListAllAppBaseInfo(ctx, nil)
		if err != nil {
			logx.W(ctx, "ListAllAppBaseInfo failed, err: %v", err)
			return kv, nil
		}
		ids = slicex.Pluck(apps, func(v *entity.AppBaseInfo) uint64 { return v.PrimaryId })
		// allRobotIDs, err := dao.GetRobotDao().GetAllValidAppIDs(ctx, 0)
		// if err != nil {
		// 	return kv, nil
		// }
		// ids = allRobotIDs
	}
	pendingIDs, err := syncOrgDataUpgradeCache.GetNotUpgradedApps(ctx, ids)
	if err != nil {
		return kv, err
	}
	for _, id := range pendingIDs {
		kv[syncOrgDataPrefix+cast.ToString(id)] = cast.ToString(id)
	}
	logx.I(ctx, "task(SyncOrgData) prepare finish, robot id count %v", len(pendingIDs))
	return kv, nil
}

func (d *SyncOrgDataTaskHandler) Init(ctx context.Context, kv task_scheduler.TaskKV) error {
	return nil
}

func (d *SyncOrgDataTaskHandler) Process(ctx context.Context, progress *task_scheduler.Progress) error {
	for k, v := range progress.TaskKV(ctx) {
		if !config.GetMainConfig().SegmentIntervene.SyncOrgDataSwitch {
			return errx.New(1000, "停止任务")
		}
		t0 := time.Now()
		appID := cast.ToUint64(v)
		logx.I(
			ctx, "待同步应用数: %d, 已完成百分比: %d/%d(%s)",
			progress.Remain(), progress.Total()-progress.Remain(), progress.Total(), progress.PercentS(),
		)
		logx.I(ctx, "sync OrgData for app %v, ", appID)
		if err := d.sync(ctx, appID); err != nil {
			logx.E(ctx, "同步应用失败 appID:%d, err: %v", appID, err)
			// 存在恢复中文档/执行任务 该应用先跳过
			continue
		}
		// 清除应用的切片redis缓存
		_ = d.cleanOrgData(ctx, appID)
		if err := progress.Finish(ctx, k); err != nil {
			logx.E(ctx, "finish %v error", k)
			return err
		}
		_ = syncOrgDataUpgradeCache.SetAppFinish(ctx, appID)
		logx.I(ctx, "appID %v upgrade success, cost: %vms",
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
		logx.E(
			ctx, "待同步应用数: %d, 已完成百分比: %d/%d(%s), 剩余未完成应用ID: %+v",
			progress.Remain(), progress.Total()-progress.Remain(), progress.Total(), progress.PercentS(), ids,
		)
		return fmt.Errorf(
			"待同步应用数: %d, 已完成百分比: %d/%d(%s)",
			progress.Remain(), progress.Total()-progress.Remain(), progress.Total(), progress.PercentS(),
		)
	} else {
		logx.I(ctx, "同步应用完成(all)")
		return nil
	}
}

// sync 单应用同步
func (d *SyncOrgDataTaskHandler) sync(ctx context.Context, appID uint64) error {
	// 获取应用
	app, err := d.getApp(ctx, appID)
	if err != nil {
		logx.E(ctx, "getApp fail, appID: %d, err: %v", appID, err)
		return err
	}
	if app.IsDeleted { // 应用已被删除, 不做处理
		return nil
	}
	// 检查当前是否允许升级，判断是否有在执行任务，是否有恢复中文档
	upgradeNow, err := d.syncNow(ctx, app)
	if err != nil {
		logx.E(ctx, "syncNow fail, appID: %d, err: %v", appID, err)
		return err
	}
	if !upgradeNow {
		return fmt.Errorf("应用当前状态无法刷新")
	}
	if d.p.ChunkSize == 0 {
		d.p.ChunkSize = getOrgDataChunkSize
	}
	for {
		var segments []*segEntity.DocSegment
		if segments, err = d.segLogic.GetSegmentSyncChunk(ctx, app.CorpPrimaryId, app.PrimaryId, 0, d.p.ChunkSize); err != nil {
			logx.E(ctx, "GetSegmentChunk fail, appID: %d, err: %v", app.PrimaryId, err)
			return err
		}
		if len(segments) == 0 {
			logx.I(ctx, "process success, appID: %d", app.PrimaryId)
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
						logx.E(ctx, "getSegmentOrgDataInfo fail, appID: %d, err: %v",
							app.PrimaryId, err)
						return err
					}
					// 拿到orgDataBizID 更新t_doc_segment表
					segIDs := make([]uint64, 0, localSegGroupLen)
					for _, seg := range localSegGroup {
						segIDs = append(segIDs, seg.ID)
					}
					err = d.segLogic.UpdateSegmentSyncOrgDataBizID(gCtx,
						localSegGroup[0].RobotID, localSegGroup[0].DocID, localSegGroup[0].CorpID,
						localSegGroup[0].StaffID, segIDs, orgDataBizID)
					if err != nil {
						logx.E(ctx, "UpdateSegmentOrgDataBizID fail, appID: %d, err: %v",
							app.PrimaryId, err)
						return err
					}
					atomic.AddInt64(&x, localSegGroupLen)
					return nil
				})
			}
			if err = g.Wait(); err != nil {
				return err
			}
			logx.I(ctx, "process segment, appID: %d, %d/%d(%.2f%%)",
				app.PrimaryId, atomic.LoadInt64(&x), len(segments), float64(atomic.LoadInt64(&x))*100/float64(len(segments)))
		}
	}
	// 标记完成
	return nil
}

// syncNow 检查当前是否允许升级，判断是否有在执行任务，是否有恢复中文档
func (d *SyncOrgDataTaskHandler) syncNow(ctx context.Context, app entity.App) (bool, error) {
	logx.I(ctx, "check if syncNow, appID: %d", app.PrimaryId)
	// 检查是否有执行中的任务, 如果有执行中的任务不允许同步
	tasks, err := scheduler.GetTasksByAppID(ctx, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "GetTasksByAppID fail, appID: %d, err: %v", app.PrimaryId, err)
		return false, err
	}
	if len(tasks) > 0 {
		for _, task := range tasks {
			// 排除已经终止的任务
			if task.Runner == "terminated" {
				continue
			} else {
				logx.W(
					ctx,
					"syncNow(no), appID: %d, taskID:%d, %d tasks still running",
					app.PrimaryId, task.ID, len(tasks),
				)
				return false, nil
			}
		}
	}
	// 检查是否有恢复中的文档，有恢复中的文档则先不升级，等恢复完成后再升级，避免恢复过程中文档只升级部分切片
	resumeDocCount, err := d.docLogic.GetResumeDocCount(ctx, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		logx.E(ctx, "syncNow(no) GetResumeDocCount fail, appID: %d, err: %v", app.PrimaryId, err)
		return false, err
	}
	if resumeDocCount > 0 {
		logx.I(ctx, "syncNow(no), doc is resuming, appID: %d, resumeDocCount:%d", app.PrimaryId, resumeDocCount)
		return false, nil
	}
	logx.I(ctx, "syncNow(yes), appID: %d", app.PrimaryId)
	return true, nil
}

func (d *SyncOrgDataTaskHandler) batch() int {
	return d.p.Batch
}

// getSegmentOrgDataInfo 获取切片 org_data 信息 不存在则创建
func (d *SyncOrgDataTaskHandler) getSegmentOrgDataInfo(ctx context.Context, seg *segEntity.DocSegment) (
	orgDataBizID uint64, err error) {
	// redis中查询org_data是否存在
	orgDataBizID, err = d.getOrgData(ctx, seg)
	if err != nil {
		logx.E(ctx, "getOrgData fail, segID: %d, err: %v", seg.ID, err)
		return 0, err
	}
	if orgDataBizID > 0 {
		return orgDataBizID, nil
	}

	// 查询org_data所需要的信息并创建
	appDB, err := d.getApp(ctx, seg.RobotID)
	if err != nil {
		logx.E(ctx, "getApp fail, segID: %d, err: %v", seg.ID, err)
		return 0, err
	}
	appBizID := appDB.BizId

	doc, err := d.docLogic.GetDocByID(ctx, seg.DocID, seg.RobotID)
	if err != nil {
		logx.E(ctx, "GetDocByID fail, segID: %d, err: %v", seg.ID, err)
		return 0, err
	}
	docBizID := doc.BusinessID

	corp, err := d.rpc.PlatformAdmin.DescribeCorpByPrimaryId(ctx, seg.CorpID)
	// corp, err := d.dao.GetCorpByID(ctx, seg.CorpPrimaryId)
	if err != nil {
		logx.E(ctx, "GetCorp fail, segID: %d, err: %v", seg.ID, err)
		return 0, err
	}

	staff, err := d.rpc.PlatformAdmin.GetStaffByID(ctx, seg.StaffID)
	if err != nil {
		logx.E(ctx, "GetStaffByID fail, segID: %d, err: %v", seg.ID, err)
		return 0, err
	}
	staffBizID := staff.BusinessID

	info, err := d.segLogic.GetSegmentPageInfosBySegIDs(ctx, seg.RobotID, []uint64{seg.ID})
	if err != nil {
		logx.E(ctx, "GetSegmentPageInfosBySegIDs fail, segID: %d, err: %v", seg.ID, err)
		return 0, err
	}
	pageNumber := ""
	sheetData := ""
	docSegmentPageInfo, ok := info[seg.ID]
	if !ok || docSegmentPageInfo == nil {
		logx.W(ctx, "GetSegmentPageInfosBySegIDs info not exist or is nil, segID: %d", seg.ID)
	} else {
		pageNumber = docSegmentPageInfo.OrgPageNumbers
		sheetData = docSegmentPageInfo.SheetData
	}

	sheetName := ""
	// 解析SheetData，获取sheet名，如有多个只取第一个（按行拆分时使用）
	var sheetDatas []segEntity.SheetData
	jsonx.Unmarshal([]byte(sheetData), &sheetDatas)
	if len(sheetDatas) > 0 {
		sheetName = sheetDatas[0].SheetName
	}

	orgDataBizID = idgen.GetId()
	orgData := &segEntity.DocSegmentOrgData{
		BusinessID:         orgDataBizID,
		AppBizID:           appBizID,
		DocBizID:           docBizID,
		CorpBizID:          corp.GetCorpId(),
		StaffBizID:         staffBizID,
		OrgData:            seg.OrgData,
		OrgPageNumbers:     pageNumber,
		SheetData:          sheetData,
		SegmentType:        seg.SegmentType,
		AddMethod:          0,
		IsTemporaryDeleted: false,
		IsDeleted:          false,
		IsDisabled:         false,
		CreateTime:         seg.CreateTime,
		UpdateTime:         seg.CreateTime,
		SheetName:          sheetName,
	}
	err = d.segLogic.CreateDocSegmentOrgData(ctx, orgData)
	if err != nil {
		logx.E(ctx, "CreateDocSegmentTemporaryOrgData fail, segID: %d, err: %v", seg.ID, err)
		return 0, err
	}
	// redis中写入org_data创建信息
	err = d.setOrgData(ctx, seg, orgDataBizID)
	if err != nil {
		logx.E(ctx, "setOrgData fail|segID:%d|orgDataBizID:%d|err:%v", seg.ID, orgDataBizID, err)
		// 回滚，删除新增的OrgData
		err1 := d.segLogic.RealityDeleteDocSegmentOrgDataByOrgDataBizID(ctx, corp.GetCorpId(), appBizID, docBizID, orgDataBizID)
		if err1 != nil {
			logx.E(ctx, "redis set fail and delete OrgData fail, 请人工处理!|segID:%d|orgDataBizID:%d|err1:%v",
				seg.ID, orgDataBizID, err1)
			return 0, err1
		}
		return 0, err
	}
	return orgDataBizID, nil
}

// 生成唯一字符串用于哈希
func generateKeyString(seg *segEntity.DocSegment) string {
	// 这里用 "|" 分隔，避免字段拼接歧义
	return fmt.Sprintf("%d|%d|%d|%d|%s|%s",
		seg.RobotID, seg.DocID, seg.CorpID, seg.StaffID, seg.SegmentType, seg.OrgData)
}

// 计算sha256哈希，返回16进制字符串
func hashKey(keyStr string) string {
	hash := sha256.Sum256([]byte(keyStr))
	return hex.EncodeToString(hash[:])
}

func groupSegments(segments []*segEntity.DocSegment) [][]*segEntity.DocSegment {
	groupMap := make(map[string][]*segEntity.DocSegment)
	for _, seg := range segments {
		keyStr := generateKeyString(seg)
		hashKeyStr := hashKey(keyStr)
		if _, ok := groupMap[hashKeyStr]; ok {
			groupMap[hashKeyStr] = append(groupMap[hashKeyStr], seg)
		} else {
			groupMap[hashKeyStr] = []*segEntity.DocSegment{seg}
		}
	}

	groups := make([][]*segEntity.DocSegment, 0, len(groupMap))
	for _, segGroup := range groupMap {
		groups = append(groups, segGroup)
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i][0].ID < groups[j][0].ID
	})
	return groups
}

func (d *SyncOrgDataTaskHandler) setOrgData(ctx context.Context, segment *segEntity.DocSegment, orgDataBizID uint64) error {
	key := orgDataKey(segment.RobotID)
	field := hashKey(generateKeyString(segment))
	value := fmt.Sprintf("%d", orgDataBizID)
	err := d.adminRdb.HSet(ctx, key, field, value).Err()
	if err != nil {
		logx.E(ctx, "setOrgData key: %s, field: %s, orgDataBizID: %d, err: %v", key, field, orgDataBizID, err)
	}
	if err = d.adminRdb.Expire(ctx, key, 3600*24*7).Err(); err != nil {
		logx.W(ctx, "setOrgData EXPIRE error:%v", err)
	}
	return nil
}

func (d *SyncOrgDataTaskHandler) getOrgData(ctx context.Context, segment *segEntity.DocSegment) (orgDataBizID uint64, err error) {
	key := orgDataKey(segment.RobotID)
	field := hashKey(generateKeyString(segment))

	res, err := d.adminRdb.HGet(ctx, key, field).Result()
	if err != nil {
		if errx.Is(err, redis.Nil) {
			return 0, nil
		}
		logx.E(ctx, "getOrgData key: %s, field: %s, error: %v", key, field, err)
		return
	}
	if orgDataBizID, err = convx.StringToUint64(res); err != nil {
		logx.E(ctx, "getOrgData key: %s, field: %s, redis reply: %s, error: %v", key, field, res, err)
		return 0, err
	}
	return orgDataBizID, nil
}

func (d *SyncOrgDataTaskHandler) cleanOrgData(ctx context.Context, appID uint64) error {
	key := orgDataKey(appID)
	if err := d.adminRdb.Del(ctx, key).Err(); err != nil {
		logx.E(ctx, "cleanOrgData Del fail, key: %s, error: %v", key, err)
		return err
	}
	return nil
}

func (d *SyncOrgDataTaskHandler) getApp(ctx context.Context, appID uint64) (entity.App, error) {
	app, err := d.rpc.AppAdmin.DescribeAppByPrimaryIdWithoutNotFoundError(ctx, appID)
	if err != nil {
		logx.E(ctx, "GetRobotByID fail, appID: %d, err: %v", appID, err)
		return entity.App{}, err
	}
	if app == nil {
		logx.E(ctx, "GetRobotByID fail, app not found, appID: %d", appID)
		return entity.App{}, errs.ErrRobotNotFound
	}
	return *app, nil
}

func (d *SyncOrgDataTaskHandler) retry(
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
	logx.I(rCtx, "run %s fail, err: %v", name, err)
	for i := 0; i < d.p.RetryTimes; i++ {
		if rCtx.Err() != nil {
			return rCtx.Err()
		}
		time.Sleep(time.Duration(d.p.RetryInterval) * time.Millisecond)
		logx.I(rCtx, "retry %s, %d time(s)", name, i)
		if err = fn(rCtx); err == nil {
			logx.I(rCtx, "retry %s, %d time(s), success", name, i)
			return nil
		}
		logx.I(rCtx, "retry %s, %d time(s), err: %v", name, i, err)
	}
	return err
}

func (d *SyncOrgDataTaskHandler) Done(ctx context.Context) error {
	return nil
}

func (d *SyncOrgDataTaskHandler) Fail(ctx context.Context) error {
	return nil
}

func (d *SyncOrgDataTaskHandler) Stop(ctx context.Context) error {
	return nil
}
