package release

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"git.woa.com/adp/common/x/encodingx/jsonx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/dao/label"
	qaDao "git.woa.com/adp/kb/kb-config/internal/dao/qa"
	releaseDao "git.woa.com/adp/kb/kb-config/internal/dao/release"
	segDao "git.woa.com/adp/kb/kb-config/internal/dao/segment"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	dbLogic "git.woa.com/adp/kb/kb-config/internal/logic/database"
	docLogic "git.woa.com/adp/kb/kb-config/internal/logic/document"
	"git.woa.com/adp/kb/kb-config/internal/logic/kb"
	labelLogic "git.woa.com/adp/kb/kb-config/internal/logic/label"
	qaLogic "git.woa.com/adp/kb/kb-config/internal/logic/qa"
	userLogic "git.woa.com/adp/kb/kb-config/internal/logic/user"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

var knowledgeQANeedAuditFiled = map[string]int{entity.ConfigItemName: 1, entity.ConfigItemRoleDescription: 2,
	entity.ConfigItemGreeting: 3, entity.ConfigItemBareAnswer: 4, entity.ConfigItemDescription: 5,
	entity.ConfigItemAvatar: 6}

func NewLogic(
	rpc *rpc.RPC,
	dbLogic *dbLogic.Logic,
	docLogic *docLogic.Logic,
	kbLogic *kb.Logic,
	labelLogic *labelLogic.Logic,
	qaLogic *qaLogic.Logic,
	rawSqlDao dao.Dao,
	qaDao qaDao.Dao,
	segDao segDao.Dao,
	labelDao label.Dao,
	releaseDao releaseDao.Dao,
	userLogic *userLogic.Logic,
) *Logic {
	return &Logic{
		rpc:        rpc,
		rawSqlDao:  rawSqlDao,
		dbLogic:    dbLogic,
		docLogic:   docLogic,
		kbLogic:    kbLogic,
		labelLogic: labelLogic,
		qaLogic:    qaLogic,
		qaDao:      qaDao,
		segDao:     segDao,
		labelDao:   labelDao,
		releaseDao: releaseDao,
		userLogic:  userLogic,
	}
}

type Logic struct {
	rpc        *rpc.RPC
	dbLogic    *dbLogic.Logic
	docLogic   *docLogic.Logic
	kbLogic    *kb.Logic
	qaLogic    *qaLogic.Logic
	labelLogic *labelLogic.Logic
	rawSqlDao  dao.Dao
	labelDao   label.Dao
	qaDao      qaDao.Dao
	segDao     segDao.Dao
	releaseDao releaseDao.Dao
	userLogic  *userLogic.Logic
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
	attrLabelJSON, _ := jsonx.MarshalToString(value)
	return attrLabelJSON
}

// parseSimilarQAAttrLabels2Json 转换相似QA的标签
// 相似问答的标签在主问答的基础上额外加上相似问答的特征标签
func parseSimilarQAAttrLabels2Json(mapAttrLabels *sync.Map, key any) string {
	if mapAttrLabels == nil {
		return ""
	}
	value, ok := mapAttrLabels.Load(key)
	if !ok {
		return ""
	}
	var attrLabelJSON string
	labels, ok := value.([]*releaseEntity.ReleaseAttrLabel)
	if ok {
		labels = append(labels, &releaseEntity.ReleaseAttrLabel{
			Name: releaseEntity.SysLabelQAFlagName, Value: releaseEntity.SysLabelQAFlagValueSimilar})
		labels = append(labels, &releaseEntity.ReleaseAttrLabel{
			Name: releaseEntity.SysLabelQAIdName, Value: fmt.Sprintf("%d", key)})
		attrLabelJSON, _ = jsonx.MarshalToString(labels)
	} else {
		attrLabelJSON, _ = jsonx.MarshalToString(value)
	}
	return attrLabelJSON
}

// loadAttrLabels TODO
func loadAttrLabels(mapAttrLabels *sync.Map, key any) []*releaseEntity.ReleaseAttrLabel {
	if mapAttrLabels == nil {
		return nil
	}
	values, ok := mapAttrLabels.Load(key)
	if !ok {
		return nil
	}
	attrLabels, ok := values.([]*releaseEntity.ReleaseAttrLabel)
	if !ok {
		return nil
	}
	return attrLabels
}

func getNeedAuditDiffConfig(needAuditFiled map[string]int, diff []entity.AppConfigDiff) ([]entity.AppConfigDiff,
	[]entity.AppConfigDiff) {
	var needAuditDiff []entity.AppConfigDiff
	var noAuditDiff []entity.AppConfigDiff
	for _, v := range diff {
		_, ok := needAuditFiled[v.ConfigItem]
		if ok {
			needAuditDiff = append(needAuditDiff, v)
		} else {
			noAuditDiff = append(noAuditDiff, v)
		}
	}
	return needAuditDiff, noAuditDiff
}

func getReleaseConfig(diff []entity.AppConfigDiff, needAudit bool,
	release *releaseEntity.Release) []*releaseEntity.ReleaseConfig {
	var cfg []*releaseEntity.ReleaseConfig
	for _, v := range diff {
		auditStatus := entity.ConfigReleaseStatusAuditing
		auditResult := ""
		if !config.AuditSwitch() || !needAudit || len(v.NewValue) == 0 {
			auditStatus = releaseEntity.ReleaseQAAuditStatusSuccess
			auditResult = "无需审核"
		}
		cfg = append(cfg, &releaseEntity.ReleaseConfig{
			CorpID:        release.CorpID,
			StaffID:       release.StaffID,
			RobotID:       release.RobotID,
			VersionID:     release.ID,
			ConfigItem:    v.ConfigItem,
			OldValue:      v.LastValue,
			Value:         v.NewValue,
			Content:       v.Content,
			Action:        v.Action,
			ReleaseStatus: releaseEntity.ConfigReleaseStatusIng,
			Message:       "",
			AuditStatus:   auditStatus,
			AuditResult:   auditResult,
			CreateTime:    time.Now(),
			UpdateTime:    time.Now(),
			ExpireTime:    time.Unix(0, 0),
		})
	}
	return cfg
}

func batchReleaseProcess[T any](ctx context.Context, items []T, processor func(ctx context.Context, items []T) error) error {
	if len(items) == 0 {
		return nil
	}
	batch := config.App().ReleaseParamConfig.BatchProcessConcurrency
	total := len(items)
	batchNums := int(math.Ceil(float64(total) / float64(batch)))
	batch = int(math.Ceil(float64(total) / float64(batchNums)))
	logx.D(ctx, "batchReleaseProcess batch:%d, batchNums:%d, total:%d", batch, batchNums, total)
	g := errgroupx.New()
	g.SetLimit(batch)
	for i := 0; i < batch; i++ {
		startIndex, endIndex := i*batchNums, (i+1)*batchNums
		if endIndex >= total {
			endIndex = total
		}
		if startIndex >= endIndex {
			continue
		}
		// 这里使用协程，没有使用trpc.CloneContext是因为g.Wait()主协程有等待结果，这里的子协程是需要和主协程保持一致的生存周期
		g.Go(func() error {
			return processor(ctx, items[startIndex:endIndex])
		})
	}
	if err := g.Wait(); err != nil {
		logx.W(ctx, "batchReleaseProcess err :%v", err)
		return err
	}
	return nil
}

// timeTrack 是一个辅助函数，用于记录函数的执行时间
func timeTrack(ctx context.Context, start time.Time, funcName string) {
	elapsed := time.Since(start)
	logx.D(ctx, "%s cost time: %d ms", funcName, elapsed.Milliseconds())
}
