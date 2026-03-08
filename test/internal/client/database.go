package client

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	tgorm "git.code.oa.com/trpc-go/trpc-database/gorm"
	"git.code.oa.com/trpc-go/trpc-database/localcache"
	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/common/v3/utils"
	"github.com/spf13/cast"
	"golang.org/x/exp/maps"
	"gorm.io/gorm"
)

const (
	expiration = 24 * 3600
	capacity   = 10000

	// NotVIP 表示非大客户数据库，忽略robotID
	NotVIP = 0
	// NoUinAndRobotID 表示没有uin和robotID，只能遍历多个数据库
	NoUinAndRobotID = 1
)

var (
	robotIDUinCache  localcache.Cache
	botBizIDUinCache localcache.Cache
)

func init() {
	robotIDUinCache = localcache.New(localcache.WithExpiration(expiration), localcache.WithCapacity(capacity))
	botBizIDUinCache = localcache.New(localcache.WithExpiration(expiration), localcache.WithCapacity(capacity))
}

var (
	// vipDBTables 路由到大客户数据库的表
	vipDBTables = []string{"t_doc_segment", "t_doc_segment_page_info", "t_doc_segment_image", "t_release_segment",
		"t_release_doc", "t_doc", "t_retrieval_node_info", "t_knowledge_vector", "t_image_vector", "t_attribute",
		"t_attribute_label", "t_attribute_prod", "t_attribute_label_prod", "t_doc_attribute_label",
		"t_release_attribute", "t_release_attribute_label", "t_doc_parse", "t_doc_segment_org_data"}

	// migrateTdsqlTables 已经迁移到了tdsql的表
	// 这个变量不要直接使用，需要使用下面的 GetTDSQLTables() 函数，以实现数据表迁移功能
	migrateTdsqlTables = []string{"t_chat_prompt", "t_check_task", "t_doc_diff_data", "t_doc_diff_task", "t_knowledge_vector",
		"t_msg_record", "t_msg_record_token_stat", "t_retrieval_node_info", "t_session", "t_user_dialog_config"}

	// tdsqlTables tdsql新表，新创建的表都是在tdsql中，这里是为了兼容从函数获取而不是直接调用tdsql client的场景
	// 比如通过DBClient()、GormClient()、GetAllDbClients()找不到表的情况下才需要把表加到这个变量中
	tdsqlTables = []string{"t_share_knowledge", "t_doc_segment_org_data", "t_doc_segment_org_data_temporary",
		"t_doc_segment_sheet_temporary", "t_doc_schema"}
)

type Router struct {
	RobotID    uint64
	RobotBizID uint64
	// AppKey vdb-manager里面的概念，在知识引擎中，其实就是RobotBizID转的字符串
	AppKey string
}

// GetTDSQLTables 获取哪些表是在tdsql里面
func GetTDSQLTables() []string {
	allTdsqlTables := append(migrateTdsqlTables, tdsqlTables...)
	allTdsqlTables = append(allTdsqlTables, config.App().DataMigrationConfig.MigrateTDSQLTables...)
	return allTdsqlTables
}

// GetVIPGroupName 判断和应用是否是VIP大客户的应用，返回vip group, 返回为空代表不是
func GetVIPGroupName(ctx context.Context, router Router) string {
	// 如果router里面没有任何数据，返回空
	if router.RobotID == 0 && router.RobotBizID == 0 && router.AppKey == "" {
		return ""
	}

	uin := pkg.Uin(ctx)
	if uin == "" {
		// 如果ctx中没有uin，从数据库中通过robotID找到corp_id，在t_corp中找到uin，写入ctx
		if router.RobotID == 0 && router.RobotBizID == 0 && router.AppKey != "" {
			botBizID, err := strconv.ParseUint(router.AppKey, 10, 64)
			if err != nil {
				log.ErrorContextf(ctx, "no uin in ctx and app key %v parse error, %v", router.AppKey, err)
				return ""
			}
			router.RobotBizID = botBizID
		}

		uinFromStorage, err := getUinByRobot(ctx, router.RobotID, router.RobotBizID)
		if err != nil {
			return ""
		}
		uin = uinFromStorage
		// 定时任务没有uin,如果是在遍历多个app,不能写入ctx,会导致uin混乱
		// pkg.WithUin(ctx, uin)
	}

	var vipInfo utilConfig.VIPInfo
	groupName := ""
	for name, infos := range utilConfig.GetMainConfig().VIPGroup {
		for _, info := range infos {
			if info.Uin == uin {
				groupName = name
				vipInfo = info
				break
			}
		}
		if groupName != "" {
			break
		}
	}
	if groupName == "" {
		return ""
	}
	if router.RobotID != 0 && utils.ContainsUint64(vipInfo.OldRobotIDList, router.RobotID) {
		return ""
	}
	if router.RobotBizID != 0 && utils.ContainsUint64(vipInfo.OldRobotBizIDList, router.RobotBizID) {
		return ""
	}
	if router.AppKey != "" {
		botBizID, err := strconv.ParseUint(router.AppKey, 10, 64)
		if err != nil {
			log.ErrorContextf(ctx, "app key %v parse error, %v", router.AppKey, err)
			return ""
		}
		if utils.ContainsUint64(vipInfo.OldRobotBizIDList, botBizID) {
			return ""
		}
	}

	return groupName
}

func getUinByRobot(ctx context.Context, robotID, botBizID uint64) (string, error) {
	cache := robotIDUinCache
	key := cast.ToString(robotID)
	if botBizID != 0 {
		cache = botBizIDUinCache
		key = cast.ToString(botBizID)
	}

	// 1. 如果缓存中存在，直接从缓存取
	uinValue, exist := cache.Get(key)
	if exist {
		uin, ok := uinValue.(string)
		if !ok {
			log.InfoContextf(ctx, "uin %v format error", uinValue)
			return "", fmt.Errorf("uin format error")
		}
		return uin, nil
	}

	// 2. 如果缓存中不存在，从数据库中读取后写入缓存。
	// 考虑映射关系是固定， 只有入库类接口无uin，单个robot并发有限，不加锁
	info, err := getUinByRobotFromDB(ctx, robotID, botBizID)
	if err != nil {
		return "", err
	}
	log.InfoContextf(ctx, "get uin from db, robot info: %+v", info)

	// 3. 写入缓存
	robotIDUinCache.Set(cast.ToString(info.ID), info.Uin)
	botBizIDUinCache.Set(cast.ToString(info.BusinessID), info.Uin)
	return info.Uin, nil
}

type robotInfo struct {
	ID         uint64 `gorm:"column:id"`
	BusinessID uint64 `gorm:"column:business_id"` // 对外关联的机器人ID
	CorpID     uint64 `gorm:"column:corp_id"`     // 企业 ID
	Uin        string `gorm:"column:uin"`         // 企业 uin
}

func getUinByRobotFromDB(ctx context.Context, robotID, botBizID uint64) (*robotInfo, error) {
	db, err := GormClient(ctx, "t_robot", NotVIP, NotVIP, client.WithCalleeMethod("getCorpIDByRobotID"))
	if err != nil {
		return nil, err
	}

	var info robotInfo
	var queryField string
	var queryValue uint64

	// 确定查询字段和值
	if robotID != 0 {
		queryField = "id"
		queryValue = robotID
	} else {
		queryField = "business_id"
		queryValue = botBizID
	}

	const maxRetries = 2
	retryCount := 0
	for {
		// 执行数据库查询
		err = db.WithContext(ctx).Table("t_robot").Select("id", "business_id", "corp_id").
			Where(queryField+" = ?", queryValue).First(&info).Error

		// 查询成功或不可重试错误时退出循环
		if err == nil || errors.Is(err, gorm.ErrRecordNotFound) {
			break
		}
		log.ErrorContextf(ctx, "get corp id form robot error, robotID: %v, biz id :%v, %v", robotID, botBizID, err)
		retryCount++
		// 达到最大重试次数时退出循环
		if retryCount >= maxRetries {
			return nil, err
		}
	}

	dbClient := DBClient(ctx, "t_corp", NotVIP, client.WithCalleeMethod("getCorpUin"))
	uin := ""
	sql := "select uin from t_corp where id = ? limit 1"
	err = dbClient.Get(ctx, &uin, sql, info.CorpID)
	if err != nil {
		log.ErrorContextf(ctx, "get uin failed from id %v, err: %v", info.CorpID, err)
		return nil, err
	}
	info.Uin = uin
	return &info, nil
}

// IsVipCorp 判断是否是vip客户，vip客户能创建百万级文档的超大应用，会导致接口超时报错
func IsVipCorp(uin string) bool {
	for _, vips := range utilConfig.GetMainConfig().VIPGroup {
		// 判断是否是vip用户，vip客户能创建百万级文档的超大应用，会导致接口超时报错
		for _, vip := range vips {
			if vip.Uin == uin {
				return true
			}
		}
	}
	return false
}

// IsVipApp 判断一个应用是否是vip用户的超大应用
func IsVipApp(uin string, botBizId uint64) bool {
	for _, vips := range utilConfig.GetMainConfig().VIPGroup {
		// 判断是否是vip用户，vip客户能创建百万级文档的超大应用，会导致接口超时报错
		for _, vip := range vips {
			if vip.Uin == uin {
				// 如果是vip用户，需要判断是否是超大应用，旧应用不属于超大应用
				isOldRobot := false
				for _, oldRobotBizID := range vip.OldRobotBizIDList {
					if botBizId == oldRobotBizID {
						// 旧应用
						isOldRobot = true
						break
					}
				}
				if !isOldRobot {
					// vip用户，但是非旧应用，属于超大应用
					return true
				}
			}
		}
	}
	return false
}

// DBClient  返回trpc封装的mysql.Client table为必填参数，非VIP表appID和appBizID可以填写为0 NotVIP
func DBClient(ctx context.Context, table string, appID uint64, opts ...client.Option) mysql.Client {
	name := "mysql.qbot.admin"
	// skip=0 表示GetCurrentFuncName函数
	// skip=1 表示DBClient函数
	// skip=2 表示调用DBClient的函数
	calleeName := util.GetCurrentFuncName(2)
	opts = append(opts, client.WithCalleeMethod(calleeName))
	groupName := GetVIPGroupName(ctx, Router{RobotID: appID})
	if groupName != "" && utils.Contains(vipDBTables, table) {
		// 处理 VIP 客户且表在分库表的情况
		name = groupName + "." + "tdsql.qbot.qbot"
	} else if utils.Contains(GetTDSQLTables(), table) {
		// 已经迁移到了tdsql的表，使用服务的tdsql
		name = "tdsql.qbot.qbot"
	}
	return mysql.NewClientProxy(name, opts...)
}

// GormClient 连接mysql和VIP大客户的tdsql gorm client，，非VIP表appID和appBizID可以填写为0 NotVIP
func GormClient(ctx context.Context, table string, appID, appBizID uint64, opts ...client.Option) (*gorm.DB, error) {
	name := "mysql.qbot.admin"
	// skip=0 表示GetCurrentFuncName函数
	// skip=1 表示GormClient函数
	// skip=2 表示调用GormClient的函数
	calleeName := util.GetCurrentFuncName(2)
	opts = append(opts, client.WithCalleeMethod(calleeName))
	groupName := GetVIPGroupName(ctx, Router{RobotID: appID, RobotBizID: appBizID})
	if groupName != "" && utils.Contains(vipDBTables, table) {
		// 处理 VIP 客户且表在分库表的情况
		name = groupName + "." + "tdsql.qbot.qbot"
	} else if utils.Contains(GetTDSQLTables(), table) {
		// 已经迁移到了tdsql的表，使用服务的tdsql
		name = "tdsql.qbot.qbot"
	}
	gormClient, err := tgorm.NewClientProxy(name, opts...)
	if err != nil {
		log.ErrorContextf(ctx, "GormClient|NewClientProxy|name: %v, err: %v", name, err)
		return nil, err
	}
	return gormClient.Debug(), nil
}

// GetAllDbClients 获取所有数据库客户端，包括VIP客户端
func GetAllDbClients(ctx context.Context, table string, opts ...client.Option) []mysql.Client {
	dbClients := make([]mysql.Client, 0)
	// 已经迁移到了tdsql的表，使用服务的tdsql
	if utils.Contains(GetTDSQLTables(), table) {
		dbClients = append(dbClients, mysql.NewClientProxy("tdsql.qbot.qbot", opts...))
	} else {
		dbClients = append(dbClients, mysql.NewClientProxy("mysql.qbot.admin", opts...))
	}
	if utils.Contains(vipDBTables, table) {
		groupNames := maps.Keys(utilConfig.GetMainConfig().VIPGroup)
		for _, groupName := range groupNames {
			dbClients = append(dbClients, mysql.NewClientProxy(groupName+".tdsql.qbot.qbot", opts...))
		}
	}
	return dbClients
}
