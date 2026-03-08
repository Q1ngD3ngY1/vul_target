package config

import (
	"fmt"

	"git.code.oa.com/trpc-go/trpc-go/config"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/configx"
)

const (
	// whitelistConfigKey 白名单配置文件
	whitelistConfigKey = "whitelist.yaml"
)

var (
	// 公司内网IP列表
	internalIps string
)

func initWhitelistConfig() {
	// whitelist config
	configx.MustWatch(whitelistConfigKey, WhitelistConfig{})

	whitelistConfig := configx.MustGetWatched(whitelistConfigKey).(WhitelistConfig)
	log.Info("\n\n--------------------------------------------------------------------------------\n" +
		fmt.Sprintf("whitelistConfig: %+v\n", whitelistConfig) +
		"================================================================================")

	internalIps = initInternalIpConfig()
	log.Info("\n\n--------------------------------------------------------------------------------\n" +
		fmt.Sprintf("internalIps: %+v\n", internalIps) +
		"================================================================================")

}

// GetWhitelistConfig 获取 whitelist.yaml 配置文件内容
func GetWhitelistConfig() WhitelistConfig {
	whitelistConfig := configx.MustGetWatched(whitelistConfigKey).(WhitelistConfig)
	return whitelistConfig
}

const (
	AllAppBizIDInWhiteList = 0
)

// WhitelistConfig 白名单配置
type WhitelistConfig struct {
	LabelOrWhitelist              map[uint64]bool            `yaml:"label_or_whitelist"`               // 应用标签OR检索白名单
	PresignedURLUinBlacklist      map[string]bool            `yaml:"presigned_url_uin_blacklist"`      // 预览URL Uin黑名单
	InfinityAttributeLabel        map[string]map[uint64]bool `yaml:"infinity_attribute_label"`         // 无限属性标签白名单，uin->appBizID->flag
	QaURLWhiteList                map[string]map[uint64]bool `yaml:"qa_url_whitelist"`                 // 问答内容url校验白名单，uin->appBizID->flag
	UpdateEmbeddingModelWhiteList map[string]map[uint64]bool `yaml:"update_embedding_model_whitelist"` // 更新embedding模型白名单，uin->appBizID->flag
	AutoDocDiffBlackList          map[string]map[uint64]bool `yaml:"auto_doc_diff_blacklist"`          // 自动文档比对黑名单，uin->appBizID->flag
	InternalDBWhiteList           map[string]map[uint64]bool `yaml:"internal_db_white_list"`           // 内网数据库，只允许内部账号访问。永远对所有
}

// IsInWhiteList 检查是否白名单
func IsInWhiteList(uin string, appBizID uint64, whiteList map[string]map[uint64]bool) bool {
	if uin == "" {
		return false
	}
	res := false
	if appBizIDMap, ok := whiteList[uin]; ok {
		if flag, ok := appBizIDMap[AllAppBizIDInWhiteList]; ok {
			// 如果配置了该UIN下的所有appBizID都加白
			res = flag
		}
		if flag, ok := appBizIDMap[appBizID]; ok {
			// 检查该appBizID是否加白
			res = flag
		}
	}
	return res
}

func initInternalIpConfig() string {
	// 获取安全部门提供的白名单IP
	internalIPCfg, err := config.Load("ip", config.WithProvider("internal-ip"))
	if err != nil {
		log.Fatalf("get internal ip config error: %v", err)
	}
	return string(internalIPCfg.Bytes())
}

// GetInternalIps 获取内网IP的配置
func GetInternalIps() string {
	return internalIps
}
