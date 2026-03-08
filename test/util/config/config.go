// KEP.bot-knowledge-config-server
//
// @(#)config.go  March 27, 2024
// Copyright(c) 2024, halelv@Tencent. All rights reserved.

// Package config TODO
package config

import (
	"fmt"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/configx"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
)

const (
	// mainConfigKey 七彩石配置文件名称
	mainConfigKey                  = "main.yaml"
	defaultAutoDocRefreshBatchSize = 50
)

// MainConfig 定义 main.yaml 的结构
type MainConfig struct {
	FetchURLUseWebParser                bool                             `yaml:"fetch_url_use_web_parser"`                // FetchURLContent接口是否使用底座WebParserServer服务
	RobotDefault                        config.RobotDefault              `yaml:"robot_default"`                           // 机器人默认配置
	RealtimeConfig                      RealtimeConfig                   `yaml:"realtime_config"`                         // 实时文档相关配置
	OfflineConfig                       OfflineConfig                    `json:"offline_config"`                          // 离线文档相关配置
	FileParseConfig                     FileParseConfig                  `yaml:"file_parse_config"`                       // 文档解析相关配置
	LinkerMergeConfig                   LinkerMergeConfig                `yaml:"linker_merge_config"`                     // 合并策略 2.4.0新增
	CleanVectorSyncHistoryConfig        CleanVectorSyncHistoryCfg        `yaml:"clean_vector_sync_history_config"`        // 定时清理t_vector_sync_history表
	BatchInterfaceLimit                 BatchInterfaceLimit              `yaml:"batch_interface_limit"`                   // 批量接口数量限制
	KnowledgeDeleteConfig               KnowledgeDeleteConfig            `yaml:"knowledge_delete_config"`                 // 知识删除配置
	CheckSystemIntegratorCharacterUsage bool                             `yaml:"check_system_integrator_character_usage"` // 是否检查集成商字符使用情况
	HandleTasks                         []string                         `yaml:"handle_tasks"`                            // 需要注册的任务处理函数列表
	EnableWorkerMode                    bool                             `yaml:"enable_worker_mode"`                      // 是否开启worker模式
	DefaultDocUnstableTimeoutMinutes    uint32                           `yaml:"default_doc_unstable_timeout_minutes"`    // 默认文档不稳定超时时间，单位：分钟
	DocUnstableTimeoutMinutes           map[uint32]uint32                `yaml:"doc_unstable_timeout_minutes"`            // 文档不稳定超时时间，单位：分钟
	SearchKnowledgeAppIdReplaceMap      map[uint64]uint64                `yaml:"search_knowledge_app_id_replace_map"`     // 搜索知识库的应用ID替换映射
	ThirdPermissionCheck                map[uint64]ThirdPermissionConfig `yaml:"third_permission_check"`                  // 第三方权限校验配置
	VIPGroup                            map[string][]VIPInfo             `yaml:"vip_group"`                               // VIPGroup key: vip group名称，如vip-group1， value: 用户uin和旧的appID列表
	Permissions                         Permissions                      `yaml:"permissions"`                             // 权限有关配置
	KnowledgeSchema                     KnowledgeSchemaConfig            `yaml:"knowledge_schema"`                        // 知识库schema配置
	Embedding                           EmbeddingClientConfig            `yaml:"embedding"`                               // 向量服务配置
	Audit                               AuditConfig                      `yaml:"audit"`                                   // 审核相关配置
	SegmentIntervene                    SegmentInterveneConfig           `yaml:"segment_intervene"`                       // 切片干预相关配置
	DefaultDatabaseCleanConfig          DatabaseCleanConfig              `yaml:"default_database_clean_config"`           // 默认数据库清理配置
	AutoDocRefreshConfig                AutoDocRefreshConfig             `yaml:"auto_doc_refresh_config"`                 // 自动文档刷新配置
	KnowledgeBaseConfig                 map[uint64]KnowledgeBaseConfig   `yaml:"knowledge_base_config"`                   // 知识库配置，corpBizId -> 知识库配置
	RetrievalConfig                     RetrievalConfig                  `yaml:"retrieval_config"`                        // 检索相关配置
}

// AutoDocRefreshConfig 自动文档刷新配置
type AutoDocRefreshConfig struct {
	Enable    bool   `yaml:"enable"`     // 是否开启定时任务
	EnvName   string `yaml:"env_name"`   // 审核需要的环境名称
	BatchSize int    `yaml:"batch_size"` // 任务每批次的文档数量
}

// RealtimeConfig 实时文档配置
type RealtimeConfig struct {
	InsertDBBatchSize          int               `yaml:"insert_db_batch_size"`           // 实时文档批量写DB每批数量
	SyncVectorAddBatchSize     int               `yaml:"sync_vector_add_batch_size"`     // 添加实时文档同步vector每批数量
	SyncVectorDeletedBatchSize int               `yaml:"sync_vector_deleted_batch_size"` // 删除实时文档同步vector每批数量
	SyncVectorMaxRetry         int               `yaml:"sync_vector_max_retry"`          // 实时文档同步vector最大重试次数
	FullTextMaxSize            int               `yaml:"full_text_max_size"`             // 文档全文提取的最大字数
	ParseTimeout               int               `yaml:"parse_timeout"`                  // 实时文档解析最大时间
	MaxFileSize                int               `yaml:"max_file_size"`                  // 实时文档文件大小，最大20M
	NeedDeleteRealtimeDoc      bool              `yaml:"need_delete_realtime_doc"`       // 是否开启删除实时文档
	RealTimeDocStageTime       int               `yaml:"real_time_doc_stage_time"`       // 实时文档暂存时间【180天】
	RealtimeTickerSecond       int               `yaml:"realtime_ticker_second"`         // 实时文档解析进度回包间隔（单位：秒）
	TaskStatusErrMsgMap        map[int32]string  `yaml:"task_status_err_msg_map"`        // 解析错误码对应的错误信息（前端展示使用）
	NeedSummaryAppID           map[uint64]bool   `yaml:"need_summary_app_id"`            // 走摘要的应用ID
	LkeDeepSeekR1Name          string            `yaml:"lke_deep_seek_r1_name"`          // lke_deep_seek_r1_name 模型名
	FileTypeSize               map[string]uint64 `yaml:"file_type_size"`                 // 文件类型及大小限制
}

// OfflineConfig 离线文档配置
type OfflineConfig struct {
	SyncVectorAddBatchSize     int `yaml:"sync_vector_add_batch_size"`     // 添加离线文档同步vector每批数量
	SyncVectorDeletedBatchSize int `yaml:"sync_vector_deleted_batch_size"` // 删除离线文档同步vector每批数量
}

// FileParseConfig 文档解析配置
type FileParseConfig struct {
	OfflineMaxWorker  int `yaml:"offline_max_worker"`  // 离线文档并行处理最大数量
	RealtimeMaxWorker int `yaml:"realtime_max_worker"` // 实时文档并行处理最大数量

	OfflineFileManagerVersion  int `yaml:"offline_file_manager_version"`  // 离线文档解析服务版本号
	RealtimeFileManagerVersion int `yaml:"realtime_file_manager_version"` // 实时文档解析服务版本号

	RealtimeParseSetting ParseSetting `yaml:"realtime_parse_setting"` // 实时文档解析配置
}

// ParseSetting 底座解析配置
type ParseSetting struct {
	ParseMode     int32        `yaml:"parse_mode"`      // 解析策略
	IsOpenSubimg  bool         `yaml:"is_open_subimg"`  // ocr是否打开子图识别
	IsOpenFormula bool         `yaml:"is_open_formula"` // ocr是否打开公式识别
	ParserConfig  ParserConfig `yaml:"parser_config"`   // 解析器配置
}

// ParserConfig 解析器配置
type ParserConfig struct {
	SingleParagraph bool `yaml:"single_paragraph"`
	SplitSubTable   bool `yaml:"split_sub_table"`
}

// LinkerMergeConfig 合并策略
type LinkerMergeConfig struct {
	IsOpenLengthLimit bool `yaml:"is_open_length_limit"` // 是否开启长度限制，两个切片都低于阈值才进行合并，否则不合并
	MergeLengthLimit  int  `yaml:"merge_length_limit"`   // 长度阈值，长度限制开启后，两个切片长度都小于阈值才会合并
}

// CleanVectorSyncHistoryCfg 定时清理t_vector_sync_history表
type CleanVectorSyncHistoryCfg struct {
	Enable         bool     `yaml:"enable"`          // 总开关
	SyncAllBot     bool     `yaml:"sync_all_bot"`    // 是否刷全量机器人
	WhiteBotList   []string `yaml:"white_bot_list"`  // upgrade_all_bot 是false的情况下，只刷白名单
	Limit          int64    `yaml:"limit"`           // 一次删除多少数据
	DeleteDuration int      `yaml:"delete_duration"` // 删除一次后sleep时间， 避免锁死数据库
}

// BatchInterfaceLimit 批量接口数量限制
type BatchInterfaceLimit struct {
	GeneralMaxLimit       int `yaml:"general_max_limit"`         // 通用最大数量限制
	DeleteDocMaxLimit     int `yaml:"delete_doc_max_limit"`      // 删除文档最大数量限制
	RetryDocParseMaxLimit int `yaml:"retry_doc_parse_max_limit"` // 文档解析重试最大数量限制
}

// KnowledgeDeleteConfig 知识删除配置
type KnowledgeDeleteConfig struct {
	// NeedDeleteTables DB需要删除的表名列表
	// map:[string]bool, key:表名 value:表删除handler name
	// 	t_doc: COMMON
	// 	t_doc_segment: CORP_ROBOT_ID
	// 	...
	NeedDeleteTables map[string]string `yaml:"need_delete_tables"` // DB需要删除的表名和handler
	DeleteBatchSize  int               `yaml:"delete_batch_size"`  // 批次删除最大数量
	QueryBatchSize   int               `yaml:"query_batch_size"`   // 批次查询最大数量
}

// ThirdPermissionConfig 第三方权限配置
type ThirdPermissionConfig struct {
	Enable                   bool              `yaml:"enable"`
	Url                      string            `yaml:"url"`
	Header                   map[string]string `yaml:"header"`
	Timeout                  int               `yaml:"timeout"`                        // 超时时间，单位毫秒，默认1000毫秒
	Retry                    int               `yaml:"retry"`                          // 重试次数，默认2次
	WorkFlowLkeUserIdAttrKey string            `yaml:"work_flow_lke_user_id_attr_key"` // 工作流用户id属性key
}

// VIPInfo 大客户信息
type VIPInfo struct {
	// Uin 用户UIN
	Uin string `yaml:"uin"`
	// VIP客户老的app列表，需要同时填写 robotID, robotBizId
	OldRobotIDList    []uint64 `yaml:"old_robot_id_list"`
	OldRobotBizIDList []uint64 `yaml:"old_robot_biz_id_list"`
}

// Permissions 权限有关配置
type Permissions struct {
	UserMaxLimit                 int    `yaml:"user_max_limit"`                  //用户最大数量
	RoleMaxLimit                 int    `yaml:"role_max_limit"`                  //角色最大数量
	CateRetrievalKey             string `yaml:"cate_retrieval_key"`              //分类向量key
	RoleRetrievalKey             string `yaml:"role_retrieval_key"`              //角色向量key
	UserNameMinLimit             int    `yaml:"user_name_min_limit"`             //名称最小长度
	UserNameMaxLimit             int    `yaml:"user_name_max_limit"`             //名称最大长度
	ThirdUserIdMaxLimit          int    `yaml:"third_user_id_max_limit"`         //third_user_id最大长度
	ThirdUserIdReg               string `yaml:"third_user_id_reg"`               //third_user_id正则校验
	RoleNameMinLimit             int    `yaml:"role_name_min_limit"`             //名称最小长度
	RoleNameMaxLimit             int    `yaml:"role_name_max_limit"`             //名称最大长度
	UpdateDocVectorSize          int    `yaml:"update_doc_vector_size"`          //更新文档向量每批id数量
	UpdateDocVectorLimit         int    `yaml:"update_doc_vector_limit"`         //更新文档向量协程最大数量
	UpdateQaVectorSize           int    `yaml:"update_qa_vector_size"`           //更新问答向量每批id数量
	UpdateQaVectorLimit          int    `yaml:"update_qa_vector_limit"`          //更新问答向量协程最大数量
	UpdateDbVectorSize           int    `yaml:"update_db_vector_size"`           //更新数据表标签每批id数量
	UpdateDbVectorLimit          int    `yaml:"update_db_vector_limit"`          //更新数据表标签协程最大数量
	UpdateVectorSwitch           bool   `yaml:"update_vector_switch"`            //更新标签任务开关
	UpdateVectorSleepSwitch      bool   `yaml:"update_vector_sleep_switch"`      //更新标签sleep开关
	UpdateVectorSleepMillisecond int    `yaml:"update_vector_sleep_millisecond"` //更新标签sleep毫秒数
	GetSegmentLimit              int    `yaml:"get_segment_limit"`               //文档切片批量获取最大条数
	ChunkNumber                  int    `yaml:"chunk_number"`                    // 块大小
}

// KnowledgeSchemaConfig 知识库schema配置
type KnowledgeSchemaConfig struct {
	MaxProcessDocCount    int               `yaml:"max_process_file_count"`   // 最大处理文档数量
	DocProcessBatchSize   int               `yaml:"doc_process_batch_size"`   // 文档处理批次大小
	DocSummaryInputLimit  int               `yaml:"doc_summary_input_limit"`  // 文档摘要大小限制
	CacheLockExpireTTL    int               `yaml:"cache_lock_expire_ttl"`    // 分布式锁过期时间，也是加锁失败的等待时间
	DocClusterThreshold   int               `yaml:"doc_cluster_threshold"`    // 文档聚类阈值，如果文档总数超过这个阈值需要进行聚类
	DocSummaryPrompt      string            `yaml:"doc_summary_prompt"`       // 文档摘要prompt
	DirSummaryPrompt      string            `yaml:"dir_summary_prompt"`       // 文件夹摘要prompt
	EmbeddingInstruction  string            `yaml:"embedding_instruction"`    // 文档向量模型指令
	TaskStatusCodeMessage map[uint32]string `yaml:"task_status_code_message"` // 任务状态码到消息的映射
}

type EmbeddingClientConfig struct {
	ModelName string `yaml:"model_name"`
	MaxLen    uint32 `yaml:"max_len"`
	Dimension uint32 `yaml:"dimension"`
	MaxRetry  int    `yaml:"max_retry"`
	// 重试线性等待时间
	RetryWaitMs int `yaml:"retry_wait_ms"`
	// query embedding添加前缀（instruction）配置
	Prefix string `yaml:"prefix"`
	// 问答、文档切片等内容embedding添加前缀（instruction）配置
	ContentPrefix string `yaml:"content_prefix"`
	// llm embedding使用，问答对、相似问答对、拒答、全局知识库的入库和所有类型的用户query
	QInstruction string `yaml:"q_instruction"`
	// llm embedding使用，知识库的文档切片、实时文档的解析切片
	DInstruction string `yaml:"d_instruction"`
}

type AuditConfig struct {
	AuditCallbackCheckCosPathPrefix []string `yaml:"audit_callback_check_cos_path_prefix"`
}

type SegmentInterveneConfig struct {
	SyncOrgDataSwitch bool `yaml:"sync_orgdata_switch"` // 历史orgdata刷新开关
}

type KnowledgeBaseConfig struct {
	// appBizId -> type -> config，appBizId为0表示对该uin下的所有应用生效
	Items map[uint64]map[uint32]string `yaml:"items"`
}

type DatabaseCleanConfig struct {
	Enable                 bool   `yaml:"enable"`                    // 是否开启定时任务
	DocBatchSize           uint32 `yaml:"doc_batch_size"`            // 每次任务删除的文档数量
	QaBatchSize            uint32 `yaml:"qa_batch_size"`             // 每次任务次删除的问答数量
	DeleteBatchSize        uint32 `yaml:"delete_batch_size"`         // 每次删除操作的数据行数
	DeleteDelayTimeMinutes uint32 `yaml:"delete_delay_time_minutes"` // 删除数据的延迟时间，比如删除24*60分钟之前软删除的数据
}

type RetrievalConfig struct {
	DefaultModelName  string `yaml:"default_model_name"`   // 默认模型名称
	DefaultTopN       uint32 `yaml:"default_top_n"`        // [废弃]默认TopN
	DefaultRecallNum  uint32 `yaml:"default_recall_num"`   // 默认最终召回数量
	DefaultRerankTopN uint32 `yaml:"default_rerank_top_n"` // 默认参与rerank的TopN
}

// GetAutoDocRefreshBatchSize 获取自动刷新文档批次数量
func GetAutoDocRefreshBatchSize() int {
	if GetMainConfig().AutoDocRefreshConfig.BatchSize == 0 {
		return defaultAutoDocRefreshBatchSize
	} else {
		return GetMainConfig().AutoDocRefreshConfig.BatchSize
	}
}

// Init 初始化配置
func Init() {
	initMainConfig()
	initWhitelistConfig()
	initDbSourceConfig()
}

func initMainConfig() {
	// main config
	configx.MustWatch(mainConfigKey, MainConfig{})

	mainConfig := configx.MustGetWatched(mainConfigKey).(MainConfig)
	log.Info("\n\n--------------------------------------------------------------------------------\n" +
		fmt.Sprintf("mainConfig: %+v\n", mainConfig) +
		"================================================================================")
}

// GetMainConfig 获取 main.yaml 配置文件内容
func GetMainConfig() MainConfig {
	mainConfig := configx.MustGetWatched(mainConfigKey).(MainConfig)
	return mainConfig
}

// GetBotKnowledgeBaseConfig 获取Yaml中的知识库配置
func GetBotKnowledgeBaseConfig(corpBizId, appBizId uint64, configType uint32) string {
	if knowledgeBaseConfig, ok := GetMainConfig().KnowledgeBaseConfig[corpBizId]; ok {
		if configItem, ok := knowledgeBaseConfig.Items[AllAppBizIDInWhiteList]; ok {
			// 如果配置了对该corpBizId下的所有应用生效
			if item, ok := configItem[configType]; ok {
				return item
			}
		}
		if configItem, ok := knowledgeBaseConfig.Items[appBizId]; ok {
			// 如果配置了该appBizId生效
			if item, ok := configItem[configType]; ok {
				return item
			}
		}
	}
	return ""
}
