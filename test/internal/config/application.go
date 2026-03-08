// Package config 写配置定义和热加载等配置相关逻辑
package config

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/exp/slices"

	"git.woa.com/adp/common/x/clientx/s3x"
	"git.woa.com/adp/common/x/configx"
	"git.woa.com/adp/common/x/logx"
	secapi "git.woa.com/sec-api/go/scurl"
)

const (
	applicationKey = "application.yaml"
)

// Application 业务配置
type Application struct {
	Storage                        Storage                     `yaml:"storage"`
	RobotDefault                   RobotDefault                `yaml:"robot_default"`
	LoginDefault                   LoginDefault                `yaml:"login_default"`
	DocPreview                     map[string]DocPreviewURL    `yaml:"doc_preview"`
	DocPreviewType                 uint8                       `yaml:"doc_preview_type"` // 预览方式：0 预览链接预览 1 提供文件下载链接
	DocQA                          DocQA                       `yaml:"doc_qa"`
	CronTask                       CronTask                    `yaml:"cron_task"`
	UserAvatar                     []string                    `yaml:"user_avatar"`
	DemoMode                       DemoMode                    `yaml:"demo_mode"`
	Tasks                          map[string]Task             `yaml:"tasks"`
	TaskPrefix                     string                      `yaml:"task_prefix"`
	TaskFetchNum                   uint32                      `yaml:"task_fetch_num"`
	TaskFetchTimeout               time.Duration               `yaml:"task_fetch_timeout"`
	TaskFetchPeriod                time.Duration               `yaml:"task_fetch_period"`
	ShortURLRootURL                string                      `yaml:"short_url_root_url"`
	PreviewURLRootURL              string                      `yaml:"preview_url_root_url"`
	OfflineShortURLScheme          string                      `yaml:"offline_short_url_scheme"`  // 短链接协议
	RealtimeShortURLScheme         string                      `yaml:"realtime_short_url_scheme"` // 短链接协议
	VectorSync                     VectorSync                  `yaml:"vector_sync"`
	UnsatisfiedReplyUnselectReason string                      `yaml:"unsatisfied_reply_unselect_reason"` // 不满意回复未选择错误类型原因
	MaxExportCount                 uint32                      `yaml:"max_export_count"`                  // 通用导出任务默认导出最大值不超过10w条数据
	MaxExportBatchSize             uint32                      `yaml:"max_export_batch_size"`             // 通用导出任务默认导出单批次
	AuditSwitch                    bool                        `yaml:"audit_switch"`                      // 审核总开关，先检查总开关，再检查子开关，公有云要打开
	FileAuditSwitch                bool                        `yaml:"file_audit_switch"`                 // 文件审核子开关，先检查总开关，再检查子开关，公有云要打开
	VideoAuditSwitch               bool                        `yaml:"video_audit_switch"`                // 视频审核子开关，先检查总开关，再检查子开关，公有云要打开
	HighLightThreshold             int                         `yaml:"high_light_threshold"`              // 高亮阈值
	WhiteListUin                   []string                    `yaml:"white_list_uin"`
	DocPlaceholder                 DocPlaceholder              `yaml:"doc_placeholder"` // 文档占位符
	SSEConnOptions                 SSEConnOptions              `yaml:"sse_conn_options"`
	DocParseError                  map[string]ParseError       `yaml:"doc_parse_error"`                    // 文档解析错误
	DocParseErrorDefault           ParseError                  `yaml:"doc_parse_error_default"`            // 文档解析错误默认值
	DocParseStop                   ParseError                  `yaml:"doc_parse_stop"`                     // 手动终止文档解析错误信息
	AttributeLabel                 AttributeLabel              `yaml:"attribute_label"`                    // 属性标签配置
	CloudAPIs                      CloudAPIs                   `yaml:"cloud_apis"`                         // 云API
	AIConf                         AIConf                      `yaml:"ai_conf"`                            // 底座权限
	PermissionAppCategory          []AppCategory               `yaml:"permission_app_category"`            // 底座权限对应的应用类型
	Trial                          Trial                       `yaml:"trial"`                              // 试用配置
	OnlyCheckDataPermAction        []string                    `yaml:"only_check_data_perm_action"`        // 只检查数据权限的动作
	CorpMaxCharSize                uint64                      `yaml:"corp_max_char_size"`                 // 企业默认最大字符数
	CorpMaxTokenUsage              uint64                      `yaml:"corp_max_token_usage"`               // 企业默认最大token使用量
	ManagedCorpIds                 []uint64                    `yaml:"managed_account_ids"`                // 管理账户的id(产品提供uin，去t_corp查询id)
	Finance                        Finance                     `yaml:"finance"`                            // 计费配置
	Text2sqlPageContentMaxLength   int                         `yaml:"text_2_sql_page_content_max_length"` // 在 t_doc_segment 中 text2sql类型数据的 page_content 的最大长度
	PageContentMaxLength           int                         `yaml:"page_content_max_length"`            // 在 t_doc_segment 中 其他类型数据的page_content 的最大长度
	OrgDataMaxLength               int                         `yaml:"org_data_max_length"`                // 在 t_doc_segment 中 orgdata 的最大长度
	SplicingInterveneDocPath       string                      `yaml:"splicing_intervene_doc_path"`        // 干预中拼接文档中间文件存储路径
	DebugConfig                    DebugConfig                 `yaml:"debug_config"`
	SecAPI                         SecAPI                      `yaml:"sec_api"`        // http安全过滤配置
	DeepSeekConf                   DeepSeekConf                `yaml:"deep_seek_conf"` // deepSeek配置
	ElasticSearchConfig            ElasticSearchConfig         `yaml:"elastic_search_config"`
	ESIndexNameConfig              map[string]string           `yaml:"es_index_name_config"` // es索引名称配置
	EsSearch                       EsSearch                    `yaml:"es_search"`            // es搜索策略配置
	DbSource                       DbSource                    `yaml:"db_source"`
	ShareKnowledgeConfig           ShareKnowledgeConfig        `yaml:"share_knowledge_config"`         // 共享知识库配置
	DataMigrationConfig            DataMigrationConfig         `yaml:"data_migration_config"`          // 数据迁移配置
	COSDocumentConfig              COSDocumentConfig           `yaml:"cos_document_config"`            // COS文档相关配置
	Database                       Database                    `yaml:"database"`                       // 数据库
	ReleaseParamConfig             ReleaseParamConfig          `yaml:"release_param_config"`           // 发布任务的参数配置
	GormDebug                      bool                        `yaml:"gorm_debug"`                     // gorm 是否开启 debug 模式
	ExceedKnowledgeResumeConfig    ExceedKnowledgeResumeConfig `yaml:"exceed_knowledge_resume_config"` // 超量知识库恢复配置
	ThirdDocConfig                 ThirdDocConfig              `yaml:"third_doc_config"`               // 第三方文档配置, 例如onedrive，sharepoint
	EmbeddingConfig                EmbeddingConfig             `yaml:"embedding_config"`               // 向量版本和模型配置
	KbSchemaExportEnv              string                      `yaml:"kb_schema_export_env"`           //product读取t_knowledge_schema_prod 否则读t_knowledge_schema
	KbPackageConfig                KbPackageConfig             `yaml:"kb_package_config"`              // 知识包配置
}

type KbPackageConfig struct {
	PackageSizeLimitMB   uint64            `yaml:"package_size_limit_mb"`  // 知识包大小限制，单位MB（默认值）
	ImportQAConcurrency  int               `yaml:"import_qa_concurrency"`  // QA导入并发数，默认为20
	ExportDocConcurrency int               `yaml:"export_doc_concurrency"` // 文档导出并发数，默认为10
	ImportDocConcurrency int               `yaml:"import_doc_concurrency"` // 文档导入并发数，默认为10
	ZipBufferSize        int               `yaml:"zip_buffer_size"`        // zip缓冲区大小，默认为8MB
	AppPackageSizeLimit  map[uint64]uint64 `yaml:"app_package_size_limit"` // 按应用配置的知识包大小限制，key为appBizId，value为限制大小（单位MB）
	SecretKey            string            `yaml:"secret_key"`             // 用于文件哈希计算的秘钥[跨平台跨环境唯一]
}

type EmbeddingConfig struct {
	EmbeddingModelMapping   map[string]string `yaml:"embedding_model_mapping"`
	DefaultEmbeddingVersion uint64            `yaml:"default_version"`
	CustomEmbeddingVersion  uint64            `yaml:"custom_version"`
	EmbeddingModelVersion   map[string]uint64 `yaml:"embedding_model_version"`
}

func (e EmbeddingConfig) GetMappingModelName(model string) string {
	if mapped, ok := e.EmbeddingModelMapping[model]; ok {
		return mapped
	}
	return model
}

// ReleaseParamConfig 发布任务的参数配置
type ReleaseParamConfig struct {
	GetReleasingRecordSize   uint32   `yaml:"get_releasing_record_size"`  // 获取发布中的记录的数量，默认500
	GetIDsChunkSize          uint32   `yaml:"get_ids_chunk_size"`         // 获取id的批量大小 默认500 即一次获取500个id，然后再批量查询
	BatchAddNodesSize        int      `yaml:"batch_add_nodes_size"`       //  发布时批量同步节点数据数量，默认100
	BatchDeleteNodeSize      int      `yaml:"batch_delete_nodes_size"`    // 发布的时候获取删除数据的分批值
	NotifyBatchSize          int      `yaml:"notify_batch_size"`          // 向量发布回调写库的并发数量, 默认是10
	RetryTimes               uint     `yaml:"retry_times"`                // 发布向量/ES等失败的重试次数 默认1
	BlackRobotIDs            []uint64 `yaml:"black_robot_ids"`            // 人工干预，异常应用，阻止发布执行
	BatchProcessConcurrency  int      `yaml:"batch_process_concurrency"`  // 发布时批量处理数据的批次大小，默认10
	CreateReleaseConcurrency int      `yaml:"create_release_concurrency"` // 发布时批量写入发布记录的并发数量，默认10
	CreateReleaseBatchSize   int      `yaml:"create_release_batch_size"`  // 发布时批量写入发布记录的批次大小，默认100
	ReleaseRecordBatchSize   int      `yaml:"release_record_batch_size"`  // 发布时批量写入发布记录的批次大小，默认100

}

// 第三方文档导入配置
type ThirdDocConfig struct {
	OneDrive OneDrive `yaml:"onedrive_config_params"` // onedrive 配置参数
}

type OneDrive struct {
	PagingSize        int `yaml:"paging_size"`          // 分页大小, 默认200
	FilterUploadCount int `yaml:"filter_upload_count" ` // 过滤上传文件数量, 最大200
}

type Database struct {
	ForbiddenDatabaseNames ForbiddenDatabaseNames `yaml:"forbidden_database_names"` // databaseType => []name
}

type ForbiddenDatabaseNames struct {
	MySQL     []string `yaml:"mysql"`
	SQLServer []string `yaml:"sql_server"`
}

type ExceedKnowledgeResumeConfig struct {
	ResumeConcurrentNum int             `yaml:"resume_concurrent_num"` // 并发恢复的数量
	ResumeBatchSize     int             `yaml:"resume_batch_size"`     // 恢复的批次大小
	PermissionIDs       map[string]bool `yaml:"permission_ids"`        // 对应权限位
}

// DataMigrationConfig 数据迁移配置
type DataMigrationConfig struct {
	// 新迁移到TDSQL的数据表，配置后立刻生效，走tdsql的路由
	MigrateTDSQLTables []string `yaml:"migrate_tdsql_tables"`
}

// DatabaseTypeConfig 数据库类型配置
type DatabaseTypeConfig struct {
	// 类型ID
	TypeId string `yaml:"type_id"`
	// 类型名称
	TypeName string `yaml:"type_name"`
}

type DbSource struct {
	Salt                 string        `yaml:"salt"`                     // 数据库源salt
	EsInputSize          int           `yaml:"es_input_size"`            // 数据库中数据存储 es 的行数
	EsCellValueMaxLength int           `yaml:"es_cell_value_max_length"` // 存储在es的数据库的单元格值的最大字符串长度
	MaxDbSourceNum       int           `yaml:"max_db_source_num"`        // 数据库源最大数量
	MaxTableNumOnce      int           `yaml:"max_table_num_once"`       // 数据库单词允许添加表的数量
	MaxTableNum          int           `yaml:"max_table_num"`            // 数据库允许添加表的数量
	MaxTableCol          int           `yaml:"max_table_col"`            // 添加表的最大列数
	MaxTableRow          int           `yaml:"max_table_row"`            // 添加表的最大行数
	EnableVdb            bool          `yaml:"enable_vdb"`               // 是否开启向量数据库
	MaxText2SqlTableNum  int           `yaml:"max_text2sql_table_num"`   // 最大text2sql表数量
	GenerateSqlTimeout   time.Duration `yaml:"generate_sql_timeout"`     // 生成sql的超时时间
	TestConnTimeout      time.Duration `yaml:"test_conn_timeout"`        // 测试连接超时时间（秒）
	ReadConnTimeout      time.Duration `yaml:"read_conn_timeout"`        // 读取连接超时时间（秒）
	ValueLinkTimeout     time.Duration `yaml:"value_link_timeout"`       // 值链接超时时间（秒）
	EsInsertTimeOut      time.Duration `yaml:"es_insert_time_out"`       // es插入超时时间（秒）
	DsnConfigMysql       string        `yaml:"dsn_config_mysql"`         // mysql dsn配置
	DsnConfigSqlServer   string        `yaml:"dsn_config_sqlserver"`     // sql server dsn配置
	DsnConfigPostgreSQL  string        `yaml:"dsn_config_postgresql"`    // postgresql dsn配置
	SyncTimeS            int           `yaml:"sync_time_s"`              // 页面触发多长时间刷新一次数据库的结构和行列数
	ValueLinkConfig      struct {
		MaxTopValueNum int `yaml:"top_value_num"`    // 获取 top value 的数量
		MaxTraverseRow int `yaml:"max_traverse_row"` // 遍历的最大行数
		MaxValueLen    int `yaml:"max_value_len"`    // 列值长度限制
		TrimThreshold  int `yaml:"trim_threshold"`   // 获取值达到的最大阈值，达到后清除。
		TrimKeepSize   int `yaml:"trim_keep_size"`   // 单次清除保留的的值数量。
	} `yaml:"value_link_config"` // 值链接配置
	DefaultSelectLimit int                  `yaml:"default_select_limit"` // select语句的默认limit
	DbSourceDecodeKey  string               `yaml:"db_source_decode_key"` // 数据库源解密key
	Proxy              string               `yaml:"proxy"`                // SOCKS5代理地址，格式: host:port
	ProxyUser          string               `yaml:"proxy_user"`           // SOCKS5代理用户名
	ProxyPd            string               `yaml:"proxy_pd"`             // SOCKS5代理密码
	ProxyEffectTime    string               `yaml:"proxy_effect_time"`    // 使用代理的时间门控(MySQL/SQL Server)，格式: 2006-01-02 15:04:05
	DatabaseTypes      []DatabaseTypeConfig `yaml:"database_types"`       // 数据库类型列表
	UnsafeDSNParams    []string             `yaml:"unsafe_dsn_params"`    // 不安全的DSN参数列表，用于安全检查
}

type EsSearch struct {
	AttributeEnableEs     bool `yaml:"attribute_enable_es"`      // 是否使用es检索，true-只使用es检索，false-只使用数据库检索
	WildcardTimeTimeoutMs int  `yaml:"wildcard_time_timeout_ms"` // 使用通配符检索的超时时间
}

// ElasticSearchConfig es配置项
type ElasticSearchConfig struct {
	Name     string `yaml:"name"`
	URL      string `yaml:"url"`      // es地址
	User     string `yaml:"user"`     // 用户名
	Password string `yaml:"password"` // 密码
	Timeout  uint   `yaml:"timeout"`  // 超时时间
	Log      struct {
		Enabled        bool `yaml:"enabled"`         // 是否开启日志
		RequestEnabled bool `yaml:"request_enabled"` // 是否开启请求日志
		RequestBody    bool `yaml:"request_body"`    // 是否开启请求日志
	} `yaml:"log"`
	EsVersion int `yaml:"es_version"` // es 7 或者 8
}

// SecAPI https://git.woa.com/sec-api/go/tree/master/trpc_scurl
type SecAPI struct {
	Enable          bool          `yaml:"enable"`            // 是否生效
	UnsafeDomains   []string      `yaml:"unsafe_domains"`    // 内网域名白名单
	AllowOuter      bool          `yaml:"allow_outer"`       // 是否允许外网域名
	AllowPolaris    []SecPolaris  `yaml:"allow_polaris"`     // 北极星白名单
	WithConfTimeout time.Duration `yaml:"with_conf_timeout"` // 请求超时时间
}

// SecPolaris 北极星配置
type SecPolaris struct {
	Env     string `yaml:"env"`     // 环境
	Service string `yaml:"service"` // 服务
}

// VectorSync 相似/评测向量同步配置
type VectorSync struct {
	Concurrent int           `yaml:"concurrent"`
	TimeBefore time.Duration `yaml:"time_before"`
	Limit      int           `yaml:"limit"`
}

// DocPlaceholder 文档占位符
type DocPlaceholder struct {
	Link string `yaml:"link"`
	Img  string `yaml:"img"`
}

// Task 任务配置
type Task struct {
	Runners           int           `yaml:"runners"`             // 执行协程数
	BindRunners       []string      `yaml:"bind_runners"`        // 需要绑定的执行器
	RetryWaitTime     time.Duration `yaml:"retry_wait_time"`     // 重试等待时间
	MaxRetry          uint          `yaml:"max_retry"`           // 最大重试次数
	Timeout           time.Duration `yaml:"timeout"`             // 超时时间
	FailTimeout       time.Duration `yaml:"fail_timeout"`        // 失败回调超时时间
	Delay             time.Duration `yaml:"delay"`               // 启动延迟
	Batch             uint          `yaml:"batch"`               // 并发批次数, 比如 Batch: 3, BatchSize: 10, 则并发处理 3 个批次, 每个批次内有 10 条待处理记录
	BatchSize         uint          `yaml:"batch_size"`          // 每批次大小, 默认为 0, 按 Batch 计算每批次大小
	StoppedResumeTime time.Duration `yaml:"stopped_resume_time"` //  任务停止后可以恢复的时效
}

// DemoMode 演示模式
type DemoMode struct {
	Enable             bool     `yaml:"enable"`               // 是否开启
	WhiteListTelephone []string `yaml:"white_list_telephone"` // 白名单手机号
	WhiteListUserID    []uint64 `yaml:"white_list_user_id"`   // 白名单用户ID
}

// LengthLimit 长度限制
type LengthLimit struct {
	MinLength int `yaml:"min_length"`
	MaxLength int `yaml:"max_length"`
}

// DocQA 文档QA配置
type DocQA struct {
	Question                 LengthLimit `yaml:"question"`
	Answer                   LengthLimit `yaml:"answer"`
	CustomParam              LengthLimit `yaml:"custom_param"`
	QuestionDesc             LengthLimit `yaml:"question_desc"`
	QACate                   LengthLimit `yaml:"qa_cate"`
	SimilarQuestion          LengthLimit `yaml:"similar_question"`
	SimilarQuestionNumLimit  int         `yaml:"similar_question_num_limit"` // 单个QA可配置的相似问数量限制
	ImportMaxLength          int         `yaml:"import_max_length"`
	CateNodeLimit            int         `yaml:"cate_node_limit"`
	SimilarQuestionPrompt    string      `yaml:"similar_question_prompt"`     // 生成相似问的Prompt
	SimilarQuestionLLMRegexp []string    `yaml:"similar_question_llm_regexp"` // 生成相似问的模型效果正则
	GenerateQALimit          int         `yaml:"generate_qa_limit"`           // 生成问答对文档单次的数量限制
	QuestionRegexp           string      `yaml:"question_regexp"`             // 生成问答对的正则表达式
	AnswerRegexp             string      `yaml:"answer_regexp"`               // 生成问答对的正则表达式
	ResumeMaxCountLimit      int         `yaml:"resume_max_count_limit"`      // 单次恢复的最大数量限制
}

// ModelLengthLimit 模型长度限制
type ModelLengthLimit struct {
	Length string `yaml:"length"` // 模型长度，4K/8K/16K等等
	Limit  int    `yaml:"limit"`  // 支持的最大字符数
}

// CronTask 定时任务
type CronTask struct {
	QASimilarTask QASimilarTask      `yaml:"qa_similar_task"`
	ExportQATask  ExportQATask       `yaml:"export_qa_task"`
	SynonymsTask  ExportSynonymsTask `yaml:"export_synonyms_task"`
}

// QASimilarTask 匹配相似问答
type QASimilarTask struct {
	WaitAMoment time.Duration `yaml:"wait_a_moment"` // 稍等一会 确保宙斯相似问答库存在新创建的问答对  单位：秒
	PageSize    int           `yaml:"page_size"`     // 单次执行处理的条数
}

// ExportQATask QA导出
type ExportQATask struct {
	PageSize          uint32        `yaml:"page_size"`            // 单次执行处理的条数
	LockDuration      time.Duration `yaml:"lock_duration"`        // 锁定时间
	MaxTryTimes       uint32        `yaml:"max_try_times"`        // 最大尝试次数
	MaxDoingTaskCount uint32        `yaml:"max_doing_task_count"` // 最大同时执行任务数
	MaxTaskPerCorp    uint32        `yaml:"max_task_per_corp"`    // 每个企业允许创建的最大任务数(待执行 / 执行中)
	MaxQACount        uint32        `yaml:"max_qa_count"`         // 允许最大导出数
}

// ExportSynonymsTask 同义词导出
type ExportSynonymsTask struct {
	PageSize          uint32        `yaml:"page_size"`            // 单次执行处理的条数
	LockDuration      time.Duration `yaml:"lock_duration"`        // 锁定时间
	MaxTryTimes       uint32        `yaml:"max_try_times"`        // 最大尝试次数
	MaxDoingTaskCount uint32        `yaml:"max_doing_task_count"` // 最大同时执行任务数
	MaxTaskPerCorp    uint32        `yaml:"max_task_per_corp"`    // 每个企业允许创建的最大任务数(待执行 / 执行中)
	MaxSynonymsCount  uint32        `yaml:"max_synonyms_count"`   // 允许最大导出数
}

// LoginDefault 登录配置
type LoginDefault struct {
	Type           []uint32      `yaml:"type"`             // 登录方式 0:手机号验证码登录 1:账号密码登录
	CQQKfUin       uint64        `yaml:"cqq_kf_uin"`       // 发送验证码主号
	SessionExpr    time.Duration `yaml:"session_expr"`     // 登陆后过期时间 单位：小时
	VerifyCode     VerifyCode    `yaml:"verify_code"`      // 验证码
	AheadRenewTime time.Duration `yaml:"ahead_renew_time"` // 续期提前时间
	CookieDomain   string        `yaml:"cookie_domain"`    // cookien站点
	Secure         bool          `yaml:"secure"`           // 安全
}

// VerifyCode 验证码配置
type VerifyCode struct {
	Expire       time.Duration `yaml:"expire"`
	MaxTry       uint64        `yaml:"max_try"`
	MaxTryExpire time.Duration `yaml:"max_try_expire"`
	DaySendLimit uint32        `yaml:"day_send_limit"`
}

// DocPreviewURL 文档预览配置
type DocPreviewURL struct {
	URL string `yaml:"url"`
}

// RobotDefault 机器人配置
type RobotDefault struct {
	Name                     string                `yaml:"name"`   // 机器人名称
	Avatar                   string                `yaml:"avatar"` // 机器人头像
	DocSplit                 RobotDocSplit         `yaml:"default_doc_split"`
	SearchVector             SearchVector          `yaml:"search_vector"`
	AppModelConfig           AppModelConfig        `yaml:"app_model"`
	AppDefaultModelConfig    AppDefaultModelConfig `yaml:"app_default_model"`
	RoleCustomModels         []string              `yaml:"role_custom_models"` // 无角色的自定义模型
	Filters                  RobotFilters          `yaml:"filters"`
	Embedding                RobotEmbedding        `yaml:"embedding"`                    // embedding 配置
	RoleDescription          string                `yaml:"role_description"`             // 机器人默认描述
	Greeting                 string                `yaml:"greeting"`                     // 机器人默认欢迎语
	ParseStrategy            int32                 `yaml:"parse_strategy"`               // 机器人默认解析规则
	MaxFileSize              uint64                `yaml:"max_file_size"`                // 上传文件大小, 最大 200M
	FileTypeSize             map[string]uint64     `yaml:"file_type_size"`               // 上传文件类型大小限制
	DocToQAMaxCharSize       uint64                `yaml:"doc_to_qa_max_char_size"`      // 文档生成问答字符最大限制，最大150w
	BotMaxCharSize           uint64                `yaml:"bot_max_char_size"`            // 单个应用最大字符数限制， 当前限制为3亿
	ModelTokenLimit          int64                 `yaml:"token_limit"`                  // 模型token限制
	BotMaxCharSizeWhiteList  map[string]uint64     `yaml:"bot_max_char_size_white_list"` // 单个应用最大字符数限制白名单
	DocReleaseMaxLimit       int                   `yaml:"doc_release_max_limit"`        // [概念统一后废弃]文档导入未发布数量限制,超限不可继续上传文档
	QaReleaseMaxLimit        int                   `yaml:"qa_release_max_limit"`         // [概念统一后废弃]问答导入未发布数量限制,超限不可继续创建问答
	BatchDownloadDocMaxLimit int                   `yaml:"batch_download_doc_max_limit"` // 批量下载文档限制
}

// Storage 对象存储配置
type Storage struct {
	s3x.Config `yaml:",inline"`

	VideoDomain string `yaml:"video_domain"`
}

// Cos cos存储
type Cos struct {
	SecretID       string        `yaml:"secret_id"`
	SecretKey      string        `yaml:"secret_key"`
	Region         string        `yaml:"region"`
	AppID          string        `yaml:"app_id"`
	Bucket         string        `yaml:"bucket"`
	Domain         string        `yaml:"domain"`
	ExpireTime     time.Duration `yaml:"expire_time"`
	CredentialTime time.Duration `yaml:"credential_time"`
}

// MinIO minio存储
type MinIO struct {
	SecretID    string        `yaml:"secret_id"`
	SecretKey   string        `yaml:"secret_key"`
	Region      string        `yaml:"region"`
	Bucket      string        `yaml:"bucket"`
	ExpireTime  time.Duration `yaml:"expire_time"`
	STSEndpoint string        `yaml:"sts_endpoint"` // sts地址:用于分配临时密钥
	EndPoint    string        `yaml:"end_point"`    // minio服务地址
	UseHTTPS    bool          `yaml:"use_https"`    // 是否使用https
}

// AttributeLabel 属性标签
type AttributeLabel struct {
	GeneralVectorAttrKey    string            `yaml:"general_vector_attr_key"`     // 通用向量属性，用于通用知识的标记
	GeneralVectorAttrKeyMap map[string]string `yaml:"general_vector_attr_key_map"` // 通用向量属性，用于通用知识的标记，系统用，外部用户不可定义
	FullLabelValue          string            `yaml:"full_label_value"`            // 属性的全量标签值
	FullLabelDesc           string            `yaml:"full_label_desc"`             // 属性的全量标签描述
	AttrKeyRegx             string            `yaml:"attr_key_regx"`               // 属性标识正则校验
	AttrNameMaxLen          int               `yaml:"attr_name_max_len"`           // 属性名称最大字符长度
	AttrLimit               int               `yaml:"attr_limit"`                  // 同一个机器人下的属性数量限制
	LabelNameMaxLen         int               `yaml:"label_name_max_len"`          // 标签名称最大字符长度
	LabelLimit              int               `yaml:"label_limit"`                 // 同一个属性下的标签数量限制
	SimilarLabelMaxLen      int               `yaml:"similar_label_max_len"`       // 相似标签最大字符长度
	SimilarLabelLimit       int               `yaml:"similar_label_limit"`         // 同一个标签下的相似标签数量限制
	DocAttrLimit            int               `yaml:"doc_attr_limit"`              // 文档关联的属性数量
	DocAttrLabelLimit       int               `yaml:"doc_attr_label_limit"`        // 文档关联的标签数量
	QAAttrLimit             int               `yaml:"qa_attr_limit"`               // 问答关联的属性数量
	QAAttrLabelLimit        int               `yaml:"qa_attr_label_limit"`         // 问答关联的标签数量
	MaxRow                  int               `yaml:"max_row"`                     // 导入最大行数
	MinRow                  int               `yaml:"min_row"`                     // 导入最小行数
	ExeclHead               []string          `yaml:"execl_head"`                  // 导入导出文件头
	RedisExpireSecond       int               `yaml:"redis_expire_second"`
}

// ParseError 解析错误
type ParseError struct {
	Msg          string `yaml:"msg"`            // 错误信息
	IsAllowRetry bool   `yaml:"is_allow_retry"` // 允许重试，true：允许 false：不允许
}

// SSEConnOptions sse相关配置
type SSEConnOptions struct {
	ConnURL       string `yaml:"conn_url"`
	ModID         int    `yaml:"mod_id"`
	CmdID         int    `yaml:"cmd_id"`
	NameSpace     string `yaml:"name_space"`
	ENV           string `yaml:"env"`
	HashKey       string `yaml:"hash_key"`
	ClientTimeOut int64  `yaml:"client_timeout"` // 客户端超时时间,单位秒
}

// CloudAPIs 云API
type CloudAPIs struct {
	SecretID    string `yaml:"secret_id"`
	SecretKey   string `yaml:"secret_key"`
	AccountHost string `yaml:"account_host"`
	Region      string `yaml:"region"`
	Version     string `yaml:"version"`
}

// AIConf 权限开通配置
type AIConf struct {
	ProductType   string `yaml:"product_type"`
	SuperAdminUin string `yaml:"super_admin_uin"`
}

// AppCategory 应用类型
type AppCategory struct {
	PermissionID string `yaml:"permission_id"` // 底座权限ID
	AppType      string `yaml:"app_type"`      // 应用类型
	Name         string `yaml:"name"`          // 应用类型名称
	Logo         string `yaml:"logo"`          // 应用类型logo
}

// CloudWhiteList 云白名单
type CloudWhiteList struct {
	Key string `yaml:"key"` // 白名单key列表
}

// Trial 试用配置
type Trial struct {
	AppMaxCharSize    uint64        `yaml:"app_max_char_size"`    // 试用应用最大字符数
	CorpAppQuota      uint64        `yaml:"corp_app_quota"`       // 试用应用数量
	CorpMaxTokenUsage uint64        `yaml:"corp_max_token_usage"` // 试用应用token数量
	CorpMaxCharSize   uint64        `yaml:"corp_max_char_size"`   // 企业默认最大字符数
	CorpPeriod        time.Duration `yaml:"corp_period"`          // 试用周期
	BanPermissionList []string      `yaml:"ban_permission_list"`  // 试用禁用权限
	EnableWhiteList   bool          `yaml:"enable_white_list"`    // 是否开启白名单
	WhiteListKey      string        `yaml:"white_list_key"`       // 白名单key
}

// Finance 计费配置
type Finance struct {
	Disabled           bool                   `yaml:"disabled"`          // 是否关闭
	ReportDisabled     bool                   `yaml:"report_disabled"`   // 是否关闭用量上报
	BillingModelMap    map[string]BillingInfo `yaml:"billing_model_map"` // <业务模型名，计费项模型信息>
	EmbeddingAndRerank struct {
		SearchReleaseSwitch bool     `yaml:"search_release_switch"` // 单独for searchKnowledgeRelease接口开关
		ReportSwitch        bool     `yaml:"report_switch"`         // 开关
		ModelList           []string `yaml:"model_list"`            // 指定模型才需要上报
	} `yaml:"embedding_and_rerank"` // embedding和rerank用量上报配置
	DelayExpiration time.Duration `yaml:"delay_expiration"` // 延迟过期时间
}

// BillingInfo 计费信息
type BillingInfo struct {
	ModelName string `yaml:"model_name"` // 计费模型名称
}

// DebugConfig 业务调试配置
type DebugConfig struct {
	DocAuditTimeoutBotList []string `yaml:"doc_audit_timeout_bot_list"` // 是否开启文档审核超时应用列表
	// 是否设置审核check回调任务达到最大重试次数的应用列表
	AuditCheckReachRetryLimitBotList []string `yaml:"audit_check_reach_retry_limit_bot_list"`
}

// DeepSeekConf deepSeek配置
type DeepSeekConf struct {
	ModelHasThink      []string `yaml:"model_has_think"`       // 深度搜索有思维链的模型名称
	NoThinkPrompt      string   `yaml:"no_think_prompt"`       // 不带思维链的prompt
	NoThinkModelParams string   `yaml:"no_think_model_params"` // 不带思维链的模型参数
}

// COSDocumentConfig COS文档配置
type COSDocumentConfig struct {
	STSEndpoint           string        `yaml:"sts_endpoint"`
	STSRegion             string        `yaml:"sts_region"`
	SecretID              string        `yaml:"secret_id"`
	SecretKey             string        `yaml:"secret_key"`
	ServiceRole           string        `yaml:"service_role"` // 服务角色
	COSServiceEndpoint    string        `yaml:"cos_service_endpoint"`
	COSBucketEndpoint     string        `yaml:"cos_bucket_endpoint"`
	CORSOrigin            string        `yaml:"cors_origin"`
	CredentialDuration    time.Duration `yaml:"credential_duration"`
	BucketCORSConcurrency int           `yaml:"bucket_cors_concurrency"`
}

// ShareKnowledgeConfig 共享知识库配置
type ShareKnowledgeConfig struct {
	DefaultQaExtractModel string `yaml:"default_qa_extract_model"` // 默认模型名称
}

// Watch 监听配置
func Watch() error {
	configx.MustWatch(applicationKey, Application{})

	initMainConfig()
	initWhitelistConfig()

	return nil
}

// App 获取应用配置
func App() Application {
	app := configx.MustGetWatched(applicationKey).(Application)
	// 如果有需要，可以在这里做一些配置处理，比如对没有配置的项赋默认值
	if len(app.Database.ForbiddenDatabaseNames.SQLServer) == 0 {
		app.Database.ForbiddenDatabaseNames.SQLServer = []string{"master", "model", "tempdb", "msdb", "Resource", "Monitor"}
	}
	return app
}

// SetApp 设置应用配置（可用于单元测试时 mock 配置）
func SetApp(c Application) {
	configx.SetWatched(applicationKey, c)
}

// IsDemoModeOpen 是否是演示模式
func IsDemoModeOpen() bool {
	return App().DemoMode.Enable
}

// CheckUserIDDemoMode 校验用户ID是否是演示模式
func CheckUserIDDemoMode(userID uint64) bool {
	demoMode := App().DemoMode
	return slices.Contains(demoMode.WhiteListUserID, userID)
}

// AuditSwitch 是否开启审核
func AuditSwitch() bool {
	return App().AuditSwitch
}

// FileAuditSwitch 是否开启文件审核
func FileAuditSwitch() bool {
	if !App().AuditSwitch {
		return false
	}
	return App().FileAuditSwitch
}

// VideoAuditSwitch 是否开启视频审核
func VideoAuditSwitch() bool {
	if !App().AuditSwitch {
		return false
	}
	return App().VideoAuditSwitch
}

// IsFinanceDisabled 是否关闭计费
func IsFinanceDisabled() bool {
	return App().Finance.Disabled
}

// IsReportDosageDisabled 是否关闭用量上报
func IsReportDosageDisabled() bool {
	return App().Finance.ReportDisabled
}

// GetBotMaxCharSize 获取默认的每个应用最大字符串
func GetBotMaxCharSize(ctx context.Context, botBizId uint64) uint64 {
	// 单应用默认3亿字符
	var botMaxCharSize uint64 = 300000000
	if App().RobotDefault.BotMaxCharSize > 0 {
		botMaxCharSize = App().RobotDefault.BotMaxCharSize
	}
	botBizIdStr := fmt.Sprintf("%d", botBizId)
	if botBizIdStr == "" || App().RobotDefault.BotMaxCharSizeWhiteList == nil {
		return botMaxCharSize
	}
	// 白名单应用单独设置
	if botMaxCharSizeWhiteList, ok := App().RobotDefault.BotMaxCharSizeWhiteList[botBizIdStr]; ok {
		botMaxCharSize = botMaxCharSizeWhiteList
	}
	logx.D(ctx, "botBizId:%d GetBotMaxCharSize:%d BotMaxCharSizeWhiteList:%+v",
		botBizId, botMaxCharSize, App().RobotDefault.BotMaxCharSizeWhiteList)
	return botMaxCharSize
}

// GetDefaultTokenLimit 获取默认的模型token限制
func GetDefaultTokenLimit() int {
	if App().RobotDefault.ModelTokenLimit == 0 {
		return 8000
	}
	return int(App().RobotDefault.ModelTokenLimit)
}

// GetAllowPolaris 获取白名单北极星
func GetAllowPolaris() []secapi.Polaris {
	var polaris []secapi.Polaris
	for _, v := range App().SecAPI.AllowPolaris {
		polaris = append(polaris, secapi.Polaris{Env: v.Env, Service: v.Service})
	}
	return polaris
}

// IsDeepSeekModeAndHasThink 是否是深度学习模型，且有思维链
func IsDeepSeekModeAndHasThink(modelName string) bool {
	if modelName == "" {
		return false
	}
	return slices.Contains(App().DeepSeekConf.ModelHasThink, modelName)
}

// DescribeExceedKnowledgeResumeCon 获取超过知识库的配置
func DescribeExceedKnowledgeResumeCon() int {
	if App().ExceedKnowledgeResumeConfig.ResumeConcurrentNum == 0 {
		return 5
	}
	return App().ExceedKnowledgeResumeConfig.ResumeConcurrentNum
}

func DescribeResumeBatchSize() int {
	if App().ExceedKnowledgeResumeConfig.ResumeBatchSize == 0 {
		return 1000
	}
	return App().ExceedKnowledgeResumeConfig.ResumeBatchSize
}

func DescribePermissionIDs() map[string]bool {
	if App().ExceedKnowledgeResumeConfig.PermissionIDs == nil || len(App().ExceedKnowledgeResumeConfig.
		PermissionIDs) == 0 {
		return map[string]bool{
			"adpAppEdit":                    true,
			"adpKnowledgeEdit":              true,
			"adpKnowledgeBaseEdit":          true,
			"adpAppCustom.knowledgeManager": true,
			"adpAppCustom.appConfig.edit":   true,
		}
	}
	return App().ExceedKnowledgeResumeConfig.PermissionIDs
}

// DescribeImportQAConcurrency 获取QA导入并发数配置
func DescribeImportQAConcurrency() int {
	if App().KbPackageConfig.ImportQAConcurrency <= 0 {
		return 20
	}
	return App().KbPackageConfig.ImportQAConcurrency
}

// DescribeExportDocConcurrency 获取文档导出并发数配置
func DescribeExportDocConcurrency() int {
	if App().KbPackageConfig.ExportDocConcurrency <= 0 {
		return 20
	}
	return App().KbPackageConfig.ExportDocConcurrency
}

// DescribeZipBufferSize 获取zip缓冲区大小配置
func DescribeZipBufferSize() int {
	if App().KbPackageConfig.ZipBufferSize <= 0 {
		return 8
	}
	return App().KbPackageConfig.ZipBufferSize
}

// DescribeImportDocConcurrency 获取文档导入并发数配置
func DescribeImportDocConcurrency() int {
	if App().KbPackageConfig.ImportDocConcurrency <= 0 {
		return 5 // 默认并发数为5
	}
	return App().KbPackageConfig.ImportDocConcurrency
}

// DescribePackageSizeLimitMB 获取知识包大小限制配置（单位：MB）
// 参数 appBizID: 应用业务ID，如果为0则返回默认配置
// 返回值: 知识包大小限制（单位：MB），如果未配置则返回默认值100MB
func DescribePackageSizeLimitMB(appBizID uint64) uint64 {
	// 1. 如果指定了应用ID，先尝试获取应用级别的配置
	if appBizID > 0 && App().KbPackageConfig.AppPackageSizeLimit != nil {
		if limit, ok := App().KbPackageConfig.AppPackageSizeLimit[appBizID]; ok && limit > 0 {
			return limit
		}
	}

	// 2. 使用全局默认配置
	if App().KbPackageConfig.PackageSizeLimitMB > 0 {
		return App().KbPackageConfig.PackageSizeLimitMB
	}

	// 3. 如果全局配置也未设置，使用硬编码默认值100MB
	return 100
}
