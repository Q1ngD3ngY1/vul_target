// Package config 写配置定义和热加载等配置相关逻辑
package config

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/configx"
	secapi "git.woa.com/sec-api/go/scurl"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/exp/slices"
)

const (
	applicationKey = "application.yaml"
	// sseDefaultTimeOut sse默认连接超时时间,单位:秒
	sseDefaultTimeOut = 150
)

// Application 业务配置
type Application struct {
	Storage                        Storage                     `yaml:"storage"`
	RobotDefault                   RobotDefault                `yaml:"robot_default"`
	LoginDefault                   LoginDefault                `yaml:"login_default"`
	DocPreview                     map[string]DocPreviewURL    `yaml:"doc_preview"`
	DocPreviewType                 uint8                       `yaml:"doc_preview_type"` // 预览方式：0 预览链接预览 1 提供文件下载链接
	DocQA                          DocQA                       `yaml:"doc_qa"`
	Synonyms                       Synonyms                    `yaml:"synonyms"`
	SampleRule                     SampleRule                  `yaml:"sample_rule"`
	SampleTest                     SampleTest                  `yaml:"sample_test"`
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
	AuditSwitch                    bool                        `yaml:"audit_switch"`                      // 审核开关，公有云要打开
	FileAuditSwitch                bool                        `yaml:"file_audit_switch"`                 // 文件审核开关，公有云要打开
	VideoAuditSwitch               bool                        `yaml:"video_audit_switch"`                // 视频审核开关，公有云要打开
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
	ManagedCorpIds                 ManagedCorpIds              `yaml:"managed_account_ids"`                // 管理账户的id(产品提供uin，去t_corp查询id)
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
	ExceedKnowledgeResumeConfig    ExceedKnowledgeResumeConfig `yaml:"exceed_knowledge_resume_config"` // 超量知识库恢复配置
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

// DbSource TODO
type DbSource struct {
	Salt                string        `yaml:"salt"`                   // 数据库源salt
	EsInputSize         int           `yaml:"es_input_size"`          // 数据库中数据存储 es 的行数
	MaxDbSourceNum      int           `yaml:"max_db_source_num"`      // 数据库源最大数量
	MaxTableNumOnce     int           `yaml:"max_table_num_once"`     // 数据库单词允许添加表的数量
	MaxTableNum         int           `yaml:"max_table_num"`          // 数据库允许添加表的数量
	MaxTableCol         int           `yaml:"max_table_col"`          // 添加表的最大列数
	MaxTableRow         int           `yaml:"max_table_row"`          // 添加表的最大行数
	EnableVdb           bool          `yaml:"enable_vdb"`             // 是否开启向量数据库
	MaxText2SqlTableNum int           `yaml:"max_text2sql_table_num"` // 最大text2sql表数量
	GenerateSqlTimeout  time.Duration `yaml:"generate_sql_timeout"`   // 生成sql的超时时间
	TestConnTimeout     time.Duration `yaml:"test_conn_timeout"`      // 测试连接超时时间（秒）
	ReadConnTimeout     time.Duration `yaml:"read_conn_timeout"`      // 读取连接超时时间（秒）
	ValueLinkTimeout    time.Duration `yaml:"value_link_timeout"`     // 值链接超时时间（秒）
	EsInsertTimeOut     time.Duration `yaml:"es_insert_time_out"`     // es插入超时时间（秒）
	DsnConfigMysql      string        `yaml:"dsn_config_mysql"`       // mysql dsn配置
	DsnConfigSqlServer  string        `yaml:"dsn_config_sqlserver"`   // sql server dsn配置
	SyncTimeS           int           `yaml:"sync_time_s"`            // 页面触发多长时间刷新一次数据库的结构和行列数
	ValueLinkConfig     struct {
		MaxTopValueNum int `yaml:"top_value_num"`    // 获取 top value 的数量
		MaxTraverseRow int `yaml:"max_traverse_row"` // 遍历的最大行数
		MaxValueLen    int `yaml:"max_value_len"`    // 列值长度限制
		TrimThreshold  int `yaml:"trim_threshold"`   // 获取值达到的最大阈值，达到后清除。
		TrimKeepSize   int `yaml:"trim_keep_size"`   // 单次清除保留的的值数量。
	} `yaml:"value_link_config"`                         // 值链接配置
	DefaultSelectLimit int `yaml:"default_select_limit"` // select语句的默认limit
}

// EsSearch TODO
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

// SampleRule 评测样本配置
type SampleRule struct {
	MaxRow          int         `yaml:"max_row"`
	MinRow          int         `yaml:"min_row"`
	CheckRow        int         `yaml:"check_row"`
	RoleLength      int         `yaml:"role_length"`
	ExcelHead       []string    `yaml:"excel_head"`
	Question        LengthLimit `yaml:"question"` // 数据库是用varchar存储的，最多只能存65535/4=16380个字符
	ExportExcelHead []string    `yaml:"export_excel_head"`
}

// SampleTest 样本评测
type SampleTest struct {
	ModelLengthLimit []ModelLengthLimit `yaml:"model_length_limit"`
	DefaultLimit     int                `yaml:"default_limit"`     // 默认限制3000，跟4K对应的限制保持一致
	SSEBufferSizeK   int                `yaml:"sse_buffer_size_k"` // SSE缓冲区大小，单位:k
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
	PageSize    uint32        `yaml:"page_size"`     // 单次执行处理的条数
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

// Synonyms 同义词配置
type Synonyms struct {
	SynonymsWordMaxLength   uint64 `yaml:"synonyms_word_max_length"`    // 标准词和同义词长度
	MaxSynonymsCountPerWord uint64 `yaml:"max_synonyms_count_per_word"` // 标准词最大同义词数量
	ImportMaxFileSize       uint64 `yaml:"import_max_file_size"`        // 导入文件大小,最大5M
	ImportMaxCount          uint64 `yaml:"import_max_count"`            // 导入的最大条数
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
	Name                     string                `yaml:"name"`        // 机器人名称
	AppNames                 AppNames              `yaml:"app_names"`   // 应用名称
	Avatar                   string                `yaml:"avatar"`      // 机器人头像
	ExpireTime               int                   `yaml:"expire_time"` // 默认过期时间 day
	DocSplit                 RobotDocSplit         `yaml:"default_doc_split"`
	SearchVector             SearchVector          `yaml:"search_vector"`
	AppModelConfig           AppModelConfig        `yaml:"app_model"`
	AppDefaultModelConfig    AppDefaultModelConfig `yaml:"app_default_model"`
	RoleCustomModels         []string              `yaml:"role_custom_models"` // 无角色的自定义模型
	Filters                  RobotFilters          `yaml:"filters"`
	Embedding                RobotEmbedding        `yaml:"embedding"` // embedding 配置
	TransferKeywords         []string              `yaml:"transfer_keywords"`
	MaxCharSize              uint64                `yaml:"max_char_size"`                // 机器人最大字符数
	RoleDescription          string                `yaml:"role_description"`             // 机器人默认描述
	Greeting                 string                `yaml:"greeting"`                     // 机器人默认欢迎语
	ParseStrategy            int32                 `yaml:"parse_strategy"`               // 机器人默认解析规则
	MaxFileSize              uint64                `yaml:"max_file_size"`                // 上传文件大小, 最大 200M
	FileTypeSize             map[string]uint64     `yaml:"file_type_size"`               // 上传文件类型大小限制
	DocToQAMaxCharSize       uint64                `yaml:"doc_to_qa_max_char_size"`      // 文档生成问答字符最大限制，最大150w
	BotMaxCharSize           uint64                `yaml:"bot_max_char_size"`            // 单个应用最大字符数限制， 当前限制为3亿
	ModelTokenLimit          int64                 `yaml:"token_limit"`                  // 模型token限制
	BotMaxCharSizeWhiteList  map[string]uint64     `yaml:"bot_max_char_size_white_list"` // 单个应用最大字符数限制白名单
	DocReleaseMaxLimit       int                   `yaml:"doc_release_max_limit"`        // 文档导入未发布数量限制,超限不可继续上传文档
	QaReleaseMaxLimit        int                   `yaml:"qa_release_max_limit"`         // 问答导入未发布数量限制,超限不可继续创建问答
	BatchDownloadDocMaxLimit int                   `yaml:"batch_download_doc_max_limit"` // 批量下载文档限制
}

// Storage 对象存储配置
type Storage struct {
	Type        string           `yaml:"type"`
	VideoDomain string           `yaml:"video_domain"`
	CosMap      map[string]Cos   `yaml:"cos_map"`
	MinIOMap    map[string]MinIO `yaml:"min_io_map"`
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
	Disabled       bool `yaml:"disabled"`        // 是否关闭
	ReportDisabled bool `yaml:"report_disabled"` // 是否关闭用量上报
}

// DebugConfig 业务调试配置
type DebugConfig struct {
	CharExceededBotList    []string `yaml:"char_exceeded_bot_list"`     // 是否开启字符超限应用列表
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
	STSEndpoint        string        `yaml:"sts_endpoint"`
	STSRegion          string        `yaml:"sts_region"`
	SecretID           string        `yaml:"secret_id"`
	SecretKey          string        `yaml:"secret_key"`
	ServiceRole        string        `yaml:"service_role"` // 服务角色
	COSServiceEndpoint string        `yaml:"cos_service_endpoint"`
	COSBucketEndpoint  string        `yaml:"cos_bucket_endpoint"`
	CORSOrigin         string        `yaml:"cors_origin"`
	CredentialDuration time.Duration `yaml:"credential_duration"`
}

// ShareKnowledgeConfig 共享知识库配置
type ShareKnowledgeConfig struct {
	DefaultQaExtractModel string `yaml:"default_qa_extract_model"` // 默认模型名称
}

// Watch 监听配置
func Watch() error {
	configx.MustWatch(applicationKey, Application{})
	return nil
}

// App 获取应用配置
func App() Application {
	app := configx.MustGetWatched(applicationKey).(Application)
	// 如果有需要，可以在这里做一些配置处理，比如对没有配置的项赋默认值
	return app
}

// SetApp 设置应用配置（可用于单元测试时 mock 配置）
func SetApp(c Application) {
	configx.SetWatched(applicationKey, c)
}

// GetDefaultTransferKeywords 默认机器人转人工关键词
func GetDefaultTransferKeywords() (string, error) {
	return jsoniter.MarshalToString(App().RobotDefault.TransferKeywords)
}

// IsDemoModeOpen 是否是演示模式
func IsDemoModeOpen() bool {
	return App().DemoMode.Enable
}

// CheckTelephoneDemoMode 校验手机号码是否是演示模式
func CheckTelephoneDemoMode(telephone string) bool {
	demoMode := App().DemoMode
	return slices.Contains(demoMode.WhiteListTelephone, telephone)
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
	return App().FileAuditSwitch
}

// IsFinanceDisabled 是否关闭计费
func IsFinanceDisabled() bool {
	return App().Finance.Disabled
}

// IsReportDosageDisabled 是否关闭用量上报
func IsReportDosageDisabled() bool {
	return App().Finance.ReportDisabled
}

// GetChatSSEConnOptions 获取sse请求配置
func GetChatSSEConnOptions() SSEConnOptions {
	return App().SSEConnOptions
}

// GetSSEClientTimeOut 获取sse客户端超时时间
func GetSSEClientTimeOut() int64 {
	if App().SSEConnOptions.ClientTimeOut == 0 {
		return sseDefaultTimeOut
	} else {
		return App().SSEConnOptions.ClientTimeOut
	}
}

// GetAppTypePermissionID 获取应用类型对应的权限ID
func GetAppTypePermissionID(appType string) (string, error) {
	for _, cate := range App().PermissionAppCategory {
		if cate.AppType == appType {
			return cate.PermissionID, nil
		}
	}
	return "", fmt.Errorf("appType %s not found", appType)
}

// GetAppTypeName 获取应用类型对应的名称
func GetAppTypeName(appType string) string {
	for _, v := range App().PermissionAppCategory {
		if v.AppType == appType {
			return v.Name
		}
	}
	return ""
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
	log.DebugContextf(ctx, "botBizId:%d GetBotMaxCharSize:%d BotMaxCharSizeWhiteList:%+v",
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
