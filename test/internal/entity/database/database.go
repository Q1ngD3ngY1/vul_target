package database

import (
	"time"

	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

const (
	DBTypeMySQL      = "mysql"
	DBTypeSQLServer  = "sqlserver"
	DBTypePostgreSQL = "postgres"
	DBTypeOracle     = "oracle"
)

const (
	LabelDBTableBizID = "db_table_biz_id"
)

const (
	MaxDbSourceAliasNameLength     = 50
	MaxDbSourceDescriptionLength   = 100
	MaxDbTableAliasNameLength      = 20
	MaxDbTableDescriptionLength    = 100
	MaxDbColumnAliasNameLength     = 15
	MaxDbColumnAliasDataTypeLength = 10
	MaxDbColumnDescriptionLength   = 100
)

// Database 数据库信息表，发布端和评测端共用一个结构体，发布端需制定table_name，不指定table name的时候默认操作评测端
type Database struct {
	ID            uint64
	CorpBizID     uint64
	AppBizID      uint64
	DBSourceBizID uint64
	DBName        string
	AliasName     string
	Description   string
	DBType        string
	Host          string
	Port          int32
	Username      string
	Password      string
	Alive         bool
	LastSyncTime  time.Time
	IsIndexed     bool
	IsDeleted     bool
	StaffID       uint64
	CreateTime    time.Time
	UpdateTime    time.Time
	SchemaName    string
	EnableScope   uint32

	// ReleaseStatus 和 NextAction仅在评测端有意义
	ReleaseStatus uint32
	NextAction    uint32

	StaffName  string
	Tables     []*Table // 数据库下的表列表
	TableNames []string // 数据库下的表名列表
}

type DatabaseFilter struct {
	CorpBizID      uint64
	AppBizID       uint64
	AppBizIDs      []uint64 // 应用业务ID数组，与AppBizID互斥使用，优先使用AppBizIDs
	DBSourceBizID  uint64
	DBSourceBizIDs []uint64
	DBTableBizIDs  []uint64
	IsDeleted      *bool // 默认是 0，如果要查删除的
	PageNumber     *uint32
	PageSize       uint32
	DBNameEq       *string
	DBNameLike     *string
	ReleaseStatus  []uint32
	IsEnable       *bool   // 为nil的时候不生效
	EnableScope    *uint32 // 为nil 不生效

	WithSyncAlive   bool // 是否同步连接状态
	WithTable       bool // 是否携带表信息
	TableNameLike   *string
	TablePageNumber *uint32
	TablePageSize   uint32
	WithStaffName   bool // 是否携带员工名称
}

type DatabaseRelease struct {
	ID            uint64    `gorm:"column:id;primaryKey;autoIncrement;not null"`
	CorpBizID     uint64    `gorm:"column:corp_biz_id;not null;index:idx_biz_id,priority:1;comment:企业业务ID"`
	AppBizID      uint64    `gorm:"column:app_biz_id;not null;index:idx_biz_id,priority:2;comment:应用业务ID"`
	DBSourceBizID uint64    `gorm:"column:db_source_biz_id;not null;index:idx_biz_id,priority:3;comment:新产生的ID，雪花算法 id"`
	DBName        string    `gorm:"column:db_name;type:varchar(100);not null;comment:原始的数据库名称"`
	AliasName     string    `gorm:"column:alias_name;type:varchar(100);not null;comment:自定义数据库名称"`
	Description   string    `gorm:"column:description;type:text;comment:数据库描述"`
	DBType        string    `gorm:"column:db_type;type:varchar(32);not null;comment:数据库类型: mysql, sqlserver"`
	Host          string    `gorm:"column:host;type:varchar(100);not null;comment:地址"`
	Port          int32     `gorm:"column:port;not null;comment:端口号"`
	Username      string    `gorm:"column:username;type:varchar(100);not null;comment:用户名"`
	Password      string    `gorm:"column:password;type:varchar(255);not null;comment:加密存储的密码"`
	Alive         bool      `gorm:"column:alive;type:tinyint(1);not null;default:1;comment:连接是否正常"`
	LastSyncTime  time.Time `gorm:"column:last_sync_time;type:datetime;comment:上次同步时间"`
	ReleaseStatus uint32    `gorm:"column:release_status;type:tinyint;not null;default:0;comment:'发布状态(1 待发布 2 发布中 3 已发布 4 发布失败)'"`
	ReleaseBizID  uint64    `gorm:"column:release_biz_id;type:bigint;not null;default:0;comment:'版本ID'"`
	Action        uint32    `gorm:"column:action;type:tinyint;not null;comment:'最后操作：1新增 2修改 3删除 4发布'"`
	IsDeleted     bool      `gorm:"column:is_deleted;type:tinyint(1);not null;default:0;comment:0:未删除,1:已删除"`
	CreateTime    time.Time `gorm:"column:create_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:创建时间"`
	UpdateTime    time.Time `gorm:"column:update_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:更新时间"`
	SchemaName    string    `gorm:"column:schema_name;type:varchar(128);comment:模式名称"`
}

type DatabaseProd struct {
	ID            uint64    `gorm:"column:id;primaryKey;autoIncrement;not null"`
	CorpBizID     uint64    `gorm:"column:corp_biz_id;not null;index:idx_biz_id,priority:1;comment:企业业务ID"`
	AppBizID      uint64    `gorm:"column:app_biz_id;not null;index:idx_biz_id,priority:2;comment:应用业务ID"`
	DBSourceBizID uint64    `gorm:"column:db_source_biz_id;not null;index:idx_biz_id,priority:3;comment:新产生的ID，雪花算法 id"`
	DBName        string    `gorm:"column:db_name;type:varchar(100);not null;comment:原始的数据库名称"`
	AliasName     string    `gorm:"column:alias_name;type:varchar(100);not null;comment:自定义数据库名称"`
	Description   string    `gorm:"column:description;type:text;comment:数据库描述"`
	DBType        string    `gorm:"column:db_type;type:varchar(32);not null;comment:数据库类型: mysql, sqlserver"`
	Host          string    `gorm:"column:host;type:varchar(100);not null;comment:地址"`
	Port          int32     `gorm:"column:port;not null;comment:端口号"`
	Username      string    `gorm:"column:username;type:varchar(100);not null;comment:用户名"`
	Password      string    `gorm:"column:password;type:varchar(255);not null;comment:加密存储的密码"`
	Alive         uint32    `gorm:"column:alive;type:tinyint(1);not null;default:1;comment:连接是否正常"`
	LastSyncTime  time.Time `gorm:"column:last_sync_time;type:datetime;comment:上次同步时间"`
	IsDeleted     uint32    `gorm:"column:is_deleted;type:tinyint(1);not null;default:0;comment:0:未删除,1:已删除"`
	CreateTime    time.Time `gorm:"column:create_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:创建时间"`
	UpdateTime    time.Time `gorm:"column:update_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:更新时间"`
	SchemaName    string    `gorm:"column:schema_name;type:varchar(128);comment:模式名称"`
}

type DatabaseConn struct {
	DbType     string
	Host       string
	DbName     string
	Username   string
	Password   string
	Port       int32
	SchemaName string
	CreateTime *time.Time // 数据库创建时间，用于判断是否使用代理
	Uin        string     // 用户UIN，必选字段，用于内网数据库白名单校验
}

const (
	// LearnStatusUnlearned 未学习
	LearnStatusUnlearned = uint32(1)
	// LearnStatusLearning 学习中
	LearnStatusLearning = uint32(2)
	// LearnStatusLearned 已学习
	LearnStatusLearned = uint32(3)
	// LearnStatusFailed 学习失败
	LearnStatusFailed = uint32(4)

	// FaceStatusLearning 界面展示状态，学习中
	FaceStatusLearning = uint32(5)
	// FaceStatusLearnFailed 界面展示状态， 学习失败
	FaceStatusLearnFailed = uint32(6)
	// FaceStatusLearnSuccess 界面展示状态， 学习成功, 用于共享知识库
	FaceStatusLearnSuccess = 7
)

// DecodePassword 解密 database 密码
func DecodePassword(pwd string) (string, error) {
	priv, err := util.GetDbSourcePrivateKey()
	if err != nil {
		return "", err
	}
	password, err := util.DecryptWithPrivateKeyPEM(priv, pwd)
	if err != nil {
		return "", errs.ErrPasswordDecodeFail
	}
	return password, nil
}
