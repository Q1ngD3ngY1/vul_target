package model

import (
	"context"
	"time"

	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
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

// DBSource 数据库信息表，发布端和评测端共用一个结构体，发布端需制定table_name，不指定table name的时候默认操作评测端
type DBSource struct {
	ID            uint64    `gorm:"column:id;primaryKey;autoIncrement;not null"`
	CorpBizID     uint64    `gorm:"column:corp_biz_id;not null;index:idx_biz_id,priority:1;comment:企业业务ID"`
	AppBizID      uint64    `gorm:"column:app_biz_id;not null;index:idx_biz_id,priority:2;comment:应用业务ID"`
	DBSourceBizID uint64    `gorm:"column:db_source_biz_id;not null;index:idx_biz_id,priority:3;comment:新产生的ID，雪花算法 id"`
	DBName        string    `gorm:"column:db_name;type:varchar(100);not null;comment:原始的数据库名称"`
	AliasName     string    `gorm:"column:alias_name;type:varchar(100);not null;comment:自定义数据库名称"`
	Description   string    `gorm:"column:description;type:text;comment:数据库描述"`
	DBType        string    `gorm:"column:db_type;type:varchar(32);not null;comment:数据库类型: mysql, sqlserver"`
	Host          string    `gorm:"column:host;type:varchar(100);not null;comment:地址"`
	Port          int       `gorm:"column:port;not null;comment:端口号"`
	Username      string    `gorm:"column:username;type:varchar(100);not null;comment:用户名"`
	Password      string    `gorm:"column:password;type:varchar(255);not null;comment:加密存储的密码"`
	Alive         bool      `gorm:"column:alive;type:tinyint(1);not null;default:1;comment:连接是否正常"`
	LastSyncTime  time.Time `gorm:"column:last_sync_time;type:datetime;comment:上次同步时间"`
	// ReleaseStatus和NextAction仅在评测端有意义
	ReleaseStatus ReleaseStatus `gorm:"column:release_status;type:tinyint;not null;default:0;comment:'发布状态(1 待发布 2 发布中 3 已发布 4 发布失败)'"`
	NextAction    ReleaseAction `gorm:"column:next_action;type:tinyint;not null;comment:'最后操作：1新增 2修改 3删除 4发布'"`
	IsIndexed     bool          `gorm:"column:is_indexed;type:tinyint(1);default:1;comment:是否参与索引"`
	IsDeleted     bool          `gorm:"column:is_deleted;type:tinyint(1);not null;default:0;comment:0:未删除,1:已删除"`
	StaffID       uint64        `gorm:"column:staff_id;not null;default:0;comment:员工ID"`
	CreateTime    time.Time     `gorm:"column:create_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:创建时间"`
	UpdateTime    time.Time     `gorm:"column:update_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:更新时间"`
}

func (DBSource) TableName() string {
	return "t_db_source"
}

type ReleaseDBSource struct {
	ID            uint64        `gorm:"column:id;primaryKey;autoIncrement;not null"`
	CorpBizID     uint64        `gorm:"column:corp_biz_id;not null;index:idx_biz_id,priority:1;comment:企业业务ID"`
	AppBizID      uint64        `gorm:"column:app_biz_id;not null;index:idx_biz_id,priority:2;comment:应用业务ID"`
	DBSourceBizID uint64        `gorm:"column:db_source_biz_id;not null;index:idx_biz_id,priority:3;comment:新产生的ID，雪花算法 id"`
	DBName        string        `gorm:"column:db_name;type:varchar(100);not null;comment:原始的数据库名称"`
	AliasName     string        `gorm:"column:alias_name;type:varchar(100);not null;comment:自定义数据库名称"`
	Description   string        `gorm:"column:description;type:text;comment:数据库描述"`
	DBType        string        `gorm:"column:db_type;type:varchar(32);not null;comment:数据库类型: mysql, sqlserver"`
	Host          string        `gorm:"column:host;type:varchar(100);not null;comment:地址"`
	Port          int           `gorm:"column:port;not null;comment:端口号"`
	Username      string        `gorm:"column:username;type:varchar(100);not null;comment:用户名"`
	Password      string        `gorm:"column:password;type:varchar(255);not null;comment:加密存储的密码"`
	Alive         bool          `gorm:"column:alive;type:tinyint(1);not null;default:1;comment:连接是否正常"`
	LastSyncTime  time.Time     `gorm:"column:last_sync_time;type:datetime;comment:上次同步时间"`
	ReleaseStatus ReleaseStatus `gorm:"column:release_status;type:tinyint;not null;default:0;comment:'发布状态(1 待发布 2 发布中 3 已发布 4 发布失败)'"`
	ReleaseBizID  uint64        `gorm:"column:release_biz_id;type:bigint;not null;default:0;comment:'版本ID'"`
	Action        ReleaseAction `gorm:"column:action;type:tinyint;not null;comment:'最后操作：1新增 2修改 3删除 4发布'"`
	IsDeleted     bool          `gorm:"column:is_deleted;type:tinyint(1);not null;default:0;comment:0:未删除,1:已删除"`
	CreateTime    time.Time     `gorm:"column:create_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:创建时间"`
	UpdateTime    time.Time     `gorm:"column:update_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:更新时间"`
}

func (ReleaseDBSource) TableName() string {
	return "t_release_db_source"
}

type ProdDBSource struct {
	ID            uint64    `gorm:"column:id;primaryKey;autoIncrement;not null"`
	CorpBizID     uint64    `gorm:"column:corp_biz_id;not null;index:idx_biz_id,priority:1;comment:企业业务ID"`
	AppBizID      uint64    `gorm:"column:app_biz_id;not null;index:idx_biz_id,priority:2;comment:应用业务ID"`
	DBSourceBizID uint64    `gorm:"column:db_source_biz_id;not null;index:idx_biz_id,priority:3;comment:新产生的ID，雪花算法 id"`
	DBName        string    `gorm:"column:db_name;type:varchar(100);not null;comment:原始的数据库名称"`
	AliasName     string    `gorm:"column:alias_name;type:varchar(100);not null;comment:自定义数据库名称"`
	Description   string    `gorm:"column:description;type:text;comment:数据库描述"`
	DBType        string    `gorm:"column:db_type;type:varchar(32);not null;comment:数据库类型: mysql, sqlserver"`
	Host          string    `gorm:"column:host;type:varchar(100);not null;comment:地址"`
	Port          int       `gorm:"column:port;not null;comment:端口号"`
	Username      string    `gorm:"column:username;type:varchar(100);not null;comment:用户名"`
	Password      string    `gorm:"column:password;type:varchar(255);not null;comment:加密存储的密码"`
	Alive         bool      `gorm:"column:alive;type:tinyint(1);not null;default:1;comment:连接是否正常"`
	LastSyncTime  time.Time `gorm:"column:last_sync_time;type:datetime;comment:上次同步时间"`
	IsDeleted     bool      `gorm:"column:is_deleted;type:tinyint(1);not null;default:0;comment:0:未删除,1:已删除"`
	CreateTime    time.Time `gorm:"column:create_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:创建时间"`
	UpdateTime    time.Time `gorm:"column:update_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:更新时间"`
}

func (ProdDBSource) TableName() string {
	return "t_db_source_prod"
}

// ----- table 表 ----
type TableSource int

const (
	TableSourceDB  TableSource = 0
	TableSourceDoc TableSource = 1
)

// DBTable 数据库信息表，发布端和评测端共用一个结构体，发布端需制定table_name，不指定table name的时候默认操作评测端
type DBTable struct {
	ID                uint64        `gorm:"column:id;primaryKey;autoIncrement;not null"`
	CorpBizID         uint64        `gorm:"column:corp_biz_id;not null;index:idx_table_id,priority:1;comment:企业业务ID"`
	AppBizID          uint64        `gorm:"column:app_biz_id;not null;index:idx_table_id,priority:2;comment:应用业务ID"`
	DBSourceBizID     uint64        `gorm:"column:db_source_biz_id;not null;index:idx_table_id,priority:3;comment:数据源ID"`
	DBTableBizID      uint64        `gorm:"column:db_table_biz_id;not null;index:idx_table_id,priority:4;comment:雪花算法 id"`
	Source            TableSource   `gorm:"column:source;not null;default:0;comment:来源(0 外部数据库 1 文档text2sql)"`
	Name              string        `gorm:"column:table_name;type:varchar(100);not null;comment:表名"`
	TableSchema       string        `gorm:"column:table_schema;type:varchar(100);not null;comment:库名"`
	TableComment      string        `gorm:"column:table_comment;type:varchar(1500);default:'';comment:备注"`
	AliasName         string        `gorm:"column:alias_name;type:varchar(100);default:'';comment:表中文名"`
	Description       string        `gorm:"column:description;type:varchar(500);default:'';comment:表描述"`
	RowCount          int64         `gorm:"column:row_count;default:0;comment:行数"`
	ColumnCount       int           `gorm:"column:column_count;default:0;comment:列数"`
	TableAddedTime    time.Time     `gorm:"column:table_added_time;type:datetime;comment:表被选择加入知识库的时间"`
	TableModifiedTime time.Time     `gorm:"column:table_modified_time;type:datetime;comment:表，或者列更新的时间"`
	LastSyncTime      time.Time     `gorm:"column:last_sync_time;type:datetime;comment:上次同步时间"`
	LearnStatus       DbTableStatus `gorm:"column:learn_status;type:tinyint;not null;default:0;comment:'学习状态(1 未学习 2 学习中 3 已学习 4  学习失败)'"`
	// ReleaseStatus和NextAction仅在评测端有意义
	ReleaseStatus ReleaseStatus `gorm:"column:release_status;type:tinyint;not null;default:0;comment:'发布状态(1 待发布 2 发布中 3 已发布 4 发布失败)'"`
	NextAction    ReleaseAction `gorm:"column:next_action;type:tinyint;not null;comment:'最后操作：1新增 2修改 3删除 4发布'"`
	Alive         bool          `gorm:"column:alive;type:tinyint(1);not null;comment:判断数据表连接是否正常"`
	IsIndexed     bool          `gorm:"column:is_indexed;type:tinyint(1);comment:是否参与索引"`
	PrevIsIndexed bool          `gorm:"column:prev_is_indexed;type:tinyint(1);comment:记录用户设置的是否参与索引"`
	IsDeleted     bool          `gorm:"column:is_deleted;type:tinyint(1);not null;default:0;comment:软删除标记"`
	StaffID       uint64        `gorm:"column:staff_id;not null;default:0;comment:员工ID"`
	CreateTime    time.Time     `gorm:"column:create_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:创建时间"`
	UpdateTime    time.Time     `gorm:"column:update_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:更新时间"`
}

// TableName sets the insert table name for this struct type
func (DBTable) TableName() string {
	return "t_db_table"
}

type ReleaseDBTable struct {
	ID                uint64        `gorm:"column:id;primaryKey;autoIncrement;not null"`
	CorpBizID         uint64        `gorm:"column:corp_biz_id;not null;index:idx_table_id,priority:1;comment:企业业务ID"`
	AppBizID          uint64        `gorm:"column:app_biz_id;not null;index:idx_table_id,priority:2;comment:应用业务ID"`
	DBSourceBizID     uint64        `gorm:"column:db_source_biz_id;not null;index:idx_table_id,priority:3;comment:数据源ID"`
	DBTableBizID      uint64        `gorm:"column:db_table_biz_id;not null;index:idx_table_id,priority:4;comment:雪花算法 id"`
	Source            TableSource   `gorm:"column:source;not null;default:0;comment:来源(0 外部数据库 1 文档text2sql)"`
	Name              string        `gorm:"column:table_name;type:varchar(100);not null;comment:表名"`
	TableSchema       string        `gorm:"column:table_schema;type:varchar(100);not null;comment:库名"`
	TableComment      string        `gorm:"column:table_comment;type:varchar(1500);default:'';comment:备注"`
	AliasName         string        `gorm:"column:alias_name;type:varchar(100);default:'';comment:表中文名"`
	Description       string        `gorm:"column:description;type:varchar(500);default:'';comment:表描述"`
	RowCount          int64         `gorm:"column:row_count;default:0;comment:行数"`
	ColumnCount       int           `gorm:"column:column_count;default:0;comment:列数"`
	TableAddedTime    time.Time     `gorm:"column:table_added_time;type:datetime;comment:表被选择加入知识库的时间"`
	TableModifiedTime time.Time     `gorm:"column:table_modified_time;type:datetime;comment:表，或者列更新的时间"`
	LastSyncTime      time.Time     `gorm:"column:last_sync_time;type:datetime;comment:上次同步时间"`
	ReleaseStatus     ReleaseStatus `gorm:"column:release_status;type:tinyint;not null;default:0;comment:'发布状态(1 待发布 2 发布中 3 已发布 4 发布失败)'"`
	ReleaseBizID      uint64        `gorm:"column:release_biz_id;type:bigint;not null;default:0;comment:'版本ID'"`
	Action            ReleaseAction `gorm:"column:action;type:tinyint;not null;default:0;comment:'操作行为：1新增2修改3删除4发布'"`
	Alive             bool          `gorm:"column:alive;type:tinyint(1);not null;default:1;comment:判断数据表连接是否正常"`
	IsIndexed         bool          `gorm:"column:is_indexed;type:tinyint(1);default:0;comment:是否参与索引"`
	IsDeleted         bool          `gorm:"column:is_deleted;type:tinyint(1);not null;default:0;comment:软删除标记"`
	CreateTime        time.Time     `gorm:"column:create_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:创建时间"`
	UpdateTime        time.Time     `gorm:"column:update_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:更新时间"`
}

func (ReleaseDBTable) TableName() string {
	return "t_release_db_table"
}

type ProdDBTable struct {
	ID                uint64        `gorm:"column:id;primaryKey;autoIncrement;not null"`
	CorpBizID         uint64        `gorm:"column:corp_biz_id;not null;index:idx_table_id,priority:1;comment:企业业务ID"`
	AppBizID          uint64        `gorm:"column:app_biz_id;not null;index:idx_table_id,priority:2;comment:应用业务ID"`
	DBSourceBizID     uint64        `gorm:"column:db_source_biz_id;not null;index:idx_table_id,priority:3;comment:数据源ID"`
	DBTableBizID      uint64        `gorm:"column:db_table_biz_id;not null;index:idx_table_id,priority:4;comment:雪花算法 id"`
	Source            TableSource   `gorm:"column:source;not null;default:0;comment:来源(0 外部数据库 1 文档text2sql)"`
	Name              string        `gorm:"column:table_name;type:varchar(100);not null;comment:表名"`
	TableSchema       string        `gorm:"column:table_schema;type:varchar(100);not null;comment:库名"`
	TableComment      string        `gorm:"column:table_comment;type:varchar(1500);default:'';comment:备注"`
	AliasName         string        `gorm:"column:alias_name;type:varchar(100);default:'';comment:表中文名"`
	Description       string        `gorm:"column:description;type:varchar(500);default:'';comment:表描述"`
	RowCount          int64         `gorm:"column:row_count;default:0;comment:行数"`
	ColumnCount       int           `gorm:"column:column_count;default:0;comment:列数"`
	TableAddedTime    time.Time     `gorm:"column:table_added_time;type:datetime;comment:表被选择加入知识库的时间"`
	TableModifiedTime time.Time     `gorm:"column:table_modified_time;type:datetime;comment:表，或者列更新的时间"`
	LastSyncTime      time.Time     `gorm:"column:last_sync_time;type:datetime;comment:上次同步时间"`
	ReleaseStatus     ReleaseStatus `gorm:"column:release_status;type:tinyint;not null;default:0;comment:'发布状态(1 待发布 2 发布中 3 已发布 4 发布失败)'"`
	Alive             bool          `gorm:"column:alive;type:tinyint(1);not null;default:1;comment:判断数据表连接是否正常"`
	IsDeleted         bool          `gorm:"column:is_deleted;type:tinyint(1);not null;default:0;comment:软删除标记"`
	CreateTime        time.Time     `gorm:"column:create_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:创建时间"`
	UpdateTime        time.Time     `gorm:"column:update_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:更新时间"`
}

func (ProdDBTable) TableName() string {
	return "t_db_table_prod"
}

// ----- column 表 ----

type DBTableColumn struct {
	ID                 int64     `gorm:"column:id;primaryKey;autoIncrement;not null"`
	CorpBizID          uint64    `gorm:"column:corp_biz_id;not null;uniqueIndex:idx_column_id,priority:1;comment:企业业务ID"`
	AppBizID           uint64    `gorm:"column:app_biz_id;not null;uniqueIndex:idx_column_id,priority:2;comment:应用业务ID"`
	DBTableBizID       uint64    `gorm:"column:db_table_biz_id;not null;uniqueIndex:idx_column_id,priority:3;comment:表元数据ID"`
	DBTableColumnBizID uint64    `gorm:"column:db_table_column_biz_id;not null;uniqueIndex:idx_column_id,priority:4;comment:列数据ID"`
	ColumnName         string    `gorm:"column:column_name;type:varchar(100);not null;comment:列名"`
	DataType           string    `gorm:"column:data_type;type:varchar(32);not null;comment:数据类型"`
	ColumnComment      string    `gorm:"column:column_comment;type:varchar(1500);default:'';comment:备注"`
	AliasName          string    `gorm:"column:alias_name;type:varchar(100);default:'';comment:别名"`
	Description        string    `gorm:"column:description;type:varchar(500);default:'';comment:列描述"`
	Unit               string    `gorm:"column:unit;type:varchar(50);default:'';comment:单位"`
	IsIndexed          bool      `gorm:"column:is_indexed;type:tinyint(1);default:0;comment:是否参与索引"`
	IsDeleted          bool      `gorm:"column:is_deleted;type:tinyint(1);not null;default:0;comment:软删除标记"`
	CreateTime         time.Time `gorm:"column:create_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:创建时间"`
	UpdateTime         time.Time `gorm:"column:update_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:更新时间"`
}

// TableName sets the insert table name for this struct type
func (DBTableColumn) TableName() string {
	return "t_db_table_column"
}

type TableInfo struct {
	ColumnInfo   []*ColumnInfo
	TableComment string
	RowCount     int64
	ColumnCount  int
}

type ColumnInfo struct {
	ColumnName string
	DataType   string
	ColComment string
}

type ConnDbSource struct {
	DbType   string
	Host     string
	DbName   string
	Username string
	Password string
	Port     int
}

// ActionDesc 状态描述
func ActionDesc(ctx context.Context, action uint32) string {
	return i18n.Translate(ctx, docActionDesc[action])
}

// DbTableTopValue 外部数据库数据表top value表
type DbTableTopValue struct {
	ID                 uint64    `gorm:"primaryKey;autoIncrement;column:id" json:"id"`
	CorpBizID          uint64    `gorm:"not null;column:corp_biz_id" json:"corpBizId"`                            // 企业业务ID
	AppBizID           uint64    `gorm:"not null;column:app_biz_id" json:"appBizId"`                              // 应用业务ID
	DBSourceBizID      uint64    `gorm:"column:db_source_biz_id" json:"dbSourceBizID"`                            // 数据库业务ID
	DbTableBizID       uint64    `gorm:"not null;column:db_table_biz_id" json:"dbTableBizId"`                     // 数据表业务ID
	DbTableColumnBizID uint64    `gorm:"not null;column:db_table_column_biz_id" json:"dbTableColumnBizId"`        // 数据列业务ID
	BusinessID         uint64    `gorm:"not null;column:business_id" json:"businessId"`                           // 新产生的ID，雪花算法 id
	ColumnName         string    `gorm:"not null;size:100;column:column_name" json:"columnName"`                  // 原始的列名称
	ColumnValue        string    `gorm:"not null;type:text;column:column_value" json:"columnValue"`               // 列别名
	ColumnComment      string    `gorm:"not null;size:1500;column:column_comment" json:"columnComment"`           // 列注释
	IsDeleted          bool      `gorm:"not null;column:is_deleted" json:"isDeleted"`                             // 0:未删除,1:已删除
	CreateTime         time.Time `gorm:"not null;default:CURRENT_TIMESTAMP;column:create_time" json:"createTime"` // 创建时间
	UpdateTime         time.Time `gorm:"not null;default:CURRENT_TIMESTAMP;column:update_time" json:"updateTime"` // 更新时间
}

// TableName 自定义表名
func (DbTableTopValue) TableName() string {
	return "t_db_table_top_value"
}
