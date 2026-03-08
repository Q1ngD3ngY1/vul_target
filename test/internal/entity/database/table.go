package database

import (
	"time"
)

const (
	TableSourceDB  = uint32(0)
	TableSourceDoc = uint32(1)
)

// Table 数据库信息表，发布端和评测端共用一个结构体，发布端需制定table_name，不指定table name的时候默认操作评测端
type Table struct {
	ID                uint64    `gorm:"column:id;primaryKey;autoIncrement;not null"`
	CorpBizID         uint64    `gorm:"column:corp_biz_id;not null;index:idx_table_id,priority:1;comment:企业业务ID"`
	AppBizID          uint64    `gorm:"column:app_biz_id;not null;index:idx_table_id,priority:2;comment:应用业务ID"`
	DBSourceBizID     uint64    `gorm:"column:db_source_biz_id;not null;index:idx_table_id,priority:3;comment:数据源ID"`
	DBTableBizID      uint64    `gorm:"column:db_table_biz_id;not null;index:idx_table_id,priority:4;comment:雪花算法 id"`
	Source            uint32    `gorm:"column:source;not null;default:0;comment:来源(0 外部数据库 1 文档text2sql)"`
	Name              string    `gorm:"column:table_name;type:varchar(100);not null;comment:表名"`
	TableSchema       string    `gorm:"column:table_schema;type:varchar(100);not null;comment:库名"`
	TableComment      string    `gorm:"column:table_comment;type:varchar(1500);default:'';comment:备注"`
	AliasName         string    `gorm:"column:alias_name;type:varchar(100);default:'';comment:表中文名"`
	Description       string    `gorm:"column:description;type:varchar(500);default:'';comment:表描述"`
	RowCount          int64     `gorm:"column:row_count;default:0;comment:行数"`
	ColumnCount       int       `gorm:"column:column_count;default:0;comment:列数"`
	TableAddedTime    time.Time `gorm:"column:table_added_time;type:datetime;comment:表被选择加入知识库的时间"`
	TableModifiedTime time.Time `gorm:"column:table_modified_time;type:datetime;comment:表，或者列更新的时间"`
	LastSyncTime      time.Time `gorm:"column:last_sync_time;type:datetime;comment:上次同步时间"`
	LearnStatus       uint32    `gorm:"column:learn_status;type:tinyint;not null;default:0;comment:'学习状态(1 未学习 2 学习中 3 已学习 4  学习失败)'"`
	// ReleaseStatus和NextAction仅在评测端有意义
	ReleaseStatus uint32    `gorm:"column:release_status;type:tinyint;not null;default:0;comment:'发布状态(1 待发布 2 发布中 3 已发布 4 发布失败)'"`
	NextAction    uint32    `gorm:"column:next_action;type:tinyint;not null;comment:'最后操作：1新增 2修改 3删除 4发布'"`
	Alive         bool      `gorm:"column:alive;type:tinyint(1);not null;comment:判断数据表连接是否正常"`
	IsIndexed     bool      `gorm:"column:is_indexed;type:tinyint(1);comment:是否参与索引"`
	PrevIsIndexed bool      `gorm:"column:prev_is_indexed;type:tinyint(1);comment:记录用户设置的是否参与索引"`
	IsDeleted     bool      `gorm:"column:is_deleted;type:tinyint(1);not null;default:0;comment:软删除标记"`
	StaffID       uint64    `gorm:"column:staff_id;not null;default:0;comment:员工ID"`
	CreateTime    time.Time `gorm:"column:create_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:创建时间"`
	UpdateTime    time.Time `gorm:"column:update_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:更新时间"`
	EnableScope   uint32    `gorm:"column:enable_scope;type:tinyint;not null;default:0;comment:检索生效范围"`

	Columns   []*Column
	Database  *Database // 关联的数据库信息
	StaffName string    // 员工名称
}

type TableFilter struct {
	Select []string // 查询哪些字段

	CorpBizID      uint64
	AppBizID       uint64
	AppBizIDs      []uint64
	DBSourceBizID  uint64
	DBSourceBizIDs []uint64
	DBTableBizID   uint64
	DBTableBizIDs  []uint64
	IsDeleted      *bool // 默认是 0，如果要查删除的
	LearnStatus    *uint32
	Name           *string // 精确查表名
	EnableScope    *uint32

	PageNumber *uint32
	PageSize   uint32

	RobotID  uint64
	NameLike *string

	WithColumn    bool // 是否携带列信息
	WithDatabase  bool // 是否携带数据库信息
	WithStaffName bool // 是否携带员工名称
}

type TableRelease struct {
	ID                uint64    `gorm:"column:id;primaryKey;autoIncrement;not null"`
	CorpBizID         uint64    `gorm:"column:corp_biz_id;not null;index:idx_table_id,priority:1;comment:企业业务ID"`
	AppBizID          uint64    `gorm:"column:app_biz_id;not null;index:idx_table_id,priority:2;comment:应用业务ID"`
	DBSourceBizID     uint64    `gorm:"column:db_source_biz_id;not null;index:idx_table_id,priority:3;comment:数据源ID"`
	DBTableBizID      uint64    `gorm:"column:db_table_biz_id;not null;index:idx_table_id,priority:4;comment:雪花算法 id"`
	Source            uint32    `gorm:"column:source;not null;default:0;comment:来源(0 外部数据库 1 文档text2sql)"`
	Name              string    `gorm:"column:table_name;type:varchar(100);not null;comment:表名"`
	TableSchema       string    `gorm:"column:table_schema;type:varchar(100);not null;comment:库名"`
	TableComment      string    `gorm:"column:table_comment;type:varchar(1500);default:'';comment:备注"`
	AliasName         string    `gorm:"column:alias_name;type:varchar(100);default:'';comment:表中文名"`
	Description       string    `gorm:"column:description;type:varchar(500);default:'';comment:表描述"`
	RowCount          int64     `gorm:"column:row_count;default:0;comment:行数"`
	ColumnCount       int       `gorm:"column:column_count;default:0;comment:列数"`
	TableAddedTime    time.Time `gorm:"column:table_added_time;type:datetime;comment:表被选择加入知识库的时间"`
	TableModifiedTime time.Time `gorm:"column:table_modified_time;type:datetime;comment:表，或者列更新的时间"`
	LastSyncTime      time.Time `gorm:"column:last_sync_time;type:datetime;comment:上次同步时间"`
	ReleaseStatus     uint32    `gorm:"column:release_status;type:tinyint;not null;default:0;comment:'发布状态(1 待发布 2 发布中 3 已发布 4 发布失败)'"`
	ReleaseBizID      uint64    `gorm:"column:release_biz_id;type:bigint;not null;default:0;comment:'版本ID'"`
	Action            uint32    `gorm:"column:action;type:tinyint;not null;default:0;comment:'操作行为：1新增2修改3删除4发布'"`
	Alive             bool      `gorm:"column:alive;type:tinyint(1);not null;default:(-);comment:判断数据表连接是否正常"`
	IsIndexed         bool      `gorm:"column:is_indexed;type:tinyint(1);default:(-);comment:是否参与索引"`
	IsDeleted         bool      `gorm:"column:is_deleted;type:tinyint(1);not null;default:0;comment:软删除标记"`
	CreateTime        time.Time `gorm:"column:create_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:创建时间"`
	UpdateTime        time.Time `gorm:"column:update_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:更新时间"`
}

type TableProd struct {
	ID                uint64    `gorm:"column:id;primaryKey;autoIncrement;not null"`
	CorpBizID         uint64    `gorm:"column:corp_biz_id;not null;index:idx_table_id,priority:1;comment:企业业务ID"`
	AppBizID          uint64    `gorm:"column:app_biz_id;not null;index:idx_table_id,priority:2;comment:应用业务ID"`
	DBSourceBizID     uint64    `gorm:"column:db_source_biz_id;not null;index:idx_table_id,priority:3;comment:数据源ID"`
	DBTableBizID      uint64    `gorm:"column:db_table_biz_id;not null;index:idx_table_id,priority:4;comment:雪花算法 id"`
	Source            uint32    `gorm:"column:source;not null;default:0;comment:来源(0 外部数据库 1 文档text2sql)"`
	Name              string    `gorm:"column:table_name;type:varchar(100);not null;comment:表名"`
	TableSchema       string    `gorm:"column:table_schema;type:varchar(100);not null;comment:库名"`
	TableComment      string    `gorm:"column:table_comment;type:varchar(1500);default:'';comment:备注"`
	AliasName         string    `gorm:"column:alias_name;type:varchar(100);default:'';comment:表中文名"`
	Description       string    `gorm:"column:description;type:varchar(500);default:'';comment:表描述"`
	RowCount          int64     `gorm:"column:row_count;default:0;comment:行数"`
	ColumnCount       int       `gorm:"column:column_count;default:0;comment:列数"`
	TableAddedTime    time.Time `gorm:"column:table_added_time;type:datetime;comment:表被选择加入知识库的时间"`
	TableModifiedTime time.Time `gorm:"column:table_modified_time;type:datetime;comment:表，或者列更新的时间"`
	LastSyncTime      time.Time `gorm:"column:last_sync_time;type:datetime;comment:上次同步时间"`
	ReleaseStatus     uint32    `gorm:"column:release_status;type:tinyint;not null;default:0;comment:'发布状态(1 待发布 2 发布中 3 已发布 4 发布失败)'"`
	Alive             bool      `gorm:"column:alive;type:tinyint(1);not null;default:1;comment:判断数据表连接是否正常"`
	IsDeleted         bool      `gorm:"column:is_deleted;type:tinyint(1);not null;default:0;comment:软删除标记"`
	CreateTime        time.Time `gorm:"column:create_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:创建时间"`
	UpdateTime        time.Time `gorm:"column:update_time;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:更新时间"`
}

type TableInfo struct {
	ColumnInfo   []*ColumnInfo
	TableComment string
	RowCount     int64
	ColumnCount  int
}
