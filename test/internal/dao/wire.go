package dao

import (
	"time"

	"git.woa.com/adp/kb/kb-config/internal/dao/llm"
	third_doc_dao "git.woa.com/adp/kb/kb-config/internal/dao/third_document"

	tgorm "git.code.oa.com/trpc-go/trpc-database/gorm"
	"git.code.oa.com/trpc-go/trpc-database/mysql"
	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/clientx/s3x"
	"git.woa.com/trpc-go/trpc-database/goes"
	"git.woa.com/trpc-go/trpc-database/goredis/v3"
	elasticv8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/google/wire"

	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/dao/category"
	"git.woa.com/adp/kb/kb-config/internal/dao/database"
	doc "git.woa.com/adp/kb/kb-config/internal/dao/document"
	"git.woa.com/adp/kb/kb-config/internal/dao/export"
	"git.woa.com/adp/kb/kb-config/internal/dao/kb"
	"git.woa.com/adp/kb/kb-config/internal/dao/label"
	"git.woa.com/adp/kb/kb-config/internal/dao/qa"
	"git.woa.com/adp/kb/kb-config/internal/dao/release"
	"git.woa.com/adp/kb/kb-config/internal/dao/segment"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	"git.woa.com/adp/kb/kb-config/internal/dao/user"
	"git.woa.com/adp/kb/kb-config/internal/dao/vector"
)

var (
	ProviderSet = wire.NewSet(
		DocSet,
		QASet,
		SegmentSet,
		VectorSet,
		ReleaseSet,
		KBSet,
		LableSet,
		CategorySet,
		UserSet,
		ExportSet,
		LlmSet,
		ThirdDocSet,

		S3Set,
		NewMySQL,
		NewTDSQL,
		NewText2sqlDB,
		NewMySQLClient,
		NewKbDeleteClient,
		NewAdminRedis,
		NewRetrievalRedis,
		NewElasticSearchClient,
	)

	S3Set = wire.NewSet(NewS3, s3x.New, NewS3xConfig)

	DocSet = wire.NewSet(
		doc.NewDao,
		DatabaseSet,
	)

	QASet = wire.NewSet(
		qa.NewDao,
	)
	SegmentSet = wire.NewSet(
		segment.NewDao,
	)

	VectorSet = wire.NewSet(
		vector.NewDao,
	)

	ReleaseSet = wire.NewSet(
		release.NewDao,
	)

	KBSet = wire.NewSet(
		kb.NewDao,
	)

	CategorySet = wire.NewSet(
		category.NewDao,
	)

	UserSet = wire.NewSet(
		user.NewDao,
	)

	LableSet = wire.NewSet(
		label.NewDao,
	)

	DatabaseSet = wire.NewSet(
		database.NewDao,
	)

	ExportSet = wire.NewSet(
		export.NewDao,
	)

	LlmSet = wire.NewSet(
		llm.NewDao,
	)

	ThirdDocSet = wire.NewSet(
		third_doc_dao.NewOnedriveDao,
	)
)

func NewS3xConfig() *s3x.Config {
	c := config.App().Storage
	return &c.Config
}

func NewKbDeleteClient() types.KbDeleteDB {
	gormDBDelete, err := tgorm.NewClientProxy("mysql.qbot.admin.delete")
	if err != nil {
		panic(err)
	}
	if config.App().GormDebug {
		gormDBDelete = gormDBDelete.Debug()
	}
	return gormDBDelete
}

func NewMySQLClient() types.MySQLClient {
	return mysql.NewClientProxy("mysql.qbot.admin")
}

func NewText2sqlDB() types.Tex2sqlDB {
	gormDB, err := tgorm.NewClientProxy("mysql.db_text2sql")
	if err != nil {
		panic(err)
	}
	if config.App().GormDebug {
		gormDB = gormDB.Debug()
	}
	return gormDB
}

func NewMySQL() types.MySQLDB {
	gormDB, err := tgorm.NewClientProxy("mysql.qbot.admin")
	if err != nil {
		panic(err)
	}
	if config.App().GormDebug {
		gormDB = gormDB.Debug()
	}
	return gormDB
}

func NewTDSQL() types.TDSQLDB {
	gormDB, err := tgorm.NewClientProxy("tdsql.qbot.qbot")
	if err != nil {
		panic(err)
	}
	if config.App().GormDebug {
		gormDB = gormDB.Debug()
	}
	return gormDB
}

func NewAdminRedis() types.AdminRedis {
	cli, err := goredis.New("redis.qbot.admin")
	if err != nil {
		panic(err)
	}
	return cli
}

func NewRetrievalRedis() types.RetrievalRedis {
	cli, err := goredis.New("redis.qbot.retrieval.config")
	if err != nil {
		panic(err)
	}
	return cli
}

func NewElasticSearchClient() types.ESClient {
	const minTimeout = 3000 * time.Millisecond
	timeout := time.Duration(config.App().ElasticSearchConfig.Timeout) * time.Millisecond
	if timeout < minTimeout {
		timeout = minTimeout
	}
	// 这里并没有使用trpc的goes客户端，不过使用了其Transport用于发送消息，可以实现有trace链路
	cli, err := elasticv8.NewTypedClient(elasticv8.Config{
		Addresses: []string{config.App().ElasticSearchConfig.URL},
		Username:  config.App().ElasticSearchConfig.User,
		Password:  config.App().ElasticSearchConfig.Password,
		Transport: goes.NewClientProxy(config.App().ElasticSearchConfig.Name, client.WithTimeout(timeout)),
		Logger:    nil,
	})
	if err != nil {
		log.Errorf("EsTypedClient|NewElasticTypedClientV8|err: %v", err)
		panic(err)
	}
	log.Infof("EsTypedClient|NewElasticTypedClientV8|cli: %+v", cli)
	return cli
}
