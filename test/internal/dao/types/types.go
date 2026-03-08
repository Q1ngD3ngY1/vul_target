package types

import (
	elasticv8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"git.code.oa.com/trpc-go/trpc-database/mysql"
)

type (
	MySQLDB        *gorm.DB
	MySQLClient    mysql.Client
	TDSQLDB        *gorm.DB
	Tex2sqlDB      *gorm.DB
	KbDeleteDB     *gorm.DB
	AdminRedis     redis.UniversalClient
	RetrievalRedis redis.UniversalClient
	ESClient       *elasticv8.TypedClient
)
