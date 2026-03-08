package export

import (
	"context"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	"git.woa.com/adp/kb/kb-config/internal/entity"
)

//go:generate mockgen -source interface.go -destination interface_mock.go -package export

type Dao interface {
	MysqlQuery() *mysqlquery.Query

	CreateExportTask(ctx context.Context, export *entity.Export) (uint64, error)
	ModifyExportTask(ctx context.Context, export *entity.Export) error
	DescribeExportTask(ctx context.Context, taskID, corpID, robotID uint64) (*entity.Export, error)
}

func NewDao(mysqlDB types.MySQLDB) Dao {
	return &daoImpl{
		mysql: mysqlquery.Use(mysqlDB),
	}
}

type daoImpl struct {
	mysql *mysqlquery.Query
}

func (d *daoImpl) MysqlQuery() *mysqlquery.Query {
	return d.mysql
}
