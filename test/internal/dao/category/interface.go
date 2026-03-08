package category

import (
	"context"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	cateEntity "git.woa.com/adp/kb/kb-config/internal/entity/category"
	"gorm.io/gorm"
)

//go:generate mockgen -source interface.go -destination interface_mock.go -package category

type Dao interface {
	DescribeCateStat(ctx context.Context, t cateEntity.CateObjectType, corpID, robotID uint64) (map[uint64]uint32, error)
	CreateCate(ctx context.Context, t cateEntity.CateObjectType, cate *cateEntity.CateInfo) (uint64, error)
	ModifyCate(ctx context.Context, t cateEntity.CateObjectType, id uint64, name string) error
	DeleteCate(ctx context.Context, t cateEntity.CateObjectType, cateIDs []uint64, uncategorizedCateID uint64, robotID uint64) error
	DeleteCateById(ctx context.Context, t cateEntity.CateObjectType, kbPrimaryId uint64) error
	DescribeCateList(ctx context.Context, t cateEntity.CateObjectType, corpID, robotID uint64) ([]*cateEntity.CateInfo, error)
	DescribeCateByID(ctx context.Context, t cateEntity.CateObjectType, id, corpID, robotID uint64) (*cateEntity.CateInfo, error)
	DescribeCateByIDs(ctx context.Context, t cateEntity.CateObjectType, ids []uint64) (map[uint64]*cateEntity.CateInfo, error)
	DescribeCateByBusinessID(ctx context.Context, t cateEntity.CateObjectType, cateBizID, corpID, robotID uint64) (*cateEntity.CateInfo, error)
	DescribeCateListByBusinessIDs(ctx context.Context, t cateEntity.CateObjectType, corpID, robotID uint64, cateBizIDs []uint64) (map[uint64]*cateEntity.CateInfo, error)
	DescribeRobotUncategorizedCateID(ctx context.Context, t cateEntity.CateObjectType, corpID, robotID uint64) (uint64, error)
	DescribeQACateBusinessIDByID(ctx context.Context, id, corpID, robotID uint64) (uint64, error)
	DescribeCateListByParent(ctx context.Context, t cateEntity.CateObjectType, corpID, robotID, parentCatePrimaryId uint64,
		pageNumber, pageSize int) ([]*cateEntity.CateInfo, error)
	VerifyCateBiz(ctx context.Context, t cateEntity.CateObjectType, corpID, cateBizID, robotID uint64) (uint64, error)
	VerifyCate(ctx context.Context, t cateEntity.CateObjectType, corpID, cateID, robotID uint64) error
	DescribeCateChildrenIDs(ctx context.Context, t cateEntity.CateObjectType, corpID, cateID, robotID uint64) ([]uint64, error)
	CreateCateTree(ctx context.Context, t cateEntity.CateObjectType, corpID, robotID uint64, tree *cateEntity.CateNode) error
	ModifyCateCache(ctx context.Context, t cateEntity.CateObjectType, corpID, appID uint64) (map[int][]int, error)
	DescribeCateCache(ctx context.Context, t cateEntity.CateObjectType, corpID, appID uint64) (map[int][]int, error)
	GroupCateObject(ctx context.Context, t cateEntity.CateObjectType, ids []uint64, cateID uint64, app *entity.App) error
	InitDefaultCategory(ctx context.Context, t cateEntity.CateObjectType, corpID, robotID uint64) error
	GetDocCategoryByCursor(ctx context.Context, corpID, robotID uint64, lastID uint64, limit int, db *gorm.DB) ([]*cateEntity.CateInfo, error)
	GetQCategoryByCursor(ctx context.Context, corpID, robotID uint64, lastID uint64, limit int, db *gorm.DB) ([]*cateEntity.CateInfo, error)
}

func NewDao(mysql types.MySQLDB, rdb types.AdminRedis) Dao {
	return &daoImpl{
		mysql: mysqlquery.Use(mysql),
		rdb:   rdb,
	}
}

type daoImpl struct {
	mysql *mysqlquery.Query
	rdb   types.AdminRedis
}
