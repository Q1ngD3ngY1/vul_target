package user

import (
	"context"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/mysqlquery"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"

	"git.woa.com/adp/kb/kb-config/internal/dao/types"
	"git.woa.com/adp/kb/kb-config/internal/entity"
)

type Dao interface {
	Query() *tdsqlquery.Query
	DescribeUserCountAndList(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.CustUserFilter) ([]*entity.CustUser, int64, error)
	DescribeUserCount(ctx context.Context,
		corpBizID, AppBizID uint64,
		filter *entity.CustUserFilter) (int64, error)
	DescribeUserList(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.CustUserFilter) ([]*entity.CustUser, error)
	CreateUser(ctx context.Context,
		custUserInfo *entity.CustUser, tx *tdsqlquery.Query) (id uint64, err error)
	ModifyUser(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.CustUserFilter,
		custUserInfo *entity.CustUser, tx *tdsqlquery.Query) (int64, error)
	CreateUserRoleList(ctx context.Context,
		userRoleList []*entity.UserRole, tx *tdsqlquery.Query) (err error)
	ModifyUserRole(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.UserRoleFilter,
		userRole *entity.UserRole, tx *tdsqlquery.Query) (int64, error)
	DescribeUserRoleList(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.UserRoleFilter) ([]*entity.UserRole, error)
	ModifyUserConfig(ctx context.Context,
		corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId uint64) error
	CreateKnowledgeRole(ctx context.Context,
		record *entity.KnowledgeRole) error
	ModifyKnowledgeRole(ctx context.Context,
		record *entity.KnowledgeRole) error
	DeleteKnowledgeRole(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.KnowledgeRoleFilter) error
	DescribeKnowledgeRoleList(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.KnowledgeRoleFilter) (int64, []*entity.KnowledgeRole, error)
	CreateKnowledgeRoleKnowList(ctx context.Context,
		records []*entity.KnowledgeRoleKnow, tx *tdsqlquery.Query) error
	DeleteKnowledgeRoleKnow(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.KnowledgeRoleKnowFilter, tx *tdsqlquery.Query) error
	DescribeKnowledgeRoleKnowList(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.KnowledgeRoleKnowFilter) ([]*entity.KnowledgeRoleKnow, error)
	ModifyKnowledgeRoleKnow(ctx context.Context,
		req *entity.KnowledgeRoleKnow, tx *tdsqlquery.Query) (int, error)
	CreateKnowledgeRoleDocList(ctx context.Context,
		records []*entity.KnowledgeRoleDoc, tx *tdsqlquery.Query) error
	DeleteKnowledgeRoleDocList(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.KnowledgeRoleDocFilter, tx *tdsqlquery.Query) error
	DescribeRoleIDListByDocBiz(ctx context.Context,
		appBizId, docBizId uint64, batchSize int) ([]uint64, error)
	DescribeKnowledgeRoleDocList(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.KnowledgeRoleDocFilter) ([]*entity.KnowledgeRoleDoc, error)
	CreateKnowledgeRoleQAList(ctx context.Context,
		records []*entity.KnowledgeRoleQA, tx *tdsqlquery.Query) error
	DeleteKnowledgeRoleQAList(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.KnowledgeRoleQAFilter, tx *tdsqlquery.Query) error
	DescribeKnowledgeRoleQAList(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.KnowledgeRoleQAFilter) ([]*entity.KnowledgeRoleQA, error)
	CreateKnowledgeRoleAttributeLabelList(ctx context.Context,
		records []*entity.KnowledgeRoleAttributeLabel, tx *tdsqlquery.Query) error
	DeleteKnowledgeRoleAttributeLabelList(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.KnowledgeRoleAttributeLabelFilter, tx *tdsqlquery.Query) (int64, error)
	DescribeKnowledgeRoleAttributeLabelList(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.KnowledgeRoleAttributeLabelFilter) ([]*entity.KnowledgeRoleAttributeLabel, error)
	DeleteKnowledgeRoleAttributeLabelByAttrAndLabelBizIDs(ctx context.Context,
		knowBizID uint64, attrBizIDs, labelBizIDs []uint64, pageSize, batchSize int) error
	CreateKnowledgeRoleCateList(ctx context.Context,
		records []*entity.KnowledgeRoleCate, tx *tdsqlquery.Query) error
	DeleteKnowledgeRoleCateList(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.KnowledgeRoleCateFilter, tx *tdsqlquery.Query) (int64, error)
	DescribeKnowledgeRoleCateList(ctx context.Context,
		corpBizID, appBizID uint64, filter *entity.KnowledgeRoleCateFilter) ([]*entity.KnowledgeRoleCate, error)
	DeleteRoleCateListByKnowAndCateBizID(ctx context.Context,
		knowBizID, cateBizID uint64, batchSize int) error
	CreateKnowledgeRoleDatabaseList(ctx context.Context,
		records []*entity.KnowledgeRoleDatabase, tx *tdsqlquery.Query) error
	DeleteKnowledgeRoleDatabaseList(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.KnowledgeRoleDatabaseFilter, tx *tdsqlquery.Query) (int64, error)
	DeleteKnowledgeRoleDbTables(ctx context.Context,
		knowBizID uint64, dbTableBizIDs []uint64, pageSize, batchSize int) error
	DescribeKnowledgeRoleDatabaseList(ctx context.Context,
		corpBizID, appBizID uint64,
		filter *entity.KnowledgeRoleDatabaseFilter) ([]*entity.KnowledgeRoleDatabase, error)
	DescribeRoleByDbBiz(ctx context.Context, knowBizID, dbBizId uint64, batchSize int) ([]uint64, error)
	DeleteKnowledgeAssociation(ctx context.Context,
		corpBizID, appBizID uint64, knowledgeBizIds []uint64) error
	ModifyRoleKnowledgeByAttrChange(ctx context.Context,
		knowBizID uint64, attrBizIds []uint64, labelBizIds []uint64) func() error
	ModifyRoleKnowledgeByCate(ctx context.Context,
		corpBizID, knowBizID uint64, cateBizIDs []uint64) error
	DescribeUserByID(ctx context.Context, id uint64) (*entity.User, error)
	DescribeSIUser(ctx context.Context, sid uint64, loginUin, loginSubAccountUin string) (*entity.User, error)
	DescribeExpUser(ctx context.Context, id uint64) (*entity.User, error)
	DescribeExpUserList(ctx context.Context, ids []uint64) ([]*entity.User, error)
	ModifyThirdUserIDCache(ctx context.Context,
		appBizID uint64, thirdUserID string, roleBizIDs []uint64) error
	DescribeThirdUserIDCache(ctx context.Context,
		appBizID uint64, thirdUserID string) ([]uint64, error)
	ModifyThirdUserIDListCache(ctx context.Context,
		appBizID uint64, thirdUserIDs []string, roleBizIDs []uint64) error
	DeleteThirdUserIDCache(ctx context.Context,
		appBizID uint64, thirdUserIDs []string) error
	ModifyUserConfigCache(ctx context.Context,
		appBizID, notSet, notUse uint64) error
	DescribeUserConfigCache(ctx context.Context,
		appBizID uint64) (uint64, uint64, error)
	DescribeKnowBizID2FilterCache(ctx context.Context,
		corpBizID, appBizID, roleBizID uint64) (bool, map[string]*retrieval.LabelExpression, error)
	ModifyKnowBizID2FilterCache(ctx context.Context,
		corpBizID, appBizID, roleBizID uint64, knowBizID2Filter map[uint64]*retrieval.LabelExpression) error
	ModifyRoleChooseAllCache(ctx context.Context,
		corpBizID, appBizID, roleBizID uint64) error
	DeleteKnowBizID2FilterCache(ctx context.Context,
		corpBizID, appBizID, roleBizID, knowBizID uint64) error
}

func NewDao(tdsql types.TDSQLDB, mysql types.MySQLDB, rdb types.AdminRedis) Dao {
	return &daoImpl{
		tdsql: tdsqlquery.Use(tdsql),
		mysql: mysqlquery.Use(mysql),
		rdb:   rdb,
	}
}

type daoImpl struct {
	tdsql *tdsqlquery.Query
	mysql *mysqlquery.Query
	rdb   types.AdminRedis
}

func (d *daoImpl) Query() *tdsqlquery.Query {
	return d.tdsql
}
