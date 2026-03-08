package permissions

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/baicaoyuan/moss/types/slicex"
	"git.woa.com/baicaoyuan/moss/utils"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/client"
	bconfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
	redisV8 "github.com/go-redis/redis/v8"
	"github.com/spf13/cast"
	"gorm.io/gorm"
)

// SyncInfoTypeDoc 文档类型
const (
	SyncInfoTypeDoc       string = "doc"        // 文档类型
	SyncInfoTypeQA        string = "qa"         // 问答类型
	SyncInfoTypeDocCate   string = "doc_cate"   // 文档分类类型
	SyncInfoTypeQaCate    string = "qa_cate"    // 问答分类类型
	SyncInfoTypeKnowledge string = "knowledge"  // 知识库类型
	SyncInfoTypeAttr      string = "attr_label" // 标签
	SyncInfoTypeBase      string = "base"       // 基础知识库类型
	SyncInfoTypeDatabase  string = "database"   // 数据库类型

)

const LableNotFound = "not found"
const KnowledgeSearchAll = "all"
const PrsetRoleLockKey = "preset_role_lock_%d"
const ModifyRoleLockKey = "modify_role_lock_%d"

const RoleCacheExpire = 3600 * 24 * 14 * time.Second

// 搜索类型常量
const (
	SearchTypeDoc      = 1 // 文档搜索类型
	SearchTypeDocCate  = 2 // 文档分类搜索类型
	SearchTypeQA       = 3 // 问答搜索类型
	SearchTypeQACate   = 4 // 问答分类搜索类型
	SearchTypeDatabase = 5 // 数据库搜索类型
)

type LogicRoler interface {
	GetTdsqlGormDB() *gorm.DB
	GenerateSeqID() uint64
	GetAttributeByBizIDs(ctx context.Context, robotID uint64, ids []uint64) (map[uint64]*model.Attribute, error)
	GetAttributeLabelByBizIDs(ctx context.Context, ids []uint64, robotID uint64) (map[uint64]*model.AttributeLabel, error)
	GetCateByIDs(ctx context.Context, t model.CateObjectType, ids []uint64) (map[uint64]*model.CateInfo, error)
	GetQAList(ctx context.Context, req *model.QAListReq) ([]*model.DocQA, error)
	GetDocByBizIDs(ctx context.Context, bizIDs []uint64, robotID uint64) (map[uint64]*model.Doc, error)
	GetCateListByBusinessIDs(ctx context.Context, t model.CateObjectType, corpID, robotID uint64, cateBizIDs []uint64) (
		map[uint64]*model.CateInfo, error)
	GetQAsByBizIDs(
		ctx context.Context, corpID, robotID uint64, qaBizIDs []uint64, offset, limit uint64,
	) ([]*model.DocQA, error)
	// BatchUpdateVector(ctx context.Context, qaIds []uint64, docTaskParams []*model.DocModifyParams) error
	RetrieveBaseSharedKnowledge(ctx context.Context, corpBizID uint64, knowledgeBizIDList []uint64) (
		[]*model.SharedKnowledgeInfo, error)

	// RedisCli() redis.Client
	GlobalRedisCli(ctx context.Context) (redisV8.UniversalClient, error)
	// Lock 加锁
	Lock(ctx context.Context, key string, duration time.Duration) error
	// UnLock 解锁
	UnLock(ctx context.Context, key string) error
	GetAppByAppBizID(ctx context.Context, bID uint64) (*model.AppDB, error)
}

type LogicRole struct {
	LogicRoler
}

// NewLogicRole 创建新的 LogicRole 实例
func NewLogicRole(l LogicRoler) *LogicRole {
	return &LogicRole{
		LogicRoler: l,
	}
}

type SyncInfo struct {
	BizID          uint64 // 文档ID 问答ID 分类ID
	Type           string
	SearchType     int8 // 知识库搜索类型
	LibType        int8 //  知识库类型
	LabelCondition int8 //  知识库标签操作符
	KnowledgeBizID uint64
	AttrBizID      uint64
	AttrLableBizID uint64
	DatabaseBizID  uint64 // 数据库ID
}

func (s *SyncInfo) String() string {
	switch s.Type {
	case SyncInfoTypeDoc, SyncInfoTypeQA, SyncInfoTypeDocCate, SyncInfoTypeQaCate:
		return fmt.Sprintf("%s: %d", s.Type, s.BizID)
	case SyncInfoTypeKnowledge:
		return fmt.Sprintf("%s: %d", s.Type, s.KnowledgeBizID)
	case SyncInfoTypeAttr:
		return fmt.Sprintf("%s: %d-%d", s.Type, s.AttrBizID, s.AttrLableBizID)
	case SyncInfoTypeBase:
		return fmt.Sprintf("%s: %d-%d-%d", s.Type, s.KnowledgeBizID, s.LibType, s.SearchType)
	case SyncInfoTypeDatabase:
		return fmt.Sprintf("%s: %d", s.Type, s.DatabaseBizID)
	default:
		return fmt.Sprintf("%s: %d", s.Type, s.BizID)
	}
}

// CheckRoleExist 检查角色是否存在
func (l *LogicRole) CheckRoleExist(ctx context.Context, appBizID uint64, roleBizID uint64, name string) (bool, error) {
	corpBizID := pkg.CorpBizID(ctx)
	client := dao.GetRoleDao(l.GetTdsqlGormDB())
	_, res, err := client.ListKnowledgeRoles(ctx, &dao.KnowledgeRoleReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
		},
		BusinessID: roleBizID,
		Name:       name,
	})
	if err != nil {
		log.ErrorContextf(ctx, "CheckRoleExist failed, err: %v", err)
		return false, err
	}
	return len(res) > 0, nil
}

// CheckDeleteRole 检查角色是否可删除
func (l *LogicRole) CheckDeleteRole(ctx context.Context, appBizID uint64, roleBizIDs []uint64) ([]*model.KnowledgeRole, error) {
	client := dao.GetRoleDao(l.GetTdsqlGormDB())
	custUserClient := dao.GetCustUserDao(l.GetTdsqlGormDB())
	corpBizID := pkg.CorpBizID(ctx)
	res, err := custUserClient.GetUserRoleList(ctx, nil, &dao.UserRoleFilter{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
			IsDeleted: pkg.GetIntPtr(dao.IsNotDeleted),
		},
		RoleBizIds: roleBizIDs,
	})
	if err != nil {
		log.ErrorContextf(ctx, "CheckDeleteRole failed, err: %v", err)
		return nil, err
	}
	roleBizIDsTmp := make([]uint64, 0, len(res))
	for _, v := range res {
		roleBizIDsTmp = append(roleBizIDsTmp, uint64(v.RoleBizID))
	}
	log.InfoContextf(ctx, "CheckDeleteRole roleBizs:%d roleBizIDsTmp: %v", roleBizIDs, roleBizIDsTmp)
	if len(roleBizIDsTmp) == 0 {
		return nil, nil
	}
	_, roles, err := client.ListKnowledgeRoles(ctx, &dao.KnowledgeRoleReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
		},
		BizIDs: roleBizIDsTmp,
		Type:   model.KnowledgeRoleTypeCustom,
	})
	if err != nil {
		log.ErrorContextf(ctx, "ListKnowledgeRoles failed, err: %v", err)
		return nil, err
	}

	return roles, nil
}

// CreateRole 创建新角色
func (l *LogicRole) CreateRole(ctx context.Context, req *pb.CreateRoleReq, bizID uint64, knowType int) (bizId uint64, syncInfos []*SyncInfo, err error) {
	syncInfos = make([]*SyncInfo, 0, len(req.GetKnowChoose()))
	tx := l.GetTdsqlGormDB().Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		if err1 := tx.Commit().Error; err1 != nil {
			err = err1
			tx.Rollback()
			return
		}
	}()
	client := dao.GetRoleDao(tx)
	role := &model.KnowledgeRole{
		CorpBizID:   pkg.CorpBizID(ctx),
		AppBizID:    cast.ToUint64(req.GetAppBizId()),
		BusinessID:  bizID,
		Name:        req.GetName(),
		Type:        int8(knowType),
		SearchType:  int8(req.GetSearchType()),
		CreateTime:  time.Now(),
		UpdateTime:  time.Now(),
		Description: req.GetDescription(),
	}
	_, oldOnes, err := client.ListKnowledgeRoles(ctx, &dao.KnowledgeRoleReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: pkg.CorpBizID(ctx),
			AppBizID:  cast.ToUint64(req.GetAppBizId()),
		},
		BusinessID: bizID,
	})
	if err == nil && len(oldOnes) > 0 { //更新
		role.ID = oldOnes[0].ID
		if err := client.UpdateKnowledgeRole(ctx, role); err != nil {
			log.ErrorContextf(ctx, "UpdateRole failed, err: %v", err)
			return 0, nil, err
		}
		return role.BusinessID, nil, nil
	} else { // 创建
		if err := client.CreateKnowledgeRole(ctx, role); err != nil {
			log.ErrorContextf(ctx, "CreateRole failed, err: %v", err)
			return 0, nil, err
		}
	}
	return role.BusinessID, nil, nil
}

// RemoveRoleAssociation 移除角色关联关系
func (l *LogicRole) RemoveRoleAssociation(ctx context.Context, appBizID, roleBizId uint64) error {
	client := dao.GetRoleDao(l.GetTdsqlGormDB())
	// 删除知识库关联
	corpBizID := pkg.CorpBizID(ctx)
	knowledgeIds := make([]uint64, 0, 3)
	knows, err := client.ListKnowledgeRoleKnow(ctx, &dao.KnowledgeRoleKnowReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
		},
		RoleBizID: roleBizId,
	})
	if err != nil {
		log.ErrorContextf(ctx, "ListKnowledgeRoleKnow failed, err: %v", err)
		return err
	}
	for _, v := range knows {
		knowledgeIds = append(knowledgeIds, v.KnowledgeBizID)
	}

	if err := client.DeleteKnowledgeRoleKnow(ctx, &dao.KnowledgeRoleKnowReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
		},
		RoleBizID: roleBizId,
	}); err != nil {
		log.ErrorContextf(ctx, "DeleteKnowledgeRoleKnow failed, err: %v", err)
		return err
	}

	// 删除文档关联
	if err := client.DeleteKnowledgeRoleDoc(ctx, &dao.KnowledgeRoleDocReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
		},
		RoleBizID:       roleBizId,
		KnowledgeBizIDs: knowledgeIds,
	}); err != nil {
		log.ErrorContextf(ctx, "DeleteKnowledgeRoleDoc failed, err: %v", err)
		return err
	}

	// 删除问答关联
	if err := client.DeleteKnowledgeRoleQA(ctx, &dao.KnowledgeRoleQAReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
		},
		RoleBizID:       roleBizId,
		KnowledgeBizIDs: knowledgeIds,
	}); err != nil {
		log.ErrorContextf(ctx, "DeleteKnowledgeRoleQA failed, err: %v", err)
		return err
	}

	// 删除分类关联
	if _, err := client.DeleteKnowledgeRoleCate(ctx, &dao.KnowledgeRoleCateReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
		},
		RoleBizID:       roleBizId,
		KnowledgeBizIDs: knowledgeIds,
	}); err != nil {
		log.ErrorContextf(ctx, "DeleteKnowledgeRoleCate failed, err: %v", err)
		return err
	}

	// 删除标签关联
	if _, err := client.DeleteKnowledgeRoleAttributeLabel(ctx, &dao.KnowledgeRoleAttributeLabelReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
		},
		RoleBizID:       roleBizId,
		KnowledgeBizIDs: knowledgeIds,
	}); err != nil {
		log.ErrorContextf(ctx, "DeleteKnowledgeRoleAttributeLabel failed, err: %v", err)
		return err
	}

	if _, err := client.DeleteKnowledgeRoleDatabase(ctx, &dao.KnowledgeRoleDatabaseReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
		},
		RoleBizID:       roleBizId,
		KnowledgeBizIDs: knowledgeIds,
	}); err != nil {
		log.ErrorContextf(ctx, "DeleteKnowledgeRoleDatabase failed, err: %v", err)
		return err
	}
	return nil
}

type AssociationInfo struct {
	BizIDs     []uint64
	Knowledges []uint64
}

func newAssociationInfo() *AssociationInfo {
	return &AssociationInfo{
		BizIDs:     make([]uint64, 0, 3),
		Knowledges: make([]uint64, 0, 3),
	}
}

func (a *AssociationInfo) Input(bizID uint64, knowBizID uint64) {
	if !slices.Contains(a.BizIDs, bizID) {
		a.BizIDs = append(a.BizIDs, bizID)
	}
	if !slices.Contains(a.Knowledges, knowBizID) {
		a.Knowledges = append(a.Knowledges, knowBizID)
	}
}

// RemoveAssociations 移除角色关联
func (l *LogicRole) RemoveAssociations(ctx context.Context, appBizID, roleBizId uint64, syncInfos []*SyncInfo) (err error) {
	docBizIds := newAssociationInfo()
	qaBizIds := newAssociationInfo()
	cateBizIds := newAssociationInfo()
	databases := newAssociationInfo()
	attrlabels := newAssociationInfo()
	attrInfos := newAssociationInfo()
	knowledgeLibs := make([]uint64, 0, 3)

	tx := l.GetTdsqlGormDB().Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		if err1 := tx.Commit().Error; err1 != nil {
			err = err1
			tx.Rollback()
			return
		}
	}()
	client := dao.GetRoleDao(tx)
	for _, v := range syncInfos {
		if v == nil {
			continue
		}
		switch v.Type {
		case SyncInfoTypeDoc: // 文档
			docBizIds.Input(v.BizID, v.KnowledgeBizID)
		case SyncInfoTypeQA: // 问答
			qaBizIds.Input(v.BizID, v.KnowledgeBizID)
		case SyncInfoTypeDocCate, SyncInfoTypeQaCate: // 分类
			cateBizIds.Input(v.BizID, v.KnowledgeBizID)
		case SyncInfoTypeKnowledge: // 知识库
			knowledgeLibs = append(knowledgeLibs, v.KnowledgeBizID)
		case SyncInfoTypeAttr: // 标签
			if v.AttrLableBizID == 0 {
				attrInfos.Input(v.AttrBizID, v.KnowledgeBizID)
			} else {
				attrlabels.Input(v.AttrLableBizID, v.KnowledgeBizID)
			}
		case SyncInfoTypeDatabase: // 数据库
			databases.Input(v.DatabaseBizID, v.KnowledgeBizID)
		default:
		}
	}
	corpBizID := pkg.CorpBizID(ctx)
	if len(knowledgeLibs) > 0 {
		client.DeleteKnowledgeRoleKnow(ctx, &dao.KnowledgeRoleKnowReq{
			KnowledgeBase: dao.KnowledgeBase{
				CorpBizID: corpBizID,
				AppBizID:  appBizID,
				Limit:     len(knowledgeLibs),
			},
			RoleBizID:       roleBizId,
			KnowledgeBizIDs: knowledgeLibs,
		})
	}
	if len(docBizIds.BizIDs) > 0 {
		if err := client.DeleteKnowledgeRoleDoc(ctx, &dao.KnowledgeRoleDocReq{
			KnowledgeBase: dao.KnowledgeBase{
				CorpBizID: corpBizID,
				AppBizID:  appBizID,
				Limit:     len(docBizIds.BizIDs),
			},
			RoleBizID:       roleBizId,
			DocBizIDs:       docBizIds.BizIDs,
			KnowledgeBizIDs: docBizIds.Knowledges,
		}); err != nil {
			return err
		}
	}
	if len(qaBizIds.BizIDs) > 0 {
		if err := client.DeleteKnowledgeRoleQA(ctx, &dao.KnowledgeRoleQAReq{
			KnowledgeBase: dao.KnowledgeBase{
				CorpBizID: corpBizID,
				AppBizID:  appBizID,
				Limit:     len(qaBizIds.BizIDs),
			},
			RoleBizID:       roleBizId,
			QABizIDs:        qaBizIds.BizIDs,
			KnowledgeBizIDs: qaBizIds.Knowledges,
		}); err != nil {
			return err
		}
	}
	if len(cateBizIds.BizIDs) > 0 {
		if _, err := client.DeleteKnowledgeRoleCate(ctx, &dao.KnowledgeRoleCateReq{
			KnowledgeBase: dao.KnowledgeBase{
				CorpBizID: corpBizID,
				AppBizID:  appBizID,
				Limit:     len(cateBizIds.BizIDs),
			},
			RoleBizID:       roleBizId,
			CateBizIDs:      cateBizIds.BizIDs,
			KnowledgeBizIDs: cateBizIds.Knowledges,
		}); err != nil {
			return err
		}
	}
	if len(attrlabels.BizIDs) > 0 {
		if _, err := client.DeleteKnowledgeRoleAttributeLabel(ctx, &dao.KnowledgeRoleAttributeLabelReq{
			KnowledgeBase: dao.KnowledgeBase{
				CorpBizID: corpBizID,
				AppBizID:  appBizID,
				Limit:     len(attrlabels.BizIDs),
			},
			RoleBizID:       roleBizId,
			LabelBizIDs:     attrlabels.BizIDs,
			KnowledgeBizIDs: attrlabels.Knowledges,
		}); err != nil {
			return err
		}
	}

	if len(attrInfos.BizIDs) > 0 {
		if _, err := client.DeleteKnowledgeRoleAttributeLabel(ctx, &dao.KnowledgeRoleAttributeLabelReq{
			KnowledgeBase: dao.KnowledgeBase{
				CorpBizID: corpBizID,
				AppBizID:  appBizID,
				Limit:     len(attrInfos.BizIDs),
			},
			RoleBizID:       roleBizId,
			AttrBizIDs:      attrInfos.BizIDs,
			LabelBizIDs:     []uint64{0},
			KnowledgeBizIDs: attrlabels.Knowledges,
		}); err != nil {
			return err
		}
	}

	if len(databases.BizIDs) > 0 {
		if _, err := client.DeleteKnowledgeRoleDatabase(ctx, &dao.KnowledgeRoleDatabaseReq{
			KnowledgeBase: dao.KnowledgeBase{
				CorpBizID: corpBizID,
				AppBizID:  appBizID,
				Limit:     len(databases.BizIDs),
			},
			RoleBizID:       roleBizId,
			DatabaseBizIDs:  databases.BizIDs,
			KnowledgeBizIDs: databases.Knowledges,
		}); err != nil {
			return err
		}
	}
	return nil
}

// AddAssociations 添加角色关联
func (l *LogicRole) AddAssociations(ctx context.Context, appBizID, roleBizId uint64, syncInfos []*SyncInfo) (err error) {
	docBizs := make([]*model.KnowledgeRoleDoc, 0, 3)
	qaBizs := make([]*model.KnowledgeRoleQA, 0, 3)
	cateBizs := make([]*model.KnowledgeRoleCate, 0, 3)
	knowledgeLibs := make([]*model.KnowledgeRoleKnow, 0, 3)
	attrLabels := make([]*model.KnowledgeRoleAttributeLabel, 0, 3)
	knowledgeKnow := &model.KnowledgeRoleKnow{}
	databases := make([]*model.KnowledgeRoleDatabase, 0, 3)

	corpBizID := pkg.CorpBizID(ctx)
	for _, v := range syncInfos {
		switch v.Type {
		case SyncInfoTypeDoc:
			docBizs = append(docBizs, &model.KnowledgeRoleDoc{
				CorpBizID:      corpBizID,
				AppBizID:       appBizID,
				RoleBizID:      roleBizId,
				DocBizID:       v.BizID,
				KnowledgeBizID: v.KnowledgeBizID,
			})
		case SyncInfoTypeQA:
			qaBizs = append(qaBizs, &model.KnowledgeRoleQA{
				CorpBizID:      corpBizID,
				AppBizID:       appBizID,
				RoleBizID:      roleBizId,
				QABizID:        v.BizID,
				KnowledgeBizID: v.KnowledgeBizID,
			})
		case SyncInfoTypeDocCate:
			cateBizs = append(cateBizs, &model.KnowledgeRoleCate{
				CorpBizID:      corpBizID,
				AppBizID:       appBizID,
				RoleBizID:      roleBizId,
				CateBizID:      v.BizID,
				CateType:       model.CateTypeDoc,
				KnowledgeBizID: v.KnowledgeBizID,
			})
		case SyncInfoTypeQaCate:
			cateBizs = append(cateBizs, &model.KnowledgeRoleCate{
				CorpBizID:      corpBizID,
				AppBizID:       appBizID,
				RoleBizID:      roleBizId,
				CateBizID:      v.BizID,
				CateType:       model.CateTypeQA,
				KnowledgeBizID: v.KnowledgeBizID,
			})
		case SyncInfoTypeKnowledge:
			knowledgeLibs = append(knowledgeLibs, &model.KnowledgeRoleKnow{
				CorpBizID:      corpBizID,
				AppBizID:       appBizID,
				RoleBizID:      roleBizId,
				KnowledgeBizID: v.KnowledgeBizID,
				KnowledgeType:  v.LibType,
				SearchType:     v.SearchType,
				LabelCondition: v.LabelCondition,
			})
		case SyncInfoTypeAttr:
			attrLabels = append(attrLabels, &model.KnowledgeRoleAttributeLabel{
				CorpBizID:      corpBizID,
				AppBizID:       appBizID,
				RoleBizID:      roleBizId,
				KnowledgeBizID: v.KnowledgeBizID,
				AttrBizID:      v.AttrBizID,
				LabelBizID:     v.AttrLableBizID,
			})
		case SyncInfoTypeBase:
			knowledgeKnow = &model.KnowledgeRoleKnow{
				CorpBizID:      corpBizID,
				AppBizID:       appBizID,
				RoleBizID:      roleBizId,
				KnowledgeBizID: v.KnowledgeBizID,
				LabelCondition: v.LabelCondition,
				KnowledgeType:  v.LibType,
				SearchType:     v.SearchType,
			}
		case SyncInfoTypeDatabase:
			databases = append(databases, &model.KnowledgeRoleDatabase{
				CorpBizID:      corpBizID,
				AppBizID:       appBizID,
				RoleBizID:      roleBizId,
				KnowledgeBizID: v.KnowledgeBizID,
				DatabaseBizID:  v.DatabaseBizID,
			})
		}
	}
	tx := l.GetTdsqlGormDB().Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		if err1 := tx.Commit().Error; err1 != nil {
			err = err1
			tx.Rollback()
			return
		}
	}()
	client := dao.GetRoleDao(tx)
	if len(docBizs) > 0 {
		err = client.BatchCreateKnowledgeRoleDoc(ctx, docBizs)
		if err != nil {
			log.ErrorContextf(ctx, "BatchCreateKnowledgeRoleDoc failed, err: %v", err)
			return err
		}
	}
	if len(qaBizs) > 0 {
		err = client.BatchCreateKnowledgeRoleQA(ctx, qaBizs)
		if err != nil {
			log.ErrorContextf(ctx, "BatchCreateKnowledgeRoleQA failed, err: %v", err)
			return err
		}
	}
	if len(cateBizs) > 0 {
		err = client.BatchCreateKnowledgeRoleCate(ctx, cateBizs)
		if err != nil {
			log.ErrorContextf(ctx, "BatchCreateKnowledgeRoleCate failed, err: %v", err)
			return err
		}
	}
	if len(knowledgeLibs) > 0 {
		err = client.BatchCreateKnowledgeRoleKnow(ctx, knowledgeLibs)
		if err != nil {
			log.ErrorContextf(ctx, "BatchCreateKnowledgeRoleKnow failed, err: %v", err)
			return err
		}
	}
	if len(attrLabels) > 0 {
		err = client.BatchCreateKnowledgeRoleAttributeLabel(ctx, attrLabels)
		if err != nil {
			log.ErrorContextf(ctx, "BatchCreateKnowledgeRoleAttributeLabel failed, err: %v", err)
			return err
		}
	}

	if knowledgeKnow.KnowledgeBizID != 0 && knowledgeKnow.RoleBizID != 0 {
		_, err = client.UpdateKnowledgeCondition(ctx, knowledgeKnow)
		if err != nil {
			log.ErrorContextf(ctx, "UpdateKnowledgeRoleKnow failed, err: %v", err)
			return err
		}
	}
	if len(databases) > 0 {
		err = client.BatchCreateKnowledgeRoleDatabase(ctx, databases)
		if err != nil {
			log.ErrorContextf(ctx, "BatchCreateKnowledgeRoleDatabase failed, err: %v", err)
			return err
		}
	}

	return nil
}

// DetailRole 获取角色详情
func (l *LogicRole) DetailRole(ctx context.Context, appBizID uint64, roleBizIDs []uint64) ([]*pb.DescribeKnowledgeRoleRsp, error) {
	// 根据Create的逻辑，查询数据库，构建CreateRoleReq对象
	corpBizID := pkg.CorpBizID(ctx)
	client := dao.GetRoleDao(l.GetTdsqlGormDB())
	res := make([]*pb.DescribeKnowledgeRoleRsp, 0, len(roleBizIDs))
	_, roles, err := client.ListKnowledgeRoles(ctx, &dao.KnowledgeRoleReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: corpBizID,
			AppBizID:  appBizID,
		},
		BizIDs: roleBizIDs,
	})
	if err != nil {
		log.ErrorContextf(ctx, "GetKnowledgeRole failed, err: %v", err)
		return nil, err
	}
	for _, role := range roles {
		knows, err := client.ListKnowledgeRoleKnow(ctx, &dao.KnowledgeRoleKnowReq{
			KnowledgeBase: dao.KnowledgeBase{
				CorpBizID: corpBizID,
				AppBizID:  appBizID,
			},
			RoleBizID: role.BusinessID,
		})
		if err != nil {
			log.ErrorContextf(ctx, "GetKnowledgeRoleKnow failed, err: %v", err)
			return nil, err
		}
		knowledgeBizID2Name := make(map[uint64]string, len(knows))
		knowledgeBizIDs := make([]uint64, 0, len(knows))
		for _, v := range knows {
			if slicex.Contains(knowledgeBizIDs, v.KnowledgeBizID) {
				continue
			}
			knowledgeBizIDs = append(knowledgeBizIDs, v.KnowledgeBizID)
		}
		knowledges, err := l.RetrieveBaseSharedKnowledge(ctx, corpBizID, knowledgeBizIDs)
		if err != nil {
			log.WarnContextf(ctx, "RetrieveBaseSharedKnowledge failed, err: %v", err)
		}
		for _, v := range knowledges {
			knowledgeBizID2Name[v.BusinessID] = v.Name
		}

		chooses := make([]*pb.KnowChoose, 0, len(knows))
		for _, knowLib := range knows {
			knowBizId := knowLib.KnowledgeBizID
			knowledgeName := ""
			if knowBizId == appBizID {
				knowledgeName = pkg.AppName(ctx)
			} else if v, ok := knowledgeBizID2Name[knowBizId]; ok {
				knowledgeName = v
			}
			know, err := l.GetAppByAppBizID(ctx, knowBizId)
			if err != nil || know == nil {
				log.ErrorContextf(ctx, "GetAppByAppBizID failed, err: %v", err)
				return nil, err
			}
			knowID := know.ID
			choose := &pb.KnowChoose{
				KnowledgeBizId:    cast.ToString(knowBizId),
				KnowledgeName:     knowledgeName,
				Type:              uint32(knowLib.KnowledgeType),
				SearchType:        uint32(knowLib.SearchType),
				DocBizIds:         make([]string, 0, 5),
				DocCateBizIds:     make([]string, 0, 5),
				QuesAnsBizIds:     make([]string, 0, 5),
				QuesAnsCateBizIds: make([]string, 0, 5),
				Condition:         int32(knowLib.LabelCondition),
				DbBizIds:          make([]string, 0, 5),
			}
			// 查询关联文档
			docs, err := client.ListKnowledgeRoleDoc(ctx, &dao.KnowledgeRoleDocReq{
				KnowledgeBase: dao.KnowledgeBase{
					CorpBizID: role.CorpBizID,
					AppBizID:  role.AppBizID,
					Fields:    []string{model.ColumnID, model.ColumnDocBizID},
				},
				RoleBizID:      role.BusinessID,
				KnowledgeBizID: knowBizId,
			})
			if err != nil {
				log.ErrorContextf(ctx, "GetKnowledgeRoleDoc failed, err: %v", err)
				return nil, err
			}
			for _, doc := range docs {
				choose.DocBizIds = append(choose.DocBizIds, cast.ToString(doc.DocBizID))
			}

			// 查询关联问答
			qas, err := client.ListKnowledgeRoleQA(ctx, &dao.KnowledgeRoleQAReq{
				KnowledgeBase: dao.KnowledgeBase{
					CorpBizID: role.CorpBizID,
					AppBizID:  role.AppBizID,
					Fields:    []string{model.ColumnID, model.ColumnQABizID},
				},
				RoleBizID:      role.BusinessID,
				KnowledgeBizID: knowBizId,
			})
			if err != nil {
				log.ErrorContextf(ctx, "GetKnowledgeRoleQA failed, err: %v", err)
				return nil, err
			}
			for _, qa := range qas {
				choose.QuesAnsBizIds = append(choose.QuesAnsBizIds, cast.ToString(qa.QABizID))
			}

			// 查询分类
			cates, err := client.ListKnowledgeRoleCate(ctx, &dao.KnowledgeRoleCateReq{
				KnowledgeBase: dao.KnowledgeBase{
					CorpBizID: role.CorpBizID,
					AppBizID:  role.AppBizID,
				},
				RoleBizID:      role.BusinessID,
				KnowledgeBizID: knowBizId,
			})
			if err != nil {
				log.ErrorContextf(ctx, "GetKnowledgeRoleCate failed, err: %v", err)
				return nil, err
			}
			for _, cate := range cates {
				switch cate.CateType {
				case model.CateTypeDoc:
					choose.DocCateBizIds = append(choose.DocCateBizIds, cast.ToString(cate.CateBizID))
				case model.CateTypeQA:
					choose.QuesAnsCateBizIds = append(choose.QuesAnsCateBizIds, cast.ToString(cate.CateBizID))
				}
			}

			databases, err := client.ListKnowledgeRoleDatabase(ctx, &dao.KnowledgeRoleDatabaseReq{
				KnowledgeBase: dao.KnowledgeBase{
					CorpBizID: role.CorpBizID,
					AppBizID:  role.AppBizID,
				},
				RoleBizID:      role.BusinessID,
				KnowledgeBizID: knowBizId,
			})
			if err != nil {
				log.ErrorContextf(ctx, "GetKnowledgeRoleDatabase failed, err: %v", err)
				return nil, err
			}
			for _, v := range databases {
				choose.DbBizIds = append(choose.DbBizIds, cast.ToString(v.DatabaseBizID))
			}

			// 查询标签
			labels, err := client.ListKnowledgeRoleAttributeLabel(ctx, &dao.KnowledgeRoleAttributeLabelReq{
				KnowledgeBase: dao.KnowledgeBase{
					CorpBizID: role.CorpBizID,
					AppBizID:  role.AppBizID,
				},
				RoleBizID:      role.BusinessID,
				KnowledgeBizID: knowBizId,
			})
			if err != nil {
				log.ErrorContextf(ctx, "GetKnowledgeRoleAttributeLabel failed, err: %v", err)
				return nil, err
			}
			if len(labels) > 0 {
				attrs := make([]uint64, 0, len(labels))
				attrLabels := make([]uint64, 0, len(labels))
				for _, v := range labels {
					if !slicex.Contains(attrs, v.AttrBizID) {
						attrs = append(attrs, v.AttrBizID)
					}
					if !slicex.Contains(attrLabels, v.LabelBizID) {
						attrLabels = append(attrLabels, v.LabelBizID)
					}
				}
				attrNames := make(map[uint64]*model.Attribute)
				if knowID != 0 {
					attrNames, err = l.GetAttributeByBizIDs(ctx, knowID, attrs)
					if err != nil {
						log.ErrorContextf(ctx, "GetAttributeByBizIDs failed, err: %v", err)
						return nil, err
					}
				}
				attrLabelNames, err := l.GetAttributeLabelByBizIDs(ctx, attrLabels, knowID)
				if err != nil {
					log.ErrorContextf(ctx, "GetAttributeLabelByBizIDs failed, err: %v", err)
					return nil, err
				}
				sort.Slice(labels, func(i, j int) bool {
					return labels[i].AttrBizID < labels[j].AttrBizID
				})
				lastAttrBizID := uint64(0)
				for _, v := range labels {
					if v.AttrBizID != lastAttrBizID {
						attrName := ""
						if t, ok := attrNames[v.AttrBizID]; ok {
							attrName = t.Name
						}
						choose.Labels = append(choose.Labels, &pb.ChooseLabel{
							AttrBizId: cast.ToString(v.AttrBizID),
							AttrName:  attrName,
							Labels:    make([]*pb.ChooseLabel_Label, 0, 2),
						})
						lastAttrBizID = v.AttrBizID
					}
					if len(choose.Labels) == 0 {
						continue
					}
					labelName := ""
					if t, ok := attrLabelNames[v.LabelBizID]; ok {
						labelName = t.Name
					}
					lastOne := choose.Labels[len(choose.Labels)-1]
					lastOne.Labels = append(lastOne.Labels, &pb.ChooseLabel_Label{
						LabelBizId: cast.ToString(v.LabelBizID),
						LabelName:  utils.When(v.LabelBizID == 0, "全部", labelName),
					})
				}
			}

			chooses = append(chooses, choose)
		}
		elem := &pb.DescribeKnowledgeRoleRsp{
			RoleInfo: &pb.RoleInfo{
				AppBizId:    role.AppBizID,
				RoleBizId:   role.BusinessID,
				Name:        role.Name,
				Type:        int32(role.Type),
				Description: role.Description,
				SearchType:  uint32(role.SearchType),
				KnowChoose:  chooses,
				IsDeleted:   uint32(role.IsDeleted),
				CreateTime:  role.CreateTime.Unix(),
				UpdateTime:  role.UpdateTime.Unix(),
			},
		}

		res = append(res, elem)
	}
	return res, nil
}

// ModifyRole 修改角色信息
func (l *LogicRole) ModifyRole(ctx context.Context, req *pb.ModifyReq, isCreate bool) (uint64, []*SyncInfo, error) {

	// 创建预设角色
	appBizID := cast.ToUint64(req.GetAppBizId())
	roleBizID := cast.ToUint64(req.GetRoleBizId())

	roleType := req.GetType()
	if roleBizID == 0 {
		roleBizID = l.GenerateSeqID()
	}

	if !isCreate && roleBizID == 1 { //编辑的时候，设定为预置角色
		roleType = model.KnowledgeRoleTypePreset
		roleBizID = l.GenerateSeqID()
	}

	key := fmt.Sprintf(ModifyRoleLockKey, roleBizID)
	if r := l.Lock(ctx, key, time.Second*30); r == nil { // 加锁成功
		defer l.UnLock(ctx, key)
	} else {
		log.WarnContextf(ctx, "ModifyRole lock failed, appBizID:%d", appBizID)
		return 0, nil, errs.ErrAlreadyLocked
	}

	roles, err := l.DetailRole(ctx,
		appBizID, []uint64{roleBizID},
	)
	if err != nil {
		log.ErrorContextf(ctx, "DetailRole failed, err: %v", err)
		return 0, nil, err
	}
	var oldChoose []*pb.KnowChoose
	if len(roles) != 0 {
		oldChoose = roles[0].RoleInfo.GetKnowChoose()
	}

	if roleBizID == 0 {
		roleBizID = l.GenerateSeqID()
	}

	// 删除角色的所有旧关系
	// l.RemoveRoleAssociation(ctx, req.GetCorpBizId(), req.GetAppBizId(), req.GetRoleBizId())
	l.CreateRole(ctx, &pb.CreateRoleReq{
		AppBizId:    req.GetAppBizId(),
		Name:        req.GetName(),
		Description: req.GetDescription(),
		SearchType:  req.GetSearchType(),
		KnowChoose:  req.GetKnowChoose(),
	}, roleBizID, int(roleType))

	deletes, adds, err := l.Diff2KonwChooses(oldChoose, req.GetKnowChoose())
	if err != nil {
		log.ErrorContextf(ctx, "Diff2KonwChooses failed, err: %v", err)
		return 0, nil, err
	}
	log.InfoContextf(ctx, "ModifyRole deletes: %v, adds: %v", deletes, adds)
	// 删除关联关系
	l.RemoveAssociations(ctx, appBizID, roleBizID, deletes)
	// 添加关联关系
	l.AddAssociations(ctx, appBizID, roleBizID, adds)
	syncInfos := append(deletes, adds...)
	log.InfoContextf(ctx, "ModifyRole syncInfos: %+v", syncInfos)

	return roleBizID, syncInfos, nil
}

// Diff2KonwChooses 对比新旧知识库选择差异
func (l *LogicRole) Diff2KonwChooses(oldOne []*pb.KnowChoose, newOne []*pb.KnowChoose) ([]*SyncInfo, []*SyncInfo, error) {
	deleteSyncInfos := make([]*SyncInfo, 0, 5)
	addSyncInfos := make([]*SyncInfo, 0, 5)
	deletes, exists, adds, _ := util.TranslateTo(oldOne, newOne, func(a *pb.KnowChoose) string {
		return cast.ToString(a.KnowledgeBizId)
	})
	for k, chooses := range [][]*pb.KnowChoose{deletes, adds} {
		syncInfos := FetchSyncInfoFromChooses(chooses)
		if k == 0 {
			deleteSyncInfos = append(deleteSyncInfos, syncInfos...)
		} else {
			addSyncInfos = append(addSyncInfos, syncInfos...)
		}
	}
	for _, v := range exists {
		old, new := v[0], v[1]
		if old == nil || new == nil {
			continue
		}

		params := make([]DiffElementsReq, 0, 5)
		if len(old.DocBizIds) != 0 || len(new.DocBizIds) != 0 { // 文档
			params = append(params, DiffElementsReq{
				Src:      old.DocBizIds,
				Dst:      new.DocBizIds,
				SyncType: SyncInfoTypeDoc,
			})
		}
		if len(old.QuesAnsBizIds) != 0 || len(new.QuesAnsBizIds) != 0 { // 问答
			params = append(params, DiffElementsReq{
				Src:      old.QuesAnsBizIds,
				Dst:      new.QuesAnsBizIds,
				SyncType: SyncInfoTypeQA,
			})
		}
		if len(old.DocCateBizIds) != 0 || len(new.DocCateBizIds) != 0 { // 文档分类
			params = append(params, DiffElementsReq{
				Src:      old.DocCateBizIds,
				Dst:      new.DocCateBizIds,
				SyncType: SyncInfoTypeDocCate,
			})
		}
		if len(old.QuesAnsCateBizIds) != 0 || len(new.QuesAnsCateBizIds) != 0 { // 问答分类
			params = append(params, DiffElementsReq{
				Src:      old.QuesAnsCateBizIds,
				Dst:      new.QuesAnsCateBizIds,
				SyncType: SyncInfoTypeQaCate,
			})
		}
		if len(old.DbBizIds) != 0 || len(new.DbBizIds) != 0 { // 数据库
			params = append(params, DiffElementsReq{
				Src:      old.DbBizIds,
				Dst:      new.DbBizIds,
				SyncType: SyncInfoTypeDatabase,
			})
		}

		mAttrInfos := [2][]string{}
		for k, attrInfo := range [2][]*pb.ChooseLabel{old.GetLabels(), new.GetLabels()} {
			attrInfos := make([]string, 0, len(attrInfo))
			for _, attr := range attrInfo {
				for _, label := range attr.GetLabels() {
					attrInfos = append(attrInfos, fmt.Sprintf("%s-%s", attr.AttrBizId, label.LabelBizId))
				}
			}
			mAttrInfos[k] = attrInfos
		}
		if len(mAttrInfos[0]) != 0 || len(mAttrInfos[1]) != 0 { // 属性标签
			params = append(params, DiffElementsReq{
				Src:      mAttrInfos[0],
				Dst:      mAttrInfos[1],
				SyncType: SyncInfoTypeAttr,
			})
		}

		for _, param := range params {
			deletes, adds, err := DiffElements(param, cast.ToUint64(new.KnowledgeBizId))
			if err != nil {
				return nil, nil, err
			}
			deleteSyncInfos = append(deleteSyncInfos, deletes...)
			addSyncInfos = append(addSyncInfos, adds...)
		}

		if TheChooseEqualKey(old) != TheChooseEqualKey(new) {
			addSyncInfos = append(addSyncInfos, &SyncInfo{
				Type:           SyncInfoTypeBase,
				LibType:        int8(new.Type),
				SearchType:     int8(new.GetSearchType()),
				LabelCondition: int8(new.Condition),
				KnowledgeBizID: cast.ToUint64(old.KnowledgeBizId),
			})
		}
	}
	return deleteSyncInfos, addSyncInfos, nil
}

// ListKnowledgeRoles 获取知识库角色列表
func (l *LogicRole) ListKnowledgeRoles(ctx context.Context, req *dao.KnowledgeRoleReq) (int64, []*model.KnowledgeRole, error) {
	client := dao.GetRoleDao(l.GetTdsqlGormDB())
	count, knowledgeRoles, err := client.ListKnowledgeRoles(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "ListKnowledgeRoles %v", err)
		return 0, nil, err
	}
	return count, knowledgeRoles, nil
}

// DeleteKnowledgeRole 删除知识库角色
func (l *LogicRole) DeleteKnowledgeRole(ctx context.Context, appBizID uint64, roleBizIDs []uint64) (err error) {
	tx := l.GetTdsqlGormDB().Begin()

	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		if err1 := tx.Commit().Error; err1 != nil {
			err = err1
			tx.Rollback()
			return
		}
	}()
	client := dao.GetRoleDao(tx)
	corpBizID := pkg.CorpBizID(ctx)
	for _, roleBizID := range roleBizIDs {
		if err := client.DeleteKnowledgeRole(ctx, &dao.KnowledgeRoleReq{
			KnowledgeBase: dao.KnowledgeBase{
				CorpBizID: corpBizID,
				AppBizID:  appBizID,
			},
			BusinessID: roleBizID,
			Type:       model.KnowledgeRoleTypeCustom,
		}); err != nil {
			return err
		}
		if err := l.RemoveRoleAssociation(ctx, appBizID, roleBizID); err != nil {
			return err
		}
	}
	return nil
}

// CheckKnowChoose 检查知识库选择权限
func (l *LogicRole) CheckKnowChoose(ctx context.Context, appBizID uint64, req *pb.KnowChoose) error {
	docBizIDs := util.ConvertSliceStringToUint64(req.GetDocBizIds())
	qaBizIDs := util.ConvertSliceStringToUint64(req.GetQuesAnsBizIds())
	docCateBizIDs := util.ConvertSliceStringToUint64(req.GetDocCateBizIds())
	qaCateBizIDs := util.ConvertSliceStringToUint64(req.GetQuesAnsCateBizIds())

	appID := pkg.AppID(ctx)
	knowID := appID
	// knowBizID := appBizID
	if appBizID != cast.ToUint64(req.GetKnowledgeBizId()) { // 共享知识库
		know, err := l.GetAppByAppBizID(ctx, cast.ToUint64(req.GetKnowledgeBizId()))
		if err != nil {
			log.ErrorContextf(ctx, "CheckKnowChoose RetrieveBaseSharedKnowledge %s err:%v", req.GetKnowledgeBizId(), err)
			return errs.ErrGetKnowledgeFailed
		}
		if know != nil {
			knowID = know.ID
			// knowBizID = knows[0].BusinessID
		}
	}

	if len(docBizIDs) != 0 {
		docs, err := l.GetDocByBizIDs(ctx, docBizIDs, knowID)
		if err != nil {
			log.ErrorContextf(ctx, "CheckKnowChoose err:%v", err)
			return err
		}
		if len(docs) != len(docBizIDs) {
			return errs.ErrHandleDocDiffTaskDocNotFoundFail
		}
		for _, doc := range docs {
			if doc.RobotID != knowID {
				return errs.ErrWrapf(errs.ErrHandleDocDiffTaskDocNotFoundFail, i18n.Translate(ctx, i18nkey.KeyDocumentNotExistOrDeleted),
					doc.BusinessID)
			}
		}
	}
	if len(qaBizIDs) != 0 {
		res, err := l.GetQAsByBizIDs(ctx, pkg.CorpID(ctx), knowID, qaBizIDs, 0, uint64(len(qaBizIDs)))
		if err != nil {
			log.ErrorContextf(ctx, "CheckKnowChoose err:%v", err)
			return err
		}
		log.InfoContext(ctx, "lokli", qaBizIDs, res)
		if len(res) != len(qaBizIDs) {
			return errs.ErrQAIsNotExist
		}
		for _, qa := range res {
			if qa.RobotID != knowID {
				return errs.ErrWrapf(errs.ErrQAIsNotExist, i18n.Translate(ctx, i18nkey.KeyQANotExistOrDeleted), qa.BusinessID)
			}
		}
	}
	if len(docCateBizIDs) != 0 {
		res, err := l.GetCateListByBusinessIDs(ctx, model.DocCate, pkg.CorpID(ctx), knowID, docCateBizIDs)
		if err != nil {
			log.ErrorContextf(ctx, "CheckKnowChoose err:%v", err)
			return err
		}
		if len(res) != len(docCateBizIDs) {
			return errs.ErrCateNotFound
		}
		for _, cate := range res {
			if cate.RobotID != knowID {
				return errs.ErrWrapf(errs.ErrCateNotFound, i18n.Translate(ctx, i18nkey.KeyCategoryNotExistOrDeleted), cate.BusinessID)
			}
		}
	}
	if len(qaCateBizIDs) != 0 {
		res, err := l.GetCateListByBusinessIDs(ctx, model.QACate, pkg.CorpID(ctx), knowID, qaCateBizIDs)
		if err != nil {
			log.ErrorContextf(ctx, "CheckKnowChoose err:%v", err)
			return err
		}
		if len(res) != len(qaCateBizIDs) {
			return errs.ErrCateNotFound
		}
		for _, cate := range res {
			if cate.RobotID != knowID {
				return errs.ErrWrapf(errs.ErrCateNotFound, i18n.Translate(ctx, i18nkey.KeyCategoryNotExistOrDeleted), cate.BusinessID)
			}
		}
	}
	return nil
}

// DescribeRoleSearch 获取角色搜索详情
func (l *LogicRole) DescribeRoleSearch(ctx context.Context, req *pb.DescribeRoleSearchReq) (*pb.DescribeRoleSearchRsp, error) {
	res := make([]*pb.DescribeRoleSearchRsp_SearchInfo, 0, len(req.GetRoleSearch()))
	appID := pkg.AppID(ctx)
	corpID := pkg.CorpID(ctx)

	knowID := appID
	knowBizID := cast.ToUint64(req.GetAppBizId())
	if req.GetAppBizId() != req.GetKnowBizId() { // 共享知识库
		know, err := l.GetAppByAppBizID(ctx, cast.ToUint64(req.GetKnowBizId()))
		if err != nil || know == nil {
			log.ErrorContextf(ctx, "DescribeRoleSearch RetrieveBaseSharedKnowledge err:%v,knowBizID:%+v", err, req.GetKnowBizId())
			return nil, errs.ErrGetKnowledgeFailed
		}
		knowID = know.ID
		knowBizID = know.BusinessID
	}

	for _, search := range req.GetRoleSearch() {
		searchType := search.Type
		bizIds := util.ConvertSliceStringToUint64(search.GetSearchBizIds())
		switch searchType {
		case SearchTypeDoc: // 搜索文档
			docs, err := l.GetDocByBizIDs(ctx, bizIds, knowID)
			if err != nil {
				log.ErrorContextf(ctx, "DescribeRoleSearch err:%v", err)
				return nil, err
			}
			cateIds := make([]uint64, 0, len(docs))
			docList := make([]*pb.DescribeRoleSearchRsp_SearchInfo, 0, len(docs))
			for _, bizId := range bizIds {
				if v, ok := docs[bizId]; ok {
					docList = append(docList, &pb.DescribeRoleSearchRsp_SearchInfo{
						Type:        searchType,
						SearchBizId: bizId,
						Name:        v.FileName,
						CateBizId:   uint64(v.CategoryID),
					})
					cateIds = append(cateIds, uint64(v.CategoryID))
				}
			}

			// 通过cate主键ID获取BussinessID
			cateBizIDMap, err := l.GetCateByIDs(ctx, model.DocCate, cateIds)
			if err != nil {
				log.ErrorContextf(ctx, "DescribeRoleSearch err:%v", err)
				return nil, err
			}
			for _, v := range docList {
				if cate, ok := cateBizIDMap[v.CateBizId]; ok {
					v.CateBizId = cate.BusinessID
				} else {
					v.CateBizId = 0
				}
			}
			res = append(res, docList...)

		case SearchTypeQA: // 搜索问答
			qas, err := l.GetQAsByBizIDs(ctx, pkg.CorpID(ctx), knowID, bizIds, 0, uint64(len(bizIds)))
			if err != nil {
				log.ErrorContextf(ctx, "DescribeRoleSearch err:%v", err)
				return nil, err
			}
			cateIds := make([]uint64, 0, len(qas))
			qaList := make([]*pb.DescribeRoleSearchRsp_SearchInfo, 0, len(qas))
			for _, qa := range qas {
				qaList = append(qaList, &pb.DescribeRoleSearchRsp_SearchInfo{
					Type:        searchType,
					SearchBizId: qa.BusinessID,
					CateBizId:   uint64(qa.CategoryID),
					Name:        qa.Question,
				})
				cateIds = append(cateIds, uint64(qa.CategoryID))
			}

			// 通过cate主键ID获取BussinessID
			cateBizIDMap, err := l.GetCateByIDs(ctx, model.DocCate, cateIds)
			if err != nil {
				log.ErrorContextf(ctx, "DescribeRoleSearch err:%v", err)
				return nil, err
			}
			for _, v := range qaList {
				if cate, ok := cateBizIDMap[v.CateBizId]; ok {
					v.CateBizId = cate.BusinessID
				} else {
					v.CateBizId = 0
				}
			}
			res = append(res, qaList...)
		case SearchTypeDocCate: // 搜索文档分类
			docCates, err := l.GetCateListByBusinessIDs(ctx, model.DocCate, corpID, knowID, bizIds)
			if err != nil {
				log.ErrorContextf(ctx, "DescribeRoleSearch err:%v", err)
				return nil, err
			}

			cateList := make([]*pb.DescribeRoleSearchRsp_SearchInfo, 0, len(docCates))
			for _, cate := range docCates {
				cateList = append(cateList, &pb.DescribeRoleSearchRsp_SearchInfo{
					Type:        searchType,
					SearchBizId: cate.BusinessID,
					Name:        cate.Name,
				})
			}
			res = append(res, cateList...)
		case SearchTypeQACate: // 搜索问答分类
			qaCates, err := l.GetCateListByBusinessIDs(ctx, model.QACate, corpID, knowID, bizIds)
			if err != nil {
				log.ErrorContextf(ctx, "DescribeRoleSearch err:%v", err)
				return nil, err
			}

			cateList := make([]*pb.DescribeRoleSearchRsp_SearchInfo, 0, len(qaCates))
			for _, cate := range qaCates {
				cateList = append(cateList, &pb.DescribeRoleSearchRsp_SearchInfo{
					Type:        searchType,
					SearchBizId: cate.BusinessID,
					Name:        cate.Name,
				})
			}
			res = append(res, cateList...)
		case SearchTypeDatabase: // 搜索数据库
			dbTableInfos, err := dao.GetDBTableDao().GetByBizIDs(ctx, pkg.CorpBizID(ctx), knowBizID, bizIds)
			if err != nil {
				log.ErrorContextf(ctx, "DescribeRoleSearch DB err:%v", err)
				return nil, err
			}
			dbList := make([]*pb.DescribeRoleSearchRsp_SearchInfo, 0, len(dbTableInfos))
			for _, table := range dbTableInfos {
				dbList = append(dbList, &pb.DescribeRoleSearchRsp_SearchInfo{
					Type:        searchType,
					SearchBizId: table.DBTableBizID,
					Name:        table.Name,
				})
			}
			res = append(res, dbList...)
		}
	}
	return &pb.DescribeRoleSearchRsp{
		RoleSearch: res,
	}, nil
}

func (l *PermisLogic) RoleSyncInfos(ctx context.Context, appBizId uint64, roleBizId uint64, infos []*SyncInfo) error {
	defer func() {
		if err := recover(); err != nil {
			log.ErrorContextf(ctx, "RoleSyncInfos err:%v", err)
			return
		}
	}()
	syncRoleInfo := make(map[uint64]*SyncRoleInfo)
	for _, v := range infos {
		if v == nil {
			continue
		}
		knowData, ok := syncRoleInfo[v.KnowledgeBizID]
		if !ok {
			knowData = &SyncRoleInfo{}
			syncRoleInfo[v.KnowledgeBizID] = knowData
		}
		switch v.Type {
		case SyncInfoTypeDoc:
			knowData.DocBizIDs = append(knowData.DocBizIDs, v.BizID)
		case SyncInfoTypeQA:
			knowData.QaBizIDs = append(knowData.QaBizIDs, v.BizID)
		case SyncInfoTypeDatabase:
			knowData.DbTableBizIDs = append(knowData.DbTableBizIDs, v.DatabaseBizID)
		default:
		}
	}
	if len(syncRoleInfo) == 0 {
		return nil
	}
	for knowBizID, v := range syncRoleInfo {
		log.DebugContextf(ctx, "RoleSyncInfos knowBizID:%v,info:%+v", knowBizID, v)
	}
	err := l.SyncRoleVector(ctx, appBizId, syncRoleInfo)
	if err != nil {
		log.ErrorContextf(ctx, "RoleSyncInfos err:%v", err)
		return err
	}

	// 更新缓存
	if len(syncRoleInfo) != 0 {
		req := &FormatFilterReq{
			AppID:     pkg.AppID(ctx),
			AppBizID:  appBizId,
			RoleBizID: roleBizId,
			CorpBizID: pkg.CorpBizID(ctx),
		}
		l.FormatFilter(ctx, req) // 确保缓存已经存在
		for k := range syncRoleInfo {
			l.UpdateKnowledgeFilterCache(ctx, req, k)
		}
	}
	return nil
}

// SyncRoleInfo 同步角色信息
type SyncRoleInfo struct {
	DocBizIDs     []uint64
	QaBizIDs      []uint64
	DbTableBizIDs []uint64
}

// SyncRoleVector 同步角色向量
func (l *PermisLogic) SyncRoleVector(ctx context.Context, appBizId uint64, req map[uint64]*SyncRoleInfo) error {
	corpID := pkg.CorpID(ctx)
	knowIDs := make(map[uint64]model.KnowData)
	for knowBizID, syncData := range req { //根据知识库分别获取文档/问答
		knowData := model.KnowData{}
		app, err := client.GetAppInfo(ctx, knowBizID, 1)
		if err != nil {
			log.ErrorContextf(ctx, "SyncRoleVector get app err:%v,knowBizID:%v,syncData:%+v", err, knowBizID, syncData)
			return err
		}
		//获取问答列表
		if len(syncData.QaBizIDs) != 0 {
			qas, err := dao.GetDocQaDao().GetAllDocQas(ctx, []string{dao.DocQaTblColId}, &dao.DocQaFilter{
				CorpId:      corpID,
				RobotId:     app.Id,
				BusinessIds: syncData.QaBizIDs,
			})
			if err != nil {
				log.ErrorContextf(ctx, "SyncRoleVector GetDocQas err:%v,knowBizID:%v,QaBizIDs:%+v",
					err, knowBizID, syncData.QaBizIDs)
				return err
			}
			for _, v := range qas {
				knowData.QaIDs = append(knowData.QaIDs, v.ID)
			}
		}
		//获取文档列表
		if len(syncData.DocBizIDs) != 0 {
			docs, err := dao.GetDocDao().GetAllDocs(ctx, []string{dao.DocTblColId},
				&dao.DocFilter{
					CorpId:      corpID,
					RobotId:     app.Id,
					BusinessIds: syncData.DocBizIDs,
				})
			if err != nil {
				log.ErrorContextf(ctx, "SyncRoleVector getDocList err:%v,knowBizID:%v,DocBizIDs:%+v",
					err, knowBizID, syncData.DocBizIDs)
				return err
			}
			for _, v := range docs {
				knowData.DocIDs = append(knowData.DocIDs, v.ID)
			}
		}
		//数据表业务ids
		knowData.DbTableBizIDs = syncData.DbTableBizIDs
		knowIDs[knowBizID] = knowData
	}
	err := dao.NewBatchUpdateVectorTask(ctx, appBizId, model.BatchUpdateVector{
		Type:      model.UpdateVectorByRole,
		CorpBizID: pkg.CorpBizID(ctx),
		AppBizID:  appBizId,
		KnowIDs:   knowIDs,
	})
	if err != nil {
		log.ErrorContextf(ctx, "SyncRoleVector BatchUpdateVector err:%v,corpBizID:%v,appBizID:%v,knowIDs:%v", err, pkg.CorpBizID(ctx), appBizId, knowIDs)
		return err
	}
	return nil
}

// TheChooseEqualKey 判定知识库基本信
func TheChooseEqualKey(a *pb.KnowChoose) string {
	return fmt.Sprintf("%s_%d_%d_%d", a.GetKnowledgeBizId(), a.GetType(), a.GetSearchType(), a.Condition)
}

// RemoveKnowledgeAssociation 移除知识库关联关系
func (l *LogicRole) RemoveKnowledgeAssociation(ctx context.Context, appBizID uint64, knowledgeBizIds []uint64) error {
	tx := l.GetTdsqlGormDB().Begin()
	var err error
	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		if err1 := tx.Commit().Error; err1 != nil {
			err = err1
			tx.Rollback()
			return
		}
	}()
	corpBizID := pkg.CorpBizID(ctx)
	client := dao.GetRoleDao(tx)
	if err = client.RemoveKnowledgeAssociation(ctx, corpBizID, appBizID, knowledgeBizIds); err != nil {
		log.ErrorContextf(ctx, "RemoveKnowledgeAssociation failed, err: %v", err)
		return err
	}
	return nil
}

// CheckPresetRole 检查预置角色
func (l *LogicRole) CheckPresetRole(ctx context.Context, appBizID uint64) (uint64, string, error) {

	cnt, roles, err := l.ListKnowledgeRoles(ctx, &dao.KnowledgeRoleReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: pkg.CorpBizID(ctx),
			AppBizID:  cast.ToUint64(appBizID),
		},
		Type: model.KnowledgeRoleTypePreset,
	})
	if err != nil {
		log.ErrorContextf(ctx, "ListKnowledgeRoles cnt:%d err:%v", cnt, err)
		return 0, "", errs.ErrGetRoleListFail
	}
	if len(roles) >= 1 {
		return roles[0].BusinessID, roles[0].Name, nil
	}

	// 分布式锁
	key := fmt.Sprintf(PrsetRoleLockKey, appBizID)
	if r := l.Lock(ctx, key, time.Second*30); r == nil { // 加锁成功
		defer l.UnLock(ctx, key)
	} else {
		log.WarnContextf(ctx, "CheckPresetRole lock failed, appBizID:%d", appBizID)
		return 0, "", errs.ErrAlreadyLocked
	}
	//double check
	cnt, roles, err = l.ListKnowledgeRoles(ctx, &dao.KnowledgeRoleReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: pkg.CorpBizID(ctx),
			AppBizID:  cast.ToUint64(appBizID),
		},
		Type: model.KnowledgeRoleTypePreset,
	})
	if err != nil {
		log.ErrorContextf(ctx, "ListKnowledgeRoles err:%v", err)
		return 0, "", errs.ErrGetRoleListFail
	}
	if len(roles) >= 1 {
		return roles[0].BusinessID, roles[0].Name, nil
	}
	log.WarnContextf(ctx, "checkPresetRole cnt:%v roles:%+v", cnt, roles)
	defaultRoleName := i18n.Translate(ctx, model.PresetRoleName)
	id, _, err := l.CreateRole(ctx, &pb.CreateRoleReq{
		AppBizId:    cast.ToString(appBizID),
		Name:        defaultRoleName,
		Description: defaultRoleName,
		SearchType:  model.RoleChooseAll,
	}, l.GenerateSeqID(), model.KnowledgeRoleTypePreset)
	if err != nil {
		log.ErrorContextf(ctx, "CreateRole err:%v", err)
		return 0, "", errs.ErrGetRoleListFail
	}
	return id, defaultRoleName, nil
}

// FormatFilterReq 参数，包含需要的全局变量默认值设定
type FormatFilterReq struct {
	AppID                uint64 // 应用id
	AppBizID             uint64 // 应用bizid
	RoleBizID            uint64 // 角色bizid
	CorpBizID            uint64 // 企业bizid
	CateKey              string // 分类向量统一key
	RoleKey              string // 角色向量统一key
	FullLabelValue       string // 标签全匹配的值
	GeneralVectorAttrKey string // 通用向量属性key
}

// GetDefault 获取默认值
func (f *FormatFilterReq) GetDefault() {
	if f.CateKey == "" {
		f.CateKey = config.GetMainConfig().Permissions.CateRetrievalKey
	}
	if f.RoleKey == "" {
		f.RoleKey = config.GetMainConfig().Permissions.RoleRetrievalKey
	}
	if f.FullLabelValue == "" {
		f.FullLabelValue = bconfig.App().AttributeLabel.FullLabelValue
	}
	if f.GeneralVectorAttrKey == "" {
		f.GeneralVectorAttrKey = bconfig.App().AttributeLabel.GeneralVectorAttrKey
	}
}

// FormatFilter 格式化过滤条件
func (l *LogicRole) FormatFilter(ctx context.Context, req *FormatFilterReq) (res map[uint64]*retrieval.LabelExpression, needSkip bool, err error) {
	redisCli, err := l.GlobalRedisCli(ctx)
	client := dao.GetRoleDao(l.GetTdsqlGormDB())
	if err != nil {
		log.ErrorContextf(ctx, "GlobalRedisCli err:%v", err)
		return nil, false, errs.ErrGetRoleListFail
	}
	key := fmt.Sprintf(model.RoleKnowledgeRedisKey, req.CorpBizID, req.AppBizID, req.RoleBizID)
	if redisCli.Exists(ctx, key).Val() == 1 { // key存在
		cacheInfo := redisCli.HGetAll(ctx, key).Val()
		res = make(map[uint64]*retrieval.LabelExpression, len(cacheInfo))
		needUpdate := make([]uint64, 0, 3)
		if len(cacheInfo) == 0 {
			return nil, false, nil
		}
		if len(cacheInfo) == 1 { // 角色全部知识库
			if v, ok := cacheInfo[""]; ok && v == "" { // 仅有""->""
				return nil, false, nil
			}
		}
		for k, v := range cacheInfo {
			knowBizID := cast.ToUint64(k)
			if v == "" { // gorm层删除的时候，置为""
				needUpdate = append(needUpdate, knowBizID)
				continue
			}
			knowFilter := &retrieval.LabelExpression{}
			if err := json.Unmarshal([]byte(v), knowFilter); err != nil {
				log.ErrorContextf(ctx, "json.Unmarshal err:%v", err)
				return nil, false, errs.ErrGetRoleListFail
			}
			res[knowBizID] = knowFilter
		}
		if len(needUpdate) != 0 { // gorm层删除的时候，置为"", 这里重新填入
			for _, know := range needUpdate {
				knowFilter, err := l.UpdateKnowledgeFilterCache(ctx, req, know)
				if err != nil {
					log.ErrorContextf(ctx, "UpdateKnowledgeFilterCache err:%v", err)
					return nil, false, errs.ErrGetRoleListFail
				}
				if knowFilter == nil {
					continue
				}
				res[know] = knowFilter
			}
		}
		err = redisCli.Expire(ctx, key, RoleCacheExpire).Err() // 延期14天
		if err != nil {
			log.ErrorContextf(ctx, "redis EXPIRE err:%v", err)
		}
		log.DebugContextf(ctx, "FormatFilter req:%+v res:%+v", req, res)
		return
	}

	// 最后写缓存
	defer func() {
		if err == nil {
			params := make([]interface{}, 0, len(res)*2)
			for key, val := range res {
				b, err := json.Marshal(val)
				if err != nil {
					log.ErrorContextf(ctx, "json.Marshal err:%v", err) // TODO
					continue
				}
				params = append(params, cast.ToString(key))
				params = append(params, string(b))
			}
			if len(params) != 0 {
				err := redisCli.HSet(ctx, key, params...).Err()
				if err != nil {
					log.ErrorContextf(ctx, "redis HMSET err:%v", err)
				}
			}
			err := redisCli.Expire(ctx, key, RoleCacheExpire).Err() // 延期14天
			if err != nil {
				log.ErrorContextf(ctx, "redis EXPIRE err:%v", err)
			}
		}
	}()

	// singleflight

	req.GetDefault()
	appBizID, roleBizID := req.AppBizID, req.RoleBizID
	res = make(map[uint64]*retrieval.LabelExpression, 2)
	_, roles, err := l.ListKnowledgeRoles(ctx, &dao.KnowledgeRoleReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: req.CorpBizID,
			AppBizID:  cast.ToUint64(appBizID),
		},
		BusinessID: roleBizID,
	})
	if err != nil || len(roles) == 0 {
		log.ErrorContextf(ctx, "ListKnowledgeRoles err:%v", err)
		return nil, true, errs.ErrGetRoleListFail
	}

	// 查询角色详情
	role := roles[0]
	if role.SearchType == model.RoleChooseAll { // 全部知识，设置 "":"" 的缓存
		key := fmt.Sprintf(model.RoleKnowledgeRedisKey, req.CorpBizID, req.AppBizID, req.RoleBizID)
		redisCli.Del(ctx, key)
		redisCli.HSet(ctx, key, "", "")
		return nil, false, nil
	}
	knows, err := client.ListKnowledgeRoleKnow(ctx, &dao.KnowledgeRoleKnowReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: req.CorpBizID,
			AppBizID:  cast.ToUint64(appBizID),
		},
		RoleBizID: roleBizID,
	})
	if len(knows) == 0 { //角色选择的知识库为空
		return nil, true, nil // 异常
	}

	if err != nil {
		log.ErrorContextf(ctx, "ListKnowledgeRoleKnow err:%v", err)
		return nil, false, errs.ErrGetRoleListFail
	}
	for _, know := range knows {
		log.DebugContextf(ctx, "FormatFilter know:%+v", know)
		knowFilter, err := l.FormatKnowledgeFilter(ctx, req, know)
		if err != nil {
			log.ErrorContextf(ctx, "FormatKnowledgeFilter err:%v,know:%+v", err, know)
			return nil, false, errs.ErrGetRoleListFail
		}
		if knowFilter == nil {
			log.WarnContextf(ctx, "FormatKnowledgeFilter knowFilter is nil,knowBizID:%d", know.KnowledgeBizID)
			continue
		}
		res[know.KnowledgeBizID] = knowFilter
	}
	log.InfoContextf(ctx, "FormatFilter req:%+v res:%+v", req, res)
	return res, false, nil
}

// FormatKnowledgeFilter 生成知识库筛选值
func (l *LogicRole) FormatKnowledgeFilter(ctx context.Context, req *FormatFilterReq, know *model.KnowledgeRoleKnow) (res *retrieval.LabelExpression, err error) {
	req.GetDefault()
	appBizID, roleBizID := req.AppBizID, req.RoleBizID
	cateKey, roleKey, fullLabelValue, generalVectorAttrKey := req.CateKey, req.RoleKey, req.FullLabelValue, req.GeneralVectorAttrKey
	knowInfo, err := l.GetAppByAppBizID(ctx, know.KnowledgeBizID)
	if err != nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID failed, err: %v", err)
		return nil, err
	}
	if knowInfo == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID failed, knowInfo is nil")
		return nil, errs.ErrGetRoleListFail
	}
	knowID := knowInfo.ID
	client := dao.GetRoleDao(l.GetTdsqlGormDB())
	switch know.SearchType {
	case model.KnowSearchLabel: // 标签
		attrs, err := client.ListKnowledgeRoleAttributeLabel(ctx, &dao.KnowledgeRoleAttributeLabelReq{
			KnowledgeBase: dao.KnowledgeBase{
				CorpBizID: req.CorpBizID,
				AppBizID:  cast.ToUint64(appBizID),
			},
			RoleBizID:      roleBizID,
			KnowledgeBizID: know.KnowledgeBizID,
		})
		if err != nil {
			log.ErrorContextf(ctx, "ListKnowledgeRoleAttributeLabel err:%v", err)
			return nil, err
		}
		attrsBizIDs := make([]uint64, 0, len(attrs))
		labelBizIDs := make([]uint64, 0, len(attrs))
		// 拼key:list 结构
		keyValues := make(map[uint64][]uint64, len(attrs))
		for _, v := range attrs {
			if !slices.Contains(attrsBizIDs, v.AttrBizID) {
				attrsBizIDs = append(attrsBizIDs, v.AttrBizID)
			}
			if !slices.Contains(labelBizIDs, v.LabelBizID) {
				labelBizIDs = append(labelBizIDs, v.LabelBizID)
			}
			if _, ok := keyValues[v.AttrBizID]; !ok {
				keyValues[v.AttrBizID] = make([]uint64, 0, 1)
			}
			keyValues[v.AttrBizID] = append(keyValues[v.AttrBizID], v.LabelBizID)
		}
		attrInfos, err := l.GetAttributeByBizIDs(ctx, knowID, attrsBizIDs)
		if err != nil {
			log.ErrorContextf(ctx, "GetAttributeByBizIDs err:%v", err)
			return nil, err
		}
		labelInfos, err := l.GetAttributeLabelByBizIDs(ctx, labelBizIDs, knowID)
		if err != nil {
			log.ErrorContextf(ctx, "GetAttributeLabelByBizIDs err:%v", err)
			return nil, err
		}
		log.DebugContextf(ctx, "FormatFilter length attrInfos:%+v labelInfos:%+v", attrInfos, labelInfos)

		attrFormats := make([]*retrieval.LabelExpression, 0, len(attrs))
		// 形成attrKey in ("label1","lables2",...)的字符串
		for key, values := range keyValues {
			attr, ok := attrInfos[key]
			if !ok {
				continue
			}
			lableStrs := make([]string, 0, len(values))
			lableStrs = append(lableStrs, fullLabelValue)
			for _, v := range values {
				if v == 0 {
					continue
				} else if labelInfo, ok := labelInfos[v]; ok {
					lableStrs = append(lableStrs, labelInfo.Name)
				}
			}
			// attrFormats = append(attrFormats, fmt.Sprintf("%s in (%s)", attr.AttrKey, strings.Join(lableStrs, ",")))
			attrFormats = append(attrFormats, &retrieval.LabelExpression{
				Operator: retrieval.LabelExpression_NOOP,
				Condition: &retrieval.LabelExpression_Condition{
					Name:   attr.AttrKey,
					Values: lableStrs,
				},
			})
		}
		if len(attrFormats) != 0 {
			attrLabels := &retrieval.LabelExpression{
				Expressions: attrFormats,
			}
			if know.LabelCondition == model.ConditionLogicAnd {
				attrLabels.Operator = retrieval.LabelExpression_AND
			} else if know.LabelCondition == model.ConditionLogicOr {
				attrLabels.Operator = retrieval.LabelExpression_OR
			}
			return &retrieval.LabelExpression{
				Operator: retrieval.LabelExpression_OR,
				Expressions: []*retrieval.LabelExpression{
					{
						Operator: retrieval.LabelExpression_NOOP,
						Condition: &retrieval.LabelExpression_Condition{
							Type:   retrieval.LabelExpression_Condition_STRING,
							Name:   generalVectorAttrKey,
							Values: []string{fullLabelValue},
						},
					},
					attrLabels,
				},
			}, nil
		} else {
			log.WarnContextf(ctx, "FormatFilter attrFormats is empty, knowID:%d", knowID)
			return nil, nil // 异常 比如属性被删除
		}
	case model.KnowSearchSpecial: // 特定知识
		knowFilter := make([]*retrieval.LabelExpression, 0, 3)
		// knowFilter = append(knowFilter, fmt.Sprintf("%s=\"%d\"", roleKey, req.RoleBizID))
		// 添加角色标签
		knowFilter = append(knowFilter, &retrieval.LabelExpression{
			Operator: retrieval.LabelExpression_NOOP,
			Condition: &retrieval.LabelExpression_Condition{
				Name:   roleKey,
				Values: []string{fmt.Sprintf("%d", req.RoleBizID)},
			},
		})
		cates, err := client.ListKnowledgeRoleCate(ctx, &dao.KnowledgeRoleCateReq{
			KnowledgeBase: dao.KnowledgeBase{
				AppBizID:  cast.ToUint64(appBizID),
				CorpBizID: req.CorpBizID,
			},
			RoleBizID:      roleBizID,
			KnowledgeBizID: know.KnowledgeBizID,
		})
		if err != nil {
			log.ErrorContextf(ctx, "ListKnowledgeRoleCate err:%v", err)
			return nil, err
		}
		if len(cates) > 0 {
			var docCateInfo map[int][]int
			var qaCateInfo map[int][]int
			// 这里会导致无法此函数无法单测
			docCateInfo, err = dao.GetCateDao(model.DocCate).GetCateCache(ctx, knowInfo.CorpID, knowInfo.ID)
			if err != nil {
				log.ErrorContextf(ctx, "GetCateCache err:%v", err)
				return nil, err
			}
			qaCateInfo, err = dao.GetCateDao(model.QACate).GetCateCache(ctx, knowInfo.CorpID, knowInfo.ID)
			if err != nil {
				log.ErrorContextf(ctx, "GetCateCache err:%v", err)
				return nil, err
			}
			cateStrs := make([]string, 0, len(cates))
			for _, v := range cates {
				cateList := make([]int, 0, 2)
				cateBizID := int(v.CateBizID)
				cateList = append(cateList, cateBizID)
				if v.CateType == model.CateTypeDoc {
					if v, ok := docCateInfo[cateBizID]; ok {
						cateList = append(cateList, v...)
					}
				} else if v.CateType == model.CateTypeQA {
					if v, ok := qaCateInfo[cateBizID]; ok {
						cateList = append(cateList, v...)
					}
				}
				cateList = slicex.Unique(cateList)
				cateStrs = append(cateStrs, util.ConvertToStringSlice(cateList)...)
			}
			// 添加分类标签
			knowFilter = append(knowFilter, &retrieval.LabelExpression{
				Operator: retrieval.LabelExpression_NOOP,
				Condition: &retrieval.LabelExpression_Condition{
					Name:   cateKey,
					Values: cateStrs,
				},
			})
		}
		return &retrieval.LabelExpression{
			Operator:    retrieval.LabelExpression_OR,
			Expressions: knowFilter,
		}, nil
	case model.KnowSearchAll:
		return &retrieval.LabelExpression{
			Operator: retrieval.LabelExpression_NOOP,
		}, nil
	default:
		return nil, nil
	}
}

// UpdateKnowledgeFilterCache 更新知识库缓存
func (l *LogicRole) UpdateKnowledgeFilterCache(ctx context.Context, req *FormatFilterReq, knowBizID uint64) (res *retrieval.LabelExpression, err error) {
	log.DebugContextf(ctx, "UpdateKnowledgeFilterCache req:%+v knowBizID:%d ", req, knowBizID)
	req.GetDefault()
	client := dao.GetRoleDao(l.GetTdsqlGormDB())
	knows, err := client.ListKnowledgeRoleKnow(ctx, &dao.KnowledgeRoleKnowReq{
		KnowledgeBase: dao.KnowledgeBase{
			AppBizID:  cast.ToUint64(req.AppBizID),
			CorpBizID: req.CorpBizID,
		},
		RoleBizID:      req.RoleBizID,
		KnowledgeBizID: knowBizID,
	})
	if err != nil {
		log.ErrorContextf(ctx, "ListKnowledgeRoleKnow err:%v", err)
		return nil, err
	}
	redisCli, err := l.GlobalRedisCli(ctx)
	if err != nil {
		log.ErrorContextf(ctx, "GlobalRedisCli err:%v", err)
		return nil, err
	}
	key := fmt.Sprintf(model.RoleKnowledgeRedisKey, req.CorpBizID, req.AppBizID, req.RoleBizID)
	if len(knows) == 0 { // 已经解绑，删除缓存
		if err := redisCli.HDel(ctx, key, cast.ToString(knowBizID)).Err(); err != nil {
			log.ErrorContextf(ctx, "HSET err:%v", err)
			return nil, err
		}
		return nil, nil
	}
	know := knows[0]
	knowFilter, err := l.FormatKnowledgeFilter(ctx, req, know)
	if err != nil {
		log.ErrorContextf(ctx, "FormatKnowledgeFilter err:%v", err)
		return nil, err
	}
	b, err := json.Marshal(knowFilter)
	if err != nil {
		log.ErrorContextf(ctx, "Marshal err:%v", err)
		return nil, err
	}
	if err := redisCli.HSet(ctx, key, know.KnowledgeBizID, string(b)).Err(); err != nil {
		log.ErrorContextf(ctx, "HSET err:%v", err)
		return nil, err
	}
	log.DebugContextf(ctx, "UpdateKnowledgeFilterCache success, key:%s, knowBizID:%s", key, know.KnowledgeBizID)
	return knowFilter, nil
}

type DiffElementsReq struct {
	Src      []string
	Dst      []string
	SyncType string
}

// DiffElements 比较两个数组的差异
func DiffElements(req DiffElementsReq, knowledgeBizId uint64) ([]*SyncInfo, []*SyncInfo, error) {
	deleteSyncInfos := make([]*SyncInfo, 0, len(req.Src))
	addSyncInfos := make([]*SyncInfo, 0, len(req.Src))
	deleteTs, _, addTs, err := util.TranslateTo(req.Src, req.Dst, func(s string) string { return s })
	if err != nil {
		return nil, nil, err
	}
	syncType := req.SyncType
	for k, elems := range [][]string{deleteTs, addTs} {
		syncInfos := make([]*SyncInfo, 0, len(elems))
		for _, v := range elems {
			syncInfo := &SyncInfo{
				Type:           syncType,
				KnowledgeBizID: knowledgeBizId,
			}
			switch syncType {
			case SyncInfoTypeDoc, SyncInfoTypeQA, SyncInfoTypeDocCate, SyncInfoTypeQaCate:
				syncInfo.BizID = cast.ToUint64(v)
			case SyncInfoTypeDatabase:
				syncInfo.DatabaseBizID = cast.ToUint64(v)
			case SyncInfoTypeAttr:
				vals := strings.Split(v, "-")
				if len(vals) != 2 {
					return nil, nil, fmt.Errorf("key:%s is invalid", v)
				}
				syncInfo.AttrBizID = cast.ToUint64(vals[0])
				syncInfo.AttrLableBizID = cast.ToUint64(vals[1])
			}
			syncInfos = append(syncInfos, syncInfo)
		}
		if k == 0 {
			deleteSyncInfos = append(deleteSyncInfos, syncInfos...)
		} else {
			addSyncInfos = append(addSyncInfos, syncInfos...)
		}
	}
	return deleteSyncInfos, addSyncInfos, nil
}

// FetchSyncInfoFromChooses 从选择器中获取同步信息
func FetchSyncInfoFromChooses(chooses []*pb.KnowChoose) []*SyncInfo {
	syncInfos := make([]*SyncInfo, 0, 5)
	for _, choose := range chooses {
		knowledgeBizID := cast.ToUint64(choose.GetKnowledgeBizId())
		syncInfos = append(syncInfos, &SyncInfo{
			KnowledgeBizID: knowledgeBizID,
			Type:           SyncInfoTypeKnowledge,
			LibType:        int8(choose.GetType()),
			SearchType:     int8(choose.GetSearchType()),
			LabelCondition: int8(choose.GetCondition()),
		})
		for _, v := range choose.GetDocBizIds() {
			syncInfos = append(syncInfos, &SyncInfo{
				BizID:          cast.ToUint64(v),
				Type:           SyncInfoTypeDoc,
				KnowledgeBizID: knowledgeBizID,
			})
		}
		for _, v := range choose.GetQuesAnsBizIds() {
			syncInfos = append(syncInfos, &SyncInfo{
				BizID:          cast.ToUint64(v),
				Type:           SyncInfoTypeQA,
				KnowledgeBizID: knowledgeBizID,
			})
		}
		for _, v := range choose.GetDocCateBizIds() {
			syncInfos = append(syncInfos, &SyncInfo{
				BizID:          cast.ToUint64(v),
				Type:           SyncInfoTypeDocCate,
				KnowledgeBizID: knowledgeBizID,
			})
		}
		for _, v := range choose.GetQuesAnsCateBizIds() {
			syncInfos = append(syncInfos, &SyncInfo{
				BizID:          cast.ToUint64(v),
				Type:           SyncInfoTypeQaCate,
				KnowledgeBizID: knowledgeBizID,
			})
		}
		for _, v := range choose.GetDbBizIds() {
			syncInfos = append(syncInfos, &SyncInfo{
				DatabaseBizID:  cast.ToUint64(v),
				Type:           SyncInfoTypeDatabase,
				KnowledgeBizID: knowledgeBizID,
			})
		}
		for _, attr := range choose.GetLabels() {
			for _, label := range attr.GetLabels() {
				syncInfos = append(syncInfos, &SyncInfo{
					Type:           SyncInfoTypeAttr,
					KnowledgeBizID: knowledgeBizID,
					AttrBizID:      cast.ToUint64(attr.GetAttrBizId()),
					AttrLableBizID: cast.ToUint64(label.GetLabelBizId()),
				})
			}
		}
	}
	return syncInfos
}
