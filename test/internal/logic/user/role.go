package user

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cast"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/distributedlockx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	bconfig "git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/entity/category"
	dbentity "git.woa.com/adp/kb/kb-config/internal/entity/database"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/internal/entity/label"
	qaEntity "git.woa.com/adp/kb/kb-config/internal/entity/qa"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	retrieval "git.woa.com/adp/pb-go/kb/kb_retrieval"
)

const (
	SyncInfoTypeDoc       string = "doc"        // 文档类型
	SyncInfoTypeQA        string = "qa"         // 问答类型
	SyncInfoTypeDocCate   string = "doc_cate"   // 文档分类类型
	SyncInfoTypeQaCate    string = "qa_cate"    // 问答分类类型
	SyncInfoTypeKnowledge string = "knowledge"  // 知识库类型
	SyncInfoTypeAttr      string = "attr_label" // 标签
	SyncInfoTypeBase      string = "base"       // 基础知识库类型
	SyncInfoTypeDatabase  string = "database"   // 数据库类型

	PresetRoleLockKey = "preset_role_lock_%d"
	ModifyRoleLockKey = "modify_role_lock_%d"
)

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

type AssociationInfo struct {
	BizIDs          []uint64
	KnowledgeBizIDs []uint64
}

func newAssociationInfo() *AssociationInfo {
	return &AssociationInfo{
		BizIDs:          make([]uint64, 0, 3),
		KnowledgeBizIDs: make([]uint64, 0, 3),
	}
}

func (a *AssociationInfo) Input(bizID uint64, knowBizID uint64) {
	if !slices.Contains(a.BizIDs, bizID) {
		a.BizIDs = append(a.BizIDs, bizID)
	}
	if !slices.Contains(a.KnowledgeBizIDs, knowBizID) {
		a.KnowledgeBizIDs = append(a.KnowledgeBizIDs, knowBizID)
	}
}

// VerifyRoleExist 验证角色是否存在
func (l *Logic) VerifyRoleExist(ctx context.Context, appBizID uint64, roleBizID uint64, name string) (bool, error) {
	corpBizID := contextx.Metadata(ctx).CorpBizID()

	_, res, err := l.dao.DescribeKnowledgeRoleList(ctx,
		corpBizID, appBizID,
		&entity.KnowledgeRoleFilter{
			BizIDs: []uint64{roleBizID},
			Name:   name,
		})
	if err != nil {
		logx.E(ctx, "CheckRoleExist failed, err: %v", err)
		return false, err
	}
	return len(res) > 0, nil
}

// VerifyDeleteRole 检查角色是否可删除
func (l *Logic) VerifyDeleteRole(ctx context.Context, appBizID uint64, roleBizIDs []uint64) ([]*entity.KnowledgeRole,
	error) {
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	res, err := l.dao.DescribeUserRoleList(ctx, corpBizID, appBizID,
		&entity.UserRoleFilter{
			RoleBizIDs: roleBizIDs,
		})
	if err != nil {
		logx.E(ctx, "VerifyDeleteRole failed, err: %v", err)
		return nil, err
	}
	roleBizIDsTmp := make([]uint64, 0, len(res))
	for _, v := range res {
		roleBizIDsTmp = append(roleBizIDsTmp, uint64(v.RoleBizID))
	}
	logx.I(ctx, "VerifyDeleteRole roleBizs:%d roleBizIDsTmp: %v", roleBizIDs, roleBizIDsTmp)
	if len(roleBizIDsTmp) == 0 {
		return nil, nil
	}
	_, roles, err := l.dao.DescribeKnowledgeRoleList(ctx, corpBizID, appBizID,
		&entity.KnowledgeRoleFilter{
			BizIDs: roleBizIDsTmp,
			Type:   entity.KnowledgeRoleTypeCustom,
		})
	if err != nil {
		logx.E(ctx, "ListKnowledgeRoles failed, err: %v", err)
		return nil, err
	}

	return roles, nil
}

// createRole 创建新角色
func (l *Logic) createRole(ctx context.Context, role *entity.KnowledgeRole) (err error) {
	now := time.Now()
	role.CreateTime = now
	role.UpdateTime = now
	role.CorpBizID = contextx.Metadata(ctx).CorpBizID()
	_, oldOnes, err := l.dao.DescribeKnowledgeRoleList(ctx,
		contextx.Metadata(ctx).CorpBizID(), role.AppBizID,
		&entity.KnowledgeRoleFilter{
			BizIDs: []uint64{role.BusinessID},
		})
	if err == nil && len(oldOnes) > 0 { // 更新
		role.ID = oldOnes[0].ID
		err := l.dao.ModifyKnowledgeRole(ctx, role)
		if err != nil {
			logx.E(ctx, "UpdateRole failed, err: %v", err)
		}
	} else { // 创建
		if err = l.dao.CreateKnowledgeRole(ctx, role); err != nil {
			logx.E(ctx, "CreateRole failed, err: %v", err)
		}
	}
	return err
}

// removeRoleAssociation 移除角色关联关系
func (l *Logic) removeRoleAssociation(ctx context.Context, appBizID, roleBizId uint64) error {
	// 删除知识库关联
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	knowledgeIds := make([]uint64, 0, 3)
	knows, err := l.dao.DescribeKnowledgeRoleKnowList(ctx, corpBizID, appBizID,
		&entity.KnowledgeRoleKnowFilter{
			RoleBizID: roleBizId,
		})
	if err != nil {
		logx.E(ctx, "ListKnowledgeRoleKnow failed, err: %v", err)
		return err
	}
	for _, v := range knows {
		knowledgeIds = append(knowledgeIds, v.KnowledgeBizID)
	}

	if err := l.dao.Query().Transaction(func(tx *tdsqlquery.Query) error {
		// 删除角色知识库关联
		if err := l.dao.DeleteKnowledgeRoleKnow(ctx, corpBizID, appBizID, &entity.KnowledgeRoleKnowFilter{
			RoleBizID: roleBizId,
		}, tx); err != nil {
			logx.E(ctx, "DeleteKnowledgeRoleKnow failed, err: %v", err)
			return err
		}

		// 删除文档关联
		if err := l.dao.DeleteKnowledgeRoleDocList(ctx, corpBizID, appBizID,
			&entity.KnowledgeRoleDocFilter{
				RoleBizID:       roleBizId,
				KnowledgeBizIDs: knowledgeIds,
			}, tx); err != nil {
			logx.E(ctx, "DeleteKnowledgeRoleDoc failed, err: %v", err)
			return err
		}

		// 删除问答关联
		if err := l.dao.DeleteKnowledgeRoleQAList(ctx, corpBizID, appBizID,
			&entity.KnowledgeRoleQAFilter{
				RoleBizID:       roleBizId,
				KnowledgeBizIDs: knowledgeIds,
			}, tx); err != nil {
			logx.E(ctx, "DeleteKnowledgeRoleQA failed, err: %v", err)
			return err
		}

		// 删除分类关联
		if _, err := l.dao.DeleteKnowledgeRoleCateList(ctx, corpBizID, appBizID,
			&entity.KnowledgeRoleCateFilter{
				RoleBizID:       roleBizId,
				KnowledgeBizIDs: knowledgeIds,
			}, tx); err != nil {
			logx.E(ctx, "DeleteKnowledgeRoleCate failed, err: %v", err)
			return err
		}

		// 删除标签关联
		if _, err := l.dao.DeleteKnowledgeRoleAttributeLabelList(ctx, corpBizID, appBizID,
			&entity.KnowledgeRoleAttributeLabelFilter{
				RoleBizID:       roleBizId,
				KnowledgeBizIDs: knowledgeIds,
			}, tx); err != nil {
			logx.E(ctx, "DeleteKnowledgeRoleAttributeLabel failed, err: %v", err)
			return err
		}

		if _, err := l.dao.DeleteKnowledgeRoleDatabaseList(ctx, corpBizID, appBizID,
			&entity.KnowledgeRoleDatabaseFilter{
				RoleBizID:       roleBizId,
				KnowledgeBizIDs: knowledgeIds,
			}, tx); err != nil {
			logx.E(ctx, "DeleteKnowledgeRoleDatabase failed, err: %v", err)
			return err
		}
		return nil
	}); err != nil {
		logx.E(ctx, "removeRoleAssociation failed, err: %+v", err)
		return err
	}
	return nil
}

// removeAssociations 移除角色关联
func (l *Logic) removeAssociations(ctx context.Context, appBizID, roleBizId uint64, syncInfos []*SyncInfo) (err error) {
	docBizIds := newAssociationInfo()
	qaBizIds := newAssociationInfo()
	cateBizIds := newAssociationInfo()
	databases := newAssociationInfo()
	attrlabels := newAssociationInfo()
	attrInfos := newAssociationInfo()
	knowledgeLibs := make([]uint64, 0, 3)

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
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	if err := l.dao.Query().Transaction(func(tx *tdsqlquery.Query) error {
		if len(knowledgeLibs) > 0 {
			if err := l.dao.DeleteKnowledgeRoleKnow(ctx, corpBizID, appBizID, &entity.KnowledgeRoleKnowFilter{
				KnowledgeBizIDs: knowledgeLibs,
				RoleBizID:       roleBizId,
				Limit:           len(knowledgeLibs),
			}, tx); err != nil {
				return err
			}
		}
		if len(docBizIds.BizIDs) > 0 {
			if err := l.dao.DeleteKnowledgeRoleDocList(ctx, corpBizID, appBizID,
				&entity.KnowledgeRoleDocFilter{
					RoleBizID:       roleBizId,
					DocBizIDs:       docBizIds.BizIDs,
					KnowledgeBizIDs: docBizIds.KnowledgeBizIDs,
				}, tx); err != nil {
				return err
			}
		}
		if len(qaBizIds.BizIDs) > 0 {
			if err := l.dao.DeleteKnowledgeRoleQAList(ctx, corpBizID, appBizID,
				&entity.KnowledgeRoleQAFilter{
					RoleBizID:       roleBizId,
					QABizIDs:        qaBizIds.BizIDs,
					KnowledgeBizIDs: qaBizIds.KnowledgeBizIDs,
				}, tx); err != nil {
				return err
			}
		}
		if len(cateBizIds.BizIDs) > 0 {
			if _, err := l.dao.DeleteKnowledgeRoleCateList(ctx, corpBizID, appBizID,
				&entity.KnowledgeRoleCateFilter{
					RoleBizID:       roleBizId,
					CateBizIDs:      cateBizIds.BizIDs,
					KnowledgeBizIDs: cateBizIds.KnowledgeBizIDs,
					Limit:           len(cateBizIds.BizIDs),
				}, tx); err != nil {
				return err
			}
		}
		if len(attrlabels.BizIDs) > 0 {
			if _, err := l.dao.DeleteKnowledgeRoleAttributeLabelList(ctx, corpBizID, appBizID,
				&entity.KnowledgeRoleAttributeLabelFilter{
					RoleBizID:       roleBizId,
					LabelBizIDs:     attrlabels.BizIDs,
					KnowledgeBizIDs: attrlabels.KnowledgeBizIDs,
				}, tx); err != nil {
				return err
			}
		}
		if len(attrInfos.BizIDs) > 0 {
			if _, err := l.dao.DeleteKnowledgeRoleAttributeLabelList(ctx, corpBizID, appBizID,
				&entity.KnowledgeRoleAttributeLabelFilter{
					RoleBizID:       roleBizId,
					AttrBizIDs:      attrInfos.BizIDs,
					KnowledgeBizIDs: attrInfos.KnowledgeBizIDs,
					LabelBizIDs:     []uint64{0},
				}, tx); err != nil {
				return err
			}
		}
		if len(databases.BizIDs) > 0 {
			if _, err := l.dao.DeleteKnowledgeRoleDatabaseList(ctx, corpBizID, appBizID,
				&entity.KnowledgeRoleDatabaseFilter{
					RoleBizID:       roleBizId,
					DatabaseBizIDs:  databases.BizIDs,
					KnowledgeBizIDs: databases.KnowledgeBizIDs,
					Limit:           len(databases.BizIDs),
				}, tx); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		logx.E(ctx, "removeAssociations failed, err: +%v", err)
		return err
	}
	return nil
}

// addAssociations 添加角色关联
func (l *Logic) addAssociations(ctx context.Context, appBizID, roleBizId uint64, syncInfos []*SyncInfo) (err error) {
	docBizs := make([]*entity.KnowledgeRoleDoc, 0, 3)
	qaBizs := make([]*entity.KnowledgeRoleQA, 0, 3)
	cateBizs := make([]*entity.KnowledgeRoleCate, 0, 3)
	knowledgeLibs := make([]*entity.KnowledgeRoleKnow, 0, 3)
	attrLabels := make([]*entity.KnowledgeRoleAttributeLabel, 0, 3)
	knowledgeKnow := &entity.KnowledgeRoleKnow{}
	databases := make([]*entity.KnowledgeRoleDatabase, 0, 3)

	corpBizID := contextx.Metadata(ctx).CorpBizID()
	for _, v := range syncInfos {
		switch v.Type {
		case SyncInfoTypeDoc:
			docBizs = append(docBizs, &entity.KnowledgeRoleDoc{
				CorpBizID:      corpBizID,
				AppBizID:       appBizID,
				RoleBizID:      roleBizId,
				DocBizID:       v.BizID,
				KnowledgeBizID: v.KnowledgeBizID,
			})
		case SyncInfoTypeQA:
			qaBizs = append(qaBizs, &entity.KnowledgeRoleQA{
				CorpBizID:      corpBizID,
				AppBizID:       appBizID,
				RoleBizID:      roleBizId,
				QABizID:        v.BizID,
				KnowledgeBizID: v.KnowledgeBizID,
			})
		case SyncInfoTypeDocCate:
			cateBizs = append(cateBizs, &entity.KnowledgeRoleCate{
				CorpBizID:      corpBizID,
				AppBizID:       appBizID,
				RoleBizID:      roleBizId,
				CateBizID:      v.BizID,
				CateType:       entity.CateTypeDoc,
				KnowledgeBizID: v.KnowledgeBizID,
			})
		case SyncInfoTypeQaCate:
			cateBizs = append(cateBizs, &entity.KnowledgeRoleCate{
				CorpBizID:      corpBizID,
				AppBizID:       appBizID,
				RoleBizID:      roleBizId,
				CateBizID:      v.BizID,
				CateType:       entity.CateTypeQA,
				KnowledgeBizID: v.KnowledgeBizID,
			})
		case SyncInfoTypeKnowledge:
			knowledgeLibs = append(knowledgeLibs, &entity.KnowledgeRoleKnow{
				CorpBizID:      corpBizID,
				AppBizID:       appBizID,
				RoleBizID:      roleBizId,
				KnowledgeBizID: v.KnowledgeBizID,
				KnowledgeType:  v.LibType,
				SearchType:     v.SearchType,
				LabelCondition: v.LabelCondition,
			})
		case SyncInfoTypeAttr:
			attrLabels = append(attrLabels, &entity.KnowledgeRoleAttributeLabel{
				CorpBizID:      corpBizID,
				AppBizID:       appBizID,
				RoleBizID:      roleBizId,
				KnowledgeBizID: v.KnowledgeBizID,
				AttrBizID:      v.AttrBizID,
				LabelBizID:     v.AttrLableBizID,
			})
		case SyncInfoTypeBase:
			knowledgeKnow = &entity.KnowledgeRoleKnow{
				CorpBizID:      corpBizID,
				AppBizID:       appBizID,
				RoleBizID:      roleBizId,
				KnowledgeBizID: v.KnowledgeBizID,
				LabelCondition: v.LabelCondition,
				KnowledgeType:  v.LibType,
				SearchType:     v.SearchType,
			}
		case SyncInfoTypeDatabase:
			databases = append(databases, &entity.KnowledgeRoleDatabase{
				CorpBizID:      corpBizID,
				AppBizID:       appBizID,
				RoleBizID:      roleBizId,
				KnowledgeBizID: v.KnowledgeBizID,
				DatabaseBizID:  v.DatabaseBizID,
			})
		}
	}
	if err := l.dao.Query().Transaction(func(tx *tdsqlquery.Query) error {
		if len(docBizs) > 0 {
			err = l.dao.CreateKnowledgeRoleDocList(ctx, docBizs, tx)
			if err != nil {
				logx.E(ctx, "CreateKnowledgeRoleDocList failed, err: %v", err)
				return err
			}
		}
		if len(qaBizs) > 0 {
			err = l.dao.CreateKnowledgeRoleQAList(ctx, qaBizs, tx)
			if err != nil {
				logx.E(ctx, "CreateKnowledgeRoleQAList failed, err: %v", err)
				return err
			}
		}
		if len(cateBizs) > 0 {
			err = l.dao.CreateKnowledgeRoleCateList(ctx, cateBizs, tx)
			if err != nil {
				logx.E(ctx, "CreateKnowledgeRoleCateList failed, err: %v", err)
				return err
			}
		}
		if len(knowledgeLibs) > 0 {
			err = l.dao.CreateKnowledgeRoleKnowList(ctx, knowledgeLibs, tx)
			if err != nil {
				logx.E(ctx, "CreateKnowledgeRoleKnowList failed, err: %v", err)
				return err
			}
		}
		if len(attrLabels) > 0 {
			err = l.dao.CreateKnowledgeRoleAttributeLabelList(ctx, attrLabels, tx)
			if err != nil {
				logx.E(ctx, "CreateKnowledgeRoleAttributeLabelList failed, err: %v", err)
				return err
			}
		}
		if knowledgeKnow.KnowledgeBizID != 0 && knowledgeKnow.RoleBizID != 0 {
			_, err = l.dao.ModifyKnowledgeRoleKnow(ctx, knowledgeKnow, tx)
			if err != nil {
				logx.E(ctx, "ModifyKnowledgeRoleKnow failed, err: %v", err)
				return err
			}
		}
		if len(databases) > 0 {
			err = l.dao.CreateKnowledgeRoleDatabaseList(ctx, databases, tx)
			if err != nil {
				logx.E(ctx, "CreateKnowledgeRoleDatabaseList failed, err: %v", err)
				return err
			}
		}
		return nil
	}); err != nil {
		logx.E(ctx, "addAssociations failed, err: %+v", err)
		return err
	}
	return nil
}

func (l *Logic) describeKnowChoose(ctx context.Context,
	corpBizID, appBizID, roleBizID uint64,
	knowledgeBizID2Name map[uint64]string,
	knowLib *entity.KnowledgeRoleKnow) (*entity.KnowledgeChoose, error) {
	knowBizId := knowLib.KnowledgeBizID
	knowledgeName := ""
	if knowBizId == appBizID {
		knowledgeName = contextx.Metadata(ctx).AppName()
	} else if v, ok := knowledgeBizID2Name[knowBizId]; ok {
		knowledgeName = v
	}
	var err error
	know, err := l.rpc.AppAdmin.DescribeAppById(ctx, knowBizId)
	if err != nil || know == nil {
		logx.E(ctx, "GetAppByAppBizID failed, err: %v", err)
		return nil, err
	}
	knowID := know.PrimaryId
	choose := &entity.KnowledgeChoose{
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
	docs, err := l.dao.DescribeKnowledgeRoleDocList(ctx, corpBizID, appBizID,
		&entity.KnowledgeRoleDocFilter{
			RoleBizID:       roleBizID,
			KnowledgeBizIDs: []uint64{knowBizId},
		})
	if err != nil {
		logx.E(ctx, "GetKnowledgeRoleDoc failed, err: %v", err)
		return nil, err
	}
	for _, doc := range docs {
		choose.DocBizIds = append(choose.DocBizIds, cast.ToString(doc.DocBizID))
	}

	// 查询关联问答
	qas, err := l.dao.DescribeKnowledgeRoleQAList(ctx, corpBizID, appBizID,
		&entity.KnowledgeRoleQAFilter{
			RoleBizID:       roleBizID,
			KnowledgeBizIDs: []uint64{knowBizId},
		})
	if err != nil {
		logx.E(ctx, "GetKnowledgeRoleQA failed, err: %v", err)
		return nil, err
	}
	for _, qa := range qas {
		choose.QuesAnsBizIds = append(choose.QuesAnsBizIds, cast.ToString(qa.QABizID))
	}

	// 查询分类
	cates, err := l.dao.DescribeKnowledgeRoleCateList(ctx, corpBizID, appBizID,
		&entity.KnowledgeRoleCateFilter{
			RoleBizID:       roleBizID,
			KnowledgeBizIDs: []uint64{knowBizId},
		})
	if err != nil {
		logx.E(ctx, "GetKnowledgeRoleCate failed, err: %v", err)
		return nil, err
	}
	for _, cate := range cates {
		switch cate.CateType {
		case entity.CateTypeDoc:
			choose.DocCateBizIds = append(choose.DocCateBizIds, cast.ToString(cate.CateBizID))
		case entity.CateTypeQA:
			choose.QuesAnsCateBizIds = append(choose.QuesAnsCateBizIds, cast.ToString(cate.CateBizID))
		}
	}

	databases, err := l.dao.DescribeKnowledgeRoleDatabaseList(ctx, corpBizID, appBizID,
		&entity.KnowledgeRoleDatabaseFilter{
			RoleBizID:       roleBizID,
			KnowledgeBizIDs: []uint64{knowBizId},
		})
	if err != nil {
		logx.E(ctx, "GetKnowledgeRoleDatabase failed, err: %v", err)
		return nil, err
	}
	for _, v := range databases {
		choose.DbBizIds = append(choose.DbBizIds, cast.ToString(v.DatabaseBizID))
	}

	// 查询标签
	labels, err := l.dao.DescribeKnowledgeRoleAttributeLabelList(ctx, corpBizID, appBizID,
		&entity.KnowledgeRoleAttributeLabelFilter{
			RoleBizID:       roleBizID,
			KnowledgeBizIDs: []uint64{knowBizId},
		})
	if err != nil {
		logx.E(ctx, "GetKnowledgeRoleAttributeLabel failed, err: %v", err)
		return nil, err
	}
	if len(labels) > 0 {
		attrs := make([]uint64, 0, len(labels))
		attrLabels := make([]uint64, 0, len(labels))
		for _, v := range labels {
			if !slices.Contains(attrs, v.AttrBizID) {
				attrs = append(attrs, v.AttrBizID)
			}
			if !slices.Contains(attrLabels, v.LabelBizID) {
				attrLabels = append(attrLabels, v.LabelBizID)
			}
		}
		attrNames := make(map[uint64]*label.Attribute)
		if knowID != 0 {
			attrNames, err = l.labelDao.GetAttributeByBizIDs(ctx, knowID, attrs)
			if err != nil {
				logx.E(ctx, "GetAttributeByBizIDs failed, err: %v", err)
				return nil, err
			}
		}
		attrLabelNames, err := l.labelDao.GetAttributeLabelByBizIDs(ctx, attrLabels, knowID)
		if err != nil {
			logx.E(ctx, "GetAttributeLabelByBizIDs failed, err: %v", err)
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
				choose.Labels = append(choose.Labels, &entity.ChooseLabel{
					AttrBizId: cast.ToString(v.AttrBizID),
					AttrName:  attrName,
					Labels:    make([]*entity.ChooseLabelLabel, 0, 2),
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
			lastOne.Labels = append(lastOne.Labels, &entity.ChooseLabelLabel{
				LabelBizId: cast.ToString(v.LabelBizID),
				LabelName:  gox.IfElse(v.LabelBizID == 0, "全部", labelName),
			})
		}
	}
	return choose, nil
}

// DescribeDetailKnowledgeRole 获取角色详情
func (l *Logic) DescribeDetailKnowledgeRole(ctx context.Context, appBizID uint64,
	roleBizIDs []uint64) ([]*entity.KnowledgeRole, map[uint64][]*entity.KnowledgeChoose, error) {
	// 根据Create的逻辑，查询数据库，构建CreateRoleReq对象
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	res := make([]*entity.KnowledgeRole, 0, len(roleBizIDs))
	_, roles, err := l.dao.DescribeKnowledgeRoleList(ctx,
		corpBizID, appBizID,
		&entity.KnowledgeRoleFilter{
			BizIDs: roleBizIDs,
		})
	if err != nil {
		logx.E(ctx, "DescribeKnowledgeRoleList failed, err: %v", err)
		return nil, nil, err
	}
	roleBizID2Chooses := make(map[uint64][]*entity.KnowledgeChoose, len(roles))
	for _, role := range roles {
		knows, err := l.dao.DescribeKnowledgeRoleKnowList(ctx, corpBizID, appBizID, &entity.KnowledgeRoleKnowFilter{
			RoleBizID: role.BusinessID,
		})
		if err != nil {
			logx.E(ctx, "GetKnowledgeRoleKnow failed, err: %v", err)
			return nil, nil, err
		}
		knowledgeBizID2Name := make(map[uint64]string, len(knows))
		knowledgeBizIDs := make([]uint64, 0, len(knows))
		for _, v := range knows {
			if slices.Contains(knowledgeBizIDs, v.KnowledgeBizID) {
				continue
			}
			knowledgeBizIDs = append(knowledgeBizIDs, v.KnowledgeBizID)
		}
		shareKnowledgeFilter := kbe.ShareKnowledgeFilter{
			CorpBizID: corpBizID,
			BizIds:    knowledgeBizIDs,
		}
		knowledges, err := l.kbDao.RetrieveBaseSharedKnowledge(ctx, &shareKnowledgeFilter)
		if err != nil && !errors.Is(err, errx.ErrNotFound) {
			logx.W(ctx, "RetrieveBaseSharedKnowledge failed, err: %v", err)
		}
		for _, v := range knowledges {
			knowledgeBizID2Name[v.BusinessID] = v.Name
		}

		chooses := make([]*entity.KnowledgeChoose, 0, len(knows))
		for _, knowLib := range knows {
			choose, err := l.describeKnowChoose(ctx, corpBizID, appBizID, role.BusinessID, knowledgeBizID2Name, knowLib)
			if err != nil {
				logx.E(ctx, "describeKnowChoose failed, err: %v", err)
				continue
			}
			chooses = append(chooses, choose)
		}
		res = append(res, role)
		roleBizID2Chooses[role.BusinessID] = chooses
	}
	return res, roleBizID2Chooses, nil
}

// ModifyRole 修改角色信息
func (l *Logic) ModifyRole(ctx context.Context,
	role *entity.KnowledgeRole, newChooses []*entity.KnowledgeChoose, isCreate bool) (uint64, []*SyncInfo, error) {
	// 创建预设角色
	appBizID := role.AppBizID
	roleBizID := role.BusinessID
	roleType := role.Type
	if roleBizID == 0 {
		roleBizID = idgen.GetId()
	}

	if !isCreate && roleBizID == 1 { // 编辑的时候，设定为预置角色
		roleType = entity.KnowledgeRoleTypePreset
		roleBizID = idgen.GetId()
	}
	role.Type = roleType
	role.BusinessID = roleBizID
	key := fmt.Sprintf(ModifyRoleLockKey, roleBizID)
	lock := distributedlockx.NewRedisLock(l.lockRdb, key, distributedlockx.WithTTL(time.Second*30))
	success, err := lock.Lock(ctx)
	if err != nil {
		logx.E(ctx, "Lock err:%v", err)
		return 0, nil, errs.ErrAlreadyLocked
	}
	if success { // 加锁成功
		defer func() {
			if err = lock.Unlock(ctx); err != nil {
				logx.E(ctx, "Unlock err:%v", err)
			}
		}()
	} else {
		logx.W(ctx, "ModifyRole lock failed, appBizID:%d", appBizID)
		return 0, nil, errs.ErrAlreadyLocked
	}

	_, roleBizID2Chooses, err := l.DescribeDetailKnowledgeRole(ctx, appBizID, []uint64{roleBizID})
	if err != nil {
		logx.E(ctx, "DescribeDetailKnowledgeRole failed, err: %v", err)
		return 0, nil, err
	}
	oldChooses := roleBizID2Chooses[roleBizID]

	if roleBizID == 0 {
		roleBizID = idgen.GetId()
		role.BusinessID = roleBizID
	}

	err = l.createRole(ctx, role)
	if err != nil {
		logx.E(ctx, "createRole failed, err: %v", err)
	}

	deletes, adds, err := l.diff2KnowChooses(oldChooses, newChooses)
	if err != nil {
		logx.E(ctx, "Diff2KonwChooses failed, err: %v", err)
		return 0, nil, err
	}
	logx.I(ctx, "ModifyRole deletes: %v, adds: %v", deletes, adds)
	// 删除关联关系
	err = l.removeAssociations(ctx, appBizID, roleBizID, deletes)
	if err != nil {
		logx.E(ctx, "removeAssociations failed, err: %v", err)
	}
	// 添加关联关系
	err = l.addAssociations(ctx, appBizID, roleBizID, adds)
	if err != nil {
		logx.E(ctx, "AddAssociations failed, err: %v", err)
	}
	syncInfos := append(deletes, adds...)
	logx.I(ctx, "ModifyRole syncInfos: %+v", syncInfos)

	return roleBizID, syncInfos, nil
}

type DiffElementsReq struct {
	Src      []string
	Dst      []string
	SyncType string
}

// diffElements 比较两个数组的差异
func diffElements(req DiffElementsReq, knowledgeBizId uint64) ([]*SyncInfo, []*SyncInfo, error) {
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

// fetchSyncInfoFromChooses 从选择器中获取同步信息
func fetchSyncInfoFromChooses(chooses []*entity.KnowledgeChoose) []*SyncInfo {
	syncInfos := make([]*SyncInfo, 0, 5)
	for _, choose := range chooses {
		knowledgeBizID := cast.ToUint64(choose.KnowledgeBizId)
		syncInfos = append(syncInfos, &SyncInfo{
			KnowledgeBizID: knowledgeBizID,
			Type:           SyncInfoTypeKnowledge,
			LibType:        int8(choose.Type),
			SearchType:     int8(choose.SearchType),
			LabelCondition: int8(choose.Condition),
		})
		for _, v := range choose.DocBizIds {
			syncInfos = append(syncInfos, &SyncInfo{
				BizID:          cast.ToUint64(v),
				Type:           SyncInfoTypeDoc,
				KnowledgeBizID: knowledgeBizID,
			})
		}
		for _, v := range choose.QuesAnsBizIds {
			syncInfos = append(syncInfos, &SyncInfo{
				BizID:          cast.ToUint64(v),
				Type:           SyncInfoTypeQA,
				KnowledgeBizID: knowledgeBizID,
			})
		}
		for _, v := range choose.DocCateBizIds {
			syncInfos = append(syncInfos, &SyncInfo{
				BizID:          cast.ToUint64(v),
				Type:           SyncInfoTypeDocCate,
				KnowledgeBizID: knowledgeBizID,
			})
		}
		for _, v := range choose.QuesAnsCateBizIds {
			syncInfos = append(syncInfos, &SyncInfo{
				BizID:          cast.ToUint64(v),
				Type:           SyncInfoTypeQaCate,
				KnowledgeBizID: knowledgeBizID,
			})
		}
		for _, v := range choose.DbBizIds {
			syncInfos = append(syncInfos, &SyncInfo{
				DatabaseBizID:  cast.ToUint64(v),
				Type:           SyncInfoTypeDatabase,
				KnowledgeBizID: knowledgeBizID,
			})
		}
		for _, attr := range choose.Labels {
			for _, label := range attr.Labels {
				syncInfos = append(syncInfos, &SyncInfo{
					Type:           SyncInfoTypeAttr,
					KnowledgeBizID: knowledgeBizID,
					AttrBizID:      cast.ToUint64(attr.AttrBizId),
					AttrLableBizID: cast.ToUint64(label.LabelBizId),
				})
			}
		}
	}
	return syncInfos
}

func verify2ChoosesEqual(a, b *entity.KnowledgeChoose) bool {
	if a.KnowledgeBizId != b.KnowledgeBizId {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	if a.SearchType != b.SearchType {
		return false
	}
	if a.Condition != b.Condition {
		return false
	}
	return true
}

// diff2KnowChooses 对比新旧知识库选择差异
func (l *Logic) diff2KnowChooses(oldChooses []*entity.KnowledgeChoose,
	newChooses []*entity.KnowledgeChoose) ([]*SyncInfo, []*SyncInfo, error) {
	deleteSyncInfos := make([]*SyncInfo, 0, 5)
	addSyncInfos := make([]*SyncInfo, 0, 5)
	deletes, exists, adds, _ := util.TranslateTo(oldChooses, newChooses, func(a *entity.KnowledgeChoose) string {
		return cast.ToString(a.KnowledgeBizId)
	})
	for k, chooses := range [][]*entity.KnowledgeChoose{deletes, adds} {
		syncInfos := fetchSyncInfoFromChooses(chooses)
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
		for k, attrInfo := range [2][]*entity.ChooseLabel{old.Labels, new.Labels} {
			attrInfos := make([]string, 0, len(attrInfo))
			for _, attr := range attrInfo {
				for _, label := range attr.Labels {
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
			deletes, adds, err := diffElements(param, cast.ToUint64(new.KnowledgeBizId))
			if err != nil {
				return nil, nil, err
			}
			deleteSyncInfos = append(deleteSyncInfos, deletes...)
			addSyncInfos = append(addSyncInfos, adds...)
		}

		if !verify2ChoosesEqual(old, new) {
			addSyncInfos = append(addSyncInfos, &SyncInfo{
				Type:           SyncInfoTypeBase,
				LibType:        int8(new.Type),
				SearchType:     int8(new.SearchType),
				LabelCondition: int8(new.Condition),
				KnowledgeBizID: cast.ToUint64(old.KnowledgeBizId),
			})
		}
	}
	return deleteSyncInfos, addSyncInfos, nil
}

func (l *Logic) DeleteKnowledgeRole(ctx context.Context, appBizID uint64, roleBizIDs []uint64) (err error) {
	corpBizID := contextx.Metadata(ctx).CorpBizID()
	for _, roleBizID := range roleBizIDs {
		if err := l.dao.DeleteKnowledgeRole(ctx, corpBizID, appBizID,
			&entity.KnowledgeRoleFilter{
				BizIDs: []uint64{roleBizID},
				Type:   entity.KnowledgeRoleTypeCustom,
			}); err != nil {
			return err
		}
		if err := l.removeRoleAssociation(ctx, appBizID, roleBizID); err != nil {
			return err
		}
	}
	return nil
}

// VerifyKnowChoose 检查知识库选择权限
func (l *Logic) VerifyKnowChoose(ctx context.Context, appBizID uint64, choose *entity.KnowledgeChoose) error {
	docBizIDs := convx.SliceStringToUint64(choose.DocBizIds)
	qaBizIDs := convx.SliceStringToUint64(choose.QuesAnsBizIds)
	docCateBizIDs := convx.SliceStringToUint64(choose.DocCateBizIds)
	qaCateBizIDs := convx.SliceStringToUint64(choose.QuesAnsCateBizIds)

	appID := contextx.Metadata(ctx).AppID()
	knowID := appID
	if appBizID != cast.ToUint64(choose.KnowledgeBizId) { // 共享知识库
		var err error
		know, err := l.rpc.AppAdmin.DescribeAppById(ctx, cast.ToUint64(choose.KnowledgeBizId))
		if err != nil {
			logx.E(ctx, "VerifyKnowChoose DescribeAppById %s err:%v", choose.KnowledgeBizId,
				err)
			return errs.ErrGetKnowledgeFailed
		}
		if know != nil {
			knowID = know.PrimaryId
		}
	}

	if len(docBizIDs) != 0 {
		docs, err := l.docLogic.GetDocByBizIDs(ctx, docBizIDs, knowID)
		if err != nil {
			logx.E(ctx, "CheckKnowChoose err:%v", err)
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
		res, err := l.qaLogic.GetQAsByBizIDs(ctx, contextx.Metadata(ctx).CorpID(), knowID, qaBizIDs, 0, len(qaBizIDs))
		if err != nil {
			logx.E(ctx, "CheckKnowChoose err:%v", err)
			return err
		}
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
		res, err := l.cateDao.DescribeCateListByBusinessIDs(ctx, category.DocCate, contextx.Metadata(ctx).CorpID(),
			knowID, docCateBizIDs)
		if err != nil {
			logx.E(ctx, "CheckKnowChoose err:%v", err)
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
		res, err := l.cateDao.DescribeCateListByBusinessIDs(ctx, category.QACate, contextx.Metadata(ctx).CorpID(),
			knowID, qaCateBizIDs)
		if err != nil {
			logx.E(ctx, "CheckKnowChoose err:%v", err)
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
func (l *Logic) DescribeRoleSearch(ctx context.Context,
	appBizID, knowBizID uint64, type2SearchBizIds map[uint32][]string) ([]*entity.RoleSearchInfo, error) {
	res := make([]*entity.RoleSearchInfo, 0, len(type2SearchBizIds))
	knowID := contextx.Metadata(ctx).AppID()
	corpID := contextx.Metadata(ctx).CorpID()
	if appBizID != knowBizID { // 共享知识库
		know, err := l.rpc.AppAdmin.DescribeAppById(ctx, knowBizID)
		if err != nil || know == nil {
			logx.E(ctx, "DescribeRoleSearch RetrieveBaseSharedKnowledge err:%v,knowBizID:%d", err, knowBizID)
			return nil, errs.ErrGetKnowledgeFailed
		}
		knowID = know.PrimaryId
		knowBizID = know.BizId
	}

	for searchType, searchBizIDs := range type2SearchBizIds {
		bizIds := convx.SliceStringToUint64(searchBizIDs)
		switch searchType {
		case entity.SearchTypeDoc: // 搜索文档
			docs, err := l.docLogic.GetDocByBizIDs(ctx, bizIds, knowID)
			if err != nil {
				logx.E(ctx, "DescribeRoleSearch err:%v", err)
				return nil, err
			}
			cateIds := make([]uint64, 0, len(docs))
			docList := make([]*entity.RoleSearchInfo, 0, len(docs))
			for _, bizId := range bizIds {
				if v, ok := docs[bizId]; ok {
					docList = append(docList, &entity.RoleSearchInfo{
						Type:        searchType,
						SearchBizId: bizId,
						Name:        v.FileName,
						CateBizId:   uint64(v.CategoryID),
					})
					cateIds = append(cateIds, uint64(v.CategoryID))
				}
			}

			// 通过cate主键ID获取BussinessID
			cateBizIDMap, err := l.cateDao.DescribeCateByIDs(ctx, category.DocCate, cateIds)
			if err != nil {
				logx.E(ctx, "DescribeRoleSearch err:%v", err)
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

		case entity.SearchTypeQA: // 搜索问答
			qas, err := l.qaLogic.GetQAsByBizIDs(ctx, contextx.Metadata(ctx).CorpID(), knowID, bizIds, 0, len(bizIds))
			if err != nil {
				logx.E(ctx, "DescribeRoleSearch err:%v", err)
				return nil, err
			}
			cateIds := make([]uint64, 0, len(qas))
			qaList := make([]*entity.RoleSearchInfo, 0, len(qas))
			for _, qa := range qas {
				qaList = append(qaList, &entity.RoleSearchInfo{
					Type:        searchType,
					SearchBizId: qa.BusinessID,
					CateBizId:   uint64(qa.CategoryID),
					Name:        qa.Question,
				})
				cateIds = append(cateIds, uint64(qa.CategoryID))
			}

			// 通过cate主键ID获取BussinessID
			cateBizIDMap, err := l.cateDao.DescribeCateByIDs(ctx, category.DocCate, cateIds)
			if err != nil {
				logx.E(ctx, "DescribeRoleSearch err:%v", err)
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
		case entity.SearchTypeDocCate: // 搜索文档分类
			docCates, err := l.cateDao.DescribeCateListByBusinessIDs(ctx, category.DocCate, corpID, knowID, bizIds)
			if err != nil {
				logx.E(ctx, "DescribeRoleSearch err:%v", err)
				return nil, err
			}

			cateList := make([]*entity.RoleSearchInfo, 0, len(docCates))
			for _, cate := range docCates {
				cateList = append(cateList, &entity.RoleSearchInfo{
					Type:        searchType,
					SearchBizId: cate.BusinessID,
					Name:        cate.Name,
				})
			}
			res = append(res, cateList...)
		case entity.SearchTypeQACate: // 搜索问答分类
			qaCates, err := l.cateDao.DescribeCateListByBusinessIDs(ctx, category.QACate, corpID, knowID, bizIds)
			if err != nil {
				logx.E(ctx, "DescribeRoleSearch err:%v", err)
				return nil, err
			}

			cateList := make([]*entity.RoleSearchInfo, 0, len(qaCates))
			for _, cate := range qaCates {
				cateList = append(cateList, &entity.RoleSearchInfo{
					Type:        searchType,
					SearchBizId: cate.BusinessID,
					Name:        cate.Name,
				})
			}
			res = append(res, cateList...)
		case entity.SearchTypeDatabase: // 搜索数据库
			tableFilter := dbentity.TableFilter{
				CorpBizID:     contextx.Metadata(ctx).CorpBizID(),
				AppBizID:      knowBizID,
				DBTableBizIDs: bizIds,
			}
			dbTableInfos, _, err := l.dbDao.DescribeTableList(ctx, &tableFilter)
			if err != nil {
				logx.E(ctx, "DescribeRoleSearch DB err:%v", err)
				return nil, err
			}
			dbList := make([]*entity.RoleSearchInfo, 0, len(dbTableInfos))
			for _, table := range dbTableInfos {
				dbList = append(dbList, &entity.RoleSearchInfo{
					Type:        searchType,
					SearchBizId: table.DBTableBizID,
					Name:        table.Name,
				})
			}
			res = append(res, dbList...)
		}
	}
	return res, nil
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
		f.CateKey = bconfig.GetMainConfig().Permissions.CateRetrievalKey
	}
	if f.RoleKey == "" {
		f.RoleKey = bconfig.GetMainConfig().Permissions.RoleRetrievalKey
	}
	if f.FullLabelValue == "" {
		f.FullLabelValue = bconfig.App().AttributeLabel.FullLabelValue
	}
	if f.GeneralVectorAttrKey == "" {
		f.GeneralVectorAttrKey = bconfig.App().AttributeLabel.GeneralVectorAttrKey
	}
}

// SyncRoleInfo 同步角色信息
type SyncRoleInfo struct {
	DocBizIDs     []uint64
	QaBizIDs      []uint64
	DbTableBizIDs []uint64
}

func (l *Logic) RoleSyncInfos(ctx context.Context, appBizId uint64, roleBizId uint64, infos []*SyncInfo) error {
	defer func() {
		if err := recover(); err != nil {
			logx.E(ctx, "RoleSyncInfos err:%v", err)
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
		logx.D(ctx, "RoleSyncInfos knowBizID:%v,info:%+v", knowBizID, v)
	}
	err := l.syncRoleVector(ctx, appBizId, syncRoleInfo)
	if err != nil {
		logx.E(ctx, "RoleSyncInfos err:%v", err)
		return err
	}

	// 更新缓存
	if len(syncRoleInfo) != 0 {
		req := &FormatFilterReq{
			AppID:     contextx.Metadata(ctx).AppID(),
			AppBizID:  appBizId,
			RoleBizID: roleBizId,
			CorpBizID: contextx.Metadata(ctx).CorpBizID(),
		}
		l.formatFilter(ctx, req) // 确保缓存已经存在
		for k := range syncRoleInfo {
			l.updateKnowledgeFilterCache(ctx, req, k)
		}
	}
	return nil
}

// syncRoleVector 同步角色向量
func (l *Logic) syncRoleVector(ctx context.Context, appBizId uint64, req map[uint64]*SyncRoleInfo) error {
	corpID := contextx.Metadata(ctx).CorpID()
	knowIDs := make(map[uint64]entity.KnowData)
	for knowBizID, syncData := range req { // 根据知识库分别获取文档/问答
		knowData := entity.KnowData{}
		appDB, err := l.rpc.AppAdmin.DescribeAppById(ctx, knowBizID)
		if err != nil {
			logx.E(ctx, "SyncRoleVector DescribeAppById err:%v,knowBizID:%v,syncData:%+v", err, knowBizID,
				syncData)
			return err
		}
		// 获取问答列表
		if len(syncData.QaBizIDs) != 0 {
			qas, err := l.qaLogic.GetAllDocQas(ctx, []string{qaEntity.DocQaTblColId}, &qaEntity.DocQaFilter{
				CorpId:      corpID,
				RobotId:     appDB.PrimaryId,
				BusinessIds: syncData.QaBizIDs,
			})
			if err != nil {
				logx.E(ctx, "SyncRoleVector GetDocQas err:%v,knowBizID:%v,QaBizIDs:%+v",
					err, knowBizID, syncData.QaBizIDs)
				return err
			}
			for _, v := range qas {
				knowData.QaIDs = append(knowData.QaIDs, v.ID)
			}
		}
		// 获取文档列表
		if len(syncData.DocBizIDs) != 0 {
			docs, err := l.docLogic.GetDao().GetAllDocs(ctx, []string{docEntity.DocTblColId},
				&docEntity.DocFilter{
					CorpId:      corpID,
					RobotId:     appDB.PrimaryId,
					BusinessIds: syncData.DocBizIDs,
				})
			if err != nil {
				logx.E(ctx, "SyncRoleVector getDocList err:%v,knowBizID:%v,DocBizIDs:%+v",
					err, knowBizID, syncData.DocBizIDs)
				return err
			}
			for _, v := range docs {
				knowData.DocIDs = append(knowData.DocIDs, v.ID)
			}
		}
		// 数据表业务ids
		knowData.DbTableBizIDs = syncData.DbTableBizIDs
		knowIDs[knowBizID] = knowData
	}
	err := scheduler.NewBatchUpdateVectorTask(ctx, appBizId, entity.BatchUpdateVector{
		Type:      entity.UpdateVectorByRole,
		CorpBizID: contextx.Metadata(ctx).CorpBizID(),
		AppBizID:  appBizId,
		KnowIDs:   knowIDs,
	})
	if err != nil {
		logx.E(ctx, "SyncRoleVector BatchUpdateVector err:%v,corpBizID:%v,appBizID:%v,knowIDs:%v", err,
			contextx.Metadata(ctx).CorpBizID(), appBizId, knowIDs)
		return err
	}
	return nil
}

// formatFilter 格式化过滤条件
func (l *Logic) formatFilter(ctx context.Context,
	req *FormatFilterReq) (map[uint64]*retrieval.LabelExpression, bool, error) {
	cacheExist, cachedKnowBizID2Filter, err := l.dao.DescribeKnowBizID2FilterCache(ctx,
		req.CorpBizID, req.AppBizID, req.RoleBizID)
	res := make(map[uint64]*retrieval.LabelExpression)
	if cacheExist { // key存在
		needUpdateKnowBizIDs := make([]uint64, 0, 3)
		if len(cachedKnowBizID2Filter) == 0 {
			return nil, false, nil
		}
		if len(cachedKnowBizID2Filter) == 1 { // 角色全部知识库
			if v, ok := cachedKnowBizID2Filter[""]; ok && v == nil { // 仅有""->""
				return nil, false, nil
			}
		}
		for k, v := range cachedKnowBizID2Filter {
			knowBizID := cast.ToUint64(k)
			if v == nil { // gorm层删除的时候，置为""
				needUpdateKnowBizIDs = append(needUpdateKnowBizIDs, knowBizID)
				continue
			}
			res[cast.ToUint64(k)] = v
		}
		if len(needUpdateKnowBizIDs) != 0 { // gorm层删除的时候，置为"", 这里重新填入
			for _, needUpdateKnowBizID := range needUpdateKnowBizIDs {
				knowFilter, err := l.updateKnowledgeFilterCache(ctx, req, cast.ToUint64(needUpdateKnowBizID))
				if err != nil {
					logx.E(ctx, "updateKnowledgeFilterCache err:%v", err)
					return nil, false, errs.ErrGetRoleListFail
				}
				if knowFilter == nil {
					continue
				}
				res[cast.ToUint64(needUpdateKnowBizID)] = knowFilter
			}
		}
		logx.D(ctx, "FormatFilter req:%+v res:%+v", req, res)
		return res, false, nil
	}

	// 最后写缓存
	defer func() {
		if err == nil {
			if cacheErr := l.dao.ModifyKnowBizID2FilterCache(ctx,
				req.CorpBizID, req.AppBizID, req.RoleBizID, res); cacheErr != nil {
				logx.E(ctx, "ModifyKnowBizID2FilterCache err:%v", cacheErr)
			}
		}
	}()

	req.GetDefault()
	appBizID, roleBizID := req.AppBizID, req.RoleBizID
	kbRoleFilter := &entity.KnowledgeRoleFilter{BizIDs: []uint64{roleBizID}}
	_, roles, err := l.dao.DescribeKnowledgeRoleList(ctx, req.CorpBizID, appBizID, kbRoleFilter)
	if err != nil {
		logx.E(ctx, "formatFilter|DescribeKnowledgeRoleList err:%v", err)
		return nil, true, errs.ErrGetRoleListFail
	}
	if len(roles) == 0 {
		logx.I(ctx, "formatFilter|DescribeKnowledgeRoleList no role found, req:%+v", req)
		return nil, true, errs.ErrGetRoleListFail
	}

	// 查询角色详情
	role := roles[0]
	if role.SearchType == entity.RoleChooseAll { // 全部知识，设置 "":"" 的缓存
		cacheErr := l.dao.ModifyRoleChooseAllCache(ctx, req.CorpBizID, req.AppBizID, req.RoleBizID)
		if cacheErr != nil {
			logx.E(ctx, "ModifyRoleChooseAllCache err:%v", cacheErr)
		}
		return nil, false, nil
	}
	knows, err := l.dao.DescribeKnowledgeRoleKnowList(ctx, req.CorpBizID, cast.ToUint64(appBizID),
		&entity.KnowledgeRoleKnowFilter{
			RoleBizID: roleBizID,
		})
	if len(knows) == 0 { // 角色选择的知识库为空
		return nil, true, nil // 异常
	}

	if err != nil {
		logx.E(ctx, "ListKnowledgeRoleKnow err:%v", err)
		return nil, false, errs.ErrGetRoleListFail
	}
	for _, know := range knows {
		logx.D(ctx, "FormatFilter know:%+v", know)
		knowFilter, err := l.formatKnowledgeFilter(ctx, req, know)
		if err != nil {
			logx.E(ctx, "FormatKnowledgeFilter err:%v,know:%+v", err, know)
			return nil, false, errs.ErrGetRoleListFail
		}
		if knowFilter == nil {
			logx.W(ctx, "FormatKnowledgeFilter knowFilter is nil,knowBizID:%d", know.KnowledgeBizID)
			continue
		}
		res[know.KnowledgeBizID] = knowFilter
	}
	logx.I(ctx, "FormatFilter req:%+v res:%+v", req, res)
	return res, false, nil
}

// formatKnowledgeFilter 生成知识库筛选值
func (l *Logic) formatKnowledgeFilter(ctx context.Context, req *FormatFilterReq,
	know *entity.KnowledgeRoleKnow) (res *retrieval.LabelExpression, err error) {
	req.GetDefault()
	appBizID, roleBizID := req.AppBizID, req.RoleBizID
	cateKey, roleKey, fullLabelValue, generalVectorAttrKey := req.CateKey, req.RoleKey, req.FullLabelValue, req.GeneralVectorAttrKey
	knowInfo, err := l.rpc.AppAdmin.DescribeAppById(ctx, know.KnowledgeBizID)
	if err != nil {
		logx.E(ctx, "GetAppByAppBizID failed, err: %v", err)
		return nil, err
	}
	if knowInfo == nil {
		logx.E(ctx, "GetAppByAppBizID failed, knowInfo is nil")
		return nil, errs.ErrGetRoleListFail
	}
	knowID := knowInfo.PrimaryId
	switch know.SearchType {
	case entity.KnowSearchLabel: // 标签
		attrs, err := l.dao.DescribeKnowledgeRoleAttributeLabelList(ctx, req.CorpBizID, cast.ToUint64(appBizID),
			&entity.KnowledgeRoleAttributeLabelFilter{
				RoleBizID:       roleBizID,
				KnowledgeBizIDs: []uint64{know.KnowledgeBizID},
			})
		if err != nil {
			logx.E(ctx, "ListKnowledgeRoleAttributeLabel err:%v", err)
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
		attrInfos, err := l.labelDao.GetAttributeByBizIDs(ctx, knowID, attrsBizIDs)
		if err != nil {
			logx.E(ctx, "GetAttributeByBizIDs err:%v", err)
			return nil, err
		}
		labelInfos, err := l.labelDao.GetAttributeLabelByBizIDs(ctx, labelBizIDs, knowID)
		if err != nil {
			logx.E(ctx, "GetAttributeLabelByBizIDs err:%v", err)
			return nil, err
		}
		logx.D(ctx, "FormatFilter length attrInfos:%+v labelInfos:%+v", attrInfos, labelInfos)

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
			if know.LabelCondition == entity.ConditionLogicAnd {
				attrLabels.Operator = retrieval.LabelExpression_AND
			} else if know.LabelCondition == entity.ConditionLogicOr {
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
			logx.W(ctx, "FormatFilter attrFormats is empty, knowID:%d", knowID)
			return nil, nil // 异常 比如属性被删除
		}
	case entity.KnowSearchSpecial: // 特定知识
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
		cates, err := l.dao.DescribeKnowledgeRoleCateList(ctx, req.CorpBizID, cast.ToUint64(appBizID),
			&entity.KnowledgeRoleCateFilter{
				RoleBizID:       roleBizID,
				KnowledgeBizIDs: []uint64{know.KnowledgeBizID},
			})
		if err != nil {
			logx.E(ctx, "ListKnowledgeRoleCate err:%v", err)
			return nil, err
		}
		if len(cates) > 0 {
			var docCateInfo map[int][]int
			var qaCateInfo map[int][]int
			// 这里会导致无法此函数无法单测
			docCateInfo, err = l.cateDao.DescribeCateCache(ctx, category.DocCate, knowInfo.CorpPrimaryId, knowInfo.PrimaryId)
			if err != nil {
				logx.E(ctx, "GetCateCache err:%v", err)
				return nil, err
			}
			qaCateInfo, err = l.cateDao.DescribeCateCache(ctx, category.QACate, knowInfo.CorpPrimaryId, knowInfo.PrimaryId)
			if err != nil {
				logx.E(ctx, "GetCateCache err:%v", err)
				return nil, err
			}
			cateStrs := make([]string, 0, len(cates))
			for _, v := range cates {
				cateList := make([]int, 0, 2)
				cateBizID := int(v.CateBizID)
				cateList = append(cateList, cateBizID)
				if v.CateType == entity.CateTypeDoc {
					if v, ok := docCateInfo[cateBizID]; ok {
						cateList = append(cateList, v...)
					}
				} else if v.CateType == entity.CateTypeQA {
					if v, ok := qaCateInfo[cateBizID]; ok {
						cateList = append(cateList, v...)
					}
				}
				cateList = slicex.Unique(cateList)
				cateStrs = append(cateStrs, convx.SliceIntToString(cateList)...)
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
	case entity.KnowSearchAll:
		return &retrieval.LabelExpression{
			Operator: retrieval.LabelExpression_NOOP,
		}, nil
	default:
		return nil, nil
	}
}

// updateKnowledgeFilterCache 更新知识库缓存
func (l *Logic) updateKnowledgeFilterCache(ctx context.Context, req *FormatFilterReq,
	knowBizID uint64) (res *retrieval.LabelExpression, err error) {
	logx.D(ctx, "UpdateKnowledgeFilterCache req:%+v knowBizID:%d ", req, knowBizID)
	req.GetDefault()
	knows, err := l.dao.DescribeKnowledgeRoleKnowList(ctx, req.CorpBizID, req.AppBizID,
		&entity.KnowledgeRoleKnowFilter{
			RoleBizID:       req.RoleBizID,
			KnowledgeBizIDs: []uint64{knowBizID},
		})
	if err != nil {
		logx.E(ctx, "ListKnowledgeRoleKnow err:%v", err)
		return nil, err
	}
	if len(knows) == 0 { // 已经解绑，删除缓存
		if err := l.dao.DeleteKnowBizID2FilterCache(ctx,
			req.CorpBizID, req.AppBizID, req.RoleBizID, knowBizID); err != nil {
			logx.E(ctx, "DeleteKnowBizID2FilterCache err:%v", err)
			return nil, err
		}
		return nil, nil
	}
	know := knows[0]
	knowFilter, err := l.formatKnowledgeFilter(ctx, req, know)
	if err != nil {
		logx.E(ctx, "FormatKnowledgeFilter err:%v", err)
		return nil, err
	}
	knowBizID2Filter := map[uint64]*retrieval.LabelExpression{
		knowBizID: knowFilter,
	}
	if err := l.dao.ModifyKnowBizID2FilterCache(ctx,
		req.CorpBizID, req.AppBizID, req.RoleBizID, knowBizID2Filter); err != nil {
		logx.E(ctx, "ModifyKnowBizID2FilterCache err:%v", err)
		return nil, err
	}
	logx.D(ctx, "UpdateKnowledgeFilterCache success, corpBizID:%d, appBizID:%d, roleBizID:%d, knowBizID:%d",
		req.CorpBizID, req.AppBizID, req.RoleBizID, knowBizID)
	return knowFilter, nil
}

func (l *Logic) VerifyPresetRole(ctx context.Context, appBizID uint64) (uint64, string, error) {
	cnt, roles, err := l.dao.DescribeKnowledgeRoleList(ctx,
		contextx.Metadata(ctx).CorpBizID(), appBizID,
		&entity.KnowledgeRoleFilter{
			Type: entity.KnowledgeRoleTypePreset,
		})
	if err != nil {
		logx.E(ctx, "ListKnowledgeRoles cnt:%d err:%v", cnt, err)
		return 0, "", errs.ErrGetRoleListFail
	}
	if len(roles) >= 1 {
		return roles[0].BusinessID, roles[0].Name, nil
	}

	// 分布式锁
	key := fmt.Sprintf(PresetRoleLockKey, appBizID)
	lock := distributedlockx.NewRedisLock(l.lockRdb, key, distributedlockx.WithTTL(time.Second*30))
	success, err := lock.Lock(ctx)
	if err != nil {
		logx.E(ctx, "Lock err:%v", err)
		return 0, "", errs.ErrAlreadyLocked
	}
	if success { // 加锁成功
		defer func() {
			if err = lock.Unlock(ctx); err != nil {
				logx.E(ctx, "Unlock err:%v", err)
			}
		}()
	} else {
		logx.W(ctx, "VerifyPresetRole lock failed, appBizID:%d", appBizID)
		return 0, "", errs.ErrAlreadyLocked
	}
	// double check
	cnt, roles, err = l.dao.DescribeKnowledgeRoleList(ctx,
		contextx.Metadata(ctx).CorpBizID(), appBizID,
		&entity.KnowledgeRoleFilter{
			Type: entity.KnowledgeRoleTypePreset,
		})
	if err != nil {
		logx.E(ctx, "VerifyPresetRole|DescribeKnowledgeRoleList err:%v", err)
		return 0, "", errs.ErrGetRoleListFail
	}
	if len(roles) >= 1 {
		return roles[0].BusinessID, roles[0].Name, nil
	}
	logx.W(ctx, "VerifyPresetRole cnt:%v roles:%+v", cnt, roles)
	defaultRoleName := i18n.Translate(ctx, entity.PresetRoleName)
	role := &entity.KnowledgeRole{
		AppBizID:    appBizID,
		BusinessID:  idgen.GetId(),
		Name:        defaultRoleName,
		Type:        entity.KnowledgeRoleTypePreset,
		Description: defaultRoleName,
		SearchType:  entity.RoleChooseAll,
	}
	err = l.createRole(ctx, role)
	if err != nil {
		logx.E(ctx, "CreateRole err:%v", err)
		return 0, "", errs.ErrGetRoleListFail
	}
	return role.BusinessID, role.Name, nil
}
