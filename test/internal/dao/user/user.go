package user

import (
	"context"
	"fmt"
	"time"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/tdsqlquery"
	"gorm.io/gen"
	"gorm.io/gen/field"

	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
)

var chunkNumber = 0

func GetChunkNumber() int {
	if chunkNumber == 0 {
		chunkNumber = config.GetMainConfig().Permissions.ChunkNumber
	}
	if chunkNumber == 0 {
		chunkNumber = 100
	}
	return chunkNumber
}

func KnowledgeUsersPO2DO(pos []*model.TKnowledgeUser) []*entity.CustUser {
	return slicex.Map(pos, func(po *model.TKnowledgeUser) *entity.CustUser {
		return KnowledgeUserPO2DO(po)
	})
}

func KnowledgeUserPO2DO(po *model.TKnowledgeUser) *entity.CustUser {
	if po == nil {
		return nil
	}
	return &entity.CustUser{
		ID:          po.ID,
		CorpID:      po.CorpBizID,
		AppID:       po.AppBizID,
		BusinessID:  po.BusinessID,
		Name:        po.Name,
		ThirdUserID: po.ThirdUserID,
		IsDeleted:   po.IsDeleted,
		CreateTime:  po.CreateTime,
		UpdateTime:  po.UpdateTime,
	}
}

func KnowledgeUserDO2PO(do *entity.CustUser) *model.TKnowledgeUser {
	if do == nil {
		return nil
	}
	return &model.TKnowledgeUser{
		ID:          do.ID,
		CorpBizID:   do.CorpID,
		AppBizID:    do.AppID,
		BusinessID:  do.BusinessID,
		Name:        do.Name,
		ThirdUserID: do.ThirdUserID,
		IsDeleted:   do.IsDeleted,
		CreateTime:  do.CreateTime,
		UpdateTime:  do.UpdateTime,
	}
}

func KnowledgeUserRolesPO2DO(pos []*model.TKnowledgeUserRole) []*entity.UserRole {
	return slicex.Map(pos, func(po *model.TKnowledgeUserRole) *entity.UserRole {
		return KnowledgeUserRolePO2DO(po)
	})
}

func KnowledgeUserRolePO2DO(po *model.TKnowledgeUserRole) *entity.UserRole {
	if po == nil {
		return nil
	}
	return &entity.UserRole{
		ID:          po.ID,
		CorpID:      po.CorpBizID,
		AppID:       po.AppBizID,
		UserBizID:   po.UserBizID,
		ThirdUserId: po.ThirdUserID,
		Type:        po.Type,
		RoleBizID:   po.RoleBizID,
		IsDeleted:   po.IsDeleted,
		CreateTime:  po.CreateTime,
		UpdateTime:  po.UpdateTime,
	}
}

func KnowledgeUserolesDO2PO(dos []*entity.UserRole) []*model.TKnowledgeUserRole {
	return slicex.Map(dos, func(do *entity.UserRole) *model.TKnowledgeUserRole {
		return KnowledgeUserRoleDO2PO(do)
	})
}

func KnowledgeUserRoleDO2PO(do *entity.UserRole) *model.TKnowledgeUserRole {
	if do == nil {
		return nil
	}
	return &model.TKnowledgeUserRole{
		ID:          do.ID,
		CorpBizID:   do.CorpID,
		AppBizID:    do.AppID,
		UserBizID:   do.UserBizID,
		ThirdUserID: do.ThirdUserId,
		Type:        do.Type,
		RoleBizID:   do.RoleBizID,
		IsDeleted:   do.IsDeleted,
		CreateTime:  do.CreateTime,
		UpdateTime:  do.UpdateTime,
	}
}

func KnowledgeRolesPO2DO(pos []*model.TKnowledgeRole) []*entity.KnowledgeRole {
	return slicex.Map(pos, func(po *model.TKnowledgeRole) *entity.KnowledgeRole {
		return KnowledgeRolePO2DO(po)
	})
}

func KnowledgeRolePO2DO(po *model.TKnowledgeRole) *entity.KnowledgeRole {
	if po == nil {
		return nil
	}
	return &entity.KnowledgeRole{
		ID:          po.ID,
		CorpBizID:   po.CorpBizID,
		AppBizID:    po.AppBizID,
		BusinessID:  po.BusinessID,
		Name:        po.Name,
		Type:        int8(po.Type),
		Description: po.Description,
		SearchType:  int8(po.SearchType),
		IsDeleted:   po.IsDeleted,
		UpdateTime:  po.UpdateTime,
		CreateTime:  po.CreateTime,
	}
}

func KnowledgeRoleDO2PO(do *entity.KnowledgeRole) *model.TKnowledgeRole {
	if do == nil {
		return nil
	}
	return &model.TKnowledgeRole{
		ID:          do.ID,
		CorpBizID:   do.CorpBizID,
		AppBizID:    do.AppBizID,
		BusinessID:  do.BusinessID,
		Name:        do.Name,
		Type:        uint32(do.Type),
		Description: do.Description,
		SearchType:  uint32(do.SearchType),
		IsDeleted:   do.IsDeleted,
		UpdateTime:  do.UpdateTime,
		CreateTime:  do.CreateTime,
	}
}

func KnowledgeRoleKnowPO2DO(po *model.TKnowledgeRoleKnow) *entity.KnowledgeRoleKnow {
	if po == nil {
		return nil
	}
	return &entity.KnowledgeRoleKnow{
		ID:             po.ID,
		CorpBizID:      po.CorpBizID,
		AppBizID:       po.AppBizID,
		RoleBizID:      po.RoleBizID,
		KnowledgeBizID: po.KnowledgeBizID,
		KnowledgeType:  int8(po.KnowledgeType),
		SearchType:     int8(po.SearchType),
		LabelCondition: int8(po.LableCondition),
		IsDeleted:      po.IsDeleted,
		UpdateTime:     po.UpdateTime,
		CreateTime:     po.CreateTime,
	}
}

func KnowledgeRoleKnowsPO2DO(pos []*model.TKnowledgeRoleKnow) []*entity.KnowledgeRoleKnow {
	return slicex.Map(pos, func(po *model.TKnowledgeRoleKnow) *entity.KnowledgeRoleKnow {
		return KnowledgeRoleKnowPO2DO(po)
	})
}

func KnowledgeRoleKnowDO2PO(do *entity.KnowledgeRoleKnow) *model.TKnowledgeRoleKnow {
	if do == nil {
		return nil
	}
	return &model.TKnowledgeRoleKnow{
		ID:             do.ID,
		CorpBizID:      do.CorpBizID,
		AppBizID:       do.AppBizID,
		RoleBizID:      do.RoleBizID,
		KnowledgeBizID: do.KnowledgeBizID,
		KnowledgeType:  uint32(do.KnowledgeType),
		SearchType:     uint32(do.SearchType),
		LableCondition: uint32(do.LabelCondition),
		IsDeleted:      do.IsDeleted,
		UpdateTime:     do.UpdateTime,
		CreateTime:     do.CreateTime,
	}
}

func KnowledgeRoleKnowsDO2PO(dos []*entity.KnowledgeRoleKnow) []*model.TKnowledgeRoleKnow {
	return slicex.Map(dos, func(do *entity.KnowledgeRoleKnow) *model.TKnowledgeRoleKnow {
		return KnowledgeRoleKnowDO2PO(do)
	})
}

func KnowledgeRoleDocPO2DO(po *model.TKnowledgeRoleDoc) *entity.KnowledgeRoleDoc {
	if po == nil {
		return nil
	}
	return &entity.KnowledgeRoleDoc{
		ID:             po.ID,
		CorpBizID:      po.CorpBizID,
		AppBizID:       po.AppBizID,
		RoleBizID:      po.RoleBizID,
		KnowledgeBizID: po.KnowledgeBizID,
		DocBizID:       po.DocBizID,
		IsDeleted:      po.IsDeleted,
		UpdateTime:     po.UpdateTime,
		CreateTime:     po.CreateTime,
	}
}

func KnowledgeRoleDocsPO2DO(pos []*model.TKnowledgeRoleDoc) []*entity.KnowledgeRoleDoc {
	return slicex.Map(pos, func(po *model.TKnowledgeRoleDoc) *entity.KnowledgeRoleDoc {
		return KnowledgeRoleDocPO2DO(po)
	})
}

func KnowledgeRoleDocDO2PO(do *entity.KnowledgeRoleDoc) *model.TKnowledgeRoleDoc {
	if do == nil {
		return nil
	}
	return &model.TKnowledgeRoleDoc{
		ID:             do.ID,
		CorpBizID:      do.CorpBizID,
		AppBizID:       do.AppBizID,
		RoleBizID:      do.RoleBizID,
		KnowledgeBizID: do.KnowledgeBizID,
		DocBizID:       do.DocBizID,
		IsDeleted:      do.IsDeleted,
		UpdateTime:     do.UpdateTime,
		CreateTime:     do.CreateTime,
	}
}

func KnowledgeRoleDocsDO2PO(dos []*entity.KnowledgeRoleDoc) []*model.TKnowledgeRoleDoc {
	return slicex.Map(dos, func(do *entity.KnowledgeRoleDoc) *model.TKnowledgeRoleDoc {
		return KnowledgeRoleDocDO2PO(do)
	})
}

func KnowledgeRoleQADO2PO(do *entity.KnowledgeRoleQA) *model.TKnowledgeRoleQa {
	if do == nil {
		return nil
	}
	return &model.TKnowledgeRoleQa{
		ID:             do.ID,
		CorpBizID:      do.CorpBizID,
		AppBizID:       do.AppBizID,
		RoleBizID:      do.RoleBizID,
		KnowledgeBizID: do.KnowledgeBizID,
		QaBizID:        do.QABizID,
		IsDeleted:      do.IsDeleted,
		UpdateTime:     do.UpdateTime,
		CreateTime:     do.CreateTime,
	}
}

func KnowledgeRoleQAsDO2PO(dos []*entity.KnowledgeRoleQA) []*model.TKnowledgeRoleQa {
	return slicex.Map(dos, func(do *entity.KnowledgeRoleQA) *model.TKnowledgeRoleQa {
		return KnowledgeRoleQADO2PO(do)
	})
}

func KnowledgeRoleQAPO2DO(po *model.TKnowledgeRoleQa) *entity.KnowledgeRoleQA {
	if po == nil {
		return nil
	}
	return &entity.KnowledgeRoleQA{
		ID:             po.ID,
		CorpBizID:      po.CorpBizID,
		AppBizID:       po.AppBizID,
		RoleBizID:      po.RoleBizID,
		KnowledgeBizID: po.KnowledgeBizID,
		QABizID:        po.QaBizID,
		IsDeleted:      po.IsDeleted,
		UpdateTime:     po.UpdateTime,
		CreateTime:     po.CreateTime,
	}
}

func KnowledgeRoleQAsPO2DO(pos []*model.TKnowledgeRoleQa) []*entity.KnowledgeRoleQA {
	return slicex.Map(pos, func(po *model.TKnowledgeRoleQa) *entity.KnowledgeRoleQA {
		return KnowledgeRoleQAPO2DO(po)
	})
}

func KnowledgeRoleAttributeLabelDO2PO(do *entity.KnowledgeRoleAttributeLabel) *model.TKnowledgeRoleAttributeLabel {
	if do == nil {
		return nil
	}
	return &model.TKnowledgeRoleAttributeLabel{
		ID:             do.ID,
		CorpBizID:      do.CorpBizID,
		AppBizID:       do.AppBizID,
		RoleBizID:      do.RoleBizID,
		KnowledgeBizID: do.KnowledgeBizID,
		AttrBizID:      do.AttrBizID,
		LabelBizID:     do.LabelBizID,
		IsDeleted:      do.IsDeleted,
		UpdateTime:     do.UpdateTime,
		CreateTime:     do.CreateTime,
	}
}

func KnowledgeRoleAttributeLabelsDO2PO(dos []*entity.KnowledgeRoleAttributeLabel) []*model.TKnowledgeRoleAttributeLabel {
	return slicex.Map(dos, func(do *entity.KnowledgeRoleAttributeLabel) *model.TKnowledgeRoleAttributeLabel {
		return KnowledgeRoleAttributeLabelDO2PO(do)
	})
}

func KnowledgeRoleAttributeLabelPO2DO(po *model.TKnowledgeRoleAttributeLabel) *entity.KnowledgeRoleAttributeLabel {
	if po == nil {
		return nil
	}
	return &entity.KnowledgeRoleAttributeLabel{
		ID:             po.ID,
		CorpBizID:      po.CorpBizID,
		AppBizID:       po.AppBizID,
		RoleBizID:      po.RoleBizID,
		KnowledgeBizID: po.KnowledgeBizID,
		AttrBizID:      po.AttrBizID,
		LabelBizID:     po.LabelBizID,
		IsDeleted:      po.IsDeleted,
		UpdateTime:     po.UpdateTime,
		CreateTime:     po.CreateTime,
	}
}

func KnowledgeRoleAttributeLabelsPO2DO(pos []*model.TKnowledgeRoleAttributeLabel) []*entity.KnowledgeRoleAttributeLabel {
	return slicex.Map(pos, func(po *model.TKnowledgeRoleAttributeLabel) *entity.KnowledgeRoleAttributeLabel {
		return KnowledgeRoleAttributeLabelPO2DO(po)
	})
}

func KnowledgeRoleCateDO2PO(do *entity.KnowledgeRoleCate) *model.TKnowledgeRoleCate {
	if do == nil {
		return nil
	}
	return &model.TKnowledgeRoleCate{
		ID:             do.ID,
		CorpBizID:      do.CorpBizID,
		AppBizID:       do.AppBizID,
		RoleBizID:      do.RoleBizID,
		KnowledgeBizID: do.KnowledgeBizID,
		Type:           do.CateType,
		CateBizID:      do.CateBizID,
		IsDeleted:      do.IsDeleted,
		UpdateTime:     do.UpdateTime,
		CreateTime:     do.CreateTime,
	}
}

func KnowledgeRoleCatesDO2PO(dos []*entity.KnowledgeRoleCate) []*model.TKnowledgeRoleCate {
	return slicex.Map(dos, func(do *entity.KnowledgeRoleCate) *model.TKnowledgeRoleCate {
		return KnowledgeRoleCateDO2PO(do)
	})
}

func KnowledgeRoleCatePO2DO(po *model.TKnowledgeRoleCate) *entity.KnowledgeRoleCate {
	if po == nil {
		return nil
	}
	return &entity.KnowledgeRoleCate{
		ID:             po.ID,
		CorpBizID:      po.CorpBizID,
		AppBizID:       po.AppBizID,
		RoleBizID:      po.RoleBizID,
		KnowledgeBizID: po.KnowledgeBizID,
		CateType:       po.Type,
		CateBizID:      po.CateBizID,
		IsDeleted:      po.IsDeleted,
		UpdateTime:     po.UpdateTime,
		CreateTime:     po.CreateTime,
	}
}

func KnowledgeRoleCatesPO2DO(pos []*model.TKnowledgeRoleCate) []*entity.KnowledgeRoleCate {
	return slicex.Map(pos, func(po *model.TKnowledgeRoleCate) *entity.KnowledgeRoleCate {
		return KnowledgeRoleCatePO2DO(po)
	})
}

func KnowledgeRoleDatabaseDO2PO(do *entity.KnowledgeRoleDatabase) *model.TKnowledgeRoleDatabase {
	if do == nil {
		return nil
	}
	return &model.TKnowledgeRoleDatabase{
		ID:             do.ID,
		CorpBizID:      do.CorpBizID,
		AppBizID:       do.AppBizID,
		RoleBizID:      do.RoleBizID,
		KnowledgeBizID: do.KnowledgeBizID,
		DatabaseBizID:  do.DatabaseBizID,
		IsDeleted:      do.IsDeleted,
		UpdateTime:     do.UpdateTime,
		CreateTime:     do.CreateTime,
	}
}

func KnowledgeRoleDatabasesDO2PO(dos []*entity.KnowledgeRoleDatabase) []*model.TKnowledgeRoleDatabase {
	return slicex.Map(dos, func(do *entity.KnowledgeRoleDatabase) *model.TKnowledgeRoleDatabase {
		return KnowledgeRoleDatabaseDO2PO(do)
	})
}

func KnowledgeRoleDatabasePO2DO(po *model.TKnowledgeRoleDatabase) *entity.KnowledgeRoleDatabase {
	if po == nil {
		return nil
	}
	return &entity.KnowledgeRoleDatabase{
		ID:             po.ID,
		CorpBizID:      po.CorpBizID,
		AppBizID:       po.AppBizID,
		RoleBizID:      po.RoleBizID,
		KnowledgeBizID: po.KnowledgeBizID,
		DatabaseBizID:  po.DatabaseBizID,
		IsDeleted:      po.IsDeleted,
		UpdateTime:     po.UpdateTime,
		CreateTime:     po.CreateTime,
	}
}

func KnowledgeRoleDatabasesPO2DO(pos []*model.TKnowledgeRoleDatabase) []*entity.KnowledgeRoleDatabase {
	return slicex.Map(pos, func(po *model.TKnowledgeRoleDatabase) *entity.KnowledgeRoleDatabase {
		return KnowledgeRoleDatabasePO2DO(po)
	})
}

func UserPO2DO(po *model.TUser) *entity.User {
	if po == nil {
		return nil
	}
	return &entity.User{
		ID:            po.ID,
		SID:           po.Sid,
		Uin:           po.Uin,
		SubAccountUin: po.SubAccountUin,
		NickName:      po.NickName,
		Avatar:        po.Avatar,
		Cellphone:     po.Cellphone,
		Account:       po.Account,
		Password:      po.Password,
		Status:        int8(po.Status),
		CreateTime:    po.CreateTime,
		UpdateTime:    po.UpdateTime,
	}
}

func ExpUserPO2DO(po *model.TExpUser) *entity.User {
	if po == nil {
		return nil
	}
	return &entity.User{
		ID:         po.ID,
		BusinessID: po.BusinessID,
		SID:        po.Sid,
		NickName:   po.NickName,
		Avatar:     po.Avatar,
		Cellphone:  po.Cellphone,
		Status:     int8(po.Status),
		CreateTime: po.CreateTime,
		UpdateTime: po.UpdateTime,
	}
}

func ExpUsersPO2DO(pos []*model.TExpUser) []*entity.User {
	return slicex.Map(pos, func(po *model.TExpUser) *entity.User {
		return ExpUserPO2DO(po)
	})
}

func (l *daoImpl) makeUserCondition(corpBizID, appBizID uint64, filter *entity.CustUserFilter) []gen.Condition {
	queryCond := []gen.Condition{
		l.tdsql.TKnowledgeUser.IsDeleted.Is(false),
	}
	if corpBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeUser.CorpBizID.Eq(corpBizID))
	}
	if appBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeUser.AppBizID.Eq(appBizID))
	}
	if filter != nil {
		if len(filter.BizIDs) > 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeUser.BusinessID.In(filter.BizIDs...))
		}
		if len(filter.ThirdUserID) > 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeUser.ThirdUserID.Eq(filter.ThirdUserID))
		}
		if len(filter.Name) > 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeUser.Name.Eq(filter.Name))
		}
		if len(filter.ThirdUserIDLike) > 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeUser.ThirdUserID.Like("%"+filter.ThirdUserIDLike+"%"))
		}
		if len(filter.NameLike) > 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeUser.Name.Like("%"+filter.NameLike+"%"))
		}
		// 这里不能写or方法，所以query的name like or third user id like只能移到外面去写
	}
	return queryCond
}
func (l *daoImpl) DescribeUserCountAndList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.CustUserFilter) ([]*entity.CustUser, int64, error) {
	count, err := l.DescribeUserCount(ctx, corpBizID, appBizID, filter)
	if err != nil {
		return nil, 0, fmt.Errorf("DescribeUserCountAndList DescribeUserCount err:%v,corpBizID:%v,appBizID:%v", err, corpBizID, appBizID)
	}
	if count == 0 {
		return nil, 0, nil
	}
	list, err := l.DescribeUserList(ctx, corpBizID, appBizID, filter)
	if err != nil {
		return nil, 0, fmt.Errorf("DescribeUserCountAndList DescribeUserList err:%v,corpBizID:%v,appBizID:%v", err, corpBizID, appBizID)
	}
	return list, count, nil
}

func (l *daoImpl) DescribeUserCount(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.CustUserFilter) (int64, error) {
	db := l.tdsql.TKnowledgeUser.WithContext(ctx)
	queryCond := l.makeUserCondition(corpBizID, appBizID, filter)
	count, err := db.Where(queryCond...).Count()
	if err != nil {
		return 0, fmt.Errorf("DescribeUserCount err:%v,corpBizID:%v,AppBizID:%v", err, corpBizID, appBizID)
	}
	return count, nil
}

func (l *daoImpl) DescribeUserList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.CustUserFilter) ([]*entity.CustUser, error) {
	db := l.tdsql.TKnowledgeUser.WithContext(ctx)
	queryCond := l.makeUserCondition(corpBizID, appBizID, filter)
	db = db.Where(queryCond...)
	if len(filter.Query) > 0 {
		db = db.Where(field.Or(l.tdsql.TKnowledgeUser.Name.Like("%"+filter.Query+"%"),
			l.tdsql.TKnowledgeUser.ThirdUserID.Like("%"+filter.Query+"%")))
	}
	if filter.OrderByModifyTimeDesc {
		db = db.Order(l.tdsql.TKnowledgeUser.UpdateTime.Desc())
	}
	if filter.Limit > 0 {
		db = db.Limit(filter.Limit)
	}
	if filter.Offset > 0 {
		db = db.Offset(filter.Offset)
	}
	qs, err := db.Find()
	if err != nil {
		return nil, fmt.Errorf("DescribeUserList err:%v,corpBizID:%v,appBizID:%v", err, corpBizID, appBizID)
	}
	return KnowledgeUsersPO2DO(qs), nil
}

func (l *daoImpl) CreateUser(ctx context.Context,
	custUserInfo *entity.CustUser, tx *tdsqlquery.Query) (id uint64, err error) {
	now := time.Now()
	custUserInfo.CreateTime = now
	custUserInfo.UpdateTime = now
	po := KnowledgeUserDO2PO(custUserInfo)
	if err = tx.TKnowledgeUser.WithContext(ctx).Create(po); err != nil {
		return 0, fmt.Errorf("CreateCustUser err:%v", err)
	}
	return po.ID, nil
}

func (l *daoImpl) ModifyUser(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.CustUserFilter,
	custUserInfo *entity.CustUser, tx *tdsqlquery.Query) (int64, error) {
	updateFields := map[string]any{
		tx.TKnowledgeUser.UpdateTime.ColumnName().String(): time.Now(),
	}
	if len(custUserInfo.Name) > 0 {
		updateFields[tx.TKnowledgeUser.Name.ColumnName().String()] = custUserInfo.Name
	}
	if len(custUserInfo.ThirdUserID) > 0 {
		updateFields[tx.TKnowledgeUser.ThirdUserID.ColumnName().String()] = custUserInfo.ThirdUserID
	}
	if custUserInfo.IsDeleted {
		updateFields[tx.TKnowledgeUser.IsDeleted.ColumnName().String()] = custUserInfo.IsDeleted
	}
	db := tx.TKnowledgeUser.WithContext(ctx)
	db = db.Where(l.makeUserCondition(corpBizID, appBizID, filter)...)
	if filter.Limit > 0 {
		db = db.Limit(filter.Limit)
	}
	info, err := db.Updates(updateFields)
	if err != nil {
		return 0, fmt.Errorf("ModifyUser err:%v", err)
	}
	return info.RowsAffected, nil
}

func (l *daoImpl) makeUserRoleCondition(corpBizID, appBizID uint64, filter *entity.UserRoleFilter) []gen.Condition {
	queryCond := []gen.Condition{
		l.tdsql.TKnowledgeUserRole.IsDeleted.Is(false),
	}
	if corpBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeUserRole.CorpBizID.Eq(corpBizID))
	}
	if appBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeUserRole.AppBizID.Eq(appBizID))
	}
	if filter != nil {
		if len(filter.UserBizIDs) > 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeUserRole.UserBizID.In(filter.UserBizIDs...))
		}
		if len(filter.ThirdUserID) > 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeUserRole.ThirdUserID.Eq(filter.ThirdUserID))
		}
		if len(filter.Types) > 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeUserRole.Type.In(filter.Types...))
		}
	}
	return queryCond
}

func (l *daoImpl) CreateUserRoleList(ctx context.Context,
	userRoleList []*entity.UserRole, tx *tdsqlquery.Query) (err error) {
	db := tx.TKnowledgeUserRole.WithContext(ctx)
	poList := KnowledgeUserolesDO2PO(userRoleList)
	if err := db.CreateInBatches(poList, 200); err != nil {
		return fmt.Errorf("CreateUserRoleList err:%v", err)
	}
	return nil
}

func (l *daoImpl) ModifyUserRole(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.UserRoleFilter,
	userRole *entity.UserRole, tx *tdsqlquery.Query) (int64, error) {
	db := tx.TKnowledgeUserRole.WithContext(ctx)
	queryCond := l.makeUserRoleCondition(corpBizID, appBizID, filter)
	if filter.Limit > 0 {
		db = db.Limit(filter.Limit)
	}
	updateFields := map[string]any{
		l.tdsql.TKnowledgeUserRole.UpdateTime.ColumnName().String(): time.Now(),
	}
	if userRole.IsDeleted {
		updateFields[l.tdsql.TKnowledgeUserRole.IsDeleted.ColumnName().String()] = userRole.IsDeleted
	}
	info, err := db.Where(queryCond...).Updates(updateFields)
	if err != nil {
		return 0, fmt.Errorf("ModifyUserRole err:%v", err)
	}
	return info.RowsAffected, nil
}

func (l *daoImpl) DescribeUserRoleList(ctx context.Context, corpBizID, appBizID uint64, filter *entity.UserRoleFilter) ([]*entity.UserRole, error) {
	db := l.tdsql.TKnowledgeUserRole.WithContext(ctx)
	queryCond := l.makeUserRoleCondition(corpBizID, appBizID, filter)
	if filter.Limit > 0 {
		db = db.Limit(filter.Limit)
	}
	qs, err := db.Where(queryCond...).Find()
	if err != nil {
		return nil, fmt.Errorf("DescribeUserRoleList err:%v", err)
	}
	return KnowledgeUserRolesPO2DO(qs), nil
}

// ModifyUserConfig 设置特殊权限配置
func (l *daoImpl) ModifyUserConfig(ctx context.Context,
	corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId uint64) error {
	db := l.tdsql.TKnowledgeUserRole.WithContext(ctx)
	// 1.先获取
	qs, err := db.Where(l.tdsql.TKnowledgeUserRole.IsDeleted.Is(false),
		l.tdsql.TKnowledgeUserRole.CorpBizID.Eq(corpBizId),
		l.tdsql.TKnowledgeUserRole.AppBizID.Eq(appBizId),
		l.tdsql.TKnowledgeUserRole.Type.In([]uint32{entity.NotSetThirdUserId, entity.NotUseThirdUserId}...)).
		Limit(2).Find()
	if err != nil {
		return fmt.Errorf("ModifyUserConfig get err:%v, corpBizID:%d, appBizID:%d, notSetRoleBizId:%d, notUseRoleBizId:%d",
			err, corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId)
	}
	// 2.已存在更新
	notSet, notUse := false, false
	for _, v := range qs {
		if v.Type == entity.NotSetThirdUserId {
			_, err := db.Where(l.tdsql.TKnowledgeUserRole.ID.Eq(v.ID)).Limit(1).Updates(map[string]any{
				l.tdsql.TKnowledgeUserRole.RoleBizID.ColumnName().String():  notSetRoleBizId,
				l.tdsql.TKnowledgeUserRole.UpdateTime.ColumnName().String(): time.Now(),
			})
			if err != nil {
				return fmt.Errorf("ModifyUserConfig modify err:%v, corpBizID:%d, appBizID:%d, notSetRoleBizId:%d, notUseRoleBizId:%d",
					err, corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId)
			}
			notSet = true
		} else if v.Type == entity.NotUseThirdUserId {
			_, err := db.Where(l.tdsql.TKnowledgeUserRole.ID.Eq(v.ID)).Limit(1).Updates(map[string]any{
				l.tdsql.TKnowledgeUserRole.RoleBizID.ColumnName().String():  notUseRoleBizId,
				l.tdsql.TKnowledgeUserRole.UpdateTime.ColumnName().String(): time.Now(),
			})
			if err != nil {
				return fmt.Errorf("ModifyUserConfig modify err:%v, corpBizID:%d, appBizID:%d, notSetRoleBizId:%d, notUseRoleBizId:%d",
					err, corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId)
			}
			notUse = true
		}
	}
	// 3.不存在创建
	if !notSet {
		err := db.Create(&model.TKnowledgeUserRole{
			CorpBizID:   corpBizId,
			AppBizID:    appBizId,
			UserBizID:   0,
			ThirdUserID: "",
			Type:        entity.NotSetThirdUserId,
			RoleBizID:   notSetRoleBizId,
			IsDeleted:   false,
			CreateTime:  time.Now(),
			UpdateTime:  time.Now(),
		})
		if err != nil {
			return fmt.Errorf("ModifyUserConfig create err:%v, corpBizID:%d, appBizID:%d, notSetRoleBizId:%d, notUseRoleBizId:%d",
				err, corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId)
		}
	}
	if !notUse {
		err := db.Create(&model.TKnowledgeUserRole{
			CorpBizID:   corpBizId,
			AppBizID:    appBizId,
			UserBizID:   0,
			ThirdUserID: "",
			Type:        entity.NotUseThirdUserId,
			RoleBizID:   notUseRoleBizId,
			IsDeleted:   false,
			CreateTime:  time.Now(),
			UpdateTime:  time.Now(),
		})
		if err != nil {
			return fmt.Errorf("ModifyUserConfig create err:%v, corpBizID:%d, appBizID:%d, notSetRoleBizId:%d, notUseRoleBizId:%d",
				err, corpBizId, appBizId, notSetRoleBizId, notUseRoleBizId)
		}
	}
	return nil
}

func (l *daoImpl) CreateKnowledgeRole(ctx context.Context,
	record *entity.KnowledgeRole) error {
	return l.tdsql.TKnowledgeRole.WithContext(ctx).Create(KnowledgeRoleDO2PO(record))
}

func (l *daoImpl) ModifyKnowledgeRole(ctx context.Context,
	role *entity.KnowledgeRole) error {
	_, err := l.tdsql.TKnowledgeRole.WithContext(ctx).
		Where(l.tdsql.TKnowledgeRole.CorpBizID.Eq(role.CorpBizID)).
		Where(l.tdsql.TKnowledgeRole.AppBizID.Eq(role.AppBizID)).
		Where(l.tdsql.TKnowledgeRole.BusinessID.Eq(role.BusinessID)).
		Updates(map[string]any{
			l.tdsql.TKnowledgeRole.Name.ColumnName().String():        role.Name,
			l.tdsql.TKnowledgeRole.Type.ColumnName().String():        role.Type,
			l.tdsql.TKnowledgeRole.Description.ColumnName().String(): role.Description,
			l.tdsql.TKnowledgeRole.SearchType.ColumnName().String():  role.SearchType,
		})
	return err
}

func (l *daoImpl) makeKnowledgeRoleCondition(corpBizID, appBizID uint64, filter *entity.KnowledgeRoleFilter) []gen.Condition {
	queryCond := []gen.Condition{
		l.tdsql.TKnowledgeRole.IsDeleted.Is(false),
	}
	if corpBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRole.CorpBizID.Eq(corpBizID))
	}
	if appBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRole.AppBizID.Eq(appBizID))
	}
	if filter != nil {
		if filter.Type > 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeRole.Type.Eq(filter.Type))
		}
		if len(filter.BizIDs) > 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeRole.BusinessID.In(filter.BizIDs...))
		}
		if len(filter.Name) > 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeRole.Name.Eq(filter.Name))
		}
		if len(filter.SearchWord) > 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeRole.Name.Like("%"+filter.SearchWord+"%"))
		}
	}
	return queryCond
}

func (l *daoImpl) DeleteKnowledgeRole(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.KnowledgeRoleFilter) error {
	db := l.tdsql.TKnowledgeRole.WithContext(ctx)
	queryCond := l.makeKnowledgeRoleCondition(corpBizID, appBizID, filter)
	_, err := db.Where(queryCond...).Updates(map[string]any{
		l.tdsql.TKnowledgeRole.IsDeleted.ColumnName().String():  1,
		l.tdsql.TKnowledgeRole.UpdateTime.ColumnName().String(): time.Now(),
	})
	return err
}

func (l *daoImpl) DescribeKnowledgeRoleList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.KnowledgeRoleFilter) (int64, []*entity.KnowledgeRole, error) {
	db := l.tdsql.TKnowledgeRole.WithContext(ctx)
	queryCond := l.makeKnowledgeRoleCondition(corpBizID, appBizID, filter)
	db = db.Where(queryCond...)
	count := int64(0)
	var err error
	if filter.NeedCount {
		count, err = db.Count()
	}
	if filter.Limit == -1 {
		// -1 表示仅查询总数
		if err != nil {
			return 0, nil, err
		}
		return count, nil, nil
	}
	if filter.Limit > 0 {
		db = db.Limit(filter.Limit).Offset(filter.Offset)
	}
	db = db.Order(l.tdsql.TKnowledgeRole.Type.Asc()).Order(l.tdsql.TKnowledgeRole.UpdateTime.Desc())
	qs, err := db.Find()
	if err != nil {
		return 0, nil, err
	}
	return count, KnowledgeRolesPO2DO(qs), nil
}

func (l *daoImpl) CreateKnowledgeRoleKnowList(ctx context.Context,
	records []*entity.KnowledgeRoleKnow, tx *tdsqlquery.Query) error {
	if tx == nil {
		tx = l.tdsql
	}
	return tx.TKnowledgeRoleKnow.WithContext(ctx).CreateInBatches(KnowledgeRoleKnowsDO2PO(records), GetChunkNumber())
}

func (l *daoImpl) makeKnowledgeRoleKnowCondition(corpBizID, appBizID uint64, filter *entity.KnowledgeRoleKnowFilter) []gen.Condition {
	queryCond := []gen.Condition{
		l.tdsql.TKnowledgeRoleKnow.IsDeleted.Is(false),
	}
	if corpBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleKnow.CorpBizID.Eq(corpBizID))
	}
	if appBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleKnow.AppBizID.Eq(appBizID))
	}
	if filter != nil {
		if filter.RoleBizID != 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeRoleKnow.RoleBizID.Eq(filter.RoleBizID))
		}
		if len(filter.KnowledgeBizIDs) > 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeRoleKnow.KnowledgeBizID.In(filter.KnowledgeBizIDs...))
		}
	}
	return queryCond
}

func (l *daoImpl) DeleteKnowledgeRoleKnow(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.KnowledgeRoleKnowFilter, tx *tdsqlquery.Query) error {
	if tx == nil {
		tx = l.tdsql
	}
	db := tx.TKnowledgeRoleKnow.WithContext(ctx)
	queryCond := l.makeKnowledgeRoleKnowCondition(corpBizID, appBizID, filter)
	_, err := db.Where(queryCond...).Updates(map[string]any{
		l.tdsql.TKnowledgeRoleKnow.IsDeleted.ColumnName().String():  1,
		l.tdsql.TKnowledgeRoleKnow.UpdateTime.ColumnName().String(): time.Now(),
	})
	return err
}

func (l *daoImpl) DescribeKnowledgeRoleKnowList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.KnowledgeRoleKnowFilter) ([]*entity.KnowledgeRoleKnow, error) {
	db := l.tdsql.TKnowledgeRoleKnow.WithContext(ctx)
	queryCond := l.makeKnowledgeRoleKnowCondition(corpBizID, appBizID, filter)
	qs, err := db.Where(queryCond...).Find()
	if err != nil {
		return nil, err
	}
	return KnowledgeRoleKnowsPO2DO(qs), nil
}

func (l *daoImpl) ModifyKnowledgeRoleKnow(ctx context.Context,
	req *entity.KnowledgeRoleKnow, tx *tdsqlquery.Query) (int, error) {
	if tx == nil {
		tx = l.tdsql
	}
	db := tx.TKnowledgeRoleKnow.WithContext(ctx)
	res, err := db.Where(tx.TKnowledgeRoleKnow.CorpBizID.Eq(req.CorpBizID),
		tx.TKnowledgeRoleKnow.AppBizID.Eq(req.AppBizID),
		tx.TKnowledgeRoleKnow.IsDeleted.Is(false),
		tx.TKnowledgeRoleKnow.RoleBizID.Eq(req.RoleBizID),
		tx.TKnowledgeRoleKnow.KnowledgeBizID.Eq(req.KnowledgeBizID)).
		Updates(map[string]any{
			tx.TKnowledgeRoleKnow.KnowledgeType.ColumnName().String():  req.KnowledgeType,
			tx.TKnowledgeRoleKnow.SearchType.ColumnName().String():     req.SearchType,
			tx.TKnowledgeRoleKnow.LableCondition.ColumnName().String(): req.LabelCondition,
		})
	if err != nil {
		return 0, err
	}
	return int(res.RowsAffected), nil
}

func (l *daoImpl) CreateKnowledgeRoleDocList(ctx context.Context,
	records []*entity.KnowledgeRoleDoc, tx *tdsqlquery.Query) error {
	if tx == nil {
		tx = l.tdsql
	}
	return tx.TKnowledgeRoleDoc.WithContext(ctx).Create(KnowledgeRoleDocsDO2PO(records)...)
}

func (l *daoImpl) makeKnowledgeRoleDocCondition(corpBizID, appBizID uint64, filter *entity.KnowledgeRoleDocFilter) []gen.Condition {
	queryCond := []gen.Condition{
		l.tdsql.TKnowledgeRoleDoc.IsDeleted.Is(false),
	}
	if corpBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleDoc.CorpBizID.Eq(corpBizID))
	}
	if appBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleDoc.AppBizID.Eq(appBizID))
	}
	if filter != nil {
		if filter.RoleBizID != 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeRoleDoc.RoleBizID.Eq(filter.RoleBizID))
		}
		if len(filter.KnowledgeBizIDs) > 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeRoleDoc.KnowledgeBizID.In(filter.KnowledgeBizIDs...))
		}
		if len(filter.DocBizIDs) > 0 {
			queryCond = append(queryCond, l.tdsql.TKnowledgeRoleDoc.DocBizID.In(filter.DocBizIDs...))
		}
	}
	return queryCond
}

func (l *daoImpl) DeleteKnowledgeRoleDocList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.KnowledgeRoleDocFilter, tx *tdsqlquery.Query) error {
	if tx == nil {
		tx = l.tdsql
	}
	batchSize := filter.BatchSize
	if batchSize == 0 {
		batchSize = GetChunkNumber()
	}
	for deleteRows := batchSize; deleteRows == batchSize; {
		db := tx.TKnowledgeRoleDoc.WithContext(ctx)
		queryCond := l.makeKnowledgeRoleDocCondition(corpBizID, appBizID, filter)
		res, err := db.Where(queryCond...).Limit(batchSize).Updates(map[string]any{
			l.tdsql.TKnowledgeRoleDoc.IsDeleted.ColumnName().String(): 1,
		})
		if err != nil {
			return err
		}
		deleteRows = int(res.RowsAffected)
	}
	return nil
}

func (l *daoImpl) DescribeRoleIDListByDocBiz(ctx context.Context,
	appBizId, docBizId uint64, batchSize int) ([]uint64, error) {
	logx.D(ctx, "feature_permission GetRoleByDocBiz appBizId:%v,docBizId:%v", appBizId, docBizId)
	roleBizIds, maxId, selectRow := make([]uint64, 0), 0, batchSize // 一次取batchSize行，因为一个文档可能被无限个角色引用，这边会有耗时问题
	for selectRow == batchSize {
		var roleList []*entity.KnowledgeRoleDoc
		qs, err := l.tdsql.TKnowledgeRoleDoc.WithContext(ctx).
			Where(l.tdsql.TKnowledgeRoleDoc.IsDeleted.Is(false)).
			Where(l.tdsql.TKnowledgeRoleDoc.KnowledgeBizID.Eq(appBizId)). // 兼容共享知识库处理
			Where(l.tdsql.TKnowledgeRoleDoc.DocBizID.Eq(docBizId)).
			Where(l.tdsql.TKnowledgeRoleDoc.ID.Gt(uint64(maxId))). // 避免深分页问题
			Select(l.tdsql.TKnowledgeRoleDoc.ID, l.tdsql.TKnowledgeRoleDoc.RoleBizID).Limit(batchSize).
			Order(l.tdsql.TKnowledgeRoleDoc.ID.Desc()).Find()
		if err != nil {
			logx.E(ctx, "feature_permission GetRoleByDocBiz err:%v,appBizId:%v,docBizId:%v",
				err, appBizId, docBizId)
			return nil, err
		}
		for _, role := range qs {
			roleBizIds = append(roleBizIds, role.RoleBizID)
		}
		selectRow = len(roleList)
		if selectRow != 0 {
			maxId = int(roleList[selectRow-1].ID)
		}
	}
	return roleBizIds, nil
}

func (l *daoImpl) DescribeKnowledgeRoleDocList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.KnowledgeRoleDocFilter) ([]*entity.KnowledgeRoleDoc, error) {
	db := l.tdsql.TKnowledgeRoleDoc.WithContext(ctx)
	queryCond := l.makeKnowledgeRoleDocCondition(corpBizID, appBizID, filter)
	var res []*entity.KnowledgeRoleDoc
	var records []*model.TKnowledgeRoleDoc
	batchSize := filter.BatchSize
	if batchSize == 0 {
		batchSize = GetChunkNumber()
	}
	err := db.Where(queryCond...).FindInBatches(&records, batchSize, func(tx gen.Dao, batch int) error {
		res = append(res, KnowledgeRoleDocsPO2DO(records)...)
		return nil
	})
	return res, err
}

func (l *daoImpl) CreateKnowledgeRoleQAList(ctx context.Context,
	records []*entity.KnowledgeRoleQA, tx *tdsqlquery.Query) error {
	if tx == nil {
		tx = l.tdsql
	}
	return tx.TKnowledgeRoleQa.WithContext(ctx).CreateInBatches(KnowledgeRoleQAsDO2PO(records), GetChunkNumber())
}

func (l *daoImpl) makeKnowledgeRoleQACondition(corpBizID, appBizID uint64,
	filter *entity.KnowledgeRoleQAFilter) []gen.Condition {
	queryCond := []gen.Condition{
		l.tdsql.TKnowledgeRoleQa.IsDeleted.Is(false),
	}
	if corpBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleQa.CorpBizID.Eq(corpBizID))
	}
	if appBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleQa.AppBizID.Eq(appBizID))
	}

	if filter.RoleBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleQa.RoleBizID.Eq(filter.RoleBizID))
	}
	if len(filter.KnowledgeBizIDs) != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleQa.KnowledgeBizID.In(filter.KnowledgeBizIDs...))
	}
	if len(filter.QABizIDs) != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleQa.QaBizID.In(filter.QABizIDs...))
	}
	return queryCond
}

func (l *daoImpl) DeleteKnowledgeRoleQAList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.KnowledgeRoleQAFilter, tx *tdsqlquery.Query) error {
	if tx == nil {
		tx = l.tdsql
	}
	db := tx.TKnowledgeRoleQa.WithContext(ctx)
	queryCond := l.makeKnowledgeRoleQACondition(corpBizID, appBizID, filter)
	batchSize := filter.BatchSize
	if batchSize == 0 {
		batchSize = GetChunkNumber()
	}
	for deleteRows := batchSize; deleteRows == batchSize; {
		res, err := db.Where(queryCond...).Updates(map[string]any{
			l.tdsql.TKnowledgeRoleQa.IsDeleted.ColumnName().String():  1,
			l.tdsql.TKnowledgeRoleQa.UpdateTime.ColumnName().String(): time.Now(),
		})
		if err != nil {
			return fmt.Errorf("delete knowledge role qa err:%v", err)
		}
		deleteRows = int(res.RowsAffected)
	}
	return nil
}

func (l *daoImpl) DescribeKnowledgeRoleQAList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.KnowledgeRoleQAFilter) ([]*entity.KnowledgeRoleQA, error) {
	db := l.tdsql.TKnowledgeRoleQa.WithContext(ctx)
	queryCond := l.makeKnowledgeRoleQACondition(corpBizID, appBizID, filter)
	var res []*entity.KnowledgeRoleQA
	var records []*model.TKnowledgeRoleQa
	batchSize := filter.BatchSize
	if batchSize == 0 {
		batchSize = GetChunkNumber()
	}
	err := db.Where(queryCond...).FindInBatches(&records, batchSize, func(tx gen.Dao, batch int) error {
		res = append(res, KnowledgeRoleQAsPO2DO(records)...)
		return nil
	})
	return res, err
}

func (l *daoImpl) CreateKnowledgeRoleAttributeLabelList(ctx context.Context,
	records []*entity.KnowledgeRoleAttributeLabel, tx *tdsqlquery.Query) error {
	if tx == nil {
		tx = l.tdsql
	}
	return tx.TKnowledgeRoleAttributeLabel.WithContext(ctx).
		CreateInBatches(KnowledgeRoleAttributeLabelsDO2PO(records), GetChunkNumber())
}

func (l *daoImpl) makeKnowledgeRoleAttributeLabelCondition(corpBizID, appBizID uint64, filter *entity.KnowledgeRoleAttributeLabelFilter) []gen.Condition {
	queryCond := []gen.Condition{
		l.tdsql.TKnowledgeRoleAttributeLabel.IsDeleted.Is(false),
	}
	if corpBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleAttributeLabel.CorpBizID.Eq(corpBizID))
	}
	if appBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleAttributeLabel.AppBizID.Eq(appBizID))
	}
	if filter.RoleBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleAttributeLabel.RoleBizID.Eq(filter.RoleBizID))
	}
	if len(filter.KnowledgeBizIDs) != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleAttributeLabel.KnowledgeBizID.In(filter.KnowledgeBizIDs...))
	}
	if len(filter.AttrBizIDs) != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleAttributeLabel.AttrBizID.In(filter.AttrBizIDs...))
	}
	if len(filter.LabelBizIDs) != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleAttributeLabel.LabelBizID.In(filter.LabelBizIDs...))
	}
	return queryCond
}

func (l *daoImpl) DeleteKnowledgeRoleAttributeLabelList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.KnowledgeRoleAttributeLabelFilter, tx *tdsqlquery.Query) (int64, error) {
	if tx == nil {
		tx = l.tdsql
	}
	db := tx.TKnowledgeRoleAttributeLabel.WithContext(ctx)
	queryCond := l.makeKnowledgeRoleAttributeLabelCondition(corpBizID, appBizID, filter)
	res, err := db.Where(queryCond...).Updates(map[string]any{
		l.tdsql.TKnowledgeRoleAttributeLabel.IsDeleted.ColumnName().String(): 1,
	})
	if err != nil {
		return 0, fmt.Errorf("delete knowledge role attribute label err:%v", err)
	}
	return res.RowsAffected, nil
}

func (l *daoImpl) DescribeKnowledgeRoleAttributeLabelList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.KnowledgeRoleAttributeLabelFilter) ([]*entity.KnowledgeRoleAttributeLabel, error) {
	db := l.tdsql.TKnowledgeRoleAttributeLabel.WithContext(ctx)
	queryCond := l.makeKnowledgeRoleAttributeLabelCondition(corpBizID, appBizID, filter)
	qs, err := db.Where(queryCond...).Find()
	return KnowledgeRoleAttributeLabelsPO2DO(qs), err
}

func (l *daoImpl) DeleteKnowledgeRoleAttributeLabelByAttrAndLabelBizIDs(ctx context.Context,
	knowBizID uint64, attrBizIDs, labelBizIDs []uint64, pageSize, batchSize int) error {
	logx.D(ctx, "feature_permission BatchDeleteRoleLabel knowBizID:%v, attrBizIDs:%v, labelBizIds:%v",
		knowBizID, attrBizIDs, labelBizIDs)

	// 提前获取所属知识库，用于更新缓存
	defer l.ModifyRoleKnowledgeByAttrChange(ctx, knowBizID, attrBizIDs, labelBizIDs)()

	if len(attrBizIDs) > 0 {
		length := len(attrBizIDs)
		for start := 0; start < length; start += pageSize {
			deleteMaxRow, end := batchSize, min(start+pageSize, length)
			tmp := attrBizIDs[start:end]
			for deleteMaxRow == batchSize { // 每次删除batchSize行
				res, err := l.tdsql.TKnowledgeRoleAttributeLabel.WithContext(ctx).
					Where(l.tdsql.TKnowledgeRoleAttributeLabel.KnowledgeBizID.Eq(knowBizID)). // 兼容共享知识库需求
					Where(l.tdsql.TKnowledgeRoleAttributeLabel.AttrBizID.In(tmp...)).Limit(batchSize).
					Updates(map[string]any{
						l.tdsql.TKnowledgeRoleAttributeLabel.IsDeleted.ColumnName().String(): 1,
					})
				if err != nil {
					logx.E(ctx, "feature_permission BatchDeleteRoleAttribute err:%v,knowBizID:%v,attrBizIDs:%v",
						err, knowBizID, attrBizIDs)
					return err
				}
				deleteMaxRow = int(res.RowsAffected)
			}
		}
	}

	if len(labelBizIDs) > 0 {
		length := len(labelBizIDs)
		for start := 0; start < length; start += pageSize {
			deleteMaxRow, end := batchSize, min(start+pageSize, length)
			tmp := labelBizIDs[start:end]
			for deleteMaxRow == batchSize { // 每次删除batchSize行
				res, err := l.tdsql.TKnowledgeRoleAttributeLabel.WithContext(ctx).
					Where(l.tdsql.TKnowledgeRoleAttributeLabel.KnowledgeBizID.Eq(knowBizID)). // 兼容共享知识库需求
					Where(l.tdsql.TKnowledgeRoleAttributeLabel.LabelBizID.In(tmp...)).Limit(batchSize).
					Updates(map[string]any{
						l.tdsql.TKnowledgeRoleAttributeLabel.IsDeleted.ColumnName().String(): 1,
					})
				if err != nil {
					logx.E(ctx, "feature_permission BatchDeleteRoleLabel err:%v,knowBizID:%v,labelBizIDs:%v",
						err, knowBizID, labelBizIDs)
					return err
				}
				deleteMaxRow = int(res.RowsAffected)
			}
		}
	}
	return nil
}

func (l *daoImpl) CreateKnowledgeRoleCateList(ctx context.Context,
	records []*entity.KnowledgeRoleCate, tx *tdsqlquery.Query) error {
	if tx == nil {
		tx = l.tdsql
	}
	return tx.TKnowledgeRoleCate.WithContext(ctx).
		CreateInBatches(KnowledgeRoleCatesDO2PO(records), GetChunkNumber())
}

func (l *daoImpl) makeKnowledgeRoleCateCondition(corpBizID, appBizID uint64, filter *entity.KnowledgeRoleCateFilter) []gen.Condition {
	queryCond := []gen.Condition{
		l.tdsql.TKnowledgeRoleCate.IsDeleted.Is(false),
	}
	if corpBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleCate.CorpBizID.Eq(corpBizID))
	}
	if appBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleCate.AppBizID.Eq(appBizID))
	}
	if filter.RoleBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleCate.RoleBizID.Eq(filter.RoleBizID))
	}
	if len(filter.KnowledgeBizIDs) != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleCate.KnowledgeBizID.In(filter.KnowledgeBizIDs...))
	}
	if len(filter.CateBizIDs) != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleCate.CateBizID.In(filter.CateBizIDs...))
	}
	if filter.CatType != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleCate.Type.Eq(filter.CatType))
	}
	return queryCond
}

func (l *daoImpl) DeleteKnowledgeRoleCateList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.KnowledgeRoleCateFilter, tx *tdsqlquery.Query) (int64, error) {
	if tx == nil {
		tx = l.tdsql
	}
	db := tx.TKnowledgeRoleCate.WithContext(ctx)
	queryCond := l.makeKnowledgeRoleCateCondition(corpBizID, appBizID, filter)
	db = db.Where(queryCond...)
	if filter.Limit > 0 {
		db = db.Limit(filter.Limit)
	}
	res, err := db.Updates(map[string]any{
		l.tdsql.TKnowledgeRoleCate.IsDeleted.ColumnName().String(): 1,
	})
	if err != nil {
		return 0, fmt.Errorf("delete knowledge role cate err:%v", err)
	}
	return res.RowsAffected, nil
}

func (l *daoImpl) DescribeKnowledgeRoleCateList(ctx context.Context,
	corpBizID, appBizID uint64, filter *entity.KnowledgeRoleCateFilter) ([]*entity.KnowledgeRoleCate, error) {
	db := l.tdsql.TKnowledgeRoleCate.WithContext(ctx)
	queryCond := l.makeKnowledgeRoleCateCondition(corpBizID, appBizID, filter)
	qs, err := db.Where(queryCond...).Find()
	return KnowledgeRoleCatesPO2DO(qs), err
}

func (l *daoImpl) DeleteRoleCateListByKnowAndCateBizID(ctx context.Context,
	knowBizID, cateBizID uint64, batchSize int) error {
	logx.D(ctx, "feature_permission BatchDeleteRoleCate knowBizID:%v,cateBizId:%v", knowBizID, cateBizID)
	for deleteRows := batchSize; deleteRows == batchSize; { // 每次删除batchSize行
		res, err := l.tdsql.TKnowledgeRoleCate.WithContext(ctx).
			Where(l.tdsql.TKnowledgeRoleCate.KnowledgeBizID.Eq(knowBizID)). // 兼容共享知识库需求
			Where(l.tdsql.TKnowledgeRoleCate.CateBizID.Eq(cateBizID)).Limit(batchSize).
			Updates(map[string]any{
				l.tdsql.TKnowledgeRoleCate.IsDeleted.ColumnName().String(): 1,
			})
		if err != nil {
			logx.E(ctx, "feature_permission BatchDeleteRoleCate err:%v,knowBizID:%v,cateBizId:%v",
				err, knowBizID, cateBizID)
			return err
		}
		deleteRows = int(res.RowsAffected)
	}
	return nil
}

func (l *daoImpl) CreateKnowledgeRoleDatabaseList(ctx context.Context, records []*entity.KnowledgeRoleDatabase, tx *tdsqlquery.Query) error {
	if tx == nil {
		tx = l.tdsql
	}
	return tx.TKnowledgeRoleDatabase.WithContext(ctx).Create(KnowledgeRoleDatabasesDO2PO(records)...)
}

func (l *daoImpl) makeKnowledgeRoleDatabaseCondition(corpBizID, appBizID uint64, filter *entity.KnowledgeRoleDatabaseFilter) []gen.Condition {
	queryCond := []gen.Condition{
		l.tdsql.TKnowledgeRoleDatabase.IsDeleted.Is(false),
		l.tdsql.TKnowledgeRoleDatabase.CorpBizID.Eq(corpBizID),
		l.tdsql.TKnowledgeRoleDatabase.AppBizID.Eq(appBizID),
	}
	if filter.RoleBizID != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleDatabase.RoleBizID.Eq(filter.RoleBizID))
	}
	if len(filter.KnowledgeBizIDs) != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleDatabase.KnowledgeBizID.In(filter.KnowledgeBizIDs...))
	}
	if len(filter.DatabaseBizIDs) != 0 {
		queryCond = append(queryCond, l.tdsql.TKnowledgeRoleDatabase.DatabaseBizID.In(filter.DatabaseBizIDs...))
	}
	return queryCond
}

func (l *daoImpl) DeleteKnowledgeRoleDatabaseList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.KnowledgeRoleDatabaseFilter, tx *tdsqlquery.Query) (int64, error) {
	if tx == nil {
		tx = l.tdsql
	}
	batchSize := filter.BatchSize
	if batchSize == 0 {
		batchSize = GetChunkNumber()
	}
	updateCnt := uint64(0)
	for deleteRows := batchSize; deleteRows == batchSize; {
		db := tx.TKnowledgeRoleDatabase.WithContext(ctx)
		queryCond := l.makeKnowledgeRoleDatabaseCondition(corpBizID, appBizID, filter)
		res, err := db.Where(queryCond...).Limit(batchSize).Updates(map[string]any{
			l.tdsql.TKnowledgeRoleDatabase.IsDeleted.ColumnName().String(): 1,
		})
		if err != nil {
			return 0, fmt.Errorf("delete knowledge role database err:%v", err)
		}
		deleteRows = int(res.RowsAffected)
		updateCnt += uint64(res.RowsAffected)
	}
	return int64(updateCnt), nil
}

func (l *daoImpl) DeleteKnowledgeRoleDbTables(ctx context.Context,
	knowBizID uint64, dbTableBizIDs []uint64, pageSize, batchSize int) error {
	logx.D(ctx, "DeleteKnowledgeRoleDbTables knowBizID:%v,dbTableBizIDs:%v", knowBizID, dbTableBizIDs)
	length := len(dbTableBizIDs) // pageSize个一批
	for start := 0; start < length; start += pageSize {
		deleteMaxRow, end := batchSize, min(start+pageSize, length)
		tmp := dbTableBizIDs[start:end]
		for deleteMaxRow == batchSize { // 每次删除batchSize行
			res, err := l.tdsql.TKnowledgeRoleDatabase.WithContext(ctx).
				Where(l.tdsql.TKnowledgeRoleDatabase.KnowledgeBizID.Eq(knowBizID)). // 兼容共享知识库需求
				Where(l.tdsql.TKnowledgeRoleDatabase.DatabaseBizID.In(tmp...)).Limit(batchSize).
				Updates(map[string]any{
					l.tdsql.TKnowledgeRoleDatabase.IsDeleted.ColumnName().String(): 1,
				})
			if err != nil {
				logx.E(ctx, "DeleteKnowledgeRoleDbTables err:%v,knowBizID:%v,dbTableBizIDs:%v",
					err, knowBizID, dbTableBizIDs)
				return err
			}
			deleteMaxRow = int(res.RowsAffected)
		}
	}
	return nil
}

func (l *daoImpl) DescribeKnowledgeRoleDatabaseList(ctx context.Context,
	corpBizID, appBizID uint64,
	filter *entity.KnowledgeRoleDatabaseFilter) ([]*entity.KnowledgeRoleDatabase, error) {
	queryCond := l.makeKnowledgeRoleDatabaseCondition(corpBizID, appBizID, filter)
	queryCond = append(queryCond, l.tdsql.TKnowledgeRoleDatabase.IsDeleted.Is(false))
	db := l.tdsql.TKnowledgeRoleDatabase.WithContext(ctx).Where(queryCond...)
	if filter.Limit > 0 {
		db = db.Limit(filter.Limit)
	}
	res, err := db.Find()
	if err != nil {
		return nil, fmt.Errorf("describe knowledge role database err:%v", err)
	}
	return KnowledgeRoleDatabasesPO2DO(res), nil
}

func (l *daoImpl) DescribeRoleByDbBiz(ctx context.Context, knowBizID, dbBizId uint64, batchSize int) ([]uint64, error) {
	logx.D(ctx, "GetRoleByDbBiz knowBizID:%v,dbBizId:%v", knowBizID, dbBizId)
	roleBizIds, maxId, selectRow := make([]uint64, 0), 0, batchSize // 一次取1万行，因为一个文档可能被无限个角色引用，这边会有耗时问题
	for selectRow == batchSize {
		qs, err := l.tdsql.TKnowledgeRoleDatabase.WithContext(ctx).
			Where(l.tdsql.TKnowledgeRoleDatabase.IsDeleted.Is(false)).
			Where(l.tdsql.TKnowledgeRoleDatabase.KnowledgeBizID.Eq(knowBizID)). // 兼容共享知识库处理
			Where(l.tdsql.TKnowledgeRoleDatabase.DatabaseBizID.Eq(dbBizId)).
			Where(l.tdsql.TKnowledgeRoleDatabase.ID.Gt(uint64(maxId))).
			Select(l.tdsql.TKnowledgeRoleDatabase.ID, l.tdsql.TKnowledgeRoleDatabase.RoleBizID).
			Limit(batchSize).
			Order(l.tdsql.TKnowledgeRoleDatabase.ID.Asc()).
			Find()
		if err != nil {
			logx.E(ctx, "GetRoleByDbBiz err:%v,knowBizID:%v,dbBizId:%v", err, knowBizID, dbBizId)
			return nil, err
		}
		for _, role := range qs {
			roleBizIds = append(roleBizIds, role.RoleBizID)
		}
		selectRow = len(qs)
		if selectRow != 0 {
			maxId = int(qs[selectRow-1].ID)
		}
	}
	return roleBizIds, nil
}

func (l *daoImpl) DeleteKnowledgeAssociation(ctx context.Context, corpBizID, appBizID uint64, knowledgeBizIds []uint64) error {
	if len(knowledgeBizIds) == 0 {
		return fmt.Errorf("knowledgeBizIds is empty")
	}
	knows, err := l.DescribeKnowledgeRoleKnowList(ctx, corpBizID, appBizID,
		&entity.KnowledgeRoleKnowFilter{
			KnowledgeBizIDs: knowledgeBizIds,
		})
	if err != nil {
		return fmt.Errorf("describe knowledge role know err:%v", err)
	}
	role2knows := make(map[uint64][]uint64, 3)
	for _, know := range knows {
		if _, ok := role2knows[know.RoleBizID]; !ok {
			role2knows[know.RoleBizID] = make([]uint64, 0, 3)
		}
		role2knows[know.RoleBizID] = append(role2knows[know.RoleBizID], know.KnowledgeBizID)
	}
	logx.D(ctx, "role2knows: %v remove:%+v", role2knows, knowledgeBizIds)

	// 删除知识库关联
	if err := l.DeleteKnowledgeRoleKnow(ctx, corpBizID, appBizID, &entity.KnowledgeRoleKnowFilter{
		KnowledgeBizIDs: knowledgeBizIds,
	}, nil); err != nil {
		return fmt.Errorf("delete knowledge role know err:%v", err)
	}
	go func(ctx context.Context) {
		defer func() {
			if err := recover(); err != nil {
				logx.E(ctx, "RemoveKnowledgeAssociation failed, err: %v", err)
				return
			}
		}()
		// 删除文档关联
		if err := l.DeleteKnowledgeRoleDocList(ctx, corpBizID, appBizID, &entity.KnowledgeRoleDocFilter{
			KnowledgeBizIDs: knowledgeBizIds,
		}, nil); err != nil {
			logx.E(ctx, "DeleteKnowledgeRoleDocList err:%v", err)
			return
		}
		// 删除问答关联
		if err := l.DeleteKnowledgeRoleQAList(ctx, corpBizID, appBizID, &entity.KnowledgeRoleQAFilter{
			KnowledgeBizIDs: knowledgeBizIds,
		}, nil); err != nil {
			logx.E(ctx, "DeleteKnowledgeRoleQAList err:%v", err)
			return
		}
		// 删除分类关联
		if _, err := l.DeleteKnowledgeRoleCateList(ctx, corpBizID, appBizID, &entity.KnowledgeRoleCateFilter{
			KnowledgeBizIDs: knowledgeBizIds,
		}, nil); err != nil {
			logx.E(ctx, "DeleteKnowledgeRoleCateList err:%v", err)
			return
		}
		// 删除标签关联
		if _, err := l.DeleteKnowledgeRoleAttributeLabelList(ctx, corpBizID, appBizID, &entity.KnowledgeRoleAttributeLabelFilter{
			KnowledgeBizIDs: knowledgeBizIds,
		}, nil); err != nil {
			logx.E(ctx, "DeleteKnowledgeRoleAttributeLabelList err:%v", err)
			return
		}
		// 删除数据库关联
		if _, err := l.DeleteKnowledgeRoleDatabaseList(ctx, corpBizID, appBizID, &entity.KnowledgeRoleDatabaseFilter{
			KnowledgeBizIDs: knowledgeBizIds,
		}, nil); err != nil {
			logx.E(ctx, "DeleteKnowledgeRoleDatabaseList err:%v", err)
			return
		}
		// 删除缓存
		for k, knows := range role2knows {
			if err := l.deleteKnowledgeCache(ctx, corpBizID, appBizID, k, knows); err != nil {
				logx.E(ctx, "CleanKnowledgeCache failed, err: %v", err)
				return
			}
		}
	}(trpc.CloneContext(ctx))
	return nil
}

func (l *daoImpl) ModifyRoleKnowledgeByAttrChange(ctx context.Context,
	knowBizID uint64, attrBizIds []uint64, labelBizIds []uint64) func() error {
	updates, err := l.DescribeKnowledgeRoleAttributeLabelList(ctx, 0, 0,
		&entity.KnowledgeRoleAttributeLabelFilter{
			KnowledgeBizIDs: []uint64{knowBizID},
			AttrBizIDs:      attrBizIds,
			LabelBizIDs:     labelBizIds,
		})
	logx.D(ctx, "UpdateRoleKnowledgeByAttrChange %+v req:%d %+v %+v", updates, knowBizID, attrBizIds, labelBizIds)
	return func() error {
		if err != nil {
			return err
		}
		for _, update := range updates {
			if err := l.modifyRoleKnowledgeCache(ctx, update.CorpBizID, update.AppBizID, update.RoleBizID, update.KnowledgeBizID); err != nil {
				logx.E(ctx, "UpdateRoleKnowledgeCache failed, err: %v", err)
				return err
			}
		}
		return nil
	}
}

func (l *daoImpl) ModifyRoleKnowledgeByCate(ctx context.Context,
	corpBizID, knowBizID uint64, cateBizIDs []uint64) error {
	defer func() {
		if err := recover(); err != nil {
			logx.E(ctx, "ModifyRoleKnowledgeByCate panic, err: %v", err)
			return
		}
	}()
	logx.D(ctx, "ModifyRoleKnowledgeByCate corpBizID: %d,  knowBizID: %d, cateBizIDs: %+v", corpBizID, knowBizID, cateBizIDs)
	updates, err := l.DescribeKnowledgeRoleCateList(ctx, corpBizID, 0,
		&entity.KnowledgeRoleCateFilter{
			KnowledgeBizIDs: []uint64{knowBizID},
			CateBizIDs:      cateBizIDs,
		})
	if err != nil {
		return err
	}
	for _, update := range updates {
		if err := l.modifyRoleKnowledgeCache(ctx, update.CorpBizID, update.AppBizID, update.RoleBizID, update.KnowledgeBizID); err != nil {
			logx.E(ctx, "UpdateRoleKnowledgeCache failed, err: %v", err)
			return err
		}
	}
	return nil
}

func (l *daoImpl) DescribeUserByID(ctx context.Context, id uint64) (*entity.User, error) {
	conds := []gen.Condition{
		l.mysql.TUser.ID.Eq(id),
	}
	qs, err := l.mysql.TUser.WithContext(ctx).Where(conds...).Find()
	if err != nil {
		return nil, fmt.Errorf("DescribeUserByID failed, err: %v", err)
	}
	if len(qs) == 0 {
		return nil, nil
	}
	return UserPO2DO(qs[0]), nil

}

func (l *daoImpl) DescribeSIUser(ctx context.Context, sid uint64, loginUin, loginSubAccountUin string) (*entity.User, error) {
	conds := []gen.Condition{
		l.mysql.TUser.Sid.Eq(sid),
		l.mysql.TUser.Uin.Eq(loginUin),
		l.mysql.TUser.SubAccountUin.Eq(loginSubAccountUin),
	}
	db := l.mysql.TUser.WithContext(ctx)
	qs, err := db.Where(conds...).Find()
	if err != nil {
		return nil, fmt.Errorf("DescribeSIUser failed, err: %v", err)
	}
	if len(qs) == 0 {
		return nil, nil
	}
	return UserPO2DO(qs[0]), nil
}

func (l *daoImpl) DescribeExpUser(ctx context.Context, id uint64) (*entity.User, error) {
	db := l.mysql.TExpUser.WithContext(ctx)
	qs, err := db.Where(l.mysql.TExpUser.ID.Eq(id)).Find()
	if err != nil {
		return nil, fmt.Errorf("DescribeExpUser failed, err: %v", err)
	}
	if len(qs) == 0 {
		return nil, nil
	}
	return ExpUserPO2DO(qs[0]), nil
}

func (l *daoImpl) DescribeExpUserList(ctx context.Context, ids []uint64) ([]*entity.User, error) {
	db := l.mysql.TExpUser.WithContext(ctx)
	qs, err := db.Where(l.mysql.TExpUser.ID.In(ids...)).Find()
	if err != nil {
		return nil, fmt.Errorf("DescribeExpUserList failed, err: %v", err)
	}
	users := make([]*entity.User, 0)
	for _, q := range qs {
		users = append(users, ExpUserPO2DO(q))
	}
	return users, nil
}

func getThirdUserIDKey(appBizID uint64, thirdUserID string) string {
	return fmt.Sprintf(RedisThirdUserID, appBizID, thirdUserID)
}
