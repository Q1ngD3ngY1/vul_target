package share_knowledge

import (
	"context"
	"fmt"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/user_resource_permission"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_common"
)

// FlushShareKbUserResourcePermission 刷新共享知识库资源权限
func FlushShareKbUserResourcePermission(ctx context.Context, db dao.Dao, startID, endID uint64) error {
	log.InfoContextf(ctx, "FlushShareKbUserResourcePermission--------------- start")
	var count uint64
	limit := 200
	for {
		kbs, err := db.GetShareKnowledgeBaseByIDRange(ctx, startID, endID, limit)
		if err != nil {
			log.ErrorContextf(ctx, "FlushShareKbUserResourcePermission.GetShareKnowledgeBaseByIDRange err:%+v", err)
			return err
		}
		if len(kbs) == 0 {
			break
		}
		for _, kb := range kbs {
			log.DebugContextf(ctx, "FlushShareKbUserResourcePermission.processing,ID:%d,kb:%+v", kb.ID, kb)
			if kb.UserBizID == 0 || kb.OwnerStaffID > 0 {
				continue
			}
			// 这里kb的UserBizID，实际上是staff的businessID，需要根据staff的businessID获取staff的userID，然后根据userID获取user的sid
			staff, err := db.GetStaffByBusinessID(ctx, kb.UserBizID)
			if err != nil {
				log.ErrorContextf(ctx, "FlushShareKbUserResourcePermission.GetStaffByID err:%+v,ID:%d", err,
					kb.ID)
				continue
			}
			if staff == nil {
				log.ErrorContextf(ctx, "FlushShareKbUserResourcePermission.staff is null,ID:%d", kb.ID)
				continue
			}
			user, err := db.GetUserByID(ctx, staff.UserID)
			if err != nil {
				log.ErrorContextf(ctx, "FlushShareKbUserResourcePermission.GetUserByID err:%+v,ID:%d", err,
					kb.ID)
				continue
			}
			if user == nil {
				log.ErrorContextf(ctx, "FlushShareKbUserResourcePermission.user is null,ID:%d", kb.ID)
				continue
			}
			kb.OwnerStaffID = staff.ID
			if err := FlushShareKbUserResourcePermissionUnit(ctx, db, user.SID, user.Uin, user.SubAccountUin,
				kb); err != nil {
				log.ErrorContextf(ctx, "FlushShareKbUserResourcePermission.unit,err:%+v,ID:%d", err, kb.ID)
				continue
			}
			count++
			if count%100 == 0 {
				log.InfoContextf(ctx, "FlushShareKbUserResourcePermission.processing,ID:%d", kb.ID)
			}
		}
		if len(kbs) < limit {
			break
		}
		startID = kbs[len(kbs)-1].ID + 1
	}
	log.InfoContextf(ctx, "FlushShareKbUserResourcePermission--------------- done")
	return nil
}

func FlushShareKbUserResourcePermissionUnit(ctx context.Context, db dao.Dao, sid int, uin, subAccountUin string,
	kb *model.SharedKnowledgeInfo) error {
	// 非云账户只需要更新数据，不需要添加权限
	if sid != model.CloudSID {
		return db.UpdateShareKnowledgeBaseOwnerStaffID(ctx, kb)
	}
	ctx = pkg.WithSID(ctx, uint64(sid))
	if err := user_resource_permission.AddUserResourcePermission(ctx, db, uin, subAccountUin, model.DefaultSpaceID,
		bot_common.ResourceType_ResourceTypeKnowledge, fmt.Sprintf("%d", kb.BusinessID)); err != nil {
		return err
	}
	return db.UpdateShareKnowledgeBaseOwnerStaffID(ctx, kb)
}
