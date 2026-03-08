package service

import (
	"context"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	Permis "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/logic/permissions"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	"github.com/spf13/cast"
)

// CreateKnowledgeRole 保存角色
func (s *Service) CreateKnowledgeRole(ctx context.Context, req *pb.CreateRoleReq) (
	rsp *pb.CreateRoleRsp, err error) {
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	ctx = pkg.WithAppID(ctx, app.ID)
	ctx = pkg.WithAppName(ctx, app.Name)
	log.InfoContextf(ctx, "CreateRole req:%v", req)
	modifyReq := &pb.ModifyReq{
		AppBizId:    req.GetAppBizId(),
		Name:        req.GetName(),
		SearchType:  req.GetSearchType(),
		KnowChoose:  req.GetKnowChoose(),
		Description: req.GetDescription(),
		RoleBizId:   "",
		Type:        model.KnowledgeRoleTypeCustom,
	}
	if err = s.checkRoleReq(ctx, modifyReq, true); err != nil {
		return rsp, err
	}
	totalCnt, _, err := s.permisLogic.ListKnowledgeRoles(ctx, &dao.KnowledgeRoleReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: pkg.CorpBizID(ctx),
			AppBizID:  cast.ToUint64(req.GetAppBizId()),
			Limit:     -1, // 仅查询数量
		},
	})
	if err != nil {
		log.ErrorContextf(ctx, "ListKnowledgeRoles err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}
	roleMax := utilConfig.GetMainConfig().Permissions.RoleMaxLimit
	if totalCnt >= int64(roleMax) {
		log.ErrorContextf(ctx, "role max limit:%d - %d", totalCnt, roleMax)
		return nil, errs.ErrRoleMaxLimit
	}

	roleBizId, syncInfos, err := s.permisLogic.ModifyRole(ctx, modifyReq, true)
	if err != nil {
		log.ErrorContextf(ctx, "CreateRole err:%v", err)
		return nil, err
	}
	s.permisLogic.RoleSyncInfos(ctx, app.BusinessID, roleBizId, syncInfos)
	rsp = &pb.CreateRoleRsp{
		RoleBizId: roleBizId,
	}
	log.InfoContextf(ctx, "CreateRole rsp:%v", roleBizId)
	return rsp, err
}

func (s *Service) checkRoleReq(ctx context.Context, req *pb.ModifyReq, isAdd bool) error {
	log.InfoContextf(ctx, "checkRoleReq req:%v", req)
	nameMin := utilConfig.GetMainConfig().Permissions.RoleNameMinLimit
	nameMax := utilConfig.GetMainConfig().Permissions.RoleNameMaxLimit
	if len([]rune(req.GetName())) < nameMin || len([]rune(req.GetName())) > nameMax {
		log.ErrorContextf(ctx, "role name length is not in limit, name:%s, min:%d, max:%d", req.GetName(), nameMin, nameMax)
		return errs.ErrRoleNameFail
	}
	if pkg.CorpBizID(ctx) == 0 || req.GetAppBizId() == "" {
		log.ErrorContextf(ctx, "checkRoleReq corpApp:%s", req.GetAppBizId())
		return errs.ErrCommonFail
	}
	if req.GetSearchType() == 0 || req.GetSearchType() > model.RoleChooseKnow {
		log.ErrorContextf(ctx, "checkRoleReq searchType:%d", req.GetSearchType())
		return errs.ErrRoleSearchTypeFail
	}
	if req.GetType() == 0 || req.GetType() > model.KnowledgeRoleTypeCustom {
		log.ErrorContextf(ctx, "checkRoleReq type:%d", req.GetType())
		return errs.ErrRoleTypeFail
	}

	for _, v := range req.GetKnowChoose() {
		if v.GetKnowledgeBizId() == "" || v.GetKnowledgeBizId() == "0" { // 知识库业务ID不能为空
			log.ErrorContextf(ctx, "checkRoleReq GetKnowledgeBizId:%s", v.GetKnowledgeBizId())
			return errs.ErrRoleKnowledgeFail
		}
		if v.Type == 0 || v.Type > model.KnowPublic {
			log.ErrorContextf(ctx, "checkRoleReq type:%d", v.Type)
			return errs.ErrRoleKnowledgeTypeFail
		}

		switch v.SearchType {
		case model.KnowSearchAll: // 全部
			if len(v.DbBizIds) != 0 || len(v.DocBizIds) != 0 || len(v.QuesAnsBizIds) != 0 || len(v.DocCateBizIds) != 0 ||
				len(v.QuesAnsCateBizIds) != 0 || len(v.Labels) != 0 {
				log.ErrorContextf(ctx, "checkRoleReq SearchType:%d", v.SearchType)
				return errs.ErrRoleKnowledgeFail
			}
			if v.Condition != 0 {
				return errs.ErrRoleConditionFail
			}
		case model.KnowSearchSpecial: // 按特定知识
			if len(v.DocBizIds) == 0 && len(v.QuesAnsBizIds) == 0 && len(v.DocCateBizIds) == 0 &&
				len(v.QuesAnsCateBizIds) == 0 && len(v.DbBizIds) == 0 {
				log.ErrorContextf(ctx, "checkRoleReq SearchType:%d %+v", v.SearchType, v)
				return errs.ErrRoleKnowledgeFail
			}
			if v.Condition != 0 {
				return errs.ErrRoleConditionFail
			}
		case model.KnowSearchLabel: // 按标签
			if len(v.DbBizIds) != 0 || len(v.DocBizIds) != 0 || len(v.QuesAnsBizIds) != 0 || len(v.DocCateBizIds) != 0 ||
				len(v.QuesAnsCateBizIds) != 0 || v.Condition == 0 || v.Condition > model.ConditionLogicOr || len(v.Labels) == 0 {
				log.ErrorContextf(ctx, "checkRoleReq SearchType:%d", v.SearchType)
				return errs.ErrRoleKnowledgeFail
			}
		default:
			return errs.ErrRoleSearchTypeFail
		}
	}
	targetKnowledgeMap := make(map[string]struct{}, len(req.GetKnowChoose()))
	for _, v := range req.GetKnowChoose() {
		if _, ok := targetKnowledgeMap[v.GetKnowledgeBizId()]; ok { // 重复的知识库
			return errs.ErrRoleKnowledgeFail
		}
		targetKnowledgeMap[v.GetKnowledgeBizId()] = struct{}{}
		if err := s.permisLogic.CheckKnowChoose(ctx, cast.ToUint64(req.GetRoleBizId()), v); err != nil {
			log.ErrorContextf(ctx, "CheckKnowChoose err:%v", err)
			return errs.ErrRoleKnowledgeFail
		}
	}

	if isAdd { // 创建角色检查名字是否重复
		exist, err := s.permisLogic.CheckRoleExist(ctx, cast.ToUint64(req.GetAppBizId()), 0, req.GetName())
		if err != nil {
			return errs.ErrGetRoleListFail
		}
		if exist { // 添加的时候名字重复
			return errs.ErrRoleNameExist
		}
	} else { // 修改角色检查是否存在
		if req.GetRoleBizId() == "" || req.GetRoleBizId() == "0" {
			return errs.ErrGetRoleListFail
		}
		exist, err := s.permisLogic.CheckRoleExist(ctx,
			cast.ToUint64(req.GetAppBizId()), cast.ToUint64(req.GetRoleBizId()), "")
		if err != nil {
			return errs.ErrGetRoleListFail
		}
		if !exist {
			return errs.ErrRoleNotExist
		}
	}
	return nil
}

// ModifyRole implements bot_knowledge_config_server.AdminService.
func (s *Service) ModifyKnowledgeRole(ctx context.Context, req *pb.ModifyReq) (*pb.ModifyRsp, error) {
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	ctx = pkg.WithAppID(ctx, app.ID)
	ctx = pkg.WithAppName(ctx, app.Name)
	log.InfoContextf(ctx, "ModifyRole req:%v", req)
	if err := s.checkRoleReq(ctx, req, false); err != nil {
		return nil, err
	}

	if req.GetType() == model.KnowledgeRoleTypePreset { // 编辑默认角色
		id, _, err := s.permisLogic.CheckPresetRole(ctx, cast.ToUint64(req.GetAppBizId()))
		if err != nil {
			log.InfoContextf(ctx, "checkPresetRole err:%v", err)
			return nil, err
		}
		if id != 0 {
			req.RoleBizId = cast.ToString(id)
		}
	}

	roleBizID, syncInfos, err := s.permisLogic.ModifyRole(ctx, req, false)
	if err != nil {
		log.ErrorContextf(ctx, "ModifyRole err:%v", err)
		return nil, errs.ErrModifyQaExpireFail
	}
	s.permisLogic.RoleSyncInfos(ctx, app.BusinessID, roleBizID, syncInfos)
	return &pb.ModifyRsp{}, nil
}

// CheckDeleteRole implements bot_knowledge_config_server.AdminService.
func (s *Service) CheckDeleteRole(ctx context.Context, req *pb.CheckDeleteRoleReq) (*pb.CheckDeleteRoleRsp, error) {
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	ctx = pkg.WithAppID(ctx, app.ID)
	ctx = pkg.WithAppName(ctx, app.Name)
	log.InfoContextf(ctx, "CheckDeleteRole req:%v", req)
	exists, err := s.permisLogic.CheckDeleteRole(ctx,
		cast.ToUint64(req.GetAppBizId()), util.ConvertSliceStringToUint64(req.GetRoleBizIds()),
	)
	if err != nil {
		log.ErrorContextf(ctx, "CheckDeleteRole err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}
	existId2 := make(map[uint64]struct{}, len(exists))
	for _, v := range exists {
		existId2[v.BusinessID] = struct{}{}
	}

	_, all, err := s.permisLogic.ListKnowledgeRoles(ctx, &dao.KnowledgeRoleReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: pkg.CorpBizID(ctx),
			AppBizID:  cast.ToUint64(req.GetAppBizId()),
		},
		BizIDs: util.ConvertSliceStringToUint64(req.GetRoleBizIds()),
	})
	if err != nil {
		log.ErrorContextf(ctx, "ListKnowledgeRoles err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}
	roleId2Name := make(map[uint64]string, len(all))
	preset := uint64(0)
	for _, v := range all {
		roleId2Name[v.BusinessID] = v.Name
		if v.Type == model.KnowledgeRoleTypePreset {
			preset = uint64(v.BusinessID)
		}
	}
	log.InfoContextf(ctx, "req:%v exist:%v roleId2Name:%+v", req.GetRoleBizIds(), exists, roleId2Name)

	res := &pb.CheckDeleteRoleRsp{
		CanDeleteRole:    make([]*pb.RoleBaseInfo, 0, len(req.GetRoleBizIds())),
		CannotDeleteRole: make([]*pb.RoleBaseInfo, 0, len(exists)),
	}
	for _, id := range req.GetRoleBizIds() {
		v := cast.ToUint64(id)
		name, ok := roleId2Name[v]
		if !ok {
			continue
		}
		// 存在不能删除
		if _, ok := existId2[v]; ok || v == preset {
			res.CannotDeleteRole = append(res.CannotDeleteRole, &pb.RoleBaseInfo{
				RoleBizId: v,
				RoleName:  name,
			})
		} else {
			res.CanDeleteRole = append(res.CanDeleteRole, &pb.RoleBaseInfo{
				RoleBizId: v,
				RoleName:  name,
			})
		}
	}
	log.InfoContextf(ctx, "CheckDeleteRoleRsp:%+v", res)
	return res, nil
}

// DeleteRole 删除角色
func (s *Service) DeleteKnowledgeRole(ctx context.Context, req *pb.DeleteRoleReq) (*pb.DeleteRoleRsp, error) {
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	ctx = pkg.WithAppID(ctx, app.ID)
	ctx = pkg.WithAppName(ctx, app.Name)
	log.InfoContextf(ctx, "DeleteRole req:%v", req)
	if len(req.GetRoleBizIds()) == 0 {
		return nil, errs.ErrDeleteRoleFail
	}
	exists, err := s.permisLogic.CheckDeleteRole(ctx,
		cast.ToUint64(req.GetAppBizId()), util.ConvertSliceStringToUint64(req.GetRoleBizIds()),
	)
	if err != nil {
		log.ErrorContextf(ctx, "CheckDeleteRole err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}

	existId2 := make(map[uint64]struct{}, len(exists))
	for _, v := range exists {
		existId2[v.BusinessID] = struct{}{}
	}
	canDelete := make([]uint64, 0, len(req.GetRoleBizIds()))
	for _, id := range req.GetRoleBizIds() {
		v := cast.ToUint64(id)
		if _, ok := existId2[v]; !ok {
			canDelete = append(canDelete, v)
		}
	}
	log.InfoContextf(ctx, "req:%v exist:%v canDelete:%+v", req.GetRoleBizIds(), exists, canDelete)
	if len(canDelete) == 0 {
		return nil, errs.ErrDeleteRoleFail
	}
	err = s.permisLogic.DeleteKnowledgeRole(ctx, cast.ToUint64(req.GetAppBizId()), canDelete)
	if err != nil {
		log.ErrorContextf(ctx, "DeleteKnowledgeRole err:%v", err)
		return nil, errs.ErrDeleteRoleFail
	}
	log.InfoContextf(ctx, "DeleteRoleRsp:%+v", canDelete)
	return &pb.DeleteRoleRsp{
		DeleteRoleBizIds: canDelete,
	}, nil
}

// DetailRole 角色详情
func (s *Service) DescribeKnowledgeRole(ctx context.Context, req *pb.DescribeKnowledgeRoleReq) (*pb.DescribeKnowledgeRoleRsp, error) {
	log.InfoContextf(ctx, "DetailRole req:%v", req)
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}
	if req.GetRoleBizId() == "" || req.GetRoleBizId() == "0" {
		return nil, errs.ErrGetRoleListFail
	}
	ctx = pkg.WithAppID(ctx, app.ID)
	ctx = pkg.WithAppName(ctx, app.Name)
	roles, err := s.permisLogic.DetailRole(ctx, cast.ToUint64(req.GetAppBizId()), []uint64{cast.ToUint64(req.GetRoleBizId())})
	if err != nil {
		log.ErrorContextf(ctx, "DetailRole err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}
	if len(roles) == 0 {
		return nil, errs.ErrGetRoleListFail
	}
	role := roles[0].RoleInfo
	log.InfoContextf(ctx, "DetailRoleRsp:%+v", role)
	return &pb.DescribeKnowledgeRoleRsp{
		RoleInfo: role,
	}, nil
}

// ListRole 查询角色列表
func (s *Service) ListKnowledgeRole(ctx context.Context, req *pb.ListRoleReq) (*pb.ListRoleRsp, error) {
	log.InfoContextf(ctx, "ListRole req:%v", req)
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	appBizId := cast.ToUint64(req.GetAppBizId())
	ctx = pkg.WithAppID(ctx, app.ID)
	ctx = pkg.WithAppName(ctx, app.Name)

	// 检测是否有默认角色
	_, _, err = s.permisLogic.CheckPresetRole(ctx, appBizId)
	if err != nil {
		log.ErrorContextf(ctx, "checkPresetRole err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}

	limit := req.GetPageSize()
	offset := (req.GetPageNumber() - 1) * req.GetPageSize()

	total, roles, err := s.permisLogic.ListKnowledgeRoles(ctx, &dao.KnowledgeRoleReq{
		KnowledgeBase: dao.KnowledgeBase{
			CorpBizID: pkg.CorpBizID(ctx),
			AppBizID:  appBizId,
			Limit:     int(limit),
			Offset:    int(offset),
		},
		SearchWord: req.GetName(),
		BizIDs:     util.ConvertSliceStringToUint64(req.GetRoleBizIds()),
	})
	if err != nil {
		log.ErrorContextf(ctx, "ListKnowledgeRoles err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}
	roleBizIds := make([]uint64, 0, len(roles))
	for _, v := range roles {
		roleBizIds = append(roleBizIds, v.BusinessID)
	}
	res := make([]*pb.ListRoleInfo, 0, req.GetPageSize())
	log.InfoContextf(ctx, "roleBizIds:%+v", roleBizIds)
	if len(roleBizIds) == 0 {
		return &pb.ListRoleRsp{
			RoleList: res,
			Total:    uint64(total),
		}, nil
	}
	roleInfos, err := s.permisLogic.DetailRole(ctx, cast.ToUint64(req.GetAppBizId()), roleBizIds)
	if err != nil {
		log.ErrorContextf(ctx, "DetailRole err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}
	for _, v := range roleInfos {
		roleInfo := v.RoleInfo
		temp := &pb.ListRoleInfo{
			AppBizId:    roleInfo.GetAppBizId(),
			RoleBizId:   roleInfo.RoleBizId,
			Name:        roleInfo.GetName(),
			Type:        roleInfo.GetType(),
			Description: roleInfo.GetDescription(),
			SearchType:  roleInfo.GetSearchType(),
			IsDeleted:   roleInfo.IsDeleted,
			CreateTime:  roleInfo.CreateTime,
			UpdateTime:  roleInfo.UpdateTime,
		}
		knows := make([]*pb.ListRoleInfo_KnowSearch, 0, len(roleInfo.KnowChoose))
		for _, v := range roleInfo.KnowChoose {
			isEmpty := 0
			switch v.SearchType {
			case model.KnowSearchAll:
			case model.KnowSearchSpecial:
				if len(v.QuesAnsBizIds) == 0 &&
					len(v.QuesAnsCateBizIds) == 0 &&
					len(v.DocBizIds) == 0 &&
					len(v.DbBizIds) == 0 &&
					len(v.DocCateBizIds) == 0 {
					isEmpty = 1
				}
			case model.KnowSearchLabel:
				if len(v.GetLabels()) == 0 {
					isEmpty = 1
				}
			}

			know := &pb.ListRoleInfo_KnowSearch{
				KnowledgeBizId: cast.ToUint64(v.GetKnowledgeBizId()),
				Type:           v.GetType(),
				KnowledgeName:  v.GetKnowledgeName(),
				SearchType:     v.GetSearchType(),
				IsEmpty:        uint32(isEmpty),
			}
			knows = append(knows, know)
		}
		temp.KnowChoose = knows
		res = append(res, temp)
	}
	log.InfoContextf(ctx, "ListRoleRsp:%+v", res)
	return &pb.ListRoleRsp{
		RoleList: res,
		Total:    uint64(total),
	}, nil
}

// DescribeRoleSearch 搜寻角色详情
func (s *Service) DescribeRoleSearch(ctx context.Context, req *pb.DescribeRoleSearchReq) (*pb.DescribeRoleSearchRsp, error) {
	app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		log.ErrorContextf(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	// 知识库校验
	if req.GetKnowBizId() == "" || req.GetKnowBizId() == "0" {
		return nil, errs.ErrGetKnowledgeFailed
	}
	ctx = pkg.WithAppID(ctx, app.ID)
	ctx = pkg.WithAppName(ctx, app.Name)
	log.InfoContextf(ctx, "DescribeRoleSearch:%+v", req)
	for _, v := range req.GetRoleSearch() {
		if v.Type == 0 || v.Type > Permis.SearchTypeDatabase || len(v.GetSearchBizIds()) == 0 {
			return nil, errs.ErrParams
		}
	}
	res, err := s.permisLogic.DescribeRoleSearch(ctx, req)
	if err != nil {
		log.ErrorContextf(ctx, "DetailRoleSearch err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}
	log.InfoContextf(ctx, "DetailRoleSearchRsp:%+v", res)
	return res, nil
}
