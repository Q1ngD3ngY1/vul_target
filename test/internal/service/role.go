package service

import (
	"context"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox/convx"
	"git.woa.com/adp/common/x/gox/slicex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"github.com/spf13/cast"
)

// CreateKnowledgeRole 保存角色
func (s *Service) CreateKnowledgeRole(ctx context.Context, req *pb.CreateRoleReq) (
	rsp *pb.CreateRoleRsp, err error) {
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, cast.ToUint64(req.GetAppBizId()))
	// app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		logx.E(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	md := contextx.Metadata(ctx)
	md.WithAppID(app.PrimaryId)
	md.WithAppName(app.Name)
	logx.I(ctx, "CreateRole req:%v", req)
	modifyReq := &pb.ModifyReq{
		AppBizId:    req.GetAppBizId(),
		Name:        req.GetName(),
		SearchType:  req.GetSearchType(),
		KnowChoose:  req.GetKnowChoose(),
		Description: req.GetDescription(),
		RoleBizId:   "",
		Type:        entity.KnowledgeRoleTypeCustom,
	}
	if err = s.checkRoleReq(ctx, modifyReq, true); err != nil {
		return rsp, err
	}
	totalCnt, _, err := s.userLogic.DescribeKnowledgeRoleList(ctx,
		contextx.Metadata(ctx).CorpBizID(), cast.ToUint64(req.GetAppBizId()),
		&entity.KnowledgeRoleFilter{
			Limit:     -1,
			NeedCount: true,
		})
	if err != nil {
		logx.E(ctx, "CreateKnowledgeRole|DescribeKnowledgeRoleList err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}
	roleMax := config.GetMainConfig().Permissions.RoleMaxLimit
	if totalCnt >= int64(roleMax) {
		logx.E(ctx, "role max limit:%d - %d", totalCnt, roleMax)
		return nil, errs.ErrRoleMaxLimit
	}

	roleBizId, syncInfos, err := s.userLogic.ModifyRole(ctx, &entity.KnowledgeRole{
		AppBizID:    cast.ToUint64(modifyReq.GetAppBizId()),
		Name:        modifyReq.GetName(),
		SearchType:  cast.ToInt8(modifyReq.GetSearchType()),
		Description: modifyReq.GetDescription(),
		Type:        entity.KnowledgeRoleTypeCustom,
	}, knowledgeChoosesPB2DO(modifyReq.KnowChoose), true)
	if err != nil {
		logx.E(ctx, "CreateRole err:%v", err)
		return nil, err
	}
	s.userLogic.RoleSyncInfos(ctx, app.BizId, roleBizId, syncInfos)
	rsp = &pb.CreateRoleRsp{
		RoleBizId: roleBizId,
	}
	logx.I(ctx, "CreateRole rsp:%v", roleBizId)
	return rsp, err
}

func (s *Service) checkRoleReq(ctx context.Context, req *pb.ModifyReq, isAdd bool) error {
	logx.I(ctx, "checkRoleReq req:%v", req)
	nameMin := config.GetMainConfig().Permissions.RoleNameMinLimit
	nameMax := config.GetMainConfig().Permissions.RoleNameMaxLimit
	if len([]rune(req.GetName())) < nameMin || len([]rune(req.GetName())) > nameMax {
		logx.E(ctx, "role name length is not in limit, name:%s, min:%d, max:%d", req.GetName(), nameMin, nameMax)
		return errs.ErrRoleNameFail
	}
	if contextx.Metadata(ctx).CorpBizID() == 0 || req.GetAppBizId() == "" {
		logx.E(ctx, "checkRoleReq corpApp:%s", req.GetAppBizId())
		return errs.ErrCommonFail
	}
	if req.GetSearchType() == 0 || req.GetSearchType() > entity.RoleChooseKnow {
		logx.E(ctx, "checkRoleReq searchType:%d", req.GetSearchType())
		return errs.ErrRoleSearchTypeFail
	}
	if req.GetType() == 0 || req.GetType() > entity.KnowledgeRoleTypeCustom {
		logx.E(ctx, "checkRoleReq type:%d", req.GetType())
		return errs.ErrRoleTypeFail
	}

	for _, v := range req.GetKnowChoose() {
		if v.GetKnowledgeBizId() == "" || v.GetKnowledgeBizId() == "0" { // 知识库业务ID不能为空
			logx.E(ctx, "checkRoleReq GetKnowledgeBizId:%s", v.GetKnowledgeBizId())
			return errs.ErrRoleKnowledgeFail
		}
		if v.Type == 0 || v.Type > entity.KnowPublic {
			logx.E(ctx, "checkRoleReq type:%d", v.Type)
			return errs.ErrRoleKnowledgeTypeFail
		}

		switch v.SearchType {
		case entity.KnowSearchAll: // 全部
			if len(v.DbBizIds) != 0 || len(v.DocBizIds) != 0 || len(v.QuesAnsBizIds) != 0 || len(v.DocCateBizIds) != 0 ||
				len(v.QuesAnsCateBizIds) != 0 || len(v.Labels) != 0 {
				logx.E(ctx, "checkRoleReq SearchType:%d", v.SearchType)
				return errs.ErrRoleKnowledgeFail
			}
			if v.Condition != 0 {
				return errs.ErrRoleConditionFail
			}
		case entity.KnowSearchSpecial: // 按特定知识
			if len(v.DocBizIds) == 0 && len(v.QuesAnsBizIds) == 0 && len(v.DocCateBizIds) == 0 &&
				len(v.QuesAnsCateBizIds) == 0 && len(v.DbBizIds) == 0 {
				logx.E(ctx, "checkRoleReq SearchType:%d %+v", v.SearchType, v)
				return errs.ErrRoleKnowledgeFail
			}
			if v.Condition != 0 {
				return errs.ErrRoleConditionFail
			}
		case entity.KnowSearchLabel: // 按标签
			if len(v.DbBizIds) != 0 || len(v.DocBizIds) != 0 || len(v.QuesAnsBizIds) != 0 || len(v.DocCateBizIds) != 0 ||
				len(v.QuesAnsCateBizIds) != 0 || v.Condition == 0 || v.Condition > entity.ConditionLogicOr || len(v.Labels) == 0 {
				logx.E(ctx, "checkRoleReq SearchType:%d", v.SearchType)
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
		if err := s.userLogic.VerifyKnowChoose(ctx, cast.ToUint64(req.GetRoleBizId()), knowledgeChoosePB2DO(v)); err != nil {
			logx.E(ctx, "CheckKnowChoose err:%v", err)
			return errs.ErrRoleKnowledgeFail
		}
	}

	if isAdd { // 创建角色检查名字是否重复
		exist, err := s.userLogic.VerifyRoleExist(ctx, cast.ToUint64(req.GetAppBizId()), 0, req.GetName())
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
		exist, err := s.userLogic.VerifyRoleExist(ctx,
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

// ModifyKnowledgeRole implements bot_knowledge_config_server.AdminService.
func (s *Service) ModifyKnowledgeRole(ctx context.Context, req *pb.ModifyReq) (*pb.ModifyRsp, error) {
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, cast.ToUint64(req.GetAppBizId()))
	// app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		logx.E(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	md := contextx.Metadata(ctx)
	md.WithAppID(app.PrimaryId)
	md.WithAppName(app.Name)
	logx.I(ctx, "ModifyRole req:%v", req)
	if err := s.checkRoleReq(ctx, req, false); err != nil {
		return nil, err
	}

	role := &entity.KnowledgeRole{
		AppBizID:    cast.ToUint64(req.GetAppBizId()),
		Name:        req.GetName(),
		SearchType:  cast.ToInt8(req.GetSearchType()),
		Description: req.GetDescription(),
		Type:        int8(req.GetType()),
		BusinessID:  cast.ToUint64(req.GetRoleBizId()),
	}
	if req.GetType() == entity.KnowledgeRoleTypePreset { // 编辑默认角色
		id, _, err := s.userLogic.VerifyPresetRole(ctx, cast.ToUint64(req.GetAppBizId()))
		if err != nil {
			logx.I(ctx, "checkPresetRole err:%v", err)
			return nil, err
		}
		if id != 0 {
			role.BusinessID = id
		}
	}

	roleBizID, syncInfos, err := s.userLogic.ModifyRole(ctx, role, knowledgeChoosesPB2DO(req.KnowChoose), false)
	if err != nil {
		logx.E(ctx, "ModifyRole err:%v", err)
		return nil, errs.ErrModifyQaExpireFail
	}
	s.userLogic.RoleSyncInfos(ctx, app.BizId, roleBizID, syncInfos)
	return &pb.ModifyRsp{}, nil
}

// CheckDeleteRole implements bot_knowledge_config_server.AdminService.
// 检查角色是否可删除
func (s *Service) CheckDeleteRole(ctx context.Context, req *pb.CheckDeleteRoleReq) (*pb.CheckDeleteRoleRsp, error) {
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, cast.ToUint64(req.GetAppBizId()))
	// app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		logx.E(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	md := contextx.Metadata(ctx)
	md.WithAppID(app.PrimaryId)
	md.WithAppName(app.Name)
	logx.I(ctx, "CheckDeleteRole req:%v", req)
	exists, err := s.userLogic.VerifyDeleteRole(ctx,
		cast.ToUint64(req.GetAppBizId()), convx.SliceStringToUint64(req.GetRoleBizIds()),
	)
	if err != nil {
		logx.E(ctx, "CheckDeleteRole err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}
	existId2 := make(map[uint64]struct{}, len(exists))
	for _, v := range exists {
		existId2[v.BusinessID] = struct{}{}
	}

	_, all, err := s.userLogic.DescribeKnowledgeRoleList(ctx,
		contextx.Metadata(ctx).CorpBizID(), cast.ToUint64(req.GetAppBizId()),
		&entity.KnowledgeRoleFilter{
			BizIDs: convx.SliceStringToUint64(req.GetRoleBizIds()),
		})
	if err != nil {
		logx.E(ctx, "CheckDeleteRole|DescribeKnowledgeRoleList err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}
	roleId2Name := make(map[uint64]string, len(all))
	preset := uint64(0)
	for _, v := range all {
		roleId2Name[v.BusinessID] = v.Name
		if v.Type == entity.KnowledgeRoleTypePreset {
			preset = uint64(v.BusinessID)
		}
	}
	logx.I(ctx, "req:%v exist:%v roleId2Name:%+v", req.GetRoleBizIds(), exists, roleId2Name)

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
	logx.I(ctx, "CheckDeleteRoleRsp:%+v", res)
	return res, nil
}

// DeleteRole 删除角色
func (s *Service) DeleteKnowledgeRole(ctx context.Context, req *pb.DeleteRoleReq) (*pb.DeleteRoleRsp, error) {
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, cast.ToUint64(req.GetAppBizId()))
	// app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		logx.E(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	md := contextx.Metadata(ctx)
	md.WithAppID(app.PrimaryId)
	md.WithAppName(app.Name)
	logx.I(ctx, "DeleteRole req:%v", req)
	if len(req.GetRoleBizIds()) == 0 {
		return nil, errs.ErrDeleteRoleFail
	}
	exists, err := s.userLogic.VerifyDeleteRole(ctx,
		cast.ToUint64(req.GetAppBizId()), convx.SliceStringToUint64(req.GetRoleBizIds()),
	)
	if err != nil {
		logx.E(ctx, "CheckDeleteRole err:%v", err)
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
	logx.I(ctx, "req:%v exist:%v canDelete:%+v", req.GetRoleBizIds(), exists, canDelete)
	if len(canDelete) == 0 {
		return nil, errs.ErrDeleteRoleFail
	}
	err = s.userLogic.DeleteKnowledgeRole(ctx, cast.ToUint64(req.GetAppBizId()), canDelete)
	if err != nil {
		logx.E(ctx, "DeleteKnowledgeRole err:%v", err)
		return nil, errs.ErrDeleteRoleFail
	}
	logx.I(ctx, "DeleteRoleRsp:%+v", canDelete)
	return &pb.DeleteRoleRsp{
		DeleteRoleBizIds: canDelete,
	}, nil
}

func knowledgeChoosePB2DO(pbChoose *pb.KnowChoose) *entity.KnowledgeChoose {
	choose := &entity.KnowledgeChoose{
		KnowledgeBizId:    pbChoose.KnowledgeBizId,
		KnowledgeName:     pbChoose.KnowledgeName,
		Type:              pbChoose.Type,
		SearchType:        pbChoose.SearchType,
		DocBizIds:         pbChoose.DocBizIds,
		DocCateBizIds:     pbChoose.DocCateBizIds,
		QuesAnsBizIds:     pbChoose.QuesAnsBizIds,
		QuesAnsCateBizIds: pbChoose.QuesAnsCateBizIds,
		DbBizIds:          pbChoose.DbBizIds,
		Condition:         pbChoose.Condition,
	}

	labels := make([]*entity.ChooseLabel, 0, len(pbChoose.Labels))
	for _, label := range pbChoose.Labels {
		chooseLabel := &entity.ChooseLabel{
			AttrBizId: label.AttrBizId,
			AttrName:  label.AttrName,
		}
		chooseLabelLabels := make([]*entity.ChooseLabelLabel, 0, len(label.Labels))
		for _, labelLabel := range label.Labels {
			chooseLabelLabel := &entity.ChooseLabelLabel{
				LabelBizId: labelLabel.LabelBizId,
				LabelName:  labelLabel.LabelName,
			}
			chooseLabelLabels = append(chooseLabelLabels, chooseLabelLabel)
		}
		chooseLabel.Labels = chooseLabelLabels
		labels = append(labels, chooseLabel)
	}
	choose.Labels = labels
	return choose
}

func knowledgeChoosesPB2DO(pbChooses []*pb.KnowChoose) []*entity.KnowledgeChoose {
	return slicex.Map(pbChooses, func(pbChoose *pb.KnowChoose) *entity.KnowledgeChoose {
		return knowledgeChoosePB2DO(pbChoose)
	})
}

func rolesInfoDO2PB(roles []*entity.KnowledgeRole, roleBizID2Chooses map[uint64][]*entity.KnowledgeChoose) []*pb.RoleInfo {
	res := make([]*pb.RoleInfo, 0, len(roles))
	for _, role := range roles {
		roleInfo := &pb.RoleInfo{
			AppBizId:    role.AppBizID,
			RoleBizId:   role.BusinessID,
			Name:        role.Name,
			Type:        int32(role.Type),
			Description: role.Description,
			SearchType:  uint32(role.SearchType),
			IsDeleted:   convx.BoolToInt[uint32](role.IsDeleted),
			CreateTime:  role.CreateTime.Unix(),
			UpdateTime:  role.UpdateTime.Unix(),
		}
		if chooses, ok := roleBizID2Chooses[role.BusinessID]; ok {
			pbChooses := make([]*pb.KnowChoose, 0, len(chooses))
			for _, choose := range chooses {
				pbChoose := &pb.KnowChoose{
					KnowledgeBizId:    choose.KnowledgeBizId,
					KnowledgeName:     choose.KnowledgeName,
					Type:              choose.Type,
					SearchType:        choose.SearchType,
					DocBizIds:         choose.DocBizIds,
					DocCateBizIds:     choose.DocCateBizIds,
					QuesAnsBizIds:     choose.QuesAnsBizIds,
					QuesAnsCateBizIds: choose.QuesAnsCateBizIds,
					DbBizIds:          choose.DbBizIds,
					Condition:         choose.Condition,
				}
				pbLabels := make([]*pb.ChooseLabel, 0, len(choose.Labels))
				for _, label := range choose.Labels {
					pbChooseLabel := &pb.ChooseLabel{
						AttrBizId: label.AttrBizId,
						AttrName:  label.AttrName,
					}
					pbChooseLabelLabels := make([]*pb.ChooseLabel_Label, 0, len(label.Labels))
					for _, labelLabel := range label.Labels {
						pbChooseLabelLabel := &pb.ChooseLabel_Label{
							LabelBizId: labelLabel.LabelBizId,
							LabelName:  labelLabel.LabelName,
						}
						pbChooseLabelLabels = append(pbChooseLabelLabels, pbChooseLabelLabel)
					}
					pbChooseLabel.Labels = pbChooseLabelLabels
					pbLabels = append(pbLabels, pbChooseLabel)
				}
				pbChoose.Labels = pbLabels
				pbChooses = append(pbChooses, pbChoose)
			}
			roleInfo.KnowChoose = pbChooses
		}
		res = append(res, roleInfo)
	}
	return res
}

// DetailRole 角色详情
func (s *Service) DescribeKnowledgeRole(ctx context.Context, req *pb.DescribeKnowledgeRoleReq) (*pb.DescribeKnowledgeRoleRsp, error) {
	logx.I(ctx, "DetailRole req:%v", req)
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, cast.ToUint64(req.GetAppBizId()))
	// app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		logx.E(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}
	if req.GetRoleBizId() == "" || req.GetRoleBizId() == "0" {
		return nil, errs.ErrGetRoleListFail
	}
	md := contextx.Metadata(ctx)
	md.WithAppID(app.PrimaryId)
	md.WithAppName(app.Name)
	roles, roleBizID2Choose, err := s.userLogic.DescribeDetailKnowledgeRole(ctx, cast.ToUint64(req.GetAppBizId()), []uint64{cast.ToUint64(req.GetRoleBizId())})
	if err != nil {
		logx.E(ctx, "DetailRole err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}
	if len(roles) == 0 {
		return nil, errs.ErrGetRoleListFail
	}
	role := rolesInfoDO2PB(roles, roleBizID2Choose)[0]
	logx.I(ctx, "DetailRoleRsp:%+v", role)
	return &pb.DescribeKnowledgeRoleRsp{
		RoleInfo: role,
	}, nil
}

// ListRole 查询角色列表
func (s *Service) ListKnowledgeRole(ctx context.Context, req *pb.ListRoleReq) (*pb.ListRoleRsp, error) {
	logx.I(ctx, "ListRole req:%v", req)
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, cast.ToUint64(req.GetAppBizId()))
	// app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		logx.E(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	appBizId := cast.ToUint64(req.GetAppBizId())
	md := contextx.Metadata(ctx)
	md.WithAppID(app.PrimaryId)
	md.WithAppName(app.Name)

	// 检测是否有默认角色
	_, _, err = s.userLogic.VerifyPresetRole(ctx, appBizId)
	if err != nil {
		logx.E(ctx, "checkPresetRole err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}

	limit := req.GetPageSize()
	offset := (req.GetPageNumber() - 1) * req.GetPageSize()
	total, roles, err := s.userLogic.DescribeKnowledgeRoleList(ctx,
		contextx.Metadata(ctx).CorpBizID(), appBizId,
		&entity.KnowledgeRoleFilter{
			Limit:      int(limit),
			Offset:     int(offset),
			SearchWord: req.GetName(),
			BizIDs:     convx.SliceStringToUint64(req.GetRoleBizIds()),
			NeedCount:  true,
		})
	if err != nil {
		logx.E(ctx, "DescribeKnowledgeRoleList err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}
	roleBizIds := make([]uint64, 0, len(roles))
	for _, v := range roles {
		roleBizIds = append(roleBizIds, v.BusinessID)
	}
	res := make([]*pb.ListRoleInfo, 0, req.GetPageSize())
	logx.I(ctx, "roleBizIds:%+v", roleBizIds)
	if len(roleBizIds) == 0 {
		return &pb.ListRoleRsp{
			RoleList: res,
			Total:    uint64(total),
		}, nil
	}
	roles, roleBizID2Choose, err := s.userLogic.DescribeDetailKnowledgeRole(ctx, cast.ToUint64(req.GetAppBizId()), roleBizIds)
	if err != nil {
		logx.E(ctx, "DetailRole err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}
	roleInfos := rolesInfoDO2PB(roles, roleBizID2Choose)
	for _, roleInfo := range roleInfos {
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
			case entity.KnowSearchAll:
			case entity.KnowSearchSpecial:
				if len(v.QuesAnsBizIds) == 0 &&
					len(v.QuesAnsCateBizIds) == 0 &&
					len(v.DocBizIds) == 0 &&
					len(v.DbBizIds) == 0 &&
					len(v.DocCateBizIds) == 0 {
					isEmpty = 1
				}
			case entity.KnowSearchLabel:
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
	logx.I(ctx, "ListRoleRsp:%+v", res)
	return &pb.ListRoleRsp{
		RoleList: res,
		Total:    uint64(total),
	}, nil
}

func roleSearchInfoListDO2PB(roleSearch []*entity.RoleSearchInfo) []*pb.DescribeRoleSearchRsp_SearchInfo {
	res := make([]*pb.DescribeRoleSearchRsp_SearchInfo, 0, len(roleSearch))
	for _, v := range roleSearch {
		res = append(res, &pb.DescribeRoleSearchRsp_SearchInfo{
			Type:        v.Type,
			Name:        v.Name,
			SearchBizId: v.SearchBizId,
			CateBizId:   v.CateBizId,
		})
	}
	return res
}

// DescribeRoleSearch 获取角色搜索详情
func (s *Service) DescribeRoleSearch(ctx context.Context, req *pb.DescribeRoleSearchReq) (*pb.DescribeRoleSearchRsp, error) {
	appid := cast.ToUint64(req.GetAppBizId())
	app, err := s.rpc.AppAdmin.DescribeAppById(ctx, appid)
	// app, err := s.dao.GetAppByAppBizID(ctx, cast.ToUint64(req.GetAppBizId()))
	if err != nil || app == nil {
		logx.E(ctx, "GetAppByAppBizID err:%v", err)
		return nil, errs.ErrGetAppFail
	}
	// 知识库校验
	if req.GetKnowBizId() == "" || req.GetKnowBizId() == "0" {
		return nil, errs.ErrGetKnowledgeFailed
	}
	md := contextx.Metadata(ctx)
	md.WithAppID(app.PrimaryId)
	md.WithAppName(app.Name)
	logx.I(ctx, "DescribeRoleSearch:%+v", req)
	for _, v := range req.GetRoleSearch() {
		if v.Type == 0 || v.Type > entity.SearchTypeDatabase || len(v.GetSearchBizIds()) == 0 {
			return nil, errs.ErrParams
		}
	}
	type2SearchBizIds := make(map[uint32][]string)
	for _, search := range req.GetRoleSearch() {
		type2SearchBizIds[search.GetType()] = search.GetSearchBizIds()
	}
	res, err := s.userLogic.DescribeRoleSearch(ctx, appid, cast.ToUint64(req.GetKnowBizId()), type2SearchBizIds)
	if err != nil {
		logx.E(ctx, "DetailRoleSearch err:%v", err)
		return nil, errs.ErrGetRoleListFail
	}
	logx.I(ctx, "DetailRoleSearchRsp:%+v", res)
	return &pb.DescribeRoleSearchRsp{
		RoleSearch: roleSearchInfoListDO2PB(res),
	}, nil
}
