package rpc

import (
	"context"
	"fmt"

	"git.woa.com/adp/common/x/errx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/gox/ptrx"
	"git.woa.com/adp/common/x/gox/timex"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/pb-go/common"
	pb "git.woa.com/adp/pb-go/platform/platform_manager"
	"github.com/spf13/cast"

	"git.woa.com/adp/kb/kb-config/internal/entity"
	kbEntity "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	releaseEntity "git.woa.com/adp/kb/kb-config/internal/entity/release"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

type PlatformAdminRPC interface {
	// 企业
	DescribeCorp(ctx context.Context, req *pb.DescribeCorpReq) (*pb.DescribeCorpRsp, error)
	DescribeCorpByBizId(context.Context, uint64) (*pb.DescribeCorpRsp, error)
	DescribeCorpByPrimaryId(context.Context, uint64) (*pb.DescribeCorpRsp, error)
	DescribeCorpList(context.Context, *pb.DescribeCorpListReq) (*pb.DescribeCorpListRsp, error)

	// 集成商
	DescribeIntegrator(ctx context.Context, uin, subAccountUin string) (*entity.SystemIntegrator, error)
	DescribeIntegratorById(ctx context.Context, sid uint64) (*entity.SystemIntegrator, error)
	IsSystemIntegrator(ctx context.Context, sid uint64) bool

	// user
	DescribeStaff(ctx context.Context, uin, subUin string) (*pb.DescribeStaffRsp, error)

	// corp staff
	DescribeCorpStaff(ctx context.Context, req *pb.DescribeCorpStaffReq) (*entity.CorpStaff, error)
	DescribeCorpStaffList(ctx context.Context, req *pb.DescribeCorpStaffListReq) ([]*entity.CorpStaff, uint64, error)
	// 从缓存获取员工信息
	DescribeStaffList(ctx context.Context, req *pb.DescribeStaffListReq) (map[uint64]*pb.StaffInfo, error)

	ModifyCorpStaff(ctx context.Context, req *pb.ModifyCorpStaffReq) error
	GetStaffByID(ctx context.Context, id uint64) (*entity.CorpStaff, error)

	// 通知
	CreateNotice(ctx context.Context, notice *releaseEntity.Notice) error

	DescribeResourceList(ctx context.Context, spaceID string, resourceType common.ResourceType) (*pb.DescribeResourceListRsp, error)
	CreateOpLog(ctx context.Context, req *pb.CreateOpLogReq) error

	// 第三方文档授权管理
	GetThirdDocPlatformAuthToken(ctx context.Context, uin, subUin string, sourceFrom common.SourceFromType) (*pb.GetAuthTokenRsp, error)
}

type PlatformLoginRPC interface {
	CheckSession(context.Context) (*pb.CheckSessionRsp, error)
	CheckPermission(context.Context, *pb.CheckPermissionReq) (*pb.CheckPermissionRsp, error)
}

type PlatformAPIRPC interface {
	ModifyCorpKnowledgeOverCapacity(ctx context.Context, corpBizID uint64, overCapacity entity.CapacityUsage) error
	ResetCorpKnowledgeOverCapacity(ctx context.Context, corpBizID uint64, overCapacity entity.CapacityUsage) error
}

// DescribeCorp 获取企业信息
func (r *RPC) DescribeCorp(ctx context.Context, req *pb.DescribeCorpReq) (*pb.DescribeCorpRsp, error) {
	return r.platformAdmin.DescribeCorp(ctx, req)
}

// DescribeCorpByBizId 通过企业ID获取企业信息
func (r *RPC) DescribeCorpByBizId(ctx context.Context, bizId uint64) (*pb.DescribeCorpRsp, error) {
	req := &pb.DescribeCorpReq{Id: bizId}
	return r.platformAdmin.DescribeCorp(ctx, req)
}

// DescribeCorpByPrimaryId 通过企业主键ID获取企业信息
func (r *RPC) DescribeCorpByPrimaryId(ctx context.Context, primaryId uint64) (*pb.DescribeCorpRsp, error) {
	req := &pb.DescribeCorpReq{PrimaryId: primaryId}
	return r.platformAdmin.DescribeCorp(ctx, req)
}

// DescribeCorpList 获取企业列表
func (r *RPC) DescribeCorpList(ctx context.Context, req *pb.DescribeCorpListReq) (*pb.DescribeCorpListRsp, error) {
	return r.platformAdmin.DescribeCorpList(ctx, req)
}

// DescribeIntegrator 获取集成商信息
func (r *RPC) DescribeIntegrator(ctx context.Context, uin, subAccountUin string) (*entity.SystemIntegrator, error) {
	req := pb.DescribeIntegratorReq{Uin: uin, SubAccountUin: subAccountUin, Status: ptrx.Uint32(entity.SystemIntegratorValid)}
	var si *entity.SystemIntegrator
	// 先按账号查集成商，查不到再按云市场ID查
	rsp, err := r.platformAdmin.DescribeIntegrator(ctx, &req)
	if err == nil {
		si = systemIntegratorPB2DO(rsp)
	} else if errx.Is(err, errx.ErrNotFound) {
		si, err = r.DescribeIntegratorById(ctx, entity.CloudSID)
	}
	if errx.Is(err, errx.ErrNotFound) {
		return nil, errs.ErrSystemIntegratorNotFound
	}
	if err != nil {
		logx.W(ctx, "DescribeIntegrator req:%s, err: %v", &req, err)
		return nil, err
	}
	return si, err
}

func systemIntegratorPB2DO(rsp *pb.DescribeIntegratorRsp) *entity.SystemIntegrator {
	if rsp == nil {
		return nil
	}
	return &entity.SystemIntegrator{
		ID:               rsp.GetId(),
		Name:             rsp.GetName(),
		Status:           rsp.GetStatus(),
		Uin:              rsp.GetUin(),
		SubAccountUin:    rsp.GetSubAccountUin(),
		IsSelfPermission: rsp.GetIsSelfPermission(),
		AllowAction:      rsp.GetAllowAction(),
		DenyAction:       rsp.GetDenyAction(),
		CorpAppQuota:     rsp.GetCorpAppQuota(),
		UpdateTime:       timex.Unix(rsp.GetUpdateTime()),
		CreateTime:       timex.Unix(rsp.GetCreateTime()),
	}
}

// DescribeIntegratorById 通过集成商ID获取集成商信息
func (r *RPC) DescribeIntegratorById(ctx context.Context, sid uint64) (*entity.SystemIntegrator, error) {
	req := &pb.DescribeIntegratorReq{Sid: uint32(sid)}
	rsp, err := r.platformAdmin.DescribeIntegrator(ctx, req)
	if errx.Is(err, errx.ErrNotFound) {
		return nil, errs.ErrSystemIntegratorNotFound
	}
	if err != nil {
		logx.W(ctx, "DescribeIntegratorById req:%s, err: %v", req, err)
		return nil, err
	}
	return systemIntegratorPB2DO(rsp), nil
}

// IsSystemIntegrator 是否是集成商
func (r *RPC) IsSystemIntegrator(ctx context.Context, sid uint64) bool {
	si, err := r.DescribeIntegratorById(ctx, sid)
	if err != nil {
		return false
	}
	return !si.IsCloudSI()
}

func (r *RPC) DescribeCorpStaff(ctx context.Context, req *pb.DescribeCorpStaffReq) (*entity.CorpStaff, error) {
	rsp, err := r.platformAdmin.DescribeCorpStaff(ctx, req)
	if err != nil {
		return nil, err
	}
	return corpStaffPB2DO(rsp), nil
}

func corpStaffPB2DO(rsp *pb.DescribeCorpStaffRsp) *entity.CorpStaff {
	if rsp == nil {
		return nil
	}
	return &entity.CorpStaff{
		ID:         rsp.GetStaffPrimaryId(),
		BusinessID: rsp.GetStaffId(),
		CorpID:     rsp.GetCorpPrimaryId(),
		UserID:     rsp.GetUserId(),
		NickName:   rsp.GetNickName(),
		Avatar:     rsp.GetAvatar(),
		Cellphone:  rsp.GetCellphone(),
		Status:     int8(rsp.GetStatus()),
		IsGenQA:    rsp.GetIsGenQa(),
		JoinTime:   timex.Unix(rsp.GetJoinTime()),
		UpdateTime: timex.Unix(rsp.GetUpdateTime()),
		CreateTime: timex.Unix(rsp.GetCreateTime()),
	}
}

func (r *RPC) DescribeCorpStaffList(ctx context.Context, req *pb.DescribeCorpStaffListReq) ([]*entity.CorpStaff, uint64, error) {
	rsp, err := r.platformAdmin.DescribeCorpStaffList(ctx, req)
	if err != nil {
		return nil, 0, err
	}
	return corpStaffsPB2DO(rsp.GetList()), rsp.GetTotal(), nil
}

func corpStaffsPB2DO(rsp []*pb.CorpStaff) []*entity.CorpStaff {
	if rsp == nil {
		return nil
	}
	staffs := make([]*entity.CorpStaff, 0, len(rsp))
	for _, staff := range rsp {
		staffs = append(staffs, &entity.CorpStaff{
			ID:         staff.GetStaffPrimaryId(),
			BusinessID: staff.GetStaffId(),
			CorpID:     staff.GetCorpPrimaryId(),
			UserID:     staff.GetUserId(),
			NickName:   staff.GetNickName(),
			Avatar:     staff.GetAvatar(),
			Cellphone:  staff.GetCellphone(),
			Status:     int8(staff.GetStatus()),
			IsGenQA:    staff.GetIsGenQa(),
			JoinTime:   timex.Unix(staff.GetJoinTime()),
			UpdateTime: timex.Unix(staff.GetUpdateTime()),
			CreateTime: timex.Unix(staff.GetCreateTime()),
		})
	}
	return staffs
}

func (r *RPC) DescribeStaffList(ctx context.Context, req *pb.DescribeStaffListReq) (map[uint64]*pb.StaffInfo, error) {
	rsp, err := r.platformAdmin.DescribeStaffList(ctx, req)
	if err != nil {
		return nil, err
	}
	staffMap := make(map[uint64]*pb.StaffInfo, len(rsp.GetList()))
	for _, v := range rsp.GetList() {
		staffMap[v.StaffId] = v
	}
	return staffMap, nil
}

func (r *RPC) ModifyCorpStaff(ctx context.Context, req *pb.ModifyCorpStaffReq) error {
	_, err := r.platformAdmin.ModifyCorpStaff(ctx, req)
	if err != nil {
		return fmt.Errorf("ModifyCorpStaff err: %w", err)
	}
	return nil
}

func noticeDO2CreateNoticeReqPB(notice *releaseEntity.Notice) *pb.CreateNoticeReq {
	res := &pb.CreateNoticeReq{
		AppId:        notice.RobotID,
		PageId:       notice.PageID,
		Type:         notice.Type,
		Level:        notice.Level,
		RelateId:     notice.RelateID,
		Subject:      notice.Subject,
		Content:      notice.Content,
		IsGlobal:     gox.IfElse(notice.IsGlobal > 0, true, false),
		IsAllowClose: gox.IfElse(notice.IsAllowClose > 0, true, false),
		CorpId:       notice.CorpID,
		StaffId:      notice.StaffID,
	}
	for _, operation := range notice.Operations {
		res.Operations = append(res.Operations, &pb.NoticeOp{
			Type: operation.Type,
			Params: &pb.NoticeParams{
				CosPath:       operation.Params.CosPath,
				VersionId:     operation.Params.VersionID,
				AppealType:    operation.Params.AppealType,
				DocBizId:      cast.ToUint64(operation.Params.DocBizID),
				FeedbackBizId: operation.Params.FeedbackBizID,
				ExtraJsonData: operation.Params.ExtraJSONData,
				QaBizId:       operation.Params.QaBizID,
			},
		})
	}
	return res
}

func (r *RPC) CreateNotice(ctx context.Context, notice *releaseEntity.Notice) error {
	req := noticeDO2CreateNoticeReqPB(notice)
	_, err := r.platformAdmin.CreateNotice(ctx, req)
	if err != nil {
		return fmt.Errorf("CreateNotice err: %w", err)
	}
	return nil
}

// DescribeStaff 查询用户 t_user 表
func (r *RPC) DescribeStaff(ctx context.Context, uin, subUin string) (*pb.DescribeStaffRsp, error) {
	logx.I(ctx, "DescribeStaff, uin: %s, subUin: %s", uin, subUin)

	req := &pb.DescribeStaffReq{
		Uin:    uin,
		SubUin: subUin,
	}
	rsp, err := r.platformAdmin.DescribeStaff(ctx, req)
	if err != nil {
		logx.E(ctx, "DescribeStaff request: %+v, error: %+v", req, err)
		return nil, err
	}

	return rsp, nil
}

func (r *RPC) DescribeResourceList(ctx context.Context, spaceID string, resourceType common.ResourceType) (*pb.DescribeResourceListRsp, error) {
	uin, subAccountUin := kbEntity.GetLoginUinAndSubAccountUin(ctx)
	logx.I(ctx, "DescribeResourceList spaceID:%s, resourceType:%d, uin:%s, subAccountUin:%s", spaceID, resourceType, uin, subAccountUin)

	req := pb.DescribeResourceListReq{
		Uin:          uin,
		SubUin:       subAccountUin,
		SpaceId:      spaceID,
		ResourceType: resourceType,
	}
	rsp, err := r.platformAdmin.DescribeResourceList(ctx, &req)
	if err != nil {
		return nil, fmt.Errorf("DescribeResourceList spaceID:%s,resourceType:%d, error:%w", spaceID, resourceType, err)
	}
	return rsp, nil
}

func (r *RPC) CheckSession(ctx context.Context) (*pb.CheckSessionRsp, error) {
	req := &pb.CheckSessionReq{}
	rsp, err := r.platformLogin.CheckSession(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("CheckSession error: %w", err)
	}
	return rsp, nil
}

func (r *RPC) CheckPermission(ctx context.Context, req *pb.CheckPermissionReq) (*pb.CheckPermissionRsp, error) {
	rsp, err := r.platformLogin.CheckPermission(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("CheckPermission error: %w", err)
	}
	return rsp, nil
}

func (r *RPC) CreateOpLog(ctx context.Context, req *pb.CreateOpLogReq) error {
	_, err := r.platformAdmin.CreateOpLog(ctx, req)
	if err != nil {
		return fmt.Errorf("CreateOpLog error: %w", err)
	}
	return nil
}

// GetStaffByID 获取企业员工
func (r *RPC) GetStaffByID(ctx context.Context, id uint64) (*entity.CorpStaff, error) {
	req := pb.DescribeCorpStaffReq{StaffPrimaryId: id}
	rsp, err := r.DescribeCorpStaff(ctx, &req)
	if err != nil {
		if errx.Is(err, errx.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetStaffByID error: %w", err)
	}
	return rsp, nil
}

func (r *RPC) GetThirdDocPlatformAuthToken(ctx context.Context, uin, subUin string, sourceFrom common.SourceFromType) (*pb.GetAuthTokenRsp, error) {
	req := &pb.GetAuthTokenReq{
		Uin:        uin,
		SubUin:     subUin,
		SourceFrom: sourceFrom,
	}
	rsp, err := r.platformApi.GetAuthToken(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("GetThirdDocPlatformAuthToken error: %w", err)
	}
	return rsp, nil
}

// ModifyCorpKnowledgeOverCapacity 上报企业知识库超量信息
func (r *RPC) ModifyCorpKnowledgeOverCapacity(ctx context.Context, corpBizID uint64, overCapacity entity.CapacityUsage) error {
	req := &pb.ModifyCorpKnowledgeOverCapacityReq{
		CorpId: corpBizID,
	}
	if overCapacity.KnowledgeCapacity > 0 {
		req.OverKnowledgeCapacity = ptrx.Uint64(uint64(overCapacity.KnowledgeCapacity))
	}
	if overCapacity.StorageCapacity > 0 {
		req.OverStorageCapacity = ptrx.Uint64(uint64(overCapacity.StorageCapacity))
	}
	if overCapacity.ComputeCapacity > 0 {
		req.OverComputeCapacity = ptrx.Uint64(uint64(overCapacity.ComputeCapacity))
	}
	_, err := r.platformApi.ModifyCorpKnowledgeOverCapacity(ctx, req)
	if err != nil {
		return fmt.Errorf("ModifyCorpKnowledgeOverCapacity error: %w", err)
	}
	return nil
}

// ResetCorpKnowledgeOverCapacity 重置企业知识库超量信息(可以传0值)
func (r *RPC) ResetCorpKnowledgeOverCapacity(ctx context.Context, corpBizID uint64, resetCapacity entity.CapacityUsage) error {
	req := &pb.ModifyCorpKnowledgeOverCapacityReq{
		CorpId:                corpBizID,
		OverKnowledgeCapacity: ptrx.Uint64(uint64(resetCapacity.KnowledgeCapacity)),
		OverStorageCapacity:   ptrx.Uint64(uint64(resetCapacity.StorageCapacity)),
		OverComputeCapacity:   ptrx.Uint64(uint64(resetCapacity.ComputeCapacity)),
	}
	_, err := r.platformApi.ModifyCorpKnowledgeOverCapacity(ctx, req)
	if err != nil {
		return fmt.Errorf("ModifyCorpKnowledgeOverCapacity error: %w", err)
	}
	return nil
}
