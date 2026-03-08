package label

import (
	"context"

	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
	labelEntity "git.woa.com/adp/kb/kb-config/internal/entity/label"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// CreateAttributeLabelReq 创建属性标签请求（用于创建和导入场景）
type CreateAttributeLabelReq struct {
	RobotID  uint64                      // 机器人ID
	BizID    uint64                      // 应用业务ID（用于白名单校验）
	Uin      string                      // 用户UIN（用于白名单校验）
	AttrKey  string                      // 属性标识
	AttrName string                      // 属性名称
	Labels   []*CreateAttributeLabelItem // 标签列表
}

// CreateAttributeLabelItem 创建属性标签项
type CreateAttributeLabelItem struct {
	LabelName     string   // 标签名称
	SimilarLabels []string // 相似标签
}

// CheckCreateAttributeLabel 检查创建属性标签的参数是否合法
// 供 service 层和导入逻辑复用
func (l *Logic) CheckCreateAttributeLabel(ctx context.Context, req *CreateAttributeLabelReq) error {
	// 1. 校验属性名称的有效性
	if err := l.checkAttrNameValid(ctx, req.AttrName); err != nil {
		return err
	}

	// 2. 检查标签有效性（白名单用户跳过标签数量限制）
	isWhiteList := config.IsInWhiteList(req.Uin, req.BizID, config.GetWhitelistConfig().InfinityAttributeLabel)
	if err := l.checkLabelsValid(ctx, req.Labels, isWhiteList); err != nil {
		return err
	}

	// 3. 检查属性标识和名称是否已存在
	if err := l.CheckAttributeKeyNameExist(ctx, req.RobotID, req.AttrKey, req.AttrName, 0); err != nil {
		return err
	}

	// 4. 检查属性数量限制（白名单用户跳过）
	if !isWhiteList {
		logx.I(ctx, "CheckCreateAttributeLabel robotID:%v", req.RobotID)
		count, err := l.dao.GetAttributeTotal(ctx, req.RobotID, "", nil)
		if err != nil {
			logx.W(ctx, "CheckCreateAttributeLabel GetAttributeTotal failed, err=%v", err)
			return err
		}
		logx.I(ctx, "CheckCreateAttributeLabel count:%v", count)
		if count >= uint64(config.App().AttributeLabel.AttrLimit) {
			return errs.ErrAttributeLabelAttrLimit
		}
	}

	return nil
}

// BuildAttributeLabelItem 构建属性标签项
// 供 service 层和导入逻辑复用
func (l *Logic) BuildAttributeLabelItem(ctx context.Context, req *CreateAttributeLabelReq) (*labelEntity.AttributeLabelItem, error) {
	attr := &labelEntity.Attribute{
		RobotID:       req.RobotID,
		BusinessID:    idgen.GetId(),
		AttrKey:       req.AttrKey,
		Name:          req.AttrName,
		ReleaseStatus: labelEntity.AttributeStatusWaitRelease,
		NextAction:    labelEntity.AttributeNextActionAdd,
		IsDeleted:     false,
		DeletedTime:   0,
	}

	labels := make([]*labelEntity.AttributeLabel, 0, len(req.Labels))
	for _, v := range req.Labels {
		similarLabel, err := parseSimilarLabels(v.SimilarLabels)
		if err != nil {
			logx.D(ctx, "parse similar labels err:%v", err)
			return nil, err
		}
		labels = append(labels, &labelEntity.AttributeLabel{
			RobotID:       req.RobotID,
			Name:          v.LabelName,
			BusinessID:    idgen.GetId(),
			SimilarLabel:  similarLabel,
			ReleaseStatus: labelEntity.AttributeStatusWaitRelease,
			NextAction:    labelEntity.AttributeNextActionAdd,
			IsDeleted:     false,
		})
	}

	return &labelEntity.AttributeLabelItem{Attr: attr, Labels: labels}, nil
}

// BuildCreateAttributeLabelReq 将 pb 请求转换为 logic 层的请求结构
func (l *Logic) BuildCreateAttributeLabelReq(ctx context.Context, appPrimaryID, appBizID uint64, req *pb.CreateAttributeLabelReq) *CreateAttributeLabelReq {
	labels := make([]*CreateAttributeLabelItem, 0, len(req.GetLabels()))
	for _, label := range req.GetLabels() {
		labels = append(labels, &CreateAttributeLabelItem{
			LabelName:     label.GetLabelName(),
			SimilarLabels: label.GetSimilarLabels(),
		})
	}
	return &CreateAttributeLabelReq{
		RobotID:  appPrimaryID,
		BizID:    appBizID,
		Uin:      contextx.Metadata(ctx).Uin(),
		AttrKey:  req.GetAttrKey(),
		AttrName: req.GetAttrName(),
		Labels:   labels,
	}
}
