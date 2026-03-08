package third_document

import (
	"context"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	"git.woa.com/adp/pb-go/common"
	knowledge "git.woa.com/adp/pb-go/kb/kb_config"

	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
)

type ThirdDocLogic interface {
	ListDoc(ctx context.Context, req *knowledge.ListThirdPartyDocReq) (*knowledge.ListThirdPartyDocRsp, error)
	ImportDoc(ctx context.Context, req *knowledge.MigrateThirdPartyDocReq) (*knowledge.MigrateThirdPartyDocRsp, error)
	GetImportProgress(ctx context.Context, req *knowledge.GetMigrateThirdPartyProcessReq) (*knowledge.GetMigrateThirdPartyProcessRsp, error)
	UpdateImportProgress(ctx context.Context, success, fail map[uint64]*model.TThirdDocMigrateProgress) error
	RefreshDoc(ctx context.Context, isAuto bool, docs []*docEntity.Doc) error
}

type Logic struct {
	*OnedriveDocLogic
}

func (l *Logic) GetThirdDocLogic(fromType common.SourceFromType) ThirdDocLogic {
	switch fromType {
	case common.SourceFromType_SOURCE_FROM_TYPE_ONEDRIVE:
		return l.OnedriveDocLogic
	default:
		return &DefaultThirdDocLogic{}
	}
}

func NewLogic(onedriveLogic *OnedriveDocLogic) *Logic {
	return &Logic{
		OnedriveDocLogic: onedriveLogic,
	}
}
