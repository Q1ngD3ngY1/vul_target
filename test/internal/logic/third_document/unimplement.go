package third_document

import (
	"context"

	"git.woa.com/adp/gorm-gen/db_llm_robot/kb-config/model"
	knowledge "git.woa.com/adp/pb-go/kb/kb_config"

	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

type DefaultThirdDocLogic struct {
}

func (d DefaultThirdDocLogic) ListDoc(ctx context.Context, req *knowledge.ListThirdPartyDocReq) (*knowledge.ListThirdPartyDocRsp, error) {
	return nil, errs.ErrParameterInvalid
}

func (d DefaultThirdDocLogic) ImportDoc(ctx context.Context, req *knowledge.MigrateThirdPartyDocReq) (*knowledge.MigrateThirdPartyDocRsp, error) {
	return nil, errs.ErrParameterInvalid
}

func (d DefaultThirdDocLogic) GetImportProgress(ctx context.Context, req *knowledge.GetMigrateThirdPartyProcessReq) (*knowledge.GetMigrateThirdPartyProcessRsp, error) {
	return nil, errs.ErrParameterInvalid
}

func (d DefaultThirdDocLogic) UpdateImportProgress(ctx context.Context, success, fail map[uint64]*model.TThirdDocMigrateProgress) error {
	return errs.ErrParameterInvalid
}

func (d DefaultThirdDocLogic) RefreshDoc(ctx context.Context, isAuto bool, docs []*docEntity.Doc) error {
	return errs.ErrParameterInvalid
}
