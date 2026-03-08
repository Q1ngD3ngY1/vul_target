package common

import (
	"context"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
)

// ConvertErrMsg 转换错误信息
func ConvertErrMsg(ctx context.Context, rpc *rpc.RPC, sID uint64, corpID uint64, oldErr error) error {
	if sID == 0 && corpID == 0 {
		return oldErr
	}
	if sID == 0 && corpID != 0 {
		corp, err := rpc.DescribeCorpByPrimaryId(ctx, corpID)
		// corp, err := d.GetCorpByID(ctx, corpID)
		if err != nil {
			logx.E(ctx, "GetCorpByID corpID:%d err:%+v", corpID, err)
			return oldErr
		}
		sID = corp.GetSid()
	}
	systemIntegrator, err := rpc.DescribeIntegratorById(ctx, sID)
	// systemIntegrator, err := l.rawSqlDao.GetSystemIntegratorByID(ctx, sID)
	if err != nil {
		logx.E(ctx, "GetSystemIntegratorByID sID:%d err:%+v", sID, err)
		return oldErr
	}
	newErr := errs.ConvertErrMsg(systemIntegrator.Name, oldErr)
	return newErr
}
