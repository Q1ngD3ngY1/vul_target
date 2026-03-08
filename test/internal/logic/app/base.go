package app

import "git.woa.com/adp/kb/kb-config/internal/rpc"

type Logic struct {
	r *rpc.RPC
}

func NewLogic(rpc *rpc.RPC) *Logic {
	return &Logic{
		r: rpc,
	}
}
