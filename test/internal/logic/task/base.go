package task

import (
	"git.woa.com/adp/kb/kb-config/internal/logic/document"
	"git.woa.com/adp/kb/kb-config/internal/logic/qa"
	"git.woa.com/adp/kb/kb-config/internal/logic/segment"
	"git.woa.com/adp/kb/kb-config/internal/rpc"
)

type Logic struct {
	qaLogic  *qa.Logic
	docLogic *document.Logic
	segLogic *segment.Logic
	rpc      *rpc.RPC
}

func NewLogic(qaLogic *qa.Logic, docLogic *document.Logic, segLogic *segment.Logic, rpc *rpc.RPC) *Logic {
	return &Logic{
		qaLogic:  qaLogic,
		docLogic: docLogic,
		segLogic: segLogic,
		rpc:      rpc,
	}
}
