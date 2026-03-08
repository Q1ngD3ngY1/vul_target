package logic

import (
	"github.com/google/wire"

	"git.woa.com/adp/kb/kb-config/internal/dao"
	"git.woa.com/adp/kb/kb-config/internal/logic/app"
	"git.woa.com/adp/kb/kb-config/internal/logic/audit"
	"git.woa.com/adp/kb/kb-config/internal/logic/category"
	"git.woa.com/adp/kb/kb-config/internal/logic/database"
	"git.woa.com/adp/kb/kb-config/internal/logic/document"
	"git.woa.com/adp/kb/kb-config/internal/logic/export"
	"git.woa.com/adp/kb/kb-config/internal/logic/finance"
	"git.woa.com/adp/kb/kb-config/internal/logic/kb"
	"git.woa.com/adp/kb/kb-config/internal/logic/kb_package"
	"git.woa.com/adp/kb/kb-config/internal/logic/label"
	"git.woa.com/adp/kb/kb-config/internal/logic/llm"
	"git.woa.com/adp/kb/kb-config/internal/logic/localcache"
	"git.woa.com/adp/kb/kb-config/internal/logic/qa"
	"git.woa.com/adp/kb/kb-config/internal/logic/release"
	"git.woa.com/adp/kb/kb-config/internal/logic/search"
	"git.woa.com/adp/kb/kb-config/internal/logic/segment"
	"git.woa.com/adp/kb/kb-config/internal/logic/task"
	"git.woa.com/adp/kb/kb-config/internal/logic/third_document"
	"git.woa.com/adp/kb/kb-config/internal/logic/user"
	"git.woa.com/adp/kb/kb-config/internal/logic/vector"
)

var ProviderSet = wire.NewSet(
	dao.ProviderSet,
	audit.NewLogic,
	localcache.NewLogic,
	document.NewLogic,
	segment.NewLogic,
	vector.NewLogic,
	qa.NewLogic,
	kb.NewLogic,
	label.NewLogic,
	kb_package.NewLogic,
	app.NewLogic,
	category.NewLogic,
	user.NewLogic,
	database.NewLogic,
	release.NewLogic,
	search.NewLogic,
	finance.NewLogic,
	export.NewLogic,
	task.NewLogic,
	llm.NewLogic,
	ThirdDocLogicSet,
)

var ThirdDocLogicSet = wire.NewSet(
	third_document.NewOnedriveDocLogic,
	third_document.NewLogic,
)
