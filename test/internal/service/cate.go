package service

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	"strings"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/dao"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/util"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/common/v3/errors"
	"git.woa.com/dialogue-platform/common/v3/sync/errgroupx"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// checkCateName 检查分类名
func checkCateName(ctx context.Context, name string) error {
	cfg := config.App().DocQA.QACate
	name = strings.TrimSpace(name)
	if len([]rune(name)) < cfg.MinLength {
		return errs.ErrWrapf(errs.ErrCodeCateNameTooShort, i18n.Translate(ctx, i18nkey.KeyCategoryTitleTooShort), cfg.MinLength)
	}
	if len([]rune(name)) > i18n.CalculateExpandedLength(ctx, i18n.UserInputCharType, cfg.MaxLength) {
		return errs.ErrWrapf(errs.ErrCodeCateNameTooLong, i18n.Translate(ctx, i18nkey.KeyCategoryTitleTooLong), cfg.MaxLength)
	}
	return nil
}

// listCate 分类列表
func (s *Service) listCate(ctx context.Context, t model.CateObjectType, req *pb.ListCateReq) (*model.CateNode,
	map[uint64]uint32, error) {
	corpID := pkg.CorpID(ctx)
	var (
		err   error
		cates []*model.CateInfo
		stat  map[uint64]uint32
	)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, nil, errs.ErrRobotNotFound
	}
	g := errgroupx.Group{}
	g.Go(func() error {
		cates, err = s.dao.GetCateList(ctx, t, corpID, app.ID)
		return err
	})
	g.Go(func() error {
		stat, err = s.dao.GetCateStat(ctx, t, corpID, app.ID)
		return err
	})
	if err = g.Wait(); err != nil {
		return nil, nil, errs.ErrSystem
	}

	tree := model.BuildCateTree(cates)
	return tree, stat, nil
}

// createCate 创建分类
func (s *Service) createCate(ctx context.Context, t model.CateObjectType, req *pb.CreateCateReq) (*pb.CreateCateRsp,
	error) {
	corpID := pkg.CorpID(ctx)
	name := strings.TrimSpace(req.GetName())
	if err := checkCateName(ctx, name); err != nil {
		return nil, err
	}

	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
	}
	cates, err := s.dao.GetCateList(ctx, t, corpID, app.ID)
	if err != nil {
		return nil, errs.ErrSystem
	}
	tree := model.BuildCateTree(cates)
	if tree.NodeCount()-1 > config.App().DocQA.CateNodeLimit {
		return nil, errs.ErrCateCountExceed
	}
	var parentID uint64
	parentBizId, err := util.CheckReqParamsIsUint64(ctx, req.GetParentBizId())
	if err != nil {
		return nil, err
	}
	if parentBizId != 0 {
		parentBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetParentBizId())
		if err != nil {
			return nil, err
		}
		parentCate, err := s.dao.GetCateByBusinessID(ctx, t, parentBizID, corpID, app.ID)
		if err != nil {
			return nil, errs.ErrCateNotFound
		}
		parentID = parentCate.ID
	}
	parent := tree.FindNode(parentID)

	if parent == nil {
		return nil, errs.ErrCateNotFound
	}
	if parent.IsUncategorized(ctx) {
		return nil, errs.ErrInvalidCateID
	}
	if parent.Depth >= uint(model.ExcelTplCateLen) {
		return nil, errs.ErrCateDepthExceed
	}

	if parent.IsNameDuplicate(name) {
		return nil, errs.ErrCateNameDuplicated
	}

	bizID := s.dao.GenerateSeqID()
	cate := &model.CateInfo{
		BusinessID: bizID,
		RobotID:    app.ID,
		CorpID:     corpID,
		Name:       name,
		ParentID:   parentID,
		IsDeleted:  model.CateIsNotDeleted,
	}

	_, err = s.dao.CreateCate(ctx, t, cate)
	if err != nil {
		return nil, errs.ErrSystem
	}
	//设置分类缓存
	if t != model.SynonymsCate {
		go func() {
			defer errors.PanicHandler()
			dao.GetCateDao(t).SetCateCache(trpc.CloneContext(ctx), corpID, app.ID)
			parents := tree.FindNodeWithParent(parentID)
			cateBizIDs := make([]uint64, 0, len(parents))
			for _, parent := range parents {
				cateBizIDs = append(cateBizIDs, parent.BusinessID)
			}
			log.DebugContextf(ctx, "FindNodeWithParent id:%d, cateBizIDs:%+v", parentID, cateBizIDs)
			dao.GetRoleDao(nil).UpdateRoleKnowledgeByCate(trpc.CloneContext(ctx), pkg.CorpBizID(ctx), app.BusinessID, cateBizIDs)
		}()
	}

	rsp := &pb.CreateCateRsp{
		CateBizId: bizID,
		CanAdd:    parent.Depth+1 < uint(model.ExcelTplCateLen),
		CanEdit:   true,
		CanDelete: true,
	}
	// add event log
	event := model.QaCateEventAdd
	if t == model.QACate {
		event = model.QaCateEventAdd
	} else if t == model.DocCate {
		event = model.DocCateEventAdd
	} else if t == model.SynonymsCate {
		event = model.SynonymsCateEventAdd
	}
	_ = s.dao.AddOperationLog(ctx, event, corpID, app.ID, req, rsp, nil, cate)

	return rsp, nil
}

// modifyCate 修改分类
func (s *Service) modifyCate(ctx context.Context, t model.CateObjectType, req *pb.ModifyCateReq) (*pb.ModifyCateRsp,
	error) {
	rsp := new(pb.ModifyCateRsp)
	corpID := pkg.CorpID(ctx)
	name := strings.TrimSpace(req.GetName())
	if err := checkCateName(ctx, name); err != nil {
		return rsp, err
	}

	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	cates, err := s.dao.GetCateList(ctx, t, corpID, app.ID)
	if err != nil {
		return rsp, errs.ErrSystem
	}

	tree := model.BuildCateTree(cates)
	var node *model.CateNode
	var id uint64
	if req.GetId() != "" {
		reqID, err := util.CheckReqParamsIsUint64(ctx, req.GetId())
		if err != nil {
			return nil, err
		}
		node = tree.FindNode(reqID)
	} else {
		cateBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
		if err != nil {
			return nil, err
		}
		cate, err := s.dao.GetCateByBusinessID(ctx, t, cateBizID, corpID, app.ID)
		if cate == nil || err != nil {
			return nil, errs.ErrCateNotFound
		}
		id = cate.ID
		node = tree.FindNode(cate.ID)
	}
	if node == nil {
		return rsp, errs.ErrCateNotFound
	}

	parent := tree.FindNode(node.ParentID)
	if parent == nil {
		return rsp, errs.ErrCateNotFound
	}

	if parent.IsNameDuplicate(name) {
		return rsp, errs.ErrCateNameDuplicated
	}
	if node.IsUncategorized(ctx) {
		return nil, errs.ErrInvalidCateID
	}
	if err = s.dao.UpdateCate(ctx, t, id, name); err != nil {
		return rsp, errs.ErrSystem
	}

	// add log
	event := model.QaCateEventEdit
	if t == model.QACate {
		event = model.QaCateEventEdit
	} else if t == model.DocCate {
		event = model.DocCateEventEdit
	} else if t == model.SynonymsCate {
		event = model.SynonymsCateEventEdit
	}
	_ = s.dao.AddOperationLog(ctx, event, corpID, app.GetAppID(), req, rsp, node.Name, name)

	return rsp, nil
}

// deleteCate 删除分类
func (s *Service) deleteCate(ctx context.Context, t model.CateObjectType, req *pb.DeleteCateReq) (*pb.DeleteCateRsp,
	error) {
	rsp := new(pb.DeleteCateRsp)
	corpID := pkg.CorpID(ctx)
	botBizID, err := util.CheckReqBotBizIDUint64(ctx, req.GetBotBizId())
	if err != nil {
		return nil, err
	}
	app, err := s.getAppByAppBizID(ctx, botBizID)
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	cates, err := s.dao.GetCateList(ctx, t, corpID, app.ID)
	if err != nil {
		return rsp, errs.ErrSystem
	}

	tree := model.BuildCateTree(cates)
	var id uint64
	if req.GetId() == "" {
		cateBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
		if err != nil {
			return nil, err
		}
		cate, err := s.dao.GetCateByBusinessID(ctx, t, cateBizID, corpID, app.ID)
		if err != nil {
			return nil, errs.ErrCateNotFound
		}
		id = cate.ID
	}
	node := tree.FindNode(id)

	if node == nil {
		return nil, errs.ErrCateNotFound
	}
	if node.IsUncategorized(ctx) {
		return nil, errs.ErrInvalidCateID
	}
	ids := append(node.ChildrenIDs(), id)

	uncateID := tree.Find([]string{model.UncategorizedCateName})
	if uncateID == -1 {
		return nil, errs.ErrCateNotFound
	}
	log.DebugContextf(ctx, "DeleteCate ids:%+v, uncateID:%+v", ids, uncateID)
	if err := dao.GetCateDao(t).DeleteCate(ctx, s.dao, t, ids, uint64(uncateID), app); err != nil {
		return nil, errs.ErrSystem
	}
	//重新构建分类缓存
	if t != model.SynonymsCate {
		go func() {
			defer errors.PanicHandler()
			dao.GetCateDao(t).SetCateCache(trpc.CloneContext(ctx), corpID, app.ID)
			//删除分类需要同步删除角色分类绑定关系 异步删除，不要影响原功能 失败也没关系
			dao.GetRoleDao(nil).BatchDeleteRoleCate(trpc.CloneContext(ctx), botBizID, node.BusinessID)
		}()
	}
	// add log
	event := model.QaCateEventDel
	if t == model.QACate {
		event = model.QaCateEventDel
	} else if t == model.DocCate {
		event = model.DocCateEventDel
	} else if t == model.SynonymsCate {
		event = model.SynonymsCateEventDel
	}
	_ = s.dao.AddOperationLog(ctx, event, corpID, app.GetAppID(), req, rsp, node, nil)
	return rsp, nil
}

// ListQACate 获取问答分类列表
func (s *Service) ListQACate(ctx context.Context, req *pb.ListQACateReq) (*pb.ListQACateRsp, error) {
	log.InfoContextf(ctx, "ListQACate Req:%+v", req)
	reqNew := &pb.ListCateReq{
		BotBizId: req.GetBotBizId(),
	}
	tree, stat, err := s.listCate(ctx, model.QACate, reqNew)
	if err != nil {
		return nil, err
	}
	return &pb.ListQACateRsp{List: []*pb.ListQACateRsp_Cate{tree.ToPbQACateTree(ctx, stat)}}, nil
}

// CreateQACate 创建问答分类
func (s *Service) CreateQACate(ctx context.Context, req *pb.CreateQACateReq) (*pb.CreateQACateRsp, error) {
	log.InfoContextf(ctx, "CreateQACate Req:%+v", req)
	reqTmp := &pb.CreateCateReq{
		Name:        req.GetName(),
		ParentBizId: req.GetParentBizId(),
		BotBizId:    req.GetBotBizId(),
	}
	rspTmp, err := s.createCate(ctx, model.QACate, reqTmp)
	if err != nil {
		return nil, err
	}
	rsp := &pb.CreateQACateRsp{
		CateBizId: rspTmp.GetCateBizId(),
		CanAdd:    rspTmp.GetCanAdd(),
		CanEdit:   rspTmp.GetCanEdit(),
		CanDelete: rspTmp.GetCanDelete(),
	}

	return rsp, nil
}

// ModifyQACate 更新问答分类
func (s *Service) ModifyQACate(ctx context.Context, req *pb.ModifyQACateReq) (*pb.ModifyQACateRsp, error) {
	log.InfoContextf(ctx, "ModifyQACate Req:%+v", req)

	reqTmp := &pb.ModifyCateReq{
		Name:      req.GetName(),
		Id:        req.GetId(),
		CateBizId: req.GetCateBizId(),
		BotBizId:  req.GetBotBizId(),
	}
	_, err := s.modifyCate(ctx, model.QACate, reqTmp)
	if err != nil {
		return nil, err
	}
	rsp := &pb.ModifyQACateRsp{}
	return rsp, nil
}

// DeleteQACate 删除QACate
func (s *Service) DeleteQACate(ctx context.Context, req *pb.DeleteQACateReq) (*pb.DeleteQACateRsp, error) {
	log.InfoContextf(ctx, "DeleteQACate Req:%+v", req)

	reqTmp := &pb.DeleteCateReq{
		Id:        req.GetId(),
		CateBizId: req.GetCateBizId(),
		BotBizId:  req.GetBotBizId(),
	}
	_, err := s.deleteCate(ctx, model.QACate, reqTmp)
	if err != nil {
		return nil, err
	}
	rsp := &pb.DeleteQACateRsp{}

	return rsp, nil
}

// ListDocCate 文档分类列表
func (s *Service) ListDocCate(ctx context.Context, req *pb.ListCateReq) (*pb.ListCateRsp, error) {
	log.InfoContextf(ctx, "ListDocCate Req:%+v", req)
	tree, stat, err := s.listCate(ctx, model.DocCate, req)
	if err != nil {
		return nil, err
	}
	return &pb.ListCateRsp{List: []*pb.CateInfo{tree.ToPbCateInfoTree(ctx, stat)}}, nil
}

// CreateDocCate 创建文档分类
func (s *Service) CreateDocCate(ctx context.Context, req *pb.CreateCateReq) (*pb.CreateCateRsp, error) {
	log.InfoContextf(ctx, "CreateDocCate Req:%+v", req)
	return s.createCate(ctx, model.DocCate, req)
}

// ModifyDocCate 修改文档分类
func (s *Service) ModifyDocCate(ctx context.Context, req *pb.ModifyCateReq) (*pb.ModifyCateRsp, error) {
	log.InfoContextf(ctx, "CreateDocCate Req:%+v", req)
	return s.modifyCate(ctx, model.DocCate, req)
}

// DeleteDocCate 删除文档分类
func (s *Service) DeleteDocCate(ctx context.Context, req *pb.DeleteCateReq) (*pb.DeleteCateRsp, error) {
	log.InfoContextf(ctx, "DeleteDocCate Req:%+v", req)
	return s.deleteCate(ctx, model.DocCate, req)
}

// ListSynonymsCate 获取同义词分类列表
func (s *Service) ListSynonymsCate(ctx context.Context, req *pb.ListCateReq) (*pb.ListCateRsp, error) {
	log.InfoContextf(ctx, "ListSynonymsCate Req:%+v", req)
	tree, stat, err := s.listCate(ctx, model.SynonymsCate, req)
	if err != nil {
		return nil, err
	}
	return &pb.ListCateRsp{List: []*pb.CateInfo{tree.ToPbCateInfoTree(ctx, stat)}}, nil
}

// CreateSynonymsCate 创建同义词分类
func (s *Service) CreateSynonymsCate(ctx context.Context, req *pb.CreateCateReq) (*pb.CreateCateRsp, error) {
	log.InfoContextf(ctx, "CreateSynonymsCate Req:%+v", req)
	return s.createCate(ctx, model.SynonymsCate, req)
}

// ModifySynonymsCate 修改同义词分类
func (s *Service) ModifySynonymsCate(ctx context.Context, req *pb.ModifyCateReq) (*pb.ModifyCateRsp, error) {
	log.InfoContextf(ctx, "ModifySynonymsCate Req:%+v", req)
	return s.modifyCate(ctx, model.SynonymsCate, req)
}

// DeleteSynonymsCate 删除同义词分类
func (s *Service) DeleteSynonymsCate(ctx context.Context, req *pb.DeleteCateReq) (*pb.DeleteCateRsp, error) {
	log.InfoContextf(ctx, "DeleteSynonymsCate Req:%+v", req)

	return s.deleteCate(ctx, model.SynonymsCate, req)
}

// getCateChildrenIDs 获取分类下的所有分类ID（主分类和所有子分类）
func (s *Service) getCateChildrenIDs(ctx context.Context, cateType model.CateObjectType,
	corpID, appID, cateID uint64) ([]uint64, error) {
	childrenIDs, err := s.dao.GetCateChildrenIDs(ctx, cateType, corpID, cateID, appID)
	if err != nil {
		return nil, err
	}
	cateAllIDs := append(childrenIDs, cateID)
	return cateAllIDs, nil
}
