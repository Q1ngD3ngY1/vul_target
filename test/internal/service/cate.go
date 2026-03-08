package service

import (
	"context"
	"strings"

	"git.woa.com/adp/common/x/logx/auditx"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.woa.com/adp/common/x/contextx"
	"git.woa.com/adp/common/x/gox"
	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/common/x/syncx/errgroupx"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity/category"
	docEntity "git.woa.com/adp/kb/kb-config/internal/entity/document"
	kbe "git.woa.com/adp/kb/kb-config/internal/entity/kb"
	"git.woa.com/adp/kb/kb-config/internal/util"
	"git.woa.com/adp/kb/kb-config/internal/util/idgen"
	"git.woa.com/adp/kb/kb-config/pkg/errs"
	"git.woa.com/adp/kb/kb-config/pkg/i18nkey"
	appConfig "git.woa.com/adp/pb-go/app/app_config"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
	"github.com/spf13/cast"
)

func (s *Service) ListCate(ctx context.Context, req *pb.ListCateReq) (*pb.ListCateRsp, error) {
	logx.I(ctx, "ListCate req:%s", req)
	tree, stat, _, err := s.listCate(ctx, category.CateObjectType(req.GetType()), req)
	if err != nil {
		return nil, err
	}
	return &pb.ListCateRsp{List: []*pb.CateInfo{tree.ToPbCateInfoTree(ctx, stat)}}, nil
}

func (s *Service) CreateCate(ctx context.Context, req *pb.CreateCateReq) (*pb.CreateCateRsp, error) {
	logx.I(ctx, "CreateCate req:%s", req)
	return s.createCate(ctx, category.CateObjectType(req.GetType()), req)
}

func (s *Service) ModifyCate(ctx context.Context, req *pb.ModifyCateReq) (*pb.ModifyCateRsp, error) {
	logx.I(ctx, "ModifyCate req:%s", req)
	return s.modifyCate(ctx, category.CateObjectType(req.GetType()), req)
}

func (s *Service) DeleteCate(ctx context.Context, req *pb.DeleteCateReq) (*pb.DeleteCateRsp, error) {
	logx.I(ctx, "DeleteCate req:%s", req)
	return s.deleteCate(ctx, category.CateObjectType(req.GetType()), req)
}

// checkCateName 检查分类名
func checkCateName(ctx context.Context, name string) error {
	cfg := config.App().DocQA.QACate
	name = strings.TrimSpace(name)
	if len([]rune(name)) < cfg.MinLength {
		return errs.ErrWrapf(errs.ErrCodeCateNameTooShort, i18n.Translate(ctx, i18nkey.KeyCategoryTitleTooShort), cfg.MinLength)
	}
	if len([]rune(name)) > cfg.MaxLength {
		return errs.ErrWrapf(errs.ErrCodeCateNameTooLong, i18n.Translate(ctx, i18nkey.KeyCategoryTitleTooLong), cfg.MaxLength)
	}
	return nil
}

// listCate 分类列表
func (s *Service) listCate(ctx context.Context, t category.CateObjectType, req *pb.ListCateReq) (*category.CateNode,
	map[uint64]uint32, map[uint64]struct{}, error) {
	var (
		err                     error
		allCates                []*category.CateInfo
		childrenCates           []*category.CateInfo
		rootTree                *category.CateNode
		catePrimaryIdNode2Count map[uint64]uint32 // 分类ID对应的节点数
	)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return nil, nil, nil, errs.ErrRobotNotFound
	}
	g := errgroupx.New()
	if req.GetQueryType() == pb.CateQueryType_PAGE && req.GetParentCateBizId() != "" {
		g.Go(func() error {
			childrenCates, err = s.cateLogic.DescribeCateListByParent(ctx, t, app.CorpPrimaryId, app.PrimaryId,
				cast.ToUint64(req.GetParentCateBizId()), int(req.GetPageNumber()), int(req.GetPageSize()))
			return err
		})
	}
	g.Go(func() error {
		allCates, err = s.cateLogic.DescribeCateList(ctx, t, app.CorpPrimaryId, app.PrimaryId)
		rootTree = category.BuildCateTree(allCates)
		return err
	})
	g.Go(func() error {
		catePrimaryIdNode2Count, err = s.cateLogic.DescribeCateStat(ctx, t, app.CorpPrimaryId, app.PrimaryId)
		return err
	})
	if err = g.Wait(); err != nil {
		return nil, nil, nil, errs.ErrSystem
	}
	catePrimaryIdTree2Count := make(map[uint64]uint32) // 分类ID子树对应的节点数
	catePrimaryIdTree2Depth := make(map[uint64]uint)   // 分类ID子树对应的深度
	leafBizIdMap := make(map[uint64]struct{})          //分类BizID为叶子节点的map
	// 递归计算并填充 catePrimaryIdTree2Count
	var dfs func(node *category.CateNode) uint32
	dfs = func(node *category.CateNode) uint32 {
		if node == nil {
			return 0
		}
		sum := catePrimaryIdNode2Count[node.ID] // 包含自己的权重
		for _, child := range node.Children {
			sum += dfs(child)
		}
		if len(node.Children) == 0 {
			leafBizIdMap[node.BusinessID] = struct{}{}
		}
		catePrimaryIdTree2Count[node.ID] = sum // 记录到表
		catePrimaryIdTree2Depth[node.ID] = node.Depth
		return sum
	}
	dfs(rootTree)
	logx.D(ctx, "listCate catePrimaryIdTree2Count:%+v, leafBizIdMap:%+v", catePrimaryIdTree2Count, leafBizIdMap)
	if req.GetQueryType() == pb.CateQueryType_PAGE {
		subRoot := &category.CateNode{
			CateInfo: &category.CateInfo{},
		}
		for _, child := range childrenCates {
			subRoot.Children = append(subRoot.Children, &category.CateNode{
				CateInfo: child,
				Depth:    catePrimaryIdTree2Depth[child.ID],
			})
		}
		return subRoot, catePrimaryIdTree2Count, leafBizIdMap, nil
	}
	return rootTree, catePrimaryIdTree2Count, leafBizIdMap, nil
}

// createCate 创建分类
func (s *Service) createCate(ctx context.Context, t category.CateObjectType, req *pb.CreateCateReq) (*pb.CreateCateRsp,
	error) {
	name := strings.TrimSpace(req.GetName())
	if err := checkCateName(ctx, name); err != nil {
		return nil, err
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return nil, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return nil, err
	}
	cates, err := s.cateLogic.DescribeCateList(ctx, t, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		return nil, errs.ErrSystem
	}
	tree := category.BuildCateTree(cates)
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
		parentCate, err := s.cateLogic.DescribeCateByBusinessID(ctx, t, parentBizID, app.CorpPrimaryId, app.PrimaryId)
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
	if parent.Depth >= uint(docEntity.ExcelTplCateLen) {
		return nil, errs.ErrCateDepthExceed
	}

	if parent.IsNameDuplicate(name) {
		return nil, errs.ErrCateNameDuplicated
	}

	bizID := idgen.GetId()
	cate := &category.CateInfo{
		BusinessID: bizID,
		RobotID:    app.PrimaryId,
		CorpID:     app.CorpPrimaryId,
		Name:       name,
		ParentID:   parentID,
		IsDeleted:  false,
	}

	_, err = s.cateLogic.CreateCate(ctx, t, cate)
	if err != nil {
		return nil, errs.ErrSystem
	}
	// 设置分类缓存
	if t != category.SynonymsCate {
		go func() {
			defer gox.Recover()
			s.cateLogic.ModifyCateCache(trpc.CloneContext(ctx), t, app.CorpPrimaryId, app.PrimaryId)
			parents := tree.FindNodeWithParent(parentID)
			cateBizIDs := make([]uint64, 0, len(parents))
			for _, parent := range parents {
				cateBizIDs = append(cateBizIDs, parent.BusinessID)
			}
			logx.D(ctx, "FindNodeWithParent id:%d, cateBizIDs:%+v", parentID, cateBizIDs)
			s.userLogic.ModifyRoleKnowledgeByCate(trpc.CloneContext(ctx), contextx.Metadata(ctx).CorpBizID(), app.BizId, cateBizIDs)
		}()
	}

	rsp := &pb.CreateCateRsp{
		CateBizId: bizID,
		CanAdd:    parent.Depth+1 < uint(docEntity.ExcelTplCateLen),
		CanEdit:   true,
		CanDelete: true,
	}
	if t == category.QACate {
		auditx.Create(auditx.BizQA).App(app.BizId).Space(app.SpaceId).Log(ctx, i18n.Translate(ctx, kbe.QACategory), cate.Name, cate.BusinessID)
	} else if t == category.DocCate {
		auditx.Create(auditx.BizDocument).App(app.BizId).Space(app.SpaceId).Log(ctx, i18n.Translate(ctx, kbe.DocCategory), cate.Name, cate.BusinessID)
	}
	return rsp, nil
}

// modifyCate 修改分类
func (s *Service) modifyCate(ctx context.Context, t category.CateObjectType, req *pb.ModifyCateReq) (*pb.ModifyCateRsp, error) {
	rsp := new(pb.ModifyCateRsp)
	name := strings.TrimSpace(req.GetName())
	if err := checkCateName(ctx, name); err != nil {
		return rsp, err
	}
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	cates, err := s.cateLogic.DescribeCateList(ctx, t, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		return rsp, errs.ErrSystem
	}

	tree := category.BuildCateTree(cates)
	var node *category.CateNode
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
		cate, err := s.cateLogic.DescribeCateByBusinessID(ctx, t, cateBizID, app.CorpPrimaryId, app.PrimaryId)
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
	if err = s.cateLogic.ModifyCate(ctx, t, id, name); err != nil {
		return rsp, errs.ErrSystem
	}
	if t == category.QACate {
		auditx.Modify(auditx.BizQA).App(app.BizId).Space(app.SpaceId).Log(ctx, i18n.Translate(ctx, kbe.QACategory), req.GetName(), req.GetCateBizId())
	} else if t == category.DocCate {
		auditx.Modify(auditx.BizDocument).App(app.BizId).Space(app.SpaceId).Log(ctx, i18n.Translate(ctx, kbe.DocCategory), req.GetName(), req.GetCateBizId())
	}
	return rsp, nil
}

// deleteCate 删除分类
func (s *Service) deleteCate(ctx context.Context, t category.CateObjectType, req *pb.DeleteCateReq) (*pb.DeleteCateRsp,
	error) {
	rsp := new(pb.DeleteCateRsp)
	app, err := s.DescribeAppAndCheckCorp(ctx, req.GetBotBizId())
	if err != nil {
		return rsp, errs.ErrRobotNotFound
	}
	if err = app.IsWriteable(); err != nil {
		return rsp, err
	}
	cates, err := s.cateLogic.DescribeCateList(ctx, t, app.CorpPrimaryId, app.PrimaryId)
	if err != nil {
		return rsp, errs.ErrSystem
	}

	tree := category.BuildCateTree(cates)
	var id uint64
	if req.GetId() == "" {
		cateBizID, err := util.CheckReqParamsIsUint64(ctx, req.GetCateBizId())
		if err != nil {
			return nil, err
		}
		cate, err := s.cateLogic.DescribeCateByBusinessID(ctx, t, cateBizID, app.CorpPrimaryId, app.PrimaryId)
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

	uncateID := tree.Find([]string{category.UncategorizedCateName})
	if uncateID == -1 {
		return nil, errs.ErrCateNotFound
	}
	logx.D(ctx, "DeleteCate ids:%+v, uncateID:%+v", ids, uncateID)
	if err := s.cateLogic.DeleteCate(ctx, t, ids, uint64(uncateID), app); err != nil {
		return nil, errs.ErrSystem
	}
	// 重新构建分类缓存
	if t != category.SynonymsCate {
		go func() {
			defer gox.Recover()
			s.cateLogic.ModifyCateCache(trpc.CloneContext(ctx), t, app.CorpPrimaryId, app.PrimaryId)
			// 删除分类需要同步删除角色分类绑定关系 异步删除，不要影响原功能 失败也没关系
			s.userLogic.DeleteRoleCateListByKnowAndCateBizID(ctx, app.BizId, node.BusinessID, 10000)
		}()
	}
	if t == category.QACate {
		auditx.Delete(auditx.BizQA).App(app.BizId).Space(app.SpaceId).Log(ctx, i18n.Translate(ctx, kbe.QACategory), node.Name, req.GetCateBizId())
	} else if t == category.DocCate {
		auditx.Delete(auditx.BizDocument).App(app.BizId).Space(app.SpaceId).Log(ctx, i18n.Translate(ctx, kbe.DocCategory), node.Name, req.GetCateBizId())
	}
	return rsp, nil
}

// ListQACate 获取问答分类列表
func (s *Service) ListQACate(ctx context.Context, req *pb.ListQACateReq) (*pb.ListQACateRsp, error) {
	logx.I(ctx, "ListQACate Req:%+v", req)
	reqNew := &pb.ListCateReq{
		BotBizId:        req.GetBotBizId(),
		QueryType:       req.GetQueryType(),
		ParentCateBizId: req.GetParentCateBizId(),
		PageNumber:      req.GetPageNumber(),
		PageSize:        req.GetPageSize(),
		Query:           req.GetQuery(),
	}
	tree, stat, leafBizIdMap, err := s.listCate(ctx, category.QACate, reqNew)
	if err != nil {
		return nil, err
	}
	rsp := &pb.ListQACateRsp{}
	if req.GetQueryType() == pb.CateQueryType_KEYWORD {
		rsp.List = []*pb.ListQACateRsp_Cate{tree.ToPbQACateQueryTree(ctx, stat, req.GetQuery())}
	} else if req.GetQueryType() == pb.CateQueryType_PAGE {
		if req.GetParentCateBizId() == "" {
			// 首次请求，没有父节点ID，返回全部分类
			rsp.List = []*pb.ListQACateRsp_Cate{
				{
					CateBizId: category.AllCateID,
					Name:      i18n.Translate(ctx, category.AllCateName),
					Total:     stat[category.AllCateID],
					CanAdd:    true,
				},
			}
		} else {
			tmp := []*pb.ListQACateRsp_Cate{tree.ToPbQACateTree(ctx, stat)}
			rsp.List = tmp[0].Children
			for _, child := range rsp.List {
				if _, ok := leafBizIdMap[child.CateBizId]; ok {
					child.IsLeaf = true
				}
			}
		}
	} else {
		rsp.List = []*pb.ListQACateRsp_Cate{tree.ToPbQACateTree(ctx, stat)}
	}
	return rsp, nil
}

// CreateQACate 创建问答分类
func (s *Service) CreateQACate(ctx context.Context, req *pb.CreateQACateReq) (*pb.CreateQACateRsp, error) {
	logx.I(ctx, "CreateQACate Req:%+v", req)
	reqTmp := &pb.CreateCateReq{
		Name:        req.GetName(),
		ParentBizId: req.GetParentBizId(),
		BotBizId:    req.GetBotBizId(),
	}
	rspTmp, err := s.createCate(ctx, category.QACate, reqTmp)
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
	logx.I(ctx, "ModifyQACate Req:%+v", req)

	reqTmp := &pb.ModifyCateReq{
		Name:      req.GetName(),
		Id:        req.GetId(),
		CateBizId: req.GetCateBizId(),
		BotBizId:  req.GetBotBizId(),
	}
	_, err := s.modifyCate(ctx, category.QACate, reqTmp)
	if err != nil {
		return nil, err
	}
	rsp := &pb.ModifyQACateRsp{}
	return rsp, nil
}

// DeleteQACate 删除QACate
func (s *Service) DeleteQACate(ctx context.Context, req *pb.DeleteQACateReq) (*pb.DeleteQACateRsp, error) {
	logx.I(ctx, "DeleteQACate Req:%+v", req)

	reqTmp := &pb.DeleteCateReq{
		Id:        req.GetId(),
		CateBizId: req.GetCateBizId(),
		BotBizId:  req.GetBotBizId(),
	}
	_, err := s.deleteCate(ctx, category.QACate, reqTmp)
	if err != nil {
		return nil, err
	}
	rsp := &pb.DeleteQACateRsp{}

	return rsp, nil
}

// ListDocCate 文档分类列表
func (s *Service) ListDocCate(ctx context.Context, req *pb.ListCateReq) (*pb.ListCateRsp, error) {
	logx.I(ctx, "ListDocCate Req:%+v", req)
	tree, stat, leafBizIdMap, err := s.listCate(ctx, category.DocCate, req)
	if err != nil {
		return nil, err
	}
	rsp := &pb.ListCateRsp{}
	if req.GetQueryType() == pb.CateQueryType_KEYWORD {
		rsp.List = []*pb.CateInfo{tree.ToPbCateQueryTree(ctx, stat, req.GetQuery())}
	} else if req.GetQueryType() == pb.CateQueryType_PAGE {
		if req.GetParentCateBizId() == "" {
			// 首次请求，没有父节点ID，返回全部分类
			rsp.List = []*pb.CateInfo{
				{
					CateBizId: category.AllCateID,
					Name:      i18n.Translate(ctx, category.AllCateName),
					Total:     stat[category.AllCateID],
					CanAdd:    true,
				},
			}
		} else {
			tmp := []*pb.CateInfo{tree.ToPbCateInfoTree(ctx, stat)}
			rsp.List = tmp[0].Children
			for _, child := range rsp.List {
				if _, ok := leafBizIdMap[child.CateBizId]; ok {
					child.IsLeaf = true
				}
			}
		}
	} else {
		rsp.List = []*pb.CateInfo{tree.ToPbCateInfoTree(ctx, stat)}
	}
	return rsp, nil
}

// CreateDocCate 创建文档分类
func (s *Service) CreateDocCate(ctx context.Context, req *pb.CreateCateReq) (*pb.CreateCateRsp, error) {
	logx.I(ctx, "CreateDocCate Req:%+v", req)
	return s.createCate(ctx, category.DocCate, req)
}

// ModifyDocCate 修改文档分类
func (s *Service) ModifyDocCate(ctx context.Context, req *pb.ModifyCateReq) (*pb.ModifyCateRsp, error) {
	logx.I(ctx, "CreateDocCate Req:%+v", req)
	return s.modifyCate(ctx, category.DocCate, req)
}

// DeleteDocCate 删除文档分类
func (s *Service) DeleteDocCate(ctx context.Context, req *pb.DeleteCateReq) (*pb.DeleteCateRsp, error) {
	logx.I(ctx, "DeleteDocCate Req:%+v", req)
	return s.deleteCate(ctx, category.DocCate, req)
}

// ListSynonymsCate 获取同义词分类列表
func (s *Service) ListSynonymsCate(ctx context.Context, req *pb.ListCateReq) (*pb.ListCateRsp, error) {
	logx.I(ctx, "ListSynonymsCate req:%s", req)
	newReq := &appConfig.DescribeCateListReq{
		BotBizId: req.GetBotBizId(),
	}
	newRsp, err := s.rpc.AppAdmin.DescribeSynonymsCateList(ctx, newReq)
	if err != nil {
		logx.E(ctx, "ListSynonymsCate DescribeSynonymsCateList err:%v", err)
		return nil, err
	}
	rsp := &pb.ListCateRsp{}
	for _, v := range newRsp.GetList() {
		rsp.List = append(rsp.List, transCateInfo(v))
	}
	logx.I(ctx, "ListSynonymsCate rsp:%s", rsp)
	return rsp, nil
}

// 递归构建 cateInfo （以及其中的 Children)
func transCateInfo(v *appConfig.CateInfo) *pb.CateInfo {
	if v == nil {
		return nil
	}
	cate := &pb.CateInfo{
		CateBizId: v.GetCateBizId(),
		Name:      v.GetName(),
		Total:     v.GetTotal(),
		CanAdd:    v.GetCanAdd(),
		CanEdit:   v.GetCanEdit(),
		CanDelete: v.GetCanDelete(),
	}
	for _, child := range v.GetChildren() {
		cate.Children = append(cate.Children, transCateInfo(child))
	}
	return cate
}

// CreateSynonymsCate 创建同义词分类
func (s *Service) CreateSynonymsCate(ctx context.Context, req *pb.CreateCateReq) (*pb.CreateCateRsp, error) {
	newReq := &appConfig.CreateCateReq{
		Name:        req.GetName(),
		ParentBizId: req.GetParentBizId(),
		BotBizId:    req.GetBotBizId(),
	}
	newRsp, err := s.rpc.AppAdmin.CreateSynonymsCate(ctx, newReq)
	if newRsp != nil {
		return &pb.CreateCateRsp{
			CateBizId: newRsp.GetCateBizId(),
			CanAdd:    newRsp.GetCanAdd(),
			CanEdit:   newRsp.GetCanEdit(),
			CanDelete: newRsp.GetCanDelete(),
		}, err
	}
	return nil, err
}

// ModifySynonymsCate 修改同义词分类
func (s *Service) ModifySynonymsCate(ctx context.Context, req *pb.ModifyCateReq) (*pb.ModifyCateRsp, error) {
	newReq := &appConfig.ModifyCateReq{
		Id:        req.GetId(),
		Name:      req.GetName(),
		BotBizId:  req.GetBotBizId(),
		CateBizId: req.GetCateBizId(),
	}
	_, err := s.rpc.AppAdmin.ModifySynonymsCate(ctx, newReq)
	return &pb.ModifyCateRsp{}, err
}

// DeleteSynonymsCate 删除同义词分类
func (s *Service) DeleteSynonymsCate(ctx context.Context, req *pb.DeleteCateReq) (*pb.DeleteCateRsp, error) {
	newReq := &appConfig.DeleteCateReq{
		Id:        req.GetId(),
		BotBizId:  req.GetBotBizId(),
		CateBizId: req.GetCateBizId(),
	}
	_, err := s.rpc.AppAdmin.DeleteSynonymsCate(ctx, newReq)
	return &pb.DeleteCateRsp{}, err
}

// getCateChildrenIDs 获取分类下的所有分类ID（主分类和所有子分类）
func (s *Service) getCateChildrenIDs(ctx context.Context, cateType category.CateObjectType,
	corpID, appID, cateID uint64) ([]uint64, error) {
	childrenIDs, err := s.cateLogic.DescribeCateChildrenIDs(ctx, cateType, corpID, cateID, appID)
	if err != nil {
		return nil, err
	}
	cateAllIDs := append(childrenIDs, cateID)
	return cateAllIDs, nil
}
