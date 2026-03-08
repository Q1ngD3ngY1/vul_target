package model

import (
	"context"
	"sort"
	"strings"

	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

// CateNode 分类节点
type CateNode struct {
	*CateInfo
	Depth    uint
	Children []*CateNode
}

// ToPbCateInfoTree 转为 PB 分类树
func (t *CateNode) ToPbCateInfoTree(ctx context.Context, stat map[uint64]uint32) *pb.CateInfo {
	if t == nil {
		return nil
	}
	var children []*pb.CateInfo
	for _, node := range t.Children {
		children = append(children, node.ToPbCateInfoTree(ctx, stat))
	}

	total := stat[t.ID]
	for _, id := range t.ChildrenIDs() {
		total += stat[id]
	}

	if t.ID == AllCateID {
		return &pb.CateInfo{
			CateBizId: AllCateID,
			Name:      i18n.Translate(ctx, AllCateName),
			Total:     total,
			CanAdd:    true,
			Children:  children,
		}
	}
	if t.Name == UncategorizedCateName {
		t.Name = i18n.Translate(ctx, UncategorizedCateName)
	}
	return &pb.CateInfo{
		CateBizId: t.BusinessID,
		Name:      t.Name,
		Total:     total,
		Children:  children,
		CanAdd:    !t.IsUncategorized(ctx) && t.Depth < uint(ExcelTplCateLen),
		CanEdit:   !t.IsUncategorized(ctx),
		CanDelete: !t.IsUncategorized(ctx),
	}
}

// ToPbQACateTree 转为 PB 分类树
func (t *CateNode) ToPbQACateTree(ctx context.Context, stat map[uint64]uint32) *pb.ListQACateRsp_Cate {
	if t == nil {
		return nil
	}
	var children []*pb.ListQACateRsp_Cate
	for _, node := range t.Children {
		children = append(children, node.ToPbQACateTree(ctx, stat))
	}

	total := stat[t.ID]
	for _, id := range t.ChildrenIDs() {
		total += stat[id]
	}

	if t.ID == AllCateID {
		return &pb.ListQACateRsp_Cate{
			CateBizId: AllCateID,
			Name:      i18n.Translate(ctx, AllCateName),
			Total:     total,
			CanAdd:    true,
			Children:  children,
		}
	}

	if t.Name == UncategorizedCateName {
		t.Name = i18n.Translate(ctx, UncategorizedCateName)
	}
	return &pb.ListQACateRsp_Cate{
		CateBizId: t.BusinessID,
		Name:      t.Name,
		Total:     total,
		Children:  children,
		CanAdd:    !t.IsUncategorized(ctx) && t.Depth < uint(ExcelTplCateLen),
		CanEdit:   !t.IsUncategorized(ctx),
		CanDelete: !t.IsUncategorized(ctx),
	}
}

// ToPbQACateTreeV1 转为 PB 分类树
func (t *CateNode) ToPbQACateTreeV1(ctx context.Context, stat map[uint64]uint32) *pb.ListQACateV1Rsp_Cate {
	if t == nil {
		return nil
	}
	var children []*pb.ListQACateV1Rsp_Cate
	for _, node := range t.Children {
		children = append(children, node.ToPbQACateTreeV1(ctx, stat))
	}

	total := stat[t.ID]
	for _, id := range t.ChildrenIDs() {
		total += stat[id]
	}

	if t.ID == AllCateID {
		return &pb.ListQACateV1Rsp_Cate{
			Id:       AllCateID,
			Name:     i18n.Translate(ctx, AllCateName),
			Total:    total,
			CanAdd:   true,
			Children: children,
		}
	}
	if t.Name == UncategorizedCateName {
		t.Name = i18n.Translate(ctx, UncategorizedCateName)
	}
	return &pb.ListQACateV1Rsp_Cate{
		Id:        int64(t.ID),
		Name:      t.Name,
		Total:     total,
		Children:  children,
		CanAdd:    !t.IsUncategorized(ctx) && t.Depth < uint(ExcelTplCateLen),
		CanEdit:   !t.IsUncategorized(ctx),
		CanDelete: !t.IsUncategorized(ctx),
	}
}

// Create 根据路径给出需要创建的分类
func (t *CateNode) Create(path []string) {
	cur := t
next:
	for _, p := range path {
		for _, node := range cur.Children {
			if node.Name == p {
				cur = node
				continue next
			}
		}
		node := &CateNode{&CateInfo{Name: p}, cur.Depth + 1, nil}
		cur.Children = append(cur.Children, node)
		cur = node
	}
}

// NodeCount 计算节点总数
func (t *CateNode) NodeCount() int {
	var c int
	for _, child := range t.Children {
		c += child.NodeCount()
	}
	return c + 1
}

// Find 根据路径找节点
func (t *CateNode) Find(path []string) int64 {
	if t == nil {
		return -1
	}
	if len(path) == 0 {
		return int64(t.ID)
	}
	for _, node := range t.Children {
		if node.Name == path[0] {
			return node.Find(path[1:])
		}
	}
	return -1
}

// FindNode 根据 ID 获取它在分类树中的节点
func (t *CateNode) FindNode(id uint64) *CateNode {
	if t == nil {
		return nil
	}
	if t.ID == id || t.BusinessID == id {
		return t
	}
	for _, node := range t.Children {
		if n := node.FindNode(id); n != nil {
			return n
		}
	}
	return nil
}

// FindNodeWithParent 根据 ID 获取它在分类树中的节点及父节点
func (t *CateNode) FindNodeWithParent(id uint64) []*CateNode {
	if t == nil {
		return nil
	}
	if t.ID == id || t.BusinessID == id {
		return []*CateNode{t}
	}
	for _, node := range t.Children {
		if n := node.FindNodeWithParent(id); n != nil {
			return append(n, node)
		}
	}
	return nil
}

// IsPathExist 判断路径是否存在
func (t *CateNode) IsPathExist(path []string) bool {
	return t.Find(path) != -1
}

// IsNameDuplicate 判断分类名称在子节点中是否重复
func (t *CateNode) IsNameDuplicate(name string) bool {
	if t == nil {
		return false
	}
	if t.Depth == 0 && strings.TrimSpace(name) == UncategorizedCateName || strings.TrimSpace(name) == i18n.Translate(
		context.Background(), UncategorizedCateName) {
		return true
	}
	for _, node := range t.Children {
		if strings.TrimSpace(node.Name) == strings.TrimSpace(name) {
			return true
		}
	}
	return false
}

// ChildrenIDs 获取某分类下所有子分类的 ID
func (t *CateNode) ChildrenIDs() []uint64 {
	if t == nil {
		return nil
	}
	var ids []uint64
	for _, node := range t.Children {
		ids = append(ids, node.ID)
		ids = append(ids, node.ChildrenIDs()...)
	}
	return ids
}

// Path 输出分类节点路径
func (t *CateNode) Path(ctx context.Context, id uint64) []string {
	if t == nil {
		return nil
	}
	for _, node := range t.Children {
		if node.ID == id {
			if node.Name == UncategorizedCateName {
				return []string{i18n.Translate(ctx, node.Name)}
			}
			return []string{node.Name}
		}
		path := node.Path(ctx, id)
		if len(path) > 0 {
			if node.Name == UncategorizedCateName {
				return append([]string{i18n.Translate(ctx, node.Name)}, path...)
			}
			return append([]string{node.Name}, path...)
		}
	}
	return nil
}

// BuildCateTree 构造分类树
func BuildCateTree(cates []*CateInfo) *CateNode {
	m := make(map[uint64][]*CateInfo, len(cates))
	for _, cate := range cates {
		m[cate.ParentID] = append(m[cate.ParentID], cate)
	}
	return build(m, &CateInfo{ID: 0, Name: AllCateName}, 0)
}

func build(cates map[uint64][]*CateInfo, parent *CateInfo, depth uint) *CateNode {
	var nodes []*CateNode
	for _, cate := range cates[parent.ID] {
		nodes = append(nodes, build(cates, cate, depth+1))
	}
	sort.SliceStable(nodes, func(i, j int) bool {
		if nodes[i].OrderNum == nodes[j].OrderNum {
			return nodes[i].ID > nodes[j].ID
		}
		return nodes[i].OrderNum < nodes[j].OrderNum
	})
	return &CateNode{parent, depth, nodes}
}

// GetCatePath 提取有效分类路径
func GetCatePath(row []string) (bool, []string) {
	if len(row) < ExcelTplCateLen {
		return false, nil
	}
	stop := -1
	for i := ExcelTplCateLen - 1; i >= 0; i-- {
		text := strings.TrimSpace(row[i])
		if text != "" {
			if stop == -1 {
				stop = i
			}
		} else if stop != -1 {
			return false, nil
		}
	}

	cates := row[0 : stop+1]
	for i := range cates {
		cates[i] = strings.TrimSpace(cates[i])
	}
	return true, cates
}

// BuildCateCache 构建分类缓存里的结构
func BuildCateCache(cates []*CateInfo) map[int][]int {
	// 1.构建分类主键id和业务id的映射
	idToBizID := make(map[int]int, len(cates))
	for _, v := range cates {
		idToBizID[int(v.ID)] = int(v.BusinessID)
	}
	// 2.构建父子关系的直接映射
	parentToChildren := make(map[int][]int, len(cates))
	for _, v := range cates {
		parentToChildren[idToBizID[int(v.ParentID)]] = append(parentToChildren[idToBizID[int(v.ParentID)]], int(v.BusinessID))
	}
	// 3.构建结果映射（包含递归子节点）
	result := make(map[int][]int, len(cates))
	// 使用记忆化技术避免重复计算
	memo := make(map[int][]int)
	// 定义递归函数
	var getChildren func(int, map[int]bool) []int
	getChildren = func(nodeID int, visited map[int]bool) []int {
		// 检查是否已经计算过
		if children, exists := memo[nodeID]; exists {
			return children
		}
		// 循环引用检测
		if visited[nodeID] {
			return []int{} // 循环了停止递归,一般不会触发
		}
		visited[nodeID] = true
		// 获取直接子节点
		directChildren := parentToChildren[nodeID]
		allChildren := make([]int, len(directChildren))
		copy(allChildren, directChildren)
		// 递归获取子节点的子节点
		for _, childID := range directChildren {
			allChildren = append(allChildren, getChildren(childID, visited)...)
		}
		// 缓存结果
		memo[nodeID] = allChildren
		return allChildren
	}
	// 为每个节点构建完整的子节点列表
	for _, v := range cates {
		visited := make(map[int]bool)
		result[int(v.BusinessID)] = getChildren(int(v.BusinessID), visited)
	}
	return result
}
