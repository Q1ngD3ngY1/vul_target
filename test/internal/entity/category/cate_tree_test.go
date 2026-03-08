package category

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"

	"git.woa.com/adp/common/x/gox"
)

func Test_BuildCateTree(t *testing.T) {
	t.Logf("%s", spew.Sdump(BuildCateTree(nil)))
	tree := BuildCateTree([]*CateInfo{
		{ID: 1, Name: "1", OrderNum: 1, ParentID: 0},
		{ID: 2, Name: "2", OrderNum: 2, ParentID: 1},
		{ID: 3, Name: "3", OrderNum: 3, ParentID: 2},
		{ID: 4, Name: "4", OrderNum: 4, ParentID: 3},
		{ID: 5, Name: "5", OrderNum: 5, ParentID: 0},
		{ID: 6, Name: "6", OrderNum: 6, ParentID: 0},
		{ID: 7, Name: "7", OrderNum: 8, ParentID: 1},
		{ID: 8, Name: "8", OrderNum: 7, ParentID: 1},
		{ID: 9, Name: "9", OrderNum: 9, ParentID: 7},
	})
	t.Logf("%s", spew.Sdump(tree))
	path, _ := tree.Path(context.Background(), 9)
	t.Logf("%+v", path)
	t.Logf("%+v", tree.IsPathExist([]string{"1", "7", "9"}))
	t.Logf("%+v", tree.FindNode(1).ChildrenIDs())
	t.Logf("%+v", tree.IsPathExist([]string{"1", "10", "11"}))
	tree.Create([]string{"1", "10"})
	t.Logf("%+v", tree.IsPathExist([]string{"1", "10", "11"}))
	tree.Create([]string{"1", "10", "11"})
	t.Logf("%+v", tree.IsPathExist([]string{"1", "10", "11"}))
	tree.Create([]string{"1", "10", "12"})
	t.Logf("%+v", tree.IsPathExist([]string{"1", "10", "12"}))
	t.Logf("%s", spew.Sdump(tree.FindNode(9)))
	t.Logf("%+v", tree.Find([]string{"未分类"}))
	tree.Create([]string{"未分类"})
	t.Logf("%+v", tree.Find([]string{"未分类"}))
	t.Logf("%+v", tree.Find([]string{"Uncategorized"}))
	tree.Create([]string{"Uncategorized"})
	t.Logf("%+v", tree.Find([]string{"Uncategorized"}))
}

func Test_buildLarge(t *testing.T) {
	size := 1000000
	cates := make([]*CateInfo, 0, size)
	for i := 1; i <= size; i++ {
		cates = append(cates, &CateInfo{
			ID:       uint64(i),
			Name:     strconv.FormatInt(int64(i), 10),
			ParentID: gox.IfElse((i-1)%10 == 0, 0, uint64(i-1)),
		})
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(cates), func(i, j int) { cates[i], cates[j] = cates[j], cates[i] })
	tree := BuildCateTree(cates)
	t.Logf("node count: %+v", tree.NodeCount())
}

func Test_getCatePath(t *testing.T) {
	row := []string{"0", "1", "2", "3", "4", "5", "6", "", "", "", ""}
	t.Log(GetCatePath(row))
}

func Test_ToPbCateTree(t *testing.T) {
	tree := BuildCateTree([]*CateInfo{
		{ID: 218, Name: "一级1", OrderNum: 0, ParentID: 0},
		{ID: 219, Name: "二级1", OrderNum: 0, ParentID: 218},
		{ID: 220, Name: "三级1", OrderNum: 0, ParentID: 219},
		{ID: 221, Name: "三级2", OrderNum: 0, ParentID: 219},
		{ID: 222, Name: "二级2", OrderNum: 0, ParentID: 218},
		{ID: 223, Name: "一级2", OrderNum: 0, ParentID: 0},
		{ID: 224, Name: "二级2", OrderNum: 0, ParentID: 223},
		{ID: 225, Name: "三级1", OrderNum: 0, ParentID: 224},
		{ID: 226, Name: "四级1", OrderNum: 0, ParentID: 225},
	})
	stat := map[uint64]uint32{
		0:   3,
		220: 1,
		221: 1,
		222: 1,
		225: 1,
		226: 1,
	}
	t.Logf("%s", spew.Sdump(tree.ToPbQACateTree(context.Background(), stat)))
}
