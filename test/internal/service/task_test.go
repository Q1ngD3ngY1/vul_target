package service

import (
	"testing"
)

func Test_buildExportQAExcel(t *testing.T) {
	// f, err := buildExportQAExcel(
	//	context.Background(),
	//	[]*model.DocQA{
	//		{Question: "question1", Answer: "answer1", DocID: 1, Source: 1, CategoryID: 1},
	//		{Question: "question2", Answer: "answer2", DocID: 2, Source: 2, CategoryID: 7},
	//		{Question: "question3", Answer: "answer3", DocID: 3, Source: 3, CategoryID: 3},
	//		{Question: "question4", Answer: "answer4", DocID: 4, Source: 1, CategoryID: 0},
	//	},
	//	[]*model.DocQACate{
	//		{ID: 1, Name: "A", OrderNum: 1, ParentID: 0}, // A
	//		{ID: 2, Name: "B", OrderNum: 2, ParentID: 1}, // A -> B
	//		{ID: 3, Name: "C", OrderNum: 3, ParentID: 2}, // A -> B -> C
	//		{ID: 4, Name: "D", OrderNum: 4, ParentID: 3}, // A -> B -> C -> D
	//		{ID: 5, Name: "E", OrderNum: 5, ParentID: 0}, // E
	//		{ID: 6, Name: "F", OrderNum: 6, ParentID: 0}, // F
	//		{ID: 7, Name: "G", OrderNum: 8, ParentID: 1}, // A -> G
	//		{ID: 8, Name: "H", OrderNum: 7, ParentID: 1}, // A -> H
	//		{ID: 9, Name: "I", OrderNum: 9, ParentID: 7}, // A -> G -> I
	//	},
	//	map[uint64]*model.Doc{
	//		1: {ID: 1, FileName: "file1"},
	//		2: {ID: 2, FileName: "file2"},
	//		3: {ID: 3, FileName: "file3"},
	//		4: {ID: 4, FileName: "file4"},
	//	},
	// )
	// assert.Nil(t, err)
	// assert.Nil(t, f.SaveAs("/tmp/test.xlsx"))
}
