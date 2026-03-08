package api

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	admin "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_admin_config_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
)

func Test_filterRefer(t *testing.T) {
	refers := []model.Refer{
		{DocType: 1, DocID: 1, Confidence: 0.5},
		{DocType: 1, DocID: 1, Confidence: 0.9},
		{DocType: 1, DocID: 1, Confidence: 0.95},

		{DocType: 1, DocID: 3, Confidence: 0.5},
		{DocType: 1, DocID: 3, Confidence: 0.99},
		{DocType: 1, DocID: 3, Confidence: 0.95},

		{DocType: 2, DocID: 1, Confidence: 0.5},
		{DocType: 2, DocID: 1, Confidence: 0.91},
		{DocType: 2, DocID: 1, Confidence: 0.95},

		{DocType: 2, DocID: 2, Confidence: 0.5},
		{DocType: 2, DocID: 2, Confidence: 0.9},
		{DocType: 2, DocID: 2, Confidence: 0.96},

		{DocType: 3, DocID: 4, Confidence: 0.92},
		{DocType: 3, DocID: 2, Confidence: 0.99},
		{DocType: 3, DocID: 4, Confidence: 0.9},
	}
	scores := []Score{
		{F: 0.5, P: 0.5, R: 0.5},
		{F: 0.9, P: 0.9, R: 0.9},
		{F: 0.95, P: 0.95, R: 0.95},

		{F: 0.5, P: 0.5, R: 0.5},
		{F: 0.99, P: 0.99, R: 0.99},
		{F: 0.95, P: 0.95, R: 0.95},

		{F: 0.5, P: 0.5, R: 0.5},
		{F: 0.91, P: 0.91, R: 0.91},
		{F: 0.95, P: 0.95, R: 0.95},

		{F: 0.5, P: 0.5, R: 0.5},
		{F: 0.9, P: 0.9, R: 0.9},
		{F: 0.96, P: 0.96, R: 0.96},

		{F: 0.92, P: 0.92, R: 0.92},
		{F: 0.99, P: 0.99, R: 0.99},
		{F: 0.9, P: 0.9, R: 0.9},
	}
	t.Run("filter1", func(t *testing.T) {
		filters := []*admin.AppFiltersInfo{
			{DocType: 1, RougeScore: &admin.RougeScore{F: 0.89, P: 0.89, R: 0.89}, TopN: 3},
			{DocType: 2, RougeScore: &admin.RougeScore{F: 0.89, P: 0.89, R: 0.89}, TopN: 3},
		}
		topN := uint32(5)
		want := []model.Refer{
			{DocType: 1, DocID: 3, Confidence: 0.99},
			{DocType: 1, DocID: 1, Confidence: 0.95},
			{DocType: 1, DocID: 3, Confidence: 0.95},
			{DocType: 2, DocID: 2, Confidence: 0.96},
			{DocType: 2, DocID: 1, Confidence: 0.95},
		}
		assert.EqualValues(t, want, filterRefer(refers, scores, filters, topN))
	})
	t.Run("filter2", func(t *testing.T) {
		filters := []*admin.AppFiltersInfo{
			{DocType: 1, RougeScore: &admin.RougeScore{F: 0.89, P: 0.89, R: 0.89}, TopN: 1},
			{DocType: 2, RougeScore: &admin.RougeScore{F: 0.89, P: 0.89, R: 0.89}, TopN: 1},
		}
		topN := uint32(5)
		want := []model.Refer{
			{DocType: 1, DocID: 3, Confidence: 0.99},
			{DocType: 2, DocID: 2, Confidence: 0.96},
		}
		assert.EqualValues(t, want, filterRefer(refers, scores, filters, topN))
	})

	t.Run("filter3", func(t *testing.T) {
		filters := []*admin.AppFiltersInfo{
			{DocType: 1, RougeScore: &admin.RougeScore{F: 0.89, P: 0.89, R: 0.89}, TopN: 1},
			{DocType: 2, RougeScore: &admin.RougeScore{F: 0.89, P: 0.89, R: 0.89}, TopN: 1},
			{DocType: 3, RougeScore: &admin.RougeScore{F: 0.89, P: 0.89, R: 0.89}, TopN: 1},
		}
		topN := uint32(5)
		want := []model.Refer{
			{DocType: 1, DocID: 3, Confidence: 0.99},
			{DocType: 2, DocID: 2, Confidence: 0.96},
			{DocType: 3, DocID: 2, Confidence: 0.99},
		}
		assert.EqualValues(t, want, filterRefer(refers, scores, filters, topN))
	})
}

func Test_docsPlaceholder(t *testing.T) {
	config.SetApp(config.Application{
		DocPlaceholder: config.DocPlaceholder{
			Link: "https://L%d",
			Img:  "https://I%d",
		},
	})
	rsp := docsPlaceholder(context.Background(), &pb.SearchRsp{Docs: []*pb.SearchRsp_Doc{
		{Question: "question: ![](http://question.com)"},
		{Answer: "answer: ![](http://answer.com) ![](http://answer.com)"},
		{OrgData: "org_data: ![](http://org.com)  ![](http://data.com) ![](http://data.com)"},
		{Question: "question: ![](http://1question.com)"},
		{Answer: "answer: ![](http://1answer.com)"},
		{OrgData: "org_data: ![](http://org.com)  ![](http://data.com)"},
	}})
	require.EqualValues(t, rsp.GetDocs()[0].Question, "question: ![](https://I0)")
	require.EqualValues(t, rsp.GetDocs()[0].QuestionPlaceholders, []*pb.Placeholder{
		{Key: "(https://I0)", Value: "(http://question.com)"},
	})
	require.EqualValues(t, rsp.GetDocs()[1].Answer, "answer: ![](https://I1) ![](https://I1)")
	require.EqualValues(t, rsp.GetDocs()[1].AnswerPlaceholders, []*pb.Placeholder{
		{Key: "(https://I1)", Value: "(http://answer.com)"},
	})
	require.EqualValues(t, rsp.GetDocs()[2].OrgData, "org_data: ![](https://I2)  ![](https://I3) ![](https://I3)")
	require.EqualValues(t, rsp.GetDocs()[2].OrgDataPlaceholders, []*pb.Placeholder{
		{Key: "(https://I2)", Value: "(http://org.com)"},
		{Key: "(https://I3)", Value: "(http://data.com)"},
	})

	switch r := reflect.ValueOf(rsp).Interface().(type) {
	case *pb.SearchRsp:
		for _, v := range r.GetDocs() {
			fmt.Printf("Doc| v: %+v\n", v)
		}
	}
}

func Test_docsUnique(t *testing.T) {
	rsp := docsUnique(context.Background(), &pb.SearchRsp{Docs: []*pb.SearchRsp_Doc{
		{DocType: model.DocTypeQA, Question: "question a"},
		{DocType: model.DocTypeSegment, OrgData: "org_data a"},
		{DocType: model.DocTypeQA, Question: "question b"},
		{DocType: model.DocTypeSegment, OrgData: "org_data a"},
		{DocType: model.DocTypeSegment, OrgData: "org_data b"},
		{DocType: model.DocTypeQA, Question: "question b"},
	}})
	require.EqualValues(t, rsp, &pb.SearchRsp{Docs: []*pb.SearchRsp_Doc{
		{DocType: model.DocTypeQA, Question: "question a"},
		{DocType: model.DocTypeSegment, OrgData: "org_data a"},
		{DocType: model.DocTypeQA, Question: "question b"},
		{DocType: model.DocTypeSegment, OrgData: "org_data b"},
		{DocType: model.DocTypeQA, Question: "question b"},
	}})
}
