package api

import (
	"context"
	"reflect"

	"git.woa.com/adp/kb/kb-config/internal/config"
	"git.woa.com/adp/kb/kb-config/internal/entity"
	"git.woa.com/adp/kb/kb-config/internal/util/markdown"
	pb "git.woa.com/adp/pb-go/kb/kb_config"
)

// 召回内容后处理
// 处理 4 种召回结果
// *pb.SearchRsp
// *pb.SearchPreviewRsp
// *pb.CustomSearchRsp
// *pb.CustomSearchPreviewRsp

// searchRsp 召回内容 rsp
type searchRsp interface {
	*pb.SearchRsp | *pb.SearchPreviewRsp | *pb.CustomSearchRsp | *pb.CustomSearchPreviewRsp |
	*pb.SearchRealtimeRsp
}

// searchDoc 召回内容
type searchDoc interface {
	*pb.SearchRsp_Doc | *pb.SearchPreviewRsp_Doc | *pb.CustomSearchRsp_Doc | *pb.CustomSearchPreviewRsp_Doc |
	*pb.SearchRealtimeRsp_Doc
}

type searchDocer interface {
	GetDocType() uint32
	GetOrgData() string
}

// searchRspPostProcess 召回内容后处理
func searchRspPostProcess[T searchRsp](ctx context.Context, usePlaceholder bool, rsp T) T {
	// 去重
	rsp = docsUnique(ctx, rsp)
	// 使用占位符替换
	if usePlaceholder {
		rsp = docsPlaceholder(ctx, rsp)
	}
	return rsp
}

// docsUnique 去重
func docsUnique[T searchRsp](ctx context.Context, rsp T) T {
	switch r := reflect.ValueOf(rsp).Interface().(type) {
	case *pb.SearchRsp:
		r.Docs = uniqueDocs(r.GetDocs())
	case *pb.SearchPreviewRsp:
		r.Docs = uniqueDocs(r.GetDocs())
	case *pb.CustomSearchRsp:
		r.Docs = uniqueDocs(r.GetDocs())
	case *pb.CustomSearchPreviewRsp:
		r.Docs = uniqueDocs(r.GetDocs())
	case *pb.SearchRealtimeRsp:
		r.Docs = uniqueDocs(r.GetDocs())
	}
	return rsp
}

func uniqueDocs[T searchDoc](docs []T) []T {
	m := make(map[string]struct{})
	var u []T
	for _, doc := range docs {
		v := searchDocer(doc)
		if v.GetDocType() == entity.DocTypeSegment {
			if _, ok := m[v.GetOrgData()]; !ok {
				m[v.GetOrgData()] = struct{}{}
				u = append(u, doc)
			}
		} else {
			u = append(u, doc)
		}
	}
	return u
}

// docsPlaceholder 使用占位符替换返回内容中的链接和图片
func docsPlaceholder[T searchRsp](ctx context.Context, rsp T) T {
	md := markdown.New(
		markdown.WithLinkPlaceholder(config.App().DocPlaceholder.Link),
		markdown.WithImgPlaceholder(config.App().DocPlaceholder.Img),
	)
	switch r := reflect.ValueOf(rsp).Interface().(type) {
	case *pb.SearchRsp:
		for i, v := range r.GetDocs() {
			r.Docs[i].Answer, r.Docs[i].AnswerPlaceholders = extractPlaceholder(md, v.GetAnswer())
			r.Docs[i].Question, r.Docs[i].QuestionPlaceholders = extractPlaceholder(md, v.GetQuestion())
			r.Docs[i].OrgData, r.Docs[i].OrgDataPlaceholders = extractPlaceholder(md, v.GetOrgData())
		}
	case *pb.SearchPreviewRsp:
		for i, v := range r.GetDocs() {
			r.Docs[i].Answer, r.Docs[i].AnswerPlaceholders = extractPlaceholder(md, v.GetAnswer())
			r.Docs[i].Question, r.Docs[i].QuestionPlaceholders = extractPlaceholder(md, v.GetQuestion())
			r.Docs[i].OrgData, r.Docs[i].OrgDataPlaceholders = extractPlaceholder(md, v.GetOrgData())
		}
	case *pb.CustomSearchRsp:
		for i, v := range r.GetDocs() {
			r.Docs[i].Answer, r.Docs[i].AnswerPlaceholders = extractPlaceholder(md, v.GetAnswer())
			r.Docs[i].Question, r.Docs[i].QuestionPlaceholders = extractPlaceholder(md, v.GetQuestion())
			r.Docs[i].OrgData, r.Docs[i].OrgDataPlaceholders = extractPlaceholder(md, v.GetOrgData())
		}
	case *pb.CustomSearchPreviewRsp:
		for i, v := range r.GetDocs() {
			r.Docs[i].Answer, r.Docs[i].AnswerPlaceholders = extractPlaceholder(md, v.GetAnswer())
			r.Docs[i].Question, r.Docs[i].QuestionPlaceholders = extractPlaceholder(md, v.GetQuestion())
			r.Docs[i].OrgData, r.Docs[i].OrgDataPlaceholders = extractPlaceholder(md, v.GetOrgData())
		}
	case *pb.SearchRealtimeRsp:
		for i, v := range r.GetDocs() {
			r.Docs[i].Answer, r.Docs[i].AnswerPlaceholders = extractKnowledgePlaceholder(md, v.GetAnswer())
			r.Docs[i].Question, r.Docs[i].QuestionPlaceholders = extractKnowledgePlaceholder(md, v.GetQuestion())
			r.Docs[i].OrgData, r.Docs[i].OrgDataPlaceholders = extractKnowledgePlaceholder(md, v.GetOrgData())
		}
	}
	return rsp
}

func extractPlaceholder(md *markdown.Markdown, content string) (string, []*pb.Placeholder) {
	c, p := md.ExtractLinkWithPlaceholder([]byte(content))

	var placeholders []*pb.Placeholder
	for _, v := range p {
		placeholders = append(placeholders, &pb.Placeholder{
			Key:   v.Key,
			Value: v.Value,
		})
	}

	return string(c), placeholders
}

func extractKnowledgePlaceholder(md *markdown.Markdown, content string) (string, []*pb.Placeholder) {
	c, p := md.ExtractLinkWithPlaceholder([]byte(content))

	var placeholders []*pb.Placeholder
	for _, v := range p {
		placeholders = append(placeholders, &pb.Placeholder{
			Key:   v.Key,
			Value: v.Value,
		})
	}

	return string(c), placeholders
}
