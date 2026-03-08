package dao

import (
	"context"
	"fmt"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/linker"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
	retrieval "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_retrieval_server"
)

// GetLinkContentsFromSearchResponse 从检索请求构造 linkContents
func (d *dao) GetLinkContentsFromSearchResponse(
	ctx context.Context, robotID uint64, docs []*retrieval.SearchRsp_Doc,
	qaFn func(doc *retrieval.SearchRsp_Doc, qa *model.DocQA) any,
	segmentFn func(doc *retrieval.SearchRsp_Doc, segment *model.DocSegmentExtend) any,
	searchEngineFn func(doc *retrieval.SearchRsp_Doc) any,
) ([]linker.Content, error) {
	linkContents := make([]linker.Content, 0, len(docs))
	var err error
	for _, doc := range docs {
		var linkContent linker.Content
		switch doc.GetDocType() {
		case model.DocTypeQA:
			linkContent, err = d.getReleaseQALinkContent(
				ctx, doc.GetRelatedId(), robotID,
				func(doc *retrieval.SearchRsp_Doc) func(qa *model.DocQA) any {
					return func(qa *model.DocQA) any {
						return qaFn(doc, qa)
					}
				}(doc),
			)
			if err != nil { // 做降级，不是直接失败
				log.WarnContextf(ctx, "getReleaseQALinkContent:%v", err)
				continue
			}
		case model.DocTypeSearchEngine:
			linkContent, err = d.getSearchEngineLinkContent(
				ctx,
				func(doc *retrieval.SearchRsp_Doc) func() any {
					return func() any {
						return searchEngineFn(doc)
					}
				}(doc),
			)
			if err != nil {
				return nil, err
			}
		case model.DocTypeSegment:
			linkContent, err = d.getReleaseSegmentLinkContent(
				ctx, robotID, doc,
				func(doc *retrieval.SearchRsp_Doc) func(segment *model.DocSegmentExtend) any {
					return func(segment *model.DocSegmentExtend) any {
						return segmentFn(doc, segment)
					}
				}(doc),
			)
			if err != nil { // 做降级，不是直接失败
				log.WarnContextf(ctx, "getReleaseSegmentLinkContent:%v", err)
				continue
			}
		default:
			return nil, errs.ErrUnknownIndexID
		}
		linkContents = append(linkContents, linkContent)
	}
	return linkContents, nil
}

// GetLinkContentsFromSearchVectorResponse 从检索请求构造 linkContents
func (d *dao) GetLinkContentsFromSearchVectorResponse(
	ctx context.Context, robotID uint64, docs []*retrieval.SearchVectorRsp_Doc,
	qaFn func(doc *retrieval.SearchVectorRsp_Doc, qa *model.DocQA) any,
	segmentFn func(doc *retrieval.SearchVectorRsp_Doc, segment *model.DocSegmentExtend) any,
	searchEngineFn func(doc *retrieval.SearchVectorRsp_Doc) any,
) ([]linker.Content, error) {
	linkContents := make([]linker.Content, 0, len(docs))
	var err error
	for _, doc := range docs {
		var linkContent linker.Content
		switch doc.GetDocType() {
		case model.DocTypeQA:
			linkContent, err = d.getPreviewQALinkContent(
				ctx, doc.GetId(),
				func(doc *retrieval.SearchVectorRsp_Doc) func(qa *model.DocQA) any {
					return func(qa *model.DocQA) any {
						return qaFn(doc, qa)
					}
				}(doc),
			)
			if err != nil {
				return nil, err
			}
		case model.DocTypeSearchEngine:
			linkContent, err = d.getSearchEngineLinkContent(
				ctx,
				func(doc *retrieval.SearchVectorRsp_Doc) func() any {
					return func() any {
						return searchEngineFn(doc)
					}
				}(doc),
			)
			if err != nil {
				return nil, err
			}
		case model.DocTypeSegment:
			linkContent, err = d.getPreviewSegmentLinkContent(
				ctx, robotID, doc,
				func(doc *retrieval.SearchVectorRsp_Doc) func(segment *model.DocSegmentExtend) any {
					return func(segment *model.DocSegmentExtend) any {
						return segmentFn(doc, segment)
					}
				}(doc),
			)
			if err != nil {
				return nil, err
			}
		default:
			return nil, errs.ErrUnknownIndexID
		}
		linkContents = append(linkContents, linkContent)
	}
	return linkContents, nil
}

// getPreviewSegmentLinkContent 获取文段合并内容
func (d *dao) getPreviewSegmentLinkContent(
	ctx context.Context, robotID uint64, doc *retrieval.SearchVectorRsp_Doc, fn func(segment *model.DocSegmentExtend) any,
) (linker.Content, error) {
	var seg *model.DocSegmentExtend
	var err error
	if doc.GetResultType() == retrieval.RetrievalResultType_TEXT2SQL { // text2sql的结果没有具体片段
		seg = &model.DocSegmentExtend{
			// 不做合并
			DocSegment: model.DocSegment{
				DocID:      doc.GetDocId(),
				LinkerKeep: true,
			},
		}
	} else { // 其他结果都有具体的片段
		seg, err = d.GetSegmentByID(ctx, doc.GetId(), robotID)
		if err != nil {
			log.ErrorContextf(ctx, "获取段落失败 segmentID: %d, err:%v ", doc.GetId(), err)
			return linker.Content{}, err
		}
	}
	if seg == nil {
		log.ErrorContextf(ctx, "段落不存在 segmentID: %d", doc.GetId())
		return linker.Content{}, errs.ErrSegmentNotFound
	}
	if seg.DocID != doc.GetDocId() {
		log.ErrorContextf(ctx, "段落和文档ID不匹配 seg.DocID: %d, doc.GetDocId():%d", seg.DocID, doc.GetDocId())
		return linker.Content{}, errs.ErrSegmentNotFound
	}
	startIndex := seg.StartChunkIndex
	endIndex := seg.EndChunkIndex
	// 如果是bigData则对应取bigData的start和end
	if doc.GetIsBigData() {
		startIndex = int(seg.BigStart)
		endIndex = int(seg.BigEnd)
		log.InfoContextf(ctx, "isBigData startIndex:%d,endIndex:%d,segmentID:%d,docID:%d",
			startIndex, endIndex, doc.GetId(), seg.DocID)
	}
	return linker.Content{
		Key:    fmt.Sprintf("%d-%d-%d", model.DocTypeSegment, seg.DocID, seg.RichTextIndex),
		Extra:  fn(seg),
		Value:  doc.GetOrgData(),
		Start:  startIndex,
		End:    endIndex,
		Prefix: seg.Title,
		Keep:   seg.LinkerKeep,
	}, nil
}

// getPreviewQALinkContent 获取问答合并内容
func (d *dao) getPreviewQALinkContent(
	ctx context.Context, qaID uint64, fn func(qa *model.DocQA) any,
) (linker.Content, error) {
	qa, err := d.GetQAByID(ctx, qaID)
	if err != nil {
		return linker.Content{}, err
	}
	if qa == nil {
		return linker.Content{}, errs.ErrQANotFound
	}
	return linker.Content{
		Extra: fn(qa),
		Keep:  true,
	}, nil
}

// getReleaseSegmentLinkContent 获取文段合并内容
func (d *dao) getReleaseSegmentLinkContent(
	ctx context.Context, robotID uint64, doc *retrieval.SearchRsp_Doc, fn func(segment *model.DocSegmentExtend) any,
) (linker.Content, error) {
	var seg *model.DocSegmentExtend
	var err error
	if doc.GetResultType() == retrieval.RetrievalResultType_TEXT2SQL { // text2sql的结果没有具体片段
		seg = &model.DocSegmentExtend{
			// 不做合并
			DocSegment: model.DocSegment{
				LinkerKeep: true,
			},
		}
	} else { // 其他结果都有具体的片段
		seg, err = d.GetSegmentByID(ctx, doc.GetRelatedId(), robotID)
		if err != nil {
			log.ErrorContextf(ctx, "获取段落失败 segmentID: %d, err:%v ", doc.GetRelatedId(), err)
			return linker.Content{}, err
		}
	}
	if seg == nil {
		log.ErrorContextf(ctx, "段落不存在 segmentID: %d", doc.GetRelatedId())
		return linker.Content{}, errs.ErrSegmentNotFound
	}
	startIndex := seg.StartChunkIndex
	endIndex := seg.EndChunkIndex
	// 如果是bigData则对应取bigData的start和end
	if doc.GetIsBigData() {
		startIndex = int(seg.BigStart)
		endIndex = int(seg.BigEnd)
		log.InfoContextf(ctx, "isBigData startIndex:%d,endIndex:%d,segmentID:%d,docID:%d",
			startIndex, endIndex, doc.GetRelatedId(), seg.DocID)
	}
	return linker.Content{
		Key:    fmt.Sprintf("%d-%d-%d", model.DocTypeSegment, seg.DocID, seg.RichTextIndex),
		Extra:  fn(seg),
		Value:  doc.GetOrgData(),
		Start:  startIndex,
		End:    endIndex,
		Prefix: seg.Title,
		Keep:   seg.LinkerKeep,
	}, nil
}

// getReleaseQALinkContent 获取QA合并内容
func (d *dao) getReleaseQALinkContent(
	ctx context.Context, qaID, robotID uint64, fn func(qa *model.DocQA) any,
) (linker.Content, error) {
	qa, err := d.GetQAByID(ctx, qaID)
	if err != nil {
		return linker.Content{}, err
	}
	if qa == nil {
		return linker.Content{}, errs.ErrQANotFound
	}
	if qa.RobotID != robotID && qa.ReleaseStatus != model.QAReleaseStatusInit { // 说明是共享知识库，只能使用待发布的数据
		log.WarnContextf(ctx, "获取QA详情失败 qaID: %d, 问答状态：%d", qaID, qa.ReleaseStatus)
		return linker.Content{}, errs.ErrSegmentNotFound
	}
	return linker.Content{
		Extra: fn(qa),
		Keep:  true,
	}, nil
}

// getSearchEngineLinkContent 获取搜索引合并内容
func (d *dao) getSearchEngineLinkContent(
	ctx context.Context, fn func() any,
) (linker.Content, error) {
	return linker.Content{
		Extra: fn(),
		Keep:  true,
	}, nil
}

// Link 连续文档合并
// 不支持泛型方法, 不作为 dao 的方法
func Link[T any](
	ctx context.Context, linkContents []linker.Content, fn func(T, linker.Content) T,
) []T {
	log.DebugContextf(ctx, "link.linkContents: %+v", linkContents)
	var r []T
	for _, v := range linker.New().Merge(ctx, linkContents) {
		d := v.Extra.(T)
		if !v.Keep {
			d = fn(d, v)
		}
		r = append(r, d)
	}
	for i, v := range r {
		log.DebugContextf(ctx, "link.r[%d]: %+v", i, v)
	}
	return r
}
