// Package linker linker
package linker

import (
	"context"
	"git.woa.com/adp/common/x/trpcx/plugins/i18n"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/i18nkey"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"golang.org/x/exp/slices"
	"strings"
	"unicode/utf8"
)

// Linker 相邻数据合并
type Linker struct {
}

// Config .
type Config struct {
}

// Content .
type Content struct {
	Key             string // 带有相同 Key 的 Content 才会进行合并
	Value           string // 需要合并的 Value
	Start           int    // Value 开始位置
	End             int    // Value 结束位置
	Prefix          string // 前缀
	RetrievalPrefix string // retrieval检索服务返回的特殊前缀，比如“文档名：XXX\n文档片段：YYY”
	Keep            bool   // 是否保持, 如果为 true , 不合其他 Content 进行合并, 也不合并其他 Content
	Extra           any    // 额外关联的数据
	idx             int
}

// New .
func New() *Linker {
	return &Linker{}
}

// Merge 合并连续文本
func (l *Linker) Merge(ctx context.Context, contents []Content) []Content {
	contentsMap := make(map[string][]Content)
	// 按 key 分组
	for i, content := range contents {
		content.idx = i
		content.Value, content.RetrievalPrefix = l.trimPrefix(ctx, content.Prefix, content.Value)
		if _, ok := contentsMap[content.Key]; !ok {
			contentsMap[content.Key] = []Content{}
		}
		contentsMap[content.Key] = append(contentsMap[content.Key], content)
	}
	var merged []Content
	// 组内合并
	for _, values := range contentsMap {
		if len(values) <= 1 {
			merged = append(merged, values...)
			continue
		}
		slices.SortStableFunc(values, func(a, b Content) int {
			return a.Start - b.Start
		})
		groupMerged := []Content{values[0]}
		for i := 1; i < len(values); i++ {
			groupMerged = append(groupMerged[0:len(groupMerged)-1], l.merge(ctx, groupMerged[len(groupMerged)-1], values[i])...)
		}
		merged = append(merged, groupMerged...)
	}
	// 还原输入排序
	slices.SortStableFunc(merged, func(a, b Content) int {
		return a.idx - b.idx
	})
	var r []Content
	// 补齐 prefix
	for _, v := range merged {
		v.Value = v.RetrievalPrefix + v.Value
		r = append(r, v)
	}
	return r
}

func (l *Linker) merge(ctx context.Context, a Content, b Content) []Content {
	if a.Keep || b.Keep {
		return []Content{a, b}
	}

	// 2.4.0 和harryhlli，mobisysfeng，springxchen 讨论
	// bad case：https://tapd.woa.com/qrobot_case/bugtrace/bugs/view?bug_id=1070108476127191751
	// 这里做一个开关和特殊逻辑判断：当两个切片的长度都小于阈值的时候才做合并否则不做合并
	if utilConfig.GetMainConfig().LinkerMergeConfig.IsOpenLengthLimit {
		if utf8.RuneCountInString(a.Value) > utilConfig.GetMainConfig().LinkerMergeConfig.MergeLengthLimit ||
			utf8.RuneCountInString(b.Value) > utilConfig.GetMainConfig().LinkerMergeConfig.MergeLengthLimit {
			return []Content{a, b}
		}
	}

	if a.Start == b.Start && a.End == b.End { // a 和 b 完全重叠
		return []Content{a}
	}
	if b.Start <= a.Start && b.End >= a.Start { // a 起点落在 b 内
		a, b = b, a
	}
	if a.Start <= b.Start && a.End >= b.Start { // b 起点落在 a 内
		if a.End >= b.End { // b 终点也落在 a 内
			return []Content{a}
		}
		// a 的尾部和 b 的头部重叠
		ra := []rune(a.Value)
		for i := 0; i < len(ra); i++ {
			if strings.HasPrefix(b.Value, string(ra[i:])) {
				return []Content{{
					Prefix:          a.Prefix,
					RetrievalPrefix: a.RetrievalPrefix,
					Key:             a.Key,
					Value:           string(ra[:i]) + b.Value,
					Start:           a.Start,
					End:             b.End,
					Keep:            false,
					Extra:           a.Extra,
					idx:             a.idx,
				}}
			}
		}
	}
	return []Content{a, b}
}

func generateRetrievePrefixStr(ctx context.Context, titlePrefix string) string {
	return i18n.Translate(ctx, i18nkey.KeyRetrievalFileNamePrefix) +
		strings.TrimRight(titlePrefix, "\n：: ") + "\n" +
		i18n.Translate(ctx, i18nkey.KeyRetrievalDocSegmentPrefix)
}

func (l *Linker) trimPrefix(ctx context.Context, titlePrefix, content string) (string, string) {
	retrievalPrefixStr := generateRetrievePrefixStr(ctx, titlePrefix)
	if strings.HasPrefix(content, retrievalPrefixStr) {
		return strings.TrimPrefix(content, retrievalPrefixStr), retrievalPrefixStr
	} else if strings.HasPrefix(content, titlePrefix) {
		return strings.TrimPrefix(content, titlePrefix), titlePrefix
	}
	return content, titlePrefix
}
