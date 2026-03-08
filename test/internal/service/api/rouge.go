package api

import (
	"regexp"
	"strings"

	"git.woa.com/baicaoyuan/moss/types/slicex"
	"github.com/go-ego/gse"
)

const whitespace = " \t\n\r\v\f"

var cutters = []*regexp.Regexp{
	regexp.MustCompile(`([。！？\?])([^”’])`),
	regexp.MustCompile(`(\.{6})([^”’])`),
	regexp.MustCompile(`(…{2})([^”’])`),
	regexp.MustCompile(`([。！？\?][”’])([^，。！？\?])`),
}

// Score represents the ROUGE-L score of a pair of sentences.
type Score struct {
	F float64
	P float64
	R float64
}

var seg gse.Segmenter

func init() {
	gse.ToLower = false
	seg.LoadDictEmbed()
	seg.LoadStopEmbed()
}

// GetRougeScore cuts two sentences and computes ROUGE-L (summary level) score of them.
func GetRougeScore(hyp string, ref string) Score {
	cutHyp := cut(hyp)
	cutRef := cut(ref)
	return rougeLSummaryLevel(cutHyp, cutRef)
}

func cut(s string) []string {
	items := slicex.Filter(cutSentence(s), func(s string) bool { return s != "" })
	for i, v := range items {
		items[i] = strings.Join(strings.Fields(v), " ")
	}
	return items
}

func cutSentence(para string) []string {
	for _, re := range cutters {
		para = re.ReplaceAllString(para, "$1\n$2")
	}
	para = strings.TrimRight(para, whitespace)
	return strings.Split(para, "\n")
}

// rougeLSummaryLevel computes ROUGE-L (summary level) of two text collections of sentences.
func rougeLSummaryLevel(evaluatedSentences, referenceSentences []string) Score {
	if len(evaluatedSentences) == 0 || len(referenceSentences) == 0 {
		return Score{}
	}

	refNgrams := []string{}
	for _, v := range referenceSentences {
		refNgrams = append(refNgrams, strings.Split(v, " ")...)
	}
	hypNgrams := []string{}
	for _, v := range evaluatedSentences {
		hypNgrams = append(hypNgrams, strings.Split(v, " ")...)
	}

	llcs := lcs(refNgrams, hypNgrams)
	r := float64(llcs) / float64(len(refNgrams))
	p := float64(llcs) / float64(len(hypNgrams))
	f := 2.0 * ((p * r) / (p + r + 1e-8))
	return Score{R: r, P: p, F: f}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// lcs computes the length of the longest common subsequence (lcs) between two strings.
func lcs(x, y []string) int {
	n, m := len(x), len(y)

	dp := make([][]int, n+1)
	for i := 0; i <= n; i++ {
		dp[i] = make([]int, m+1)
	}

	for i := 0; i <= n; i++ {
		for j := 0; j <= m; j++ {
			if i == 0 || j == 0 {
				dp[i][j] = 0
			} else if x[i-1] == y[j-1] {
				dp[i][j] = dp[i-1][j-1] + 1
			} else {
				dp[i][j] = max(dp[i-1][j], dp[i][j-1])
			}
		}
	}

	return dp[n][m]
}
