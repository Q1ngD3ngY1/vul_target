package config

import (
	"context"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"

	"git.code.oa.com/trpc-go/trpc-go/log"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	jsoniter "github.com/json-iterator/go"
)

const (
	// MergerAmount 按数量合并
	MergerAmount = "amount"
	// MergerLength 按长度合并
	MergerLength = "length"
	// SplitterSentence 按句切分
	SplitterSentence = "sentence"
	// SplitterToken 按token切分
	SplitterToken = "token"
)

// SearchVector  检索相似问题
type SearchVector struct {
	Confidence float32 `yaml:"confidence" json:"confidence"` // 置信度
	TopN       uint32  `yaml:"top_n" json:"top_n"`           // 最多匹配条数
}

// RobotFilters 机器人索引配置
type RobotFilters map[string]RobotFilter

// RobotFilter 机器人索引配置
type RobotFilter struct {
	TopN   uint32              `yaml:"top_n" json:"top_n"`
	Filter []RobotFilterDetail `yaml:"filter" json:"filter"`
}

// RougeScore Rouge 分数
type RougeScore struct {
	F float64 `yaml:"f" json:"f"`
	P float64 `yaml:"p" json:"p"`
	R float64 `yaml:"r" json:"r"`
}

// RobotFilterDetail 机器人索引配置
type RobotFilterDetail struct {
	DocType    uint32     `yaml:"doc_type" json:"doc_type"`
	IndexID    uint32     `yaml:"index_id" json:"index_id"`
	Confidence float32    `yaml:"confidence" json:"confidence"`
	TopN       uint32     `yaml:"top_n" json:"top_n"`
	RougeScore RougeScore `yaml:"rouge_score" json:"rouge_score"`
	IsEnabled  bool       `yaml:"is_enable" json:"is_enable"`
}

// RobotDocSplit 问答切分重组配置
type RobotDocSplit map[string]PaginationConfig

// PaginationConfig 切分合并配置
type PaginationConfig struct {
	PatternSplitterConfig PatternSplitterConfig `yaml:"pattern_splitter_config" json:"pattern_splitter_config"` // 模式切分配置
	ParserConfig          ParserConfig          `yaml:"parser_config" json:"parser_config"`                     // 解析配置
	SplitterConfig        SplitterConfig        `yaml:"splitter_config" json:"splitter_config"`                 // 切分配置
	MergerConfig          MergerConfig          `yaml:"merger_config" json:"merger_config"`                     // 合并配置
	RechunkConfig         RechunkConfig         `yaml:"rechunk_config" json:"rechunk_config"`                   // 重组配置
}

// ParserConfig 分页配置
type ParserConfig struct {
	SingleParagraph bool `yaml:"single_paragraph" json:"single_paragraph"` // 是否作为一整个段落处理
	SplitSubTable   bool `yaml:"split_sub_table" json:"split_sub_table"`   // 是否切分子表
}

// SplitterConfig 拆分配置
type SplitterConfig struct {
	Splitter               string                 `yaml:"splitter" json:"splitter"`                                 // 切分器
	SplitterSentenceConfig SplitterSentenceConfig `yaml:"splitter_sentence_config" json:"splitter_sentence_config"` // 按句拆分配置
	SplitterTokenConfig    SplitterTokenConfig    `yaml:"splitter_token_config" json:"splitter_token_config"`       // 按 token 拆分配置
}

// PatternSplitterConfig 模式切分配置
type PatternSplitterConfig struct {
	RegexpJSON string `yaml:"regexp_json" json:"regexp_json"`
}

// SplitterSentenceConfig 按句拆分配置
type SplitterSentenceConfig struct {
	EnableTable        bool     `yaml:"enable_table" json:"enable_table"`                   // 是否处理表格
	EnableImage        bool     `yaml:"enable_image" json:"enable_image"`                   // 是否处理图片
	SentenceSymbols    []string `yaml:"sentence_symbols" json:"sentence_symbols"`           // 句终止符
	MaxMiniChunkLength uint     `yaml:"max_mini_chunk_length" json:"max_mini_chunk_length"` // 最大分块大小
}

// SplitterTokenConfig 按 token 拆分配置
type SplitterTokenConfig struct {
	EnableTable     bool `yaml:"enable_table" json:"enable_table"`           // 是否处理表格
	EnableImage     bool `yaml:"enable_image" json:"enable_image"`           // 是否处理图片
	MiniChunkLength uint `yaml:"mini_chunk_length" json:"mini_chunk_length"` // 分块大小 (uint: rune)
}

// MergerConfig 合并配置
type MergerConfig struct {
	Merger             string             `yaml:"merger" json:"merger"`                             // 合并器
	MergerAmountConfig MergerAmountConfig `yaml:"merger_amount_config" json:"merger_amount_config"` // 按数量合并
	MergerLengthConfig MergerLengthConfig `yaml:"merger_length_config" json:"merger_length_config"` // 按长度合并
}

// MergerAmountConfig 按数量合并
type MergerAmountConfig struct {
	// 文本分页结构
	// 例: HeadOverlapSize: 1, TailOverlapSize: 1, PageContentSize: 7 (MiniChunkLength: 50)
	//     则总长度为 1 * 50 + 7 * 50 + 1 * 50 = 450
	// mini_chunk -------> HeadOverlapSize -------> 1 * 50 = 50
	// ----------------------------
	// mini_chunk 1               |
	// mini_chunk 2               |
	// mini_chunk 3               v
	// mini_chunk 4        PageContentSize -------> 7 * 50 = 350
	// mini_chunk 5               ^
	// mini_chunk 6               |
	// mini_chunk 7               |
	// ----------------------------
	// mini_chunk -------> TailOverlapSize -------> 1 * 50 = 50

	PageContentSize uint `yaml:"page_content_size" json:"page_content_size"` // 分页大小 (unit: count of mini chunk)
	HeadOverlapSize uint `yaml:"head_overlap_size" json:"head_overlap_size"` // 重组为分页时, 头部分块的重叠数 (unit: count of mini chunk)
	TailOverlapSize uint `yaml:"tail_overlap_size" json:"tail_overlap_size"` // 重组为分页时, 尾部分块的重叠数 (unit: count of mini chunk)

	// 表格分页结构
	// 例: TablePageContentLength: 400, TableHeadOverlapSize: 1, TableTailOverlapSize: 1 (MiniChunkLength: 50)
	// mini_chunk -------> TableHeadOverlapSize -------> 1 * 50 = 50
	// ----------------------------
	// table_header               |
	// table_row 1                v
	// table_row 2     TablePageContentLength -------> table_header + 4 * table_row ≤ 400
	// table_row 3                ^
	// table_row 4                |
	// ----------------------------
	// mini_chunk -------> TableTailOverlapSize -------> 1 * 50 = 50

	TablePageContentLength uint   `yaml:"table_page_content_length" json:"table_page_content_length"` // 表格分页长度 (unit: rune)
	TableHeadOverlapSize   uint   `yaml:"table_head_overlap_size" json:"table_head_overlap_size"`     // 表格重组为分页时, 头部分块的重叠数 (unit: count of mini chunk)
	TableTailOverlapSize   uint   `yaml:"table_tail_overlap_size" json:"table_tail_overlap_size"`     // 表格重组为分页时, 尾部分块的重叠数 (unit: count of mini chunk)
	PageContentPrefix      string `yaml:"page_content_prefix" json:"page_content_prefix"`             // 文本前缀
}

// MergerLengthConfig 按长度合并
type MergerLengthConfig struct {
	// 文本分页结构
	// 例: HeadOverlapLength: 50, TailOverlapLength: 50, PageContentLength: 200
	//     则总长度为 50 + 50 + 200 = 300
	// mini_chunk -------> HeadOverlapLength -------> ≤ 50
	// ----------------------------
	// mini_chunk 1               |
	// mini_chunk 2               |
	// mini_chunk 3               v
	// mini_chunk 4        PageContentLength -------> ≤ 200
	// mini_chunk 5               ^
	// mini_chunk 6               |
	// mini_chunk 7               |
	// ----------------------------
	// mini_chunk -------> TailOverlapLength -------> ≤ 50
	MiniChunkLength   uint `yaml:"mini_chunk_length" json:"mini_chunk_length"`     // MiniChunk 长度
	PageContentLength uint `yaml:"page_content_length" json:"page_content_length"` // 分页长度 (unit: rune)
	HeadOverlapLength uint `yaml:"head_overlap_length" json:"head_overlap_length"` // 重组为分页时, 头部分块的长度 (unit: rune)
	TailOverlapLength uint `yaml:"tail_overlap_length" json:"tail_overlap_length"` // 重组为分页时, 尾部分块的长度 (unit: rune)

	// 表格分页结构
	// 例: TablePageContentLength: 400, TableHeadOverlapLength: 50, TableTailOverlapLength: 50
	// mini_chunk -------> TableHeadOverlapLength -------> ≤ 50
	// ----------------------------
	// table_header               |
	// table_row 1                v
	// table_row 2     TablePageContentLength -------> table_header + 4 * table_row ≤ 400
	// table_row 3                ^
	// table_row 4                |
	// ----------------------------
	// mini_chunk -------> TableTailOverlapLength -------> ≤ 50

	TablePageContentLength uint   `yaml:"table_page_content_length" json:"table_page_content_length"` // 表格分页长度 (unit: rune)
	TableHeadOverlapLength uint   `yaml:"table_head_overlap_length" json:"table_head_overlap_length"` // 表格重组为分页时, 头部分块的长度 (unit: rune)
	TableTailOverlapLength uint   `yaml:"table_tail_overlap_length" json:"table_tail_overlap_length"` // 表格重组为分页时, 尾部分块的长度 (unit: rune)
	PageContentPrefix      string `yaml:"page_content_prefix" json:"page_content_prefix"`             // 文本前缀
}

// RechunkConfig 重组配置
type RechunkConfig struct {
	HeadOverlapSize uint     `yaml:"head_overlap_size" json:"head_overlap_size"` // 重组时, 头部分块的重组数 (unit: count of mini chunk)
	TailOverlapSize uint     `yaml:"tail_overlap_size" json:"tail_overlap_size"` // 重组时, 尾部分块的重组数 (unit: count of mini chunk)
	TrimBySymbols   []string `yaml:"trim_by_symbols" json:"trim_by_symbols"`     // 句终止符
}

// RobotEmbedding embedding 配置
type RobotEmbedding struct {
	Version        uint64 `yaml:"version" json:"version"`
	UpgradeVersion uint64 `yaml:"upgrade_version" json:"upgrade_version"`
}

// ToPB 转换为 *pb.RobotSplitDoc
func (c RobotDocSplit) ToPB() map[string]*pb.RobotSplitDoc {
	r := make(map[string]*pb.RobotSplitDoc)
	for k, v := range c {
		r[k] = &pb.RobotSplitDoc{
			ParserConfig:          v.ParserConfig.ToPB(),
			PatternSplitterConfig: v.PatternSplitterConfig.ToPB(),
			SplitterConfig:        v.SplitterConfig.ToPB(),
			MergerConfig:          v.MergerConfig.ToPB(),
			RechunkConfig:         v.RechunkConfig.ToPB(),
		}
	}
	return r
}

// ToPB 转换为 *pb.RobotSplitDocParserConfig
func (c *ParserConfig) ToPB() *pb.RobotSplitDocParserConfig {
	return &pb.RobotSplitDocParserConfig{SingleParagraph: c.SingleParagraph}
}

// ToAppPB 转换为 *pb.AppSplitDocParserConfig
func (c *ParserConfig) ToAppPB() *pb.AppSplitDocParserConfig {
	return &pb.AppSplitDocParserConfig{SingleParagraph: c.SingleParagraph}
}

// ToPB 转换为 *pb.RobotSplitDocPatternSplitterConfig
func (c *PatternSplitterConfig) ToPB() *pb.RobotSplitDocPatternSplitterConfig {
	return &pb.RobotSplitDocPatternSplitterConfig{RegexpJson: c.RegexpJSON}
}

// ToAppPB 转换为 *pb.AppSplitDocPatternSplitterConfig
func (c *PatternSplitterConfig) ToAppPB() *pb.AppSplitDocPatternSplitterConfig {
	return &pb.AppSplitDocPatternSplitterConfig{RegexpJson: c.RegexpJSON}
}

// ToPB 转换为 pagination.Splitter
func (c *SplitterConfig) ToPB() *pb.RobotSplitDocSplitterConfig {
	splitterConfig := &pb.RobotSplitDocSplitterConfig{
		Splitter: c.Splitter,
	}
	switch c.Splitter {
	case SplitterSentence:
		splitterConfig.SplitterSentenceConfig = &pb.SplitterSentenceConfig{
			EnableTable:        c.SplitterSentenceConfig.EnableTable,
			EnableImage:        c.SplitterSentenceConfig.EnableImage,
			SentenceSymbols:    c.SplitterSentenceConfig.SentenceSymbols,
			MaxMiniChunkLength: uint64(c.SplitterSentenceConfig.MaxMiniChunkLength),
		}
	case SplitterToken:
		splitterConfig.SplitterTokenConfig = &pb.SplitterTokenConfig{
			EnableTable:     c.SplitterTokenConfig.EnableTable,
			EnableImage:     c.SplitterTokenConfig.EnableImage,
			MiniChunkLength: uint32(c.SplitterTokenConfig.MiniChunkLength),
		}
	}
	return splitterConfig
}

// ToAppPB 转换为 pagination.Splitter
func (c *SplitterConfig) ToAppPB() *pb.AppSplitDocSplitterConfig {
	splitterConfig := &pb.AppSplitDocSplitterConfig{
		Splitter: c.Splitter,
	}
	switch c.Splitter {
	case SplitterSentence:
		splitterConfig.SplitterSentenceConfig = &pb.SplitterSentenceConfig{
			EnableTable:        c.SplitterSentenceConfig.EnableTable,
			EnableImage:        c.SplitterSentenceConfig.EnableImage,
			SentenceSymbols:    c.SplitterSentenceConfig.SentenceSymbols,
			MaxMiniChunkLength: uint64(c.SplitterSentenceConfig.MaxMiniChunkLength),
		}
	case SplitterToken:
		splitterConfig.SplitterTokenConfig = &pb.SplitterTokenConfig{
			EnableTable:     c.SplitterTokenConfig.EnableTable,
			EnableImage:     c.SplitterTokenConfig.EnableImage,
			MiniChunkLength: uint32(c.SplitterTokenConfig.MiniChunkLength),
		}
	}
	return splitterConfig
}

// ToMergerParse 转换为 pagination.Merger
func ToMergerParse(prefix string, c MergerConfig) (MergerConfig, error) {
	mergerConfig := c
	if c.Merger == MergerAmount {
		mergerConfig.MergerAmountConfig.PageContentPrefix = prefix
		mergerConfig.MergerAmountConfig.HeadOverlapSize = c.MergerAmountConfig.HeadOverlapSize
		mergerConfig.MergerAmountConfig.PageContentSize = c.MergerAmountConfig.PageContentSize
		mergerConfig.MergerAmountConfig.TailOverlapSize = c.MergerAmountConfig.TailOverlapSize
		mergerConfig.MergerAmountConfig.TableHeadOverlapSize = c.MergerAmountConfig.TableHeadOverlapSize
		mergerConfig.MergerAmountConfig.TablePageContentLength = c.MergerAmountConfig.TablePageContentLength
		mergerConfig.MergerAmountConfig.TableTailOverlapSize = c.MergerAmountConfig.TableTailOverlapSize
		return mergerConfig, nil
	} else if c.Merger == MergerLength {
		mergerConfig.MergerLengthConfig.MiniChunkLength = 50
		mergerConfig.MergerLengthConfig.PageContentPrefix = prefix
		mergerConfig.MergerLengthConfig.HeadOverlapLength = c.MergerLengthConfig.HeadOverlapLength
		mergerConfig.MergerLengthConfig.PageContentLength = c.MergerLengthConfig.PageContentLength
		mergerConfig.MergerLengthConfig.TailOverlapLength = c.MergerLengthConfig.TailOverlapLength
		mergerConfig.MergerLengthConfig.TableHeadOverlapLength = c.MergerLengthConfig.TableHeadOverlapLength
		mergerConfig.MergerLengthConfig.TablePageContentLength = c.MergerLengthConfig.TablePageContentLength
		mergerConfig.MergerLengthConfig.TableTailOverlapLength = c.MergerLengthConfig.TableTailOverlapLength
		return mergerConfig, nil
	} else {
		return mergerConfig, errs.ErrUnknownMergerType
	}
}

// ToPB 转换为 pagination.Merger
func (c MergerConfig) ToPB() *pb.RobotSplitDocMergerConfig {
	splitDocMergerConfig := &pb.RobotSplitDocMergerConfig{
		Merger: c.Merger,
	}
	switch c.Merger {
	case MergerAmount:
		splitDocMergerConfig.MergerAmountConfig = &pb.MergerAmountConfig{
			PageContentSize:        uint32(c.MergerAmountConfig.PageContentSize),
			HeadOverlapSize:        uint32(c.MergerAmountConfig.HeadOverlapSize),
			TailOverlapSize:        uint32(c.MergerAmountConfig.TailOverlapSize),
			TablePageContentLength: uint32(c.MergerAmountConfig.TablePageContentLength),
			TableHeadOverlapSize:   uint32(c.MergerAmountConfig.TableHeadOverlapSize),
			TableTailOverlapSize:   uint32(c.MergerAmountConfig.TableTailOverlapSize),
			TrimBySymbols:          []string{"。"},
		}
	case MergerLength:
		splitDocMergerConfig.MergerLengthConfig = &pb.MergerLengthConfig{
			PageContentLength:      uint32(c.MergerLengthConfig.PageContentLength),
			HeadOverlapLength:      uint32(c.MergerLengthConfig.HeadOverlapLength),
			TailOverlapLength:      uint32(c.MergerLengthConfig.TailOverlapLength),
			TablePageContentLength: uint32(c.MergerLengthConfig.TablePageContentLength),
			TableHeadOverlapLength: uint32(c.MergerLengthConfig.TableHeadOverlapLength),
			TableTailOverlapLength: uint32(c.MergerLengthConfig.TableTailOverlapLength),
			TrimBySymbols:          []string{"。"},
		}

	}
	return splitDocMergerConfig
}

// ToAppPB 转换为 pagination.Merger
func (c MergerConfig) ToAppPB() *pb.AppSplitDocMergerConfig {
	splitDocMergerConfig := &pb.AppSplitDocMergerConfig{
		Merger: c.Merger,
	}
	switch c.Merger {
	case MergerAmount:
		splitDocMergerConfig.MergerAmountConfig = &pb.MergerAmountConfig{
			PageContentSize:        uint32(c.MergerAmountConfig.PageContentSize),
			HeadOverlapSize:        uint32(c.MergerAmountConfig.HeadOverlapSize),
			TailOverlapSize:        uint32(c.MergerAmountConfig.TailOverlapSize),
			TablePageContentLength: uint32(c.MergerAmountConfig.TablePageContentLength),
			TableHeadOverlapSize:   uint32(c.MergerAmountConfig.TableHeadOverlapSize),
			TableTailOverlapSize:   uint32(c.MergerAmountConfig.TableTailOverlapSize),
			TrimBySymbols:          []string{"。"},
		}
	case MergerLength:
		splitDocMergerConfig.MergerLengthConfig = &pb.MergerLengthConfig{
			PageContentLength:      uint32(c.MergerLengthConfig.PageContentLength),
			HeadOverlapLength:      uint32(c.MergerLengthConfig.HeadOverlapLength),
			TailOverlapLength:      uint32(c.MergerLengthConfig.TailOverlapLength),
			TablePageContentLength: uint32(c.MergerLengthConfig.TablePageContentLength),
			TableHeadOverlapLength: uint32(c.MergerLengthConfig.TableHeadOverlapLength),
			TableTailOverlapLength: uint32(c.MergerLengthConfig.TableTailOverlapLength),
			TrimBySymbols:          []string{"。"},
		}

	}
	return splitDocMergerConfig
}

// ToPB 转换为 rechunk.Config
func (c *RechunkConfig) ToPB() *pb.RobotSplitDocRechunkConfig {
	return &pb.RobotSplitDocRechunkConfig{
		HeadOverlapSize: uint32(c.HeadOverlapSize),
		TailOverlapSize: uint32(c.TailOverlapSize),
		TrimBySymbols:   c.TrimBySymbols,
	}
}

// ToAppPB 转换为 rechunk.Config
func (c *RechunkConfig) ToAppPB() *pb.AppSplitDocRechunkConfig {
	return &pb.AppSplitDocRechunkConfig{
		HeadOverlapSize: uint32(c.HeadOverlapSize),
		TailOverlapSize: uint32(c.TailOverlapSize),
		TrimBySymbols:   c.TrimBySymbols,
	}
}

// ToPB 转换为 map[string]*pb.RobotFilters
func (c RobotFilters) ToPB() map[string]*pb.RobotFilters {
	r := make(map[string]*pb.RobotFilters)
	for k, v := range c {
		r[k] = &pb.RobotFilters{TopN: v.TopN}
		for _, f := range v.Filter {
			r[k].Filter = append(r[k].Filter, &pb.RobotFiltersInfo{
				DocType:    f.DocType,
				IndexId:    f.IndexID,
				Confidence: f.Confidence,
				TopN:       f.TopN,
			})
		}
	}
	return r
}

// ToAppPB 转换为 map[string]*pb.RobotFilters
func (c RobotFilters) ToAppPB() map[string]*pb.AppFilters {
	r := make(map[string]*pb.AppFilters)
	for k, v := range c {
		r[k] = &pb.AppFilters{TopN: v.TopN}
		for _, f := range v.Filter {
			r[k].Filter = append(r[k].Filter, &pb.AppFiltersInfo{
				DocType:    f.DocType,
				IndexId:    f.IndexID,
				Confidence: f.Confidence,
				TopN:       f.TopN,
				IsEnable:   f.IsEnabled,
				RougeScore: &pb.RougeScore{
					F: f.RougeScore.F,
					P: f.RougeScore.P,
					R: f.RougeScore.R,
				},
			})
		}
	}
	return r
}

// ToPB 转换为 pb
func (c *SearchVector) ToPB() *pb.RobotSearchVector {
	return &pb.RobotSearchVector{Confidence: c.Confidence, TopN: c.TopN}
}

// ToAppPB 转换为 pb
func (c *SearchVector) ToAppPB() *pb.AppSearchVector {
	return &pb.AppSearchVector{Confidence: c.Confidence, TopN: c.TopN}
}

// GetSplitStrategy 获取解析策略
func (c RobotDocSplit) GetSplitStrategy(ctx context.Context, prefix string, typ string) (string, error) {
	conf, ok := c[typ]
	if !ok {
		log.ErrorContextf(ctx, "unknown split type: %s, RobotDocSplit: %+v", typ, c)
		return "", errs.ErrUnknownSplitConfig
	}
	var err error
	conf.MergerConfig, err = ToMergerParse(prefix, conf.MergerConfig)
	if err != nil {
		log.ErrorContextf(ctx, "获取拆分策略配置失败 conf.MergerConfig:%+v err:%+v", conf.MergerConfig, err)
		return "", errs.ErrUnknownSplitConfig
	}
	log.DebugContextf(ctx, "get splitStrategy config for type: %s, prefix: %s, conf: %+v", typ, prefix, conf)
	splitStrategy, err := jsoniter.MarshalToString(conf)
	log.DebugContextf(ctx, "get config splitStrategy: %s err:%+v", splitStrategy, err)
	if err != nil {
		log.ErrorContextf(ctx, "获取拆分策略配置失败 splitStrategy:%+v err:%+v", splitStrategy, err)
		return "", errs.ErrUnknownSplitConfig
	}
	return splitStrategy, nil
}

// ToAppPB 转换为 *pb.AppSplitDoc
func (c RobotDocSplit) ToAppPB() map[string]*pb.AppSplitDoc {
	r := make(map[string]*pb.AppSplitDoc)
	for k, v := range c {
		r[k] = &pb.AppSplitDoc{
			ParserConfig:          v.ParserConfig.ToAppPB(),
			PatternSplitterConfig: v.PatternSplitterConfig.ToAppPB(),
			SplitterConfig:        v.SplitterConfig.ToAppPB(),
			MergerConfig:          v.MergerConfig.ToAppPB(),
			RechunkConfig:         v.RechunkConfig.ToAppPB(),
		}
	}
	return r
}

// MarshalToString .
func (c *SearchVector) MarshalToString() (string, error) {
	if c == nil {
		return "", nil
	}
	return jsoniter.MarshalToString(c)
}

// MarshalToString .
func (c RobotFilters) MarshalToString() (string, error) {
	if len(c) == 0 {
		return "", nil
	}
	return jsoniter.MarshalToString(c)
}

// MarshalToString .
func (c RobotDocSplit) MarshalToString() (string, error) {
	if len(c) == 0 {
		return "", nil
	}
	return jsoniter.MarshalToString(c)
}

// ParseSearchVectorFromPB .
func ParseSearchVectorFromPB(searchVector *pb.RobotSearchVector) *SearchVector {
	if searchVector == nil {
		return nil
	}
	return &SearchVector{
		Confidence: searchVector.GetConfidence(),
		TopN:       searchVector.GetTopN(),
	}
}

// ParseRobotFiltersFromPB .
func ParseRobotFiltersFromPB(mapRobotFilters map[string]*pb.RobotFilters) RobotFilters {
	r := make(RobotFilters)
	for k, v := range mapRobotFilters {
		filters := make([]RobotFilterDetail, 0, len(v.GetFilter()))
		for _, f := range v.GetFilter() {
			filters = append(filters, RobotFilterDetail{
				TopN:       f.GetTopN(),
				DocType:    f.GetDocType(),
				IndexID:    f.GetIndexId(),
				Confidence: f.GetConfidence(),
			})
		}
		r[k] = RobotFilter{TopN: v.GetTopN(), Filter: filters}
	}
	return r
}

// ParseRobotDocSplitFromPB .
func ParseRobotDocSplitFromPB(mapSplitDoc map[string]*pb.RobotSplitDoc) RobotDocSplit {
	r := make(RobotDocSplit)
	for k, v := range mapSplitDoc {
		splitterConfig := SplitterConfig{
			Splitter: v.GetSplitterConfig().GetSplitter(),
		}
		switch v.GetSplitterConfig().GetSplitter() {
		case SplitterSentence:
			splitterConfig.SplitterSentenceConfig = SplitterSentenceConfig{
				EnableTable:        v.GetSplitterConfig().GetSplitterSentenceConfig().GetEnableTable(),
				EnableImage:        v.GetSplitterConfig().GetSplitterSentenceConfig().GetEnableImage(),
				SentenceSymbols:    v.GetSplitterConfig().GetSplitterSentenceConfig().GetSentenceSymbols(),
				MaxMiniChunkLength: uint(v.GetSplitterConfig().GetSplitterSentenceConfig().GetMaxMiniChunkLength()),
			}
		case SplitterToken:
			splitterConfig.SplitterTokenConfig = SplitterTokenConfig{
				EnableTable:     v.GetSplitterConfig().GetSplitterTokenConfig().GetEnableTable(),
				EnableImage:     v.GetSplitterConfig().GetSplitterTokenConfig().GetEnableImage(),
				MiniChunkLength: uint(v.GetSplitterConfig().GetSplitterTokenConfig().GetMiniChunkLength()),
			}
		}
		mergerConfig := MergerConfig{
			Merger: v.GetMergerConfig().GetMerger(),
		}
		switch v.GetMergerConfig().GetMerger() {
		case MergerAmount:
			mergerConfig.MergerAmountConfig = MergerAmountConfig{
				PageContentSize: uint(v.GetMergerConfig().GetMergerAmountConfig().GetPageContentSize()),
				HeadOverlapSize: uint(v.GetMergerConfig().GetMergerAmountConfig().GetHeadOverlapSize()),
				TailOverlapSize: uint(v.GetMergerConfig().GetMergerAmountConfig().GetTailOverlapSize()),
				TablePageContentLength: uint(
					v.GetMergerConfig().GetMergerAmountConfig().GetTablePageContentLength(),
				),
				TableHeadOverlapSize: uint(v.GetMergerConfig().GetMergerAmountConfig().GetTableHeadOverlapSize()),
				TableTailOverlapSize: uint(v.GetMergerConfig().GetMergerAmountConfig().GetTableTailOverlapSize()),
			}
		case MergerLength:
			mergerConfig.MergerLengthConfig = MergerLengthConfig{
				PageContentLength: uint(v.GetMergerConfig().GetMergerLengthConfig().GetPageContentLength()),
				HeadOverlapLength: uint(v.GetMergerConfig().GetMergerLengthConfig().GetHeadOverlapLength()),
				TailOverlapLength: uint(v.GetMergerConfig().GetMergerLengthConfig().GetTailOverlapLength()),
				TablePageContentLength: uint(
					v.GetMergerConfig().GetMergerLengthConfig().GetTablePageContentLength(),
				),
				TableHeadOverlapLength: uint(
					v.GetMergerConfig().GetMergerLengthConfig().GetTableHeadOverlapLength(),
				),
				TableTailOverlapLength: uint(
					v.GetMergerConfig().GetMergerLengthConfig().GetTableTailOverlapLength(),
				),
			}
		}
		r[k] = PaginationConfig{
			ParserConfig: ParserConfig{
				SingleParagraph: v.GetParserConfig().GetSingleParagraph(),
			},
			PatternSplitterConfig: PatternSplitterConfig{
				RegexpJSON: v.GetPatternSplitterConfig().GetRegexpJson(),
			},
			SplitterConfig: splitterConfig,
			MergerConfig:   mergerConfig,
			RechunkConfig: RechunkConfig{
				HeadOverlapSize: uint(v.GetRechunkConfig().GetHeadOverlapSize()),
				TailOverlapSize: uint(v.GetRechunkConfig().GetTailOverlapSize()),
				TrimBySymbols:   v.GetRechunkConfig().GetTrimBySymbols(),
			},
		}
	}
	return r
}

// MarshalToString .
func (c *RobotEmbedding) MarshalToString() (string, error) {
	if c == nil {
		return "", nil
	}
	return jsoniter.MarshalToString(c)
}

// ToPB .
func (c *RobotEmbedding) ToPB() *pb.RobotEmbedding {
	return &pb.RobotEmbedding{Version: c.Version}
}

// ToAppPB .
func (c *RobotEmbedding) ToAppPB() *pb.AppEmbedding {
	return &pb.AppEmbedding{Version: c.Version}
}

// ParseRobotEmbeddingFromPB .
func ParseRobotEmbeddingFromPB(embedding *pb.RobotEmbedding) *RobotEmbedding {
	if embedding == nil {
		return nil
	}
	return &RobotEmbedding{Version: embedding.GetVersion()}
}

// ToFilterMap .
func (r *RobotFilter) ToFilterMap() map[uint32]RobotFilterDetail {
	filter := make(map[uint32]RobotFilterDetail)
	for _, v := range r.Filter {
		filter[v.DocType] = v
	}
	return filter
}
