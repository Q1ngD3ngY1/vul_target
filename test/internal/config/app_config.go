package config

import (
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	jsoniter "github.com/json-iterator/go"
)

// AppModel 机器人model配置
type AppModel map[string]AppModelDetail

// AppModelDetail 机器人model配置
type AppModelDetail struct {
	Prompt            string `yaml:"prompt" json:"prompt"`
	Path              string `yaml:"path" json:"path"`
	PromptWordsLimit  uint32 `yaml:"prompt_words_limit" json:"prompt_words_limit"`
	Target            string `yaml:"target" json:"target"`
	Type              uint32 `yaml:"type" json:"type"`
	HistoryLimit      uint32 `yaml:"history_limit" json:"history_limit"`
	HistoryWordsLimit uint32 `yaml:"history_words_limit" json:"history_words_limit"`
	ModelName         string `yaml:"model_name" json:"model_name"`
	ServiceName       string `yaml:"service_name" json:"service_name"`
	IsEnabled         bool   `yaml:"is_enabled" json:"is_enabled"`
}

// ToAppPB 转换为 map[string]*pb.RobotModelInfo
func (c AppModel) ToAppPB() map[string]*pb.AppModelInfo {
	m := make(map[string]*pb.AppModelInfo, len(c))
	for k, v := range c {
		m[k] = &pb.AppModelInfo{
			Prompt:            v.Prompt,
			PromptWordsLimit:  v.PromptWordsLimit,
			Path:              v.Path,
			Target:            v.Target,
			Type:              v.Type,
			HistoryLimit:      v.HistoryLimit,
			HistoryWordsLimit: v.HistoryWordsLimit,
			ModelName:         v.ModelName,
			ServiceName:       v.ServiceName,
			IsEnabled:         v.IsEnabled,
		}
	}
	return m
}

// ToPB 转换为 map[string]*pb.RobotModelInfo
func (c AppModel) ToPB() map[string]*pb.RobotModelInfo {
	m := make(map[string]*pb.RobotModelInfo, len(c))
	for k, v := range c {
		m[k] = &pb.RobotModelInfo{
			Prompt:            v.Prompt,
			PromptWordsLimit:  v.PromptWordsLimit,
			Path:              v.Path,
			Target:            v.Target,
			Type:              v.Type,
			HistoryLimit:      v.HistoryLimit,
			HistoryWordsLimit: v.HistoryWordsLimit,
			ModelName:         v.ModelName,
			ServiceName:       v.ServiceName,
			IsEnabled:         v.IsEnabled,
		}
	}
	return m
}

// AppModelConfig 应用模型配置
type AppModelConfig struct {
	KnowledgeQaAppModel AppModel `yaml:"knowledge_qa_model"`
	SummaryAppModel     AppModel `yaml:"summary_model"`
	ClassifyAppModel    AppModel `yaml:"classify_model"`
}

// AppDefaultModelConfig 应用默认model配置
type AppDefaultModelConfig map[string][]AppModelInfo

// AppModelInfo 模型信息
type AppModelInfo struct {
	Name      string            `yaml:"name"` // 内部名称
	Desc      string            `yaml:"desc"`
	AliasName string            `yaml:"alias_name"` // 对外名称
	IsDefault bool              `yaml:"is_default"` // 默认模型，创建应用的时候，使用
	Scenes    map[string]uint32 `yaml:"scenes"`     // 支持的场景列表

	// 扩展协议，主要应对知识摘要和标签提取判别
	Prompt            string `yaml:"prompt" json:"prompt"`
	PromptWordsLimit  uint32 `yaml:"prompt_words_limit" json:"prompt_words_limit"`
	HistoryLimit      uint32 `yaml:"history_limit" json:"history_limit"`
	HistoryWordsLimit uint32 `yaml:"history_words_limit" json:"history_words_limit"`
	ServiceName       string `yaml:"service_name" json:"service_name"`
}

// SupportScenes 当前场景是否支持
func (a AppModelInfo) SupportScenes(scenes string) bool {
	if a.IsDefault && len(scenes) == 0 {
		return true
	}
	if len(scenes) == 0 || len(a.Scenes) == 0 {
		return false
	}
	if _, ok := a.Scenes[scenes]; ok {
		return true
	}
	return false
}

// ParseRobotModelFromPB .
func ParseRobotModelFromPB(mapRobotModel map[string]*pb.RobotModelInfo) AppModel {
	r := make(AppModel)
	for k, v := range mapRobotModel {
		r[k] = AppModelDetail{
			Prompt:            v.GetPrompt(),
			Path:              v.GetPath(),
			PromptWordsLimit:  v.GetPromptWordsLimit(),
			Target:            v.GetTarget(),
			Type:              v.GetType(),
			HistoryLimit:      v.GetHistoryLimit(),
			HistoryWordsLimit: v.GetHistoryWordsLimit(),
			ModelName:         v.GetModelName(),
			ServiceName:       v.GetServiceName(),
			IsEnabled:         v.GetIsEnabled(),
		}
	}
	return r
}

// MarshalToString .
func (c AppModel) MarshalToString() (string, error) {
	if len(c) == 0 {
		return "", nil
	}
	return jsoniter.MarshalToString(c)
}

// AppNames 应用名称
type AppNames map[string]string

// GetAppDefaultName 获取应用默认名称
func (n AppNames) GetAppDefaultName(appType string) (string, error) {
	if v, ok := n[appType]; ok {
		return v, nil
	}
	return "", errs.ErrAppTypeInvalid
}

// ParseAppModelFromAppPB .
func ParseAppModelFromAppPB(mapAppModel map[string]*pb.AppModelInfo) AppModel {
	r := make(AppModel)
	for k, v := range mapAppModel {
		r[k] = AppModelDetail{
			Prompt:            v.GetPrompt(),
			Path:              v.GetPath(),
			PromptWordsLimit:  v.GetPromptWordsLimit(),
			Target:            v.GetTarget(),
			Type:              v.GetType(),
			HistoryLimit:      v.GetHistoryLimit(),
			HistoryWordsLimit: v.GetHistoryWordsLimit(),
			ModelName:         v.GetModelName(),
			ServiceName:       v.GetServiceName(),
			IsEnabled:         v.GetIsEnabled(),
		}
	}
	return r
}

// ParseRobotDocSplitFromAppPB .
func ParseRobotDocSplitFromAppPB(mapSplitDoc map[string]*pb.AppSplitDoc) RobotDocSplit {
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

// ParseSearchVectorFromAppPB .
func ParseSearchVectorFromAppPB(searchVector *pb.AppSearchVector) *SearchVector {
	if searchVector == nil {
		return nil
	}
	return &SearchVector{
		Confidence: searchVector.GetConfidence(),
		TopN:       searchVector.GetTopN(),
	}
}

// ParseRobotFiltersFromAppPB .
func ParseRobotFiltersFromAppPB(mapAppFilters map[string]*pb.AppFilters) RobotFilters {
	r := make(RobotFilters)
	for k, v := range mapAppFilters {
		filters := make([]RobotFilterDetail, 0, len(v.GetFilter()))
		for _, f := range v.GetFilter() {
			filters = append(filters, RobotFilterDetail{
				TopN:       f.GetTopN(),
				DocType:    f.GetDocType(),
				IndexID:    f.GetIndexId(),
				Confidence: f.GetConfidence(),
				IsEnabled:  f.GetIsEnable(),
				RougeScore: RougeScore{
					F: f.GetRougeScore().GetF(),
					P: f.GetRougeScore().GetP(),
					R: f.GetRougeScore().GetR(),
				},
			})
		}
		r[k] = RobotFilter{TopN: v.GetTopN(), Filter: filters}
	}
	return r
}

// ManagedCorpIds 体验中心管理账户配置
type ManagedCorpIds []uint64

// Contains 包含corpID
func (m ManagedCorpIds) Contains(corpID uint64) bool {
	if len(m) == 0 {
		return false
	}
	for i := range m {
		if m[i] != corpID {
			continue
		}
		return true
	}
	return false
}
