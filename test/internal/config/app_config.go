package config

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
	PromptVersion     string `yaml:"prompt_version" json:"prompt_version"`
}

// ToAppPB 转换为 map[string]*pb.RobotModelInfo
// func (c AppModel) ToAppPB() map[string]*pb.AppModelInfo {
// 	m := make(map[string]*pb.AppModelInfo, len(c))
// 	for k, v := range c {
// 		m[k] = &pb.AppModelInfo{
// 			Prompt:            v.Prompt,
// 			PromptWordsLimit:  v.PromptWordsLimit,
// 			Path:              v.Path,
// 			Target:            v.Target,
// 			Type:              v.Type,
// 			HistoryLimit:      v.HistoryLimit,
// 			HistoryWordsLimit: v.HistoryWordsLimit,
// 			ModelName:         v.ModelName,
// 			ServiceName:       v.ServiceName,
// 			IsEnabled:         v.IsEnabled,
// 		}
// 	}
// 	return m
// }

// ToPB 转换为 map[string]*pb.RobotModelInfo
// func (c AppModel) ToPB() map[string]*pb.RobotModelInfo {
// 	m := make(map[string]*pb.RobotModelInfo, len(c))
// 	for k, v := range c {
// 		m[k] = &pb.RobotModelInfo{
// 			Prompt:            v.Prompt,
// 			PromptWordsLimit:  v.PromptWordsLimit,
// 			Path:              v.Path,
// 			Target:            v.Target,
// 			Type:              v.Type,
// 			HistoryLimit:      v.HistoryLimit,
// 			HistoryWordsLimit: v.HistoryWordsLimit,
// 			ModelName:         v.ModelName,
// 			ServiceName:       v.ServiceName,
// 			IsEnabled:         v.IsEnabled,
// 		}
// 	}
// 	return m
// }

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
// func (a AppModelInfo) SupportScenes(scenes string) bool {
// 	if a.IsDefault && len(scenes) == 0 {
// 		return true
// 	}
// 	if len(scenes) == 0 || len(a.Scenes) == 0 {
// 		return false
// 	}
// 	if _, ok := a.Scenes[scenes]; ok {
// 		return true
// 	}
// 	return false
// }

// MarshalToString .
// func (c AppModel) MarshalToString() (string, error) {
// 	if len(c) == 0 {
// 		return "", nil
// 	}
// 	return jsonx.MarshalToString(c)
// }
