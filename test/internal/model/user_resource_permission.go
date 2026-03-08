package model

import "git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_common"

const (
	// ResourceTypeApp 资源-应用
	ResourceTypeApp = "app"
	// ResourceTypeKnowledge 资源-知识库
	ResourceTypeKnowledge = "knowledge"
	// ResourceTypePlugin 资源-插件
	ResourceTypePlugin = "plugin"
	// ResourceTypePromptTemplate 资源-提示词模版
	ResourceTypePromptTemplate = "prompt_template"
)

var MapResourceType = map[bot_common.ResourceType]string{
	bot_common.ResourceType_ResourceTypeApp:            ResourceTypeApp,
	bot_common.ResourceType_ResourceTypeKnowledge:      ResourceTypeKnowledge,
	bot_common.ResourceType_ResourceTypePlugin:         ResourceTypePlugin,
	bot_common.ResourceType_ResourceTypePromptTemplate: ResourceTypePromptTemplate,
}
