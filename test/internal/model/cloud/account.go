// Package cloud TODO
package cloud

// DescribeNicknameRsp 获取昵称响应
type DescribeNicknameRsp struct {
	Response NicknameInfo `json:"Response"`
}

// NicknameInfo 获取昵称
type NicknameInfo struct {
	Nickname    string
	DisplayName string
	RequestID   string
}

// BatchCheckWhitelistRsp 批量检查白名单响应
type BatchCheckWhitelistRsp struct {
	Response struct {
		MatchedWhitelist []struct {
			WhitelistKey     string   `json:"WhitelistKey"`
			WhitelistUinList []string `json:"WhitelistUinList"`
		} `json:"MatchedWhitelist"`
	} `json:"Response"`
}
