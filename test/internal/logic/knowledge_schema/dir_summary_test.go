// bot-knowledge-config-server
//
// @(#)dir_summary_test.go  星期二, 五月 27, 2025
// Copyright(c) 2025, reinhold@Tencent. All rights reserved.

package knowledge_schema

import "testing"

func Test_getDirNameSummary(t *testing.T) {

	tests := []struct {
		name       string
		input      string
		dirName    string
		dirSummary string
	}{
		{
			name: "标准格式",
			input: `<摘要>
					该文件夹包含两款北汽BJ40车型的相关文件，分别是全新BJ40城市猎人版的OTA升级用户明细和2020款BJ40雨林穿越版的车用户手册。
					</摘要>
					
					<文件夹名>
					BJ40车型文件
					</文件夹名>`,
			dirName:    "BJ40车型文件",
			dirSummary: "该文件夹包含两款北汽BJ40车型的相关文件，分别是全新BJ40城市猎人版的OTA升级用户明细和2020款BJ40雨林穿越版的车用户手册。",
		},
		{
			name: "文件夹名标签未闭合",
			input: `<摘要>
					该文件夹包含两款北汽BJ40车型的相关文件，分别是全新BJ40城市猎人版的OTA升级用户明细和2020款BJ40雨林穿越版的车用户手册。
					</摘要>
					
					<文件夹名>
					BJ40车型文件`,
			dirName:    "BJ40车型文件",
			dirSummary: "该文件夹包含两款北汽BJ40车型的相关文件，分别是全新BJ40城市猎人版的OTA升级用户明细和2020款BJ40雨林穿越版的车用户手册。",
		},
		{
			name: "摘要标签未闭合",
			input: `<摘要>
					该文件夹包含两款北汽BJ40车型的相关文件，分别是全新BJ40城市猎人版的OTA升级用户明细和2020款BJ40雨林穿越版的车用户手册。
					<文件夹名>
					BJ40车型文件</文件夹名>`,
			dirName:    "BJ40车型文件",
			dirSummary: "该文件夹包含两款北汽BJ40车型的相关文件，分别是全新BJ40城市猎人版的OTA升级用户明细和2020款BJ40雨林穿越版的车用户手册。",
		},
		{
			name: "标签均未闭合",
			input: `<摘要>
					该文件夹包含两款北汽BJ40车型的相关文件，分别是全新BJ40城市猎人版的OTA升级用户明细和2020款BJ40雨林穿越版的车用户手册。
					<文件夹名>
					BJ40车型文件 `,
			dirName:    "BJ40车型文件",
			dirSummary: "该文件夹包含两款北汽BJ40车型的相关文件，分别是全新BJ40城市猎人版的OTA升级用户明细和2020款BJ40雨林穿越版的车用户手册。",
		},
		{
			name: "没有文件夹名",
			input: `<摘要>
					该文件夹包含两款北汽BJ40车型的相关文件，分别是全新BJ40城市猎人版的OTA升级用户明细和2020款BJ40雨林穿越版的车用户手册。
					`,
			dirName:    "1",
			dirSummary: "该文件夹包含两款北汽BJ40车型的相关文件，分别是全新BJ40城市猎人版的OTA升级用户明细和2020款BJ40雨林穿越版的车用户手册。",
		},
		{
			name:       "没有任何标签",
			input:      `没有任何标签`,
			dirName:    "1",
			dirSummary: "没有任何标签",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := getDirNameSummary(tt.input)
			if got != tt.dirName {
				t.Errorf("getDirNameSummary() got = %v, want %v", got, tt.dirName)
			}
			if got1 != tt.dirSummary {
				t.Errorf("getDirNameSummary() got1 = %v, want %v", got1, tt.dirSummary)
			}
		})
	}
}
