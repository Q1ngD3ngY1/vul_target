package util

import (
	"reflect"
	"testing"
)

func TestExtractImagesFromHTML(t *testing.T) {
	type args struct {
		htmlCode string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "提取html图片",
			args: args{
				htmlCode: `
				<p>阿啊阿啊<img src="https://qidian-qbot-test-1251316161.cos.ap-guangzhou.myqcloud.com/public/4lhFYNKgYuebqiukkyqY-7251157790.png" alt="4lhFYNKgYuebqiukkyqY-7251157790.png" data-href="https://qidian-qbot-test-1251316161.cos.ap-guangzhou.myqcloud.com/public/4lhFYNKgYuebqiukkyqY-7251157790.png" style=""/><img src="https://qidian-qbot-test-1251316161.cos.ap-guangzhou.myqcloud.com/public/4lhFYNKgYuebqiukkyqY-7251157790.png" alt="4lhFYNKgYuebqiukkyqY-7251157790.png" data-href="https://qidian-qbot-test-1251316161.cos.ap-guangzhou.myqcloud.com/public/4lhFYNKgYuebqiukkyqY-7251157790.png" style=""/><img src="https://qidian-qbot-test-1251316161.cos.ap-guangzhou.myqcloud.com/public/4lhFYNKgYuebqiukkyqY-7251157790.png" alt="4lhFYNKgYuebqiukkyqY-7251157790.png" data-href="https://qidian-qbot-test-1251316161.cos.ap-guangzhou.myqcloud.com/public/4lhFYNKgYuebqiukkyqY-7251157790.png" style=""/></p>
			`,
			},
			want: []string{
				"https://qidian-qbot-test-1251316161.cos.ap-guangzhou.myqcloud.com/public/4lhFYNKgYuebqiukkyqY-7251157790.png",
				"https://qidian-qbot-test-1251316161.cos.ap-guangzhou.myqcloud.com/public/4lhFYNKgYuebqiukkyqY-7251157790.png",
				"https://qidian-qbot-test-1251316161.cos.ap-guangzhou.myqcloud.com/public/4lhFYNKgYuebqiukkyqY-7251157790.png",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExtractImagesFromHTML(tt.args.htmlCode); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExtractImagesFromHTML() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractImagesFromMarkdown(t *testing.T) {
	type args struct {
		markdownText string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "提取markdown图片",
			args: args{
				markdownText: "zzzz![wZWiCe75nc9sGHAECsix-3415180312.jpg](https://qidian-qbot-test-1251316161.cos.ap-guangzhou.myqcloud.com/public/wZWiCe75nc9sGHAECsix-3415180312.jpg)",
			},
			want: []string{
				"https://qidian-qbot-test-1251316161.cos.ap-guangzhou.myqcloud.com/public/wZWiCe75nc9sGHAECsix-3415180312.jpg",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExtractImagesFromMarkdown(tt.args.markdownText); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExtractImagesFromMarkdown() = %v, want %v", got, tt.want)
			}
		})
	}
}
