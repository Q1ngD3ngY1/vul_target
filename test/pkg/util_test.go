package pkg

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToUTF8(t *testing.T) {
	t.Run("gbk", func(t *testing.T) {
		body, err := os.ReadFile("gbk.txt")
		assert.Nil(t, err)
		fmt.Println(string(ToUTF8(body)))
	})
}

func TestGetSaltPassword(t *testing.T) {
	type args struct {
		password string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "测试密码",
			args: args{password: "e10adc3949ba59abbe56e057f20f883e"},
			want: "9a0facaaddc2b917a38dc173f48eedf9",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, GetSaltPassword(tt.args.password), "GetSaltPassword(%v)", tt.args.password)
		})
	}
}

func TestIsValidURL(t *testing.T) {
	testCases := []struct {
		url     string
		isValid bool
	}{
		{"https://example.com/image.jpg", true},
		{"http://example.com/image.jpg", true},
		{"ftp://example.com/image.jpg", false}, // 不支持的协议
		{"example", false},
		{"http://example", true},
		{"", false},
	}

	for _, testCase := range testCases {
		result := IsValidURL(testCase.url)
		if result != testCase.isValid {
			t.Errorf("Expected isValidURL(%q) to be %v, got %v", testCase.url, testCase.isValid, result)
		}
	}
}
