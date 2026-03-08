package document

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_isRealtimeURLLegal(t *testing.T) {
	type args struct {
		url      string
		appBizID uint64
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "test1",
			args: args{
				url:      "/corp/123213/3243243/doc/weew.md",
				appBizID: 3243243,
			},
			want: true,
		},
		{
			name: "test2",
			args: args{
				url:      "/corp/0/3243243/doc/weew.md",
				appBizID: 3243243,
			},
			want: true,
		},
		{
			name: "test3",
			args: args{
				url:      "/corp//3243243/doc/weew.md",
				appBizID: 3243243,
			},
			want: false,
		},
		{
			name: "test4",
			args: args{
				url:      "/corp/sdfdsewrewr/3243243/doc/weew.md",
				appBizID: 3243243,
			},
			want: false,
		},
		{
			name: "test5",
			args: args{
				url:      "/corp/3432/3243243/docer/weew.md",
				appBizID: 3243243,
			},
			want: false,
		},
		{
			name: "test6",
			args: args{
				url:      "/corp/3432/324324323434/doc/weew.md",
				appBizID: 3243243,
			},
			want: false,
		},
		{
			name: "test7",
			args: args{
				url:      "/corp/3432/3243243/doc/dfgfdwer",
				appBizID: 3243243,
			},
			want: true,
		},
		{
			name: "test8",
			args: args{
				url:      "/corp/3432/3243243/doc/sdfs.sdfwer.txt",
				appBizID: 3243243,
			},
			want: true,
		},
		{
			name: "test9",
			args: args{
				url:      "/corp/3432/3243243/doc/",
				appBizID: 3243243,
			},
			want: false,
		},
		{
			name: "test10",
			args: args{
				url:      "/corp/4354323/3243243/doc/xxx.doc",
				appBizID: 3243243,
			},
			want: true,
		},
		{
			name: "test11",
			args: args{
				url:      "/corp/4354323/3243243/doc/似懂非懂.doc",
				appBizID: 3243243,
			},
			want: true,
		},
		{
			name: "test12",
			args: args{
				url:      "/corp/1747257290937597952/1798021795107307520/doc/YaoWqrVwHKoGaGHfdxgt-1798198606990934016.docx",
				appBizID: 1798021795107307520,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, isRealtimeURLLegal(tt.args.url, tt.args.appBizID), "isRealtimeURLLegal(%v, %v)", tt.args.url, tt.args.appBizID)
		})
	}
}
