package pkg

import (
	"context"
	"git.woa.com/dialogue-platform/lke_proto/pb-protocol/bot_knowledge_config_server"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_getAppBizID(t *testing.T) {
	type args struct {
		ctx context.Context
		req any
	}
	tests := []struct {
		name string
		args args
		want uint64
	}{
		{
			name: "test",
			args: args{
				ctx: context.Background(),
				req: &pb.ListReferSharedKnowledgeReq{
					AppBizId: 1904370549674213376,
				},
			},
			want: 1904370549674213376,
		},
		{
			name: "test",
			args: args{
				ctx: context.Background(),
				req: bot_knowledge_config_server.ListDocReq{
					BotBizId: "1904370549674213376",
				},
			},
			want: 1904370549674213376,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, getAppBizID(tt.args.ctx, tt.args.req), "getAppBizID(%v, %v)", tt.args.ctx, tt.args.req)
		})
	}
}
