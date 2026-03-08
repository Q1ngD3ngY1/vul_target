package util

import (
	"git.code.oa.com/trpc-go/trpc-go/client"
	"git.code.oa.com/trpc-go/trpc-go/naming/selector"
)

// WithTRPCSelector 还原为 trpc 默认 Selector
func WithTRPCSelector() client.Option {
	return func(o *client.Options) {
		o.Selector = &selector.TrpcSelector{}
	}
}
