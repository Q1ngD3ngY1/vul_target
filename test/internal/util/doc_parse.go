package util

import (
	"fmt"
	pb "git.woa.com/dialogue-platform/lke_proto/pb-protocol/knowledge"
)

// String .
func String(c *pb.PageContent) string {
	r := c.Prefix
	for _, v := range c.Head {
		r = append(r, v.Runes...)
	}
	for _, v := range c.Body {
		r = append(r, v.Runes...)
	}
	for _, v := range c.Tail {
		r = append(r, v.Runes...)
	}
	return string(r)
}

func InterveneOldDocCosHashToNewDocRedisKey(corpID, botBizID, docBizID uint64, oldDocCosHash string) string {
	return fmt.Sprintf("InterveneOldDocCosHashToNewDoc:%d:%d:%d:%s", corpID, botBizID, docBizID, oldDocCosHash)
}
