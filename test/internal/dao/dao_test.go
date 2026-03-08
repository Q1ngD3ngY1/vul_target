// Package dao ...
// @Author: halelv
// @Date: 2024/4/26 14:17
package dao

import (
	"net"
	"testing"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"github.com/bwmarrin/snowflake"
)

func TestGenerateDuplicateID(t *testing.T) {
	// "github.com/bwmarrin/snowflake" total 22 bits to share between NodeBits/StepBits
	// 机器码：使用IP二进制的后16位
	snowflake.NodeBits = 16
	// 序列号：6位，每毫秒最多生成64个ID [0,64)
	snowflake.StepBits = 6

	nodeNum := int64(0)

	// 取IP二进制的后16位
	ip := net.ParseIP("255.255.255.255").To4()
	if ip != nil && len(ip) == 4 {
		nodeNum = (int64(ip[2]) << 8) + int64(ip[3])
	}

	log.Infof("GenerateSeqID ip:%s nodeNum:%d NodeBits:%d StepBits:%d",
		ip, nodeNum, snowflake.NodeBits, snowflake.StepBits)

	node, _ := snowflake.NewNode(nodeNum)

	var x, y snowflake.ID
	for i := 0; i < 1000000; i++ {
		y = node.Generate()
		if x == y {
			t.Errorf("x(%d) & y(%d) are the same", x, y)
		}
		x = y
	}
}
