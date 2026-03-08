package idgen

import (
	"fmt"
	"net"

	"github.com/bwmarrin/snowflake"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-go/log"
)

var snowFlakeNode *snowflake.Node

func init() {
	// "github.com/bwmarrin/snowflake" total 22 bits to share between NodeBits/StepBits
	// 机器码：使用IP二进制的后16位
	snowflake.NodeBits = 16
	// 序列号：6位，每毫秒最多生成64个ID [0,64)
	snowflake.StepBits = 6

	nodeNum := int64(0)

	// 取IP二进制的后16位
	ip := net.ParseIP(getClientIP()).To4()
	if ip != nil && len(ip) == 4 {
		nodeNum = (int64(ip[2]) << 8) + int64(ip[3])
	}

	node, err := snowflake.NewNode(nodeNum)
	log.Infof("GenerateSeqID ip:%s nodeNum:%d NodeBits:%d StepBits:%d", ip, nodeNum, snowflake.NodeBits, snowflake.StepBits)
	if err != nil {
		panic(fmt.Errorf("GenerateSeqID ip:%s nodeNum:%d NodeBits:%d StepBits:%d err:%+v", ip, nodeNum, snowflake.NodeBits, snowflake.StepBits, err))
	}
	snowFlakeNode = node
}

// getClientIP 获取本机IP
func getClientIP() string {
	ip := trpc.GetIP("eth1")
	if len(ip) > 0 {
		return ip
	}
	if addresses, err := net.InterfaceAddrs(); err == nil {
		for _, addr := range addresses {
			if ipNet, ok := addr.(*net.IPNet); ok {
				if !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
					ip = ipNet.IP.To4().String()
					// only ignore 127.0.0.1, return ip
					return ip
				}
			}
		}
	}
	panic("getClientIP failed")
}

// GetId 生成唯一ID
func GetId() uint64 {
	return uint64(snowFlakeNode.Generate())
}
