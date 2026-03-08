package db_source

import (
	"context"
	"net"
	"strings"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/pkg/errs"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
)

// getInternalIps 获取内网IP网段列表
func getInternalIps() []string {
	ipsStr := config.GetInternalIps()
	// 按行分割配置字符串
	lines := strings.Split(ipsStr, "\n")
	var cidrs []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			cidrs = append(cidrs, line)
		}
	}
	return cidrs
}

// isInternalIP 检查IP是否在内网地址段中
func isInternalIP(ctx context.Context, ip string) bool {
	internalCIDRs := getInternalIps()

	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return false
	}

	for _, cidr := range internalCIDRs {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			log.WarnContextf(ctx, "parse CIDR %s failed: %v", cidr, err)
			continue
		}
		if network.Contains(parsedIP) {
			log.InfoContextf(ctx, "%s is in internal IP, in %v", ip, cidr)
			return true
		}
	}
	return false
}

// resolveHost 解析主机名为IP地址（如果是域名则解析，如果已经是IP则直接返回）
func resolveHost(_ context.Context, host string) ([]string, error) {
	// 先检查是否已经是IP地址
	if ip := net.ParseIP(host); ip != nil {
		return []string{host}, nil
	}

	// 如果不是IP，尝试解析域名
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}

	var ipStrings []string
	for _, ip := range ips {
		ipStrings = append(ipStrings, ip.String())
	}
	return ipStrings, nil
}

// CheckInternalDBAccess 检查用户是否有权限访问内网数据库
// 如果host是内网地址，则检查uin是否在白名单中
// 返回解析后的IP地址（如果输入已经是IP则直接返回，如果是域名则返回解析后的第一个IP）
func CheckInternalDBAccess(ctx context.Context, host string, appBizID uint64) (string, error) {
	// 获取用户UIN
	uin := pkg.Uin(ctx)

	// 解析host为IP地址
	ips, err := resolveHost(ctx, host)
	if err != nil {
		log.WarnContextf(ctx, "resolve host %s failed: %v", host, err)
		// 解析失败时，谨慎起见，不允许访问
		return "", errs.ErrInvalidDbSourceHost
	}

	// 检查是否有任一IP在内网地址段中
	isInternal := false
	for _, ip := range ips {
		if isInternalIP(ctx, ip) {
			isInternal = true
			break
		}
	}

	// 如果是内网地址，检查白名单
	if isInternal && !config.IsInWhiteList(uin, appBizID, config.GetWhitelistConfig().InternalDBWhiteList) {
		log.ErrorContextf(ctx, "%s is in internal DB, %v not in white list", host, uin)
		return "", errs.ErrInvalidDbSourceHost
	}

	// 返回第一个解析的IP地址
	resolvedIP := ips[0]
	if host != resolvedIP {
		log.InfoContextf(ctx, "resolved host %s to IP %s for security check", host, resolvedIP)
	}

	return resolvedIP, nil
}
