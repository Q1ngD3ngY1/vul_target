package util

import (
	"regexp"

	"github.com/spf13/cast"
)

var (
	// 手机号正则
	telephonePattern = regexp.MustCompile(`^1\d{10}$`)
	// 账号正则
	accountPattern = regexp.MustCompile(`^[a-zA-Z0-9]{2,20}$`)
	// 机器人名称正则
	robotNamePattern = regexp.MustCompile(`\d{2}$`)
)

// ValidateTelephone 手机号校验
func ValidateTelephone(telephone string) bool {
	return telephonePattern.MatchString(telephone)
}

// ValidateAccount 账号校验
func ValidateAccount(account string) bool {
	return accountPattern.MatchString(account)
}

// ExtractRobotSerialNumber 提取机器人序列号
func ExtractRobotSerialNumber(names []string) []uint32 {
	robotSerialNumbers := make([]uint32, 0)
	for _, name := range names {
		s := robotNamePattern.FindString(name)
		if s == "" {
			continue
		}
		robotSerialNumbers = append(robotSerialNumbers, cast.ToUint32(s))
	}
	return robotSerialNumbers
}
