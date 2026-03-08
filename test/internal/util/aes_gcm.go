package util

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"strings"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	utilConfig "git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/util/config"
	"git.woa.com/dialogue-platform/common/v3/utils"
)

// Encrypt 加密
func Encrypt(plainText string) (string, error) {
	aesKey, err := base64.StdEncoding.DecodeString(config.App().DbSource.Salt)
	if err != nil {
		return "", err
	}
	cipherText, err := utils.AesEncrypt([]byte(plainText), aesKey)
	if err != nil {
		return "", err
	}
	// 将二进制数据编码为Base64字符串
	return base64.StdEncoding.EncodeToString(cipherText), nil
}

// Decrypt 解密
func Decrypt(cipherText string) (string, error) {
	aesKey, err := base64.StdEncoding.DecodeString(config.App().DbSource.Salt)
	if err != nil {
		return "", err
	}

	// 先解码Base64字符串
	cipherBytes, err := base64.StdEncoding.DecodeString(cipherText)
	if err != nil {
		return "", err
	}

	plainText, err := utils.AesDecrypt(cipherBytes, aesKey)
	if err != nil {
		return "", err
	}
	return string(plainText), nil
}

// GetDbSourcePrivateKey RSA-OAEP SHA-256 长度 2048 位
func GetDbSourcePrivateKey() (string, error) {
	// 获取私钥, 进行 base64 转换
	base64Code := utilConfig.GetDbSourceConfig()
	privateKey, err := base64.StdEncoding.DecodeString(base64Code)
	if err != nil {
		return "", err
	}
	return string(privateKey), nil
}

func GeneratePublicKeyPEMByPrivateKey(privateKeyPEM string) (publicKeyPEM string, err error) {
	// 解析PEM格式的私钥
	block, _ := pem.Decode([]byte(privateKeyPEM))
	if block == nil {
		return "", fmt.Errorf("failed to parse private key PEM")
	}

	// 解析PKCS1格式的私钥
	priv, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return "", err
	}

	// 从私钥中提取公钥
	pubASN1, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return "", err
	}

	// 将公钥编码为PEM格式
	pubPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubASN1,
	})

	// 返回Base64编码的公钥
	//return base64.StdEncoding.EncodeToString(pubPEM), nil
	return pemToBase64(string(pubPEM)), nil
}

func pemToBase64(pemStr string) string {
	lines := strings.Split(pemStr, "\n")
	var b64Lines []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" ||
			strings.HasPrefix(line, "-----BEGIN") ||
			strings.HasPrefix(line, "-----END") {
			continue
		}
		b64Lines = append(b64Lines, line)
	}
	return strings.Join(b64Lines, "")
}

// 私钥解密
func DecryptWithPrivateKeyPEM(privateKeyPEM string, cipherTextBase64 string) (string, error) {
	block, _ := pem.Decode([]byte(privateKeyPEM))
	if block == nil {
		return "", fmt.Errorf("failed to parse private key PEM")
	}
	priv, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return "", err
	}
	cipherText, err := base64.StdEncoding.DecodeString(cipherTextBase64)
	if err != nil {
		return "", err
	}
	plainText, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, priv, cipherText, nil)
	if err != nil {
		return "", err
	}
	return string(plainText), nil
}
