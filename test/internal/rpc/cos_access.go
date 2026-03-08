package rpc

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	cloudsts "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/sts/v20180813"
	"github.com/tencentyun/cos-go-sdk-v5"

	"git.woa.com/adp/common/x/logx"
	"git.woa.com/adp/kb/kb-config/internal/config"
)

const (
	// ErrCORSConfigNotExist CORS配置不存在
	ErrCORSConfigNotExist = "NoSuchCORSConfiguration"
)

type CosAccessRPC interface {
	ListBucketByCredential(ctx context.Context, credential *cloudsts.Credentials) (*cos.ServiceGetResult, error)
	AddBucketCORSRule(ctx context.Context, credential *cloudsts.Credentials, bucket, region, origin string) (
		*cos.BucketCORSRule, bool, error)
	GetCOSObject(ctx context.Context, credential *cloudsts.Credentials, bucket, region, fileKey string) ([]byte, error)
}

// ListBucketByCredential 通过凭证获取存储桶列表
func (r *RPC) ListBucketByCredential(ctx context.Context, credential *cloudsts.Credentials) (
	*cos.ServiceGetResult, error) {
	// NOTICE: 正式环境 https://service.cos.myqcloud.com
	su, _ := url.Parse(config.App().COSDocumentConfig.COSServiceEndpoint)
	logx.D(ctx, "ListBucketByCredential, su: %+v", su)

	client := cos.NewClient(&cos.BaseURL{
		ServiceURL: su,
	}, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:     *credential.TmpSecretId,
			SecretKey:    *credential.TmpSecretKey,
			SessionToken: *credential.Token, // 临时密钥必须携带
		},
	})

	result, _, err := client.Service.Get(ctx)
	if err != nil {
		logx.E(ctx, "ListBucketByCredential, Service.Get failed, error: %+v", err)
		return nil, err
	}

	logx.I(ctx, "ListBucketByCredential, result: %+v", result)
	return result, nil
}

// NewCOSClient 创建 COS client
func NewCOSClient(ctx context.Context, credential *cloudsts.Credentials, bucket, region string) *cos.Client {
	// bucket示例："example-bucket-1250000000"
	// COSBucketEndpoint："https://%s.cos.%s.myqcloud.com"
	// bu, _ := url.Parse(fmt.Sprintf(config.App().COSDocumentConfig.COSBucketEndpoint, bucket, region))
	endpoint := config.App().COSDocumentConfig.COSBucketEndpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://%s.cos.%s.myqcloud.com", bucket, region)
	}

	bu, _ := url.Parse(endpoint)
	client := cos.NewClient(&cos.BaseURL{
		BucketURL: bu,
	}, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:     *credential.TmpSecretId,
			SecretKey:    *credential.TmpSecretKey,
			SessionToken: *credential.Token, // 临时密钥必须携带
		},
	})

	client.Host = fmt.Sprintf("%s.cos.%s.myqcloud.com", bucket, region)
	return client
}

// getBucketCORS 获取桶CORS
func getBucketCORS(ctx context.Context, c *cos.Client) (*cos.BucketGetCORSResult, error) {
	result, _, err := c.Bucket.GetCORS(ctx)
	if err != nil {
		if e, ok := err.(*cos.ErrorResponse); ok &&
			strings.EqualFold(e.Code, ErrCORSConfigNotExist) {
			return &cos.BucketGetCORSResult{
				Rules: []cos.BucketCORSRule{},
			}, nil
		}

		return nil, err
	}

	return result, nil
}

// putBucketCORS 设置桶CORS
func putBucketCORS(ctx context.Context, c *cos.Client, rules []cos.BucketCORSRule, enableVary bool) error {
	options := &cos.BucketPutCORSOptions{
		Rules: rules,
	}

	if enableVary {
		options.ResponseVary = "true"
	} else {
		options.ResponseVary = "false"
	}

	_, err := c.Bucket.PutCORS(ctx, options)
	return err
}

// 查找包含 origin 的 CORS 规则
func findCORSByOrigin(ctx context.Context, corsRules []cos.BucketCORSRule, origin string) (*cos.BucketCORSRule, bool) {
	corsID := fmt.Sprintf("cors-%s", origin)
	for _, rule := range corsRules {
		if rule.ID == corsID {
			return &rule, true
		}
	}

	logx.I(ctx, "findCORSByOrigin, rule not found, origin: %s", origin)
	return nil, false
}

// AddBucketCORSRule 添加CORS规则
func (r *RPC) AddBucketCORSRule(ctx context.Context, credential *cloudsts.Credentials,
	bucket, region, origin string) (*cos.BucketCORSRule, bool, error) {
	client := NewCOSClient(ctx, credential, bucket, region)

	corsResult, err := getBucketCORS(ctx, client)
	if err != nil {
		return nil, false, err
	}

	rule, exist := findCORSByOrigin(ctx, corsResult.Rules, origin)
	if exist {
		return rule, false, nil
	}

	newRule := &cos.BucketCORSRule{
		ID:             fmt.Sprintf("cors-%s", origin),
		AllowedOrigins: []string{origin},
		// , "OPTIONS"
		AllowedMethods: []string{"GET", "POST", "HEAD"},
		AllowedHeaders: []string{"Authorization", "Content-Type", "*"},
		// "x-cos-meta-*"
		ExposeHeaders: []string{"ETag", "x-cos-request-id"},
		MaxAgeSeconds: 3600,
	}
	rules := append(corsResult.Rules, *newRule)

	err = putBucketCORS(ctx, client, rules, true)
	if err != nil {
		logx.E(ctx, "AddBucketCORSRule, PutBucketCORS failed, error: %+v", err)
		return nil, false, err
	}

	return newRule, true, nil
}

// GetCOSObject 通过凭证获取存储桶COS对象
func (r *RPC) GetCOSObject(ctx context.Context, credential *cloudsts.Credentials, bucket, region, fileKey string) (
	[]byte, error) {
	client := NewCOSClient(ctx, credential, bucket, region)
	resp, err := client.Object.Get(ctx, fileKey, nil)
	if err != nil {
		logx.E(ctx, "GetCOSObject failed, error: %+v", err)
		return nil, err
	}
	if resp == nil {
		logx.E(ctx, "GetCOSObject failed, resp is nil")
		return nil, errors.New("GetCOSObject failed, resp is nil")
	}
	defer resp.Body.Close()
	decryptedData, _ := ioutil.ReadAll(resp.Body)
	return decryptedData, nil
}
