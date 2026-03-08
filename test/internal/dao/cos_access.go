package dao

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	//"strings"

	"git.code.oa.com/trpc-go/trpc-go/log"
	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/config"
	cloudsts "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/sts/v20180813"
	"github.com/tencentyun/cos-go-sdk-v5"
)

const (
	// ErrCORSConfigNotExist CORS不存在
	ErrCORSConfigNotExist = "NoSuchCORSConfiguration"
)

// ListBucketByCredential 通过凭证获取存储桶列表
func (d *dao) ListBucketByCredential(ctx context.Context, credential *cloudsts.Credentials) (
	*cos.ServiceGetResult, error) {
	// NOTICE: 正式环境 https://service.cos.myqcloud.com
	su, _ := url.Parse(config.App().COSDocumentConfig.COSServiceEndpoint)

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
		log.ErrorContextf(ctx, "ListBucketByCredential, Service.Get failed, error: %+v", err)
		return nil, err
	}

	log.InfoContextf(ctx, "ListBucketByCredential, result: %+v", result)
	return result, nil
}

// NewCOSClient 创建 COS client
// TODO: 移至 client package
func NewCOSClient(ctx context.Context, credential *cloudsts.Credentials, bucket, region string) *cos.Client {
	// bucket示例："example-bucket-1250000000"
	// COSBucketEndpoint："https://%s.cos.%s.myqcloud.com"
	//bu, _ := url.Parse(fmt.Sprintf(config.App().COSDocumentConfig.COSBucketEndpoint, bucket, region))
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

// PutBucketCORS 设置桶CORS
// TODO: 移至 client package
func PutBucketCORS(ctx context.Context, c *cos.Client, rules []cos.BucketCORSRule, enableVary bool) error {
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

// AddBucketCORSRule 添加CORS规则
func (d *dao) AddBucketCORSRule(ctx context.Context, credential *cloudsts.Credentials,
	bucket, region, origin string) (*cos.BucketCORSRule, bool, error) {
	client := NewCOSClient(ctx, credential, bucket, region)

	newRule := &cos.BucketCORSRule{
		ID:             fmt.Sprintf("cors-%s", origin),
		AllowedOrigins: []string{origin},
		//, "OPTIONS"
		AllowedMethods: []string{"GET", "POST", "HEAD"},
		AllowedHeaders: []string{"Authorization", "Content-Type", "*"},
		// "x-cos-meta-*"
		ExposeHeaders: []string{"ETag", "x-cos-request-id"},
		MaxAgeSeconds: 3600,
	}

	rules := []cos.BucketCORSRule{
		*newRule,
	}
	
	err := PutBucketCORS(ctx, client, rules, true)
	if err != nil {
		log.ErrorContextf(ctx, "AddBucketCORSRule, PutBucketCORS failed, error: %+v", err)
		return nil, false, err
	}

	return newRule, true, nil
}

// GetCOSObject 通过凭证获取存储桶COS对象
func (d *dao) GetCOSObject(ctx context.Context, credential *cloudsts.Credentials, bucket, region, fileKey string) (
	[]byte, error) {
	client := NewCOSClient(ctx, credential, bucket, region)
	resp, err := client.Object.Get(ctx, fileKey, nil)
	if err != nil {
		log.ErrorContextf(ctx, "GetCOSObject failed, error: %+v", err)
		return nil, err
	}
	if resp == nil {
		log.ErrorContextf(ctx, "GetCOSObject failed, resp is nil")
		return nil, errors.New("GetCOSObject failed, resp is nil")
	}
	defer resp.Body.Close()
	decryptedData, _ := ioutil.ReadAll(resp.Body)
	return decryptedData, nil
}
