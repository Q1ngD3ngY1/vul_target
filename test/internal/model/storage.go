package model

const (
	// StorageTypeCOS COS 存储
	StorageTypeCOS = "cos"
	// StorageTypeMinIO MinIO 存储
	StorageTypeMinIO = "minio"

	// SharePath 共享的文件夹
	SharePath = "/corp/0/doc/"
	// PublicPath 共有读私有写文件夹 用来存储手动录入md文件附件
	PublicPath = "/public/"

	// ActionDownload 下载操作
	ActionDownload = "download"
	// ActionUpload 上传操作
	ActionUpload = "upload"
	// ActionUpAndDownload 上传和下载操作
	ActionUpAndDownload = "upload_download"

	// OfflineStorageTypeKey 离线存储Key
	OfflineStorageTypeKey = "offline"
	// RealtimeStorageTypeKey 实时存储Key
	RealtimeStorageTypeKey = "realtime"
)

// COS 操作权限
// 文档 https://cloud.tencent.com/document/product/436/31923
// iWiki https://iwiki.woa.com/pages/viewpage.action?pageId=4006971558
const (
	PutObject               = "name/cos:PutObject"               // 简单上传操作
	GetObject               = "name/cos:GetObject"               // 下载对象
	InitiateMultipartUpload = "name/cos:InitiateMultipartUpload" // 分块上传：初始化分块操作
	ListMultipartUploads    = "name/cos:ListMultipartUploads"    // 分块上传：List 进行中的分块上传
	ListParts               = "name/cos:ListParts"               // 分块上传：List 已上传分块操作
	UploadPart              = "name/cos:UploadPart"              // 分块上传：上传分块块操作
	CompleteMultipartUpload = "name/cos:CompleteMultipartUpload" // 分块上传：完成所有分块上传操作
	AbortMultipartUpload    = "name/cos:AbortMultipartUpload"    // 取消分块上传操作
)

// MinIO 操作权限
// 文档 https://min.io/docs/minio/linux/administration/identity-access-management/
// policy-based-access-control.html#minio-policy
const (
	// MinIOVersionID 策略版本
	MinIOVersionID = "2012-10-17"

	MinIOPutObject            = "s3:PutObject"                // 简单上传操作
	MinIOGetObject            = "s3:GetObject"                // 下载对象
	MinIOListMultipartUploads = "s3:ListMultipartUploadParts" // 分块上传：List 进行中的分块上传
	MinIOAbortMultipartUpload = "s3:AbortMultipartUpload"     // 取消分块上传操作
)

// COSUpAndDownload COS 上传与下载
var COSUpAndDownload = []string{
	PutObject,
	GetObject,
	InitiateMultipartUpload,
	ListMultipartUploads,
	ListParts,
	UploadPart,
	CompleteMultipartUpload,
	AbortMultipartUpload,
}

// COSUpload COS 上传
var COSUpload = []string{
	PutObject,
	InitiateMultipartUpload,
	ListMultipartUploads,
	ListParts,
	UploadPart,
	CompleteMultipartUpload,
	AbortMultipartUpload,
}

// CosDownload COS 下载
var CosDownload = []string{
	GetObject,
}

// MinIOUpAndDownload minio 上传与下载
var MinIOUpAndDownload = []string{
	MinIOPutObject,
	MinIOGetObject,
	MinIOListMultipartUploads,
	MinIOAbortMultipartUpload,
}

// MinIOUpload minio 上传
var MinIOUpload = []string{
	MinIOPutObject,
	MinIOListMultipartUploads,
	MinIOAbortMultipartUpload,
}

// MinIODownload minio 下载
var MinIODownload = []string{
	MinIOGetObject,
}

// CredentialResult 临时密钥
type CredentialResult struct {
	Credentials *Credentials
	ExpiredTime int64
	StartTime   int64
}

// Credentials 临时密钥
type Credentials struct {
	TmpSecretID  string
	TmpSecretKey string
	SessionToken string
}

// CredentialPolicyStatement 策略语句
type CredentialPolicyStatement struct {
	Action    []string                  `json:",omitempty"`
	Effect    string                    `json:",omitempty"`
	Resource  []string                  `json:",omitempty"`
	Condition map[string]map[string]any `json:",omitempty"`
}

// CredentialPolicy 密钥策略
type CredentialPolicy struct {
	Version   string                      `json:",omitempty"`
	Statement []CredentialPolicyStatement `json:",omitempty"`
}

// ObjectInfo object元数据信息
type ObjectInfo struct {
	Hash string `json:x_cos_hash_crc64ecma` // x-cos-hash-crc64ecma
	ETag string `json:"e_tag"`              // MD5 checksum of the object
	Size int64  `json:"size"`               // Size of the object
}

// GetStorageCosAction 获取Cos存储操作权限
func GetStorageCosAction(storageAction string) []string {
	switch storageAction {
	case ActionDownload:
		return CosDownload
	case ActionUpload:
		return COSUpload
	case ActionUpAndDownload:
		return COSUpAndDownload
	}
	return nil
}

// GetStorageMinIOAction 获取minIO存储操作权限
func GetStorageMinIOAction(storageAction string) []string {
	switch storageAction {
	case ActionDownload:
		return MinIODownload
	case ActionUpload:
		return MinIOUpload
	case ActionUpAndDownload:
		return MinIOUpAndDownload
	}
	return MinIODownload
}
