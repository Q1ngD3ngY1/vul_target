package third_doc

// CommonDocInfo 通用的文档信息结构，用于统一不同第三方平台的返回数据
type CommonDocInfo struct {
	ID               string                 `json:"id"`                 // 文档唯一标识
	Name             string                 `json:"name"`               // 文档名称
	Size             int64                  `json:"size"`               // 文档大小（字节）
	MimeType         string                 `json:"mime_type"`          // 文档类型
	IsFolder         bool                   `json:"is_folder"`          // 是否为文件夹
	DownloadURL      string                 `json:"download_url"`       // 下载链接
	LastModifiedTime string                 `json:"last_modified_time"` // 最后修改时间
	CreatedBy        string                 `json:"created_by"`         // 创建者
	ParentID         string                 `json:"parent_id"`          // 父文件夹ID
	Extension        string                 `json:"extension"`          // 文件扩展名
	Extra            map[string]interface{} `json:"extra,omitempty"`    // 扩展字段，用于存储平台特有的数据
}

// CommonDocListResult 通用的文档列表返回结构
type CommonDocListResult struct {
	Docs       []*CommonDocInfo       `json:"docs"`            // 文档列表
	TotalCount int64                  `json:"total_count"`     // 总数
	NextLink   string                 `json:"next_link"`       // 分页token（用于下一页）
	Extra      map[string]interface{} `json:"extra,omitempty"` // 扩展字段
}

// ListDocOptions 查询文档的选项参数（使用 Option 模式）
type ListDocOptions struct {
	ItemID      string              `json:"item_id"`         // 单个文件ID -- 优先级最高, 不与 folderID/keyword 组合使用
	FolderID    string              `json:"folder_id"`       // 文件夹ID  -- 与 keyword 组合使用
	Keyword     string              `json:"keyword"`         // 搜索关键词
	AccessToken string              `json:"access_token"`    // 访问令牌
	PageSize    int                 `json:"page_size"`       // 每页数量
	PageToken   string              `json:"page_token"`      // 分页token
	FileTypes   []string            `json:"file_types"`      // 文件类型过滤
	NextLink    string              `json:"next_link"`       // 下一页链接, 用于分页
	MinSize     int64               `json:"min_size"`        // 最小文件大小
	MaxSize     int64               `json:"max_size"`        // 最大文件大小
	Extra       map[string][]string `json:"extra,omitempty"` // 扩展参数，用于平台特有的查询条件
}

// ListDocOption 是用于构建 ListDocOptions 的函数类型
type ListDocOption func(*ListDocOptions)

// WithFolderID 设置文件夹ID
func WithFolderID(folderID string) ListDocOption {
	return func(opts *ListDocOptions) {
		opts.FolderID = folderID
	}
}

// WithKeyword 设置搜索关键词
func WithKeyword(keyword string) ListDocOption {
	return func(opts *ListDocOptions) {
		opts.Keyword = keyword
	}
}

// WithAccessToken 设置访问令牌
func WithAccessToken(token string) ListDocOption {
	return func(opts *ListDocOptions) {
		opts.AccessToken = token
	}
}

func WithItemID(itemID string) ListDocOption {
	return func(opts *ListDocOptions) {
		opts.ItemID = itemID
	}
}

// WithNextLink 设置下一页链接
func WithNextLink(nextLink string) ListDocOption {
	return func(opts *ListDocOptions) {
		opts.NextLink = nextLink
	}
}

// WithPageSize 设置每页数量
func WithPageSize(size int) ListDocOption {
	return func(opts *ListDocOptions) {
		opts.PageSize = size
	}
}

// WithPageToken 设置分页token
func WithPageToken(token string) ListDocOption {
	return func(opts *ListDocOptions) {
		opts.PageToken = token
	}
}

// WithFileTypes 设置文件类型过滤, 如果第三方平台支持
func WithFileTypes(types []string) ListDocOption {
	return func(opts *ListDocOptions) {
		opts.FileTypes = types
	}
}

// WithSizeRange 设置文件大小范围, 如果第三方平台支持
func WithSizeRange(minSize, maxSize int64) ListDocOption {
	return func(opts *ListDocOptions) {
		opts.MinSize = minSize
		opts.MaxSize = maxSize
	}
}

// WithExtra 设置扩展参数
func WithExtra(key string, v []string) ListDocOption {
	return func(opts *ListDocOptions) {
		if opts.Extra == nil {
			opts.Extra = make(map[string][]string)
		}
		opts.Extra[key] = append(opts.Extra[key], v...)
	}
}

func WithMaxSize(maxSize int64) ListDocOption {
	return func(opts *ListDocOptions) {
		opts.MaxSize = maxSize
	}
}

// NewListDocOptions 创建 ListDocOptions 实例
func NewListDocOptions(opts ...ListDocOption) *ListDocOptions {
	options := &ListDocOptions{
		FolderID: DefaultFolder,
		PageSize: 20, // 默认每页20条
	}
	for _, opt := range opts {
		opt(options)
	}
	return options
}
