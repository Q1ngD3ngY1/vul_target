package third_doc

type OnedriveFileType struct {
	MimeType string `json:"mimeType"`
}

type user struct {
	DisplayName string `json:"displayName"`
}
type createdBy struct {
	User user `json:"user"`
}

type folder struct {
	ChildCount int64 `json:"childCount"`
}

type TThirdOnedriveDocInfo struct {
	ID               string           `json:"id"`
	Name             string           `json:"name"`
	Size             int64            `json:"size"`
	LastModifiedTime string           `json:"lastModifiedDateTime"`
	DownloadURL      string           `json:"@microsoft.graph.downloadUrl"`
	OnedriveFileType OnedriveFileType `json:"file"`
	CreatedBy        createdBy        `json:"createdBy"`
	Folder           *folder          `json:"folder"`
}

type TThirdOnedriveDocList struct {
	Values   []*TThirdOnedriveDocInfo `json:"value"`
	NextLink string                   `json:"@odata.nextLink"`
}
