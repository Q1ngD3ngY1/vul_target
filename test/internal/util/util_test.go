package util

import (
	"fmt"
	"testing"
)

func TestMergeJson(t *testing.T) {
	json1 := `{"name": "John", "age": 30}`
	json2 := `{"city": "New York"}`

	merged, err := MergeJsonString(json1, json2)
	fmt.Println(merged, err)
}
