package dao

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server/internal/model"
)

func TestExtractDuplicateWordMD5(t *testing.T) {
	tests := []struct {
		name            string
		errorMessage    string
		expectedWordMD5 string
	}{
		{
			name:            "standard_error_message",
			errorMessage:    "Duplicate entry '12345-abcdef1234567890abcdef1234567890-0' for key 't_synonyms.uk_word_md5'",
			expectedWordMD5: "abcdef1234567890abcdef1234567890",
		},
		{
			name:            "different_robot_id",
			errorMessage:    "Duplicate entry '67890-1234567890abcdef1234567890abcdef-0' for key 't_synonyms.uk_word_md5'",
			expectedWordMD5: "1234567890abcdef1234567890abcdef",
		},
		{
			name: "additional_information",
			errorMessage: "Duplicate entry '11111-abcdefabcdefabcdefabcdefabcdef-0' for key 't_synonyms.uk_word_md5' " +
				"with some extra text",
			expectedWordMD5: "abcdefabcdefabcdefabcdefabcdef",
		},
		{
			name:            "no_match",
			errorMessage:    "Some other error message",
			expectedWordMD5: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wordMD5 := extractDuplicateWordMD5(tt.errorMessage)
			if wordMD5 != tt.expectedWordMD5 {
				t.Errorf("extractDuplicateWordMD5() = %v, want %v", wordMD5, tt.expectedWordMD5)
			}
		})
	}
}

func TestReplaceSynonyms(t *testing.T) {
	tests := []struct {
		name           string
		originalQuery  string
		nerInfos       []*model.NerInfo
		expectedOutput string
	}{
		{
			name:          "Basic replacement",
			originalQuery: "I want to eat an apple",
			nerInfos: []*model.NerInfo{
				{Offset: 17, NumTokens: 1, OriginalText: "apple", RefValue: "fruit"},
			},
			expectedOutput: "I want to eat an fruit",
		},
		{
			name:          "Multiple replacements",
			originalQuery: "I want to eat an apple and a banana",
			nerInfos: []*model.NerInfo{
				{Offset: 17, NumTokens: 1, OriginalText: "apple", RefValue: "fruit"},
				{Offset: 29, NumTokens: 1, OriginalText: "banana", RefValue: "fruit"},
			},
			expectedOutput: "I want to eat an fruit and a fruit",
		},
		{
			name:           "No replacements",
			originalQuery:  "I want to eat an apple",
			nerInfos:       []*model.NerInfo{},
			expectedOutput: "I want to eat an apple",
		},
		{
			name:          "Out of bounds replacement",
			originalQuery: "I want to eat an apple",
			nerInfos: []*model.NerInfo{
				{Offset: 30, NumTokens: 1, OriginalText: "apple", RefValue: "fruit"},
			},
			expectedOutput: "I want to eat an apple",
		},
		{
			name:          "Replacement with different length",
			originalQuery: "I want to eat an apple",
			nerInfos: []*model.NerInfo{
				{Offset: 17, NumTokens: 1, OriginalText: "apple", RefValue: "delicious fruit"},
			},
			expectedOutput: "I want to eat an delicious fruit",
		},
		{
			name:          "中文",
			originalQuery: "我想要吃一个苹果",
			nerInfos: []*model.NerInfo{
				{Offset: 6, NumTokens: 1, OriginalText: "苹果", RefValue: "水果"},
			},
			expectedOutput: "我想要吃一个水果",
		},
		{
			name:          "中英文混合 1",
			originalQuery: "我想要eat一个apple",
			nerInfos: []*model.NerInfo{
				{Offset: 3, NumTokens: 1, OriginalText: "eat", RefValue: "吃"},
				{Offset: 8, NumTokens: 1, OriginalText: "apple", RefValue: "苹果"},
			},
			expectedOutput: "我想要吃一个苹果",
		},
		{
			name:          "中英文混合 2",
			originalQuery: "我想要 eat 一个 apple",
			nerInfos: []*model.NerInfo{
				{Offset: 1, NumTokens: 2, OriginalText: "想要", RefValue: "want to"},
				{Offset: 8, NumTokens: 2, OriginalText: "一个", RefValue: "one"},
			},
			expectedOutput: "我want to eat one苹果",
		},
		{
			name:          "多次替换",
			originalQuery: "四八柜和二八柜及三八柜是什么",
			nerInfos: []*model.NerInfo{
				{Offset: 0, NumTokens: 3, OriginalText: "四八柜", RefValue: "二四六八柜"},
				{Offset: 4, NumTokens: 3, OriginalText: "二八柜", RefValue: "二四六八柜"},
				{Offset: 8, NumTokens: 3, OriginalText: "三八柜", RefValue: "二四六八柜"},
			},
			expectedOutput: "二四六八柜和二四六八柜及二四六八柜是什么",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := replaceSynonyms(tt.originalQuery, tt.nerInfos)
			if output != tt.expectedOutput {
				t.Errorf("expected %v, got %v", tt.expectedOutput, output)
			}
		})
	}
}

func TestGetConditionAndArgsFromListReq(t *testing.T) {
	tests := []struct {
		name     string
		req      *model.SynonymsListReq
		prefix   string
		wantCond string
		wantArgs []any
	}{
		{
			name: "basic case",
			req: &model.SynonymsListReq{
				RobotID:   123,
				CorpID:    456,
				IsDeleted: 0,
			},
			prefix:   "",
			wantCond: " AND is_deleted = 0",
			wantArgs: []any{uint64(456), uint64(123)},
		},
		{
			name: "with query",
			req: &model.SynonymsListReq{
				RobotID:   123,
				CorpID:    456,
				IsDeleted: 0,
				Query:     "test",
			},
			prefix:   "",
			wantCond: " AND is_deleted = 0 AND word LIKE ?",
			wantArgs: []any{uint64(456), uint64(123), "%test%"},
		},
		{
			name: "with categories",
			req: &model.SynonymsListReq{
				RobotID:   123,
				CorpID:    456,
				IsDeleted: 0,
				CateIDs:   []uint64{1, 2, 3},
			},
			prefix:   "",
			wantCond: " AND is_deleted = 0 AND category_id IN (?, ?, ?)",
			wantArgs: []any{uint64(456), uint64(123), uint64(1), uint64(2), uint64(3)},
		},
		{
			name: "with release status",
			req: &model.SynonymsListReq{
				RobotID:       123,
				CorpID:        456,
				IsDeleted:     0,
				ReleaseStatus: []uint32{1, 2},
			},
			prefix:   "",
			wantCond: " AND is_deleted = 0 AND release_status IN (?, ?)",
			wantArgs: []any{uint64(456), uint64(123), uint32(1), uint32(2)},
		},
		{
			name: "with update time",
			req: &model.SynonymsListReq{
				RobotID:    123,
				CorpID:     456,
				IsDeleted:  0,
				UpdateTime: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC),
			},
			prefix:   "",
			wantCond: " AND is_deleted = 0 AND update_time > ?",
			wantArgs: []any{uint64(456), uint64(123), time.Date(2023, 10, 1, 0, 0,
				0, 0, time.UTC)},
		},
		{
			name: "with update time equal and ID",
			req: &model.SynonymsListReq{
				RobotID:         123,
				CorpID:          456,
				IsDeleted:       0,
				UpdateTimeEqual: true,
				UpdateTime:      time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC),
				ID:              789,
			},
			prefix:   "",
			wantCond: " AND is_deleted = 0 AND update_time = ? AND id >= ?",
			wantArgs: []any{uint64(456), uint64(123), time.Date(2023, 10, 1, 0, 0, 0,
				0, time.UTC), uint64(789)},
		},
		{
			name: "with prefix s.",
			req: &model.SynonymsListReq{
				RobotID:   123,
				CorpID:    456,
				IsDeleted: 0,
				Query:     "test",
			},
			prefix:   "s.",
			wantCond: " AND s.is_deleted = 0 AND s.word LIKE ?",
			wantArgs: []any{uint64(456), uint64(123), "%test%"},
		},
		{
			name: "with prefix p.",
			req: &model.SynonymsListReq{
				RobotID:   123,
				CorpID:    456,
				IsDeleted: 0,
				CateIDs:   []uint64{1, 2},
			},
			prefix:   "p.",
			wantCond: " AND p.is_deleted = 0 AND p.category_id IN (?, ?)",
			wantArgs: []any{uint64(456), uint64(123), uint64(1), uint64(2)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &dao{}
			condition, args := d.getConditionAndArgsFromListReq(context.Background(), tt.req)
			assert.Equal(t, tt.wantCond, condition)
			assert.Equal(t, tt.wantArgs, args)
		})
	}
}
