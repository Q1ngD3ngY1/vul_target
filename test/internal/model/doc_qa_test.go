package model

import (
	"testing"
)

func TestDocQA_IsDisable(t *testing.T) {
	type fields struct {
		ID uint64

		AttributeFlag uint64
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "test1",
			fields: fields{
				AttributeFlag: uint64(1),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DocQA{
				AttributeFlag: tt.fields.AttributeFlag,
			}
			if got := d.IsDisable(); got != tt.want {
				t.Errorf("IsDisable() = %v, want %v", got, tt.want)
			}
		})
	}
}
