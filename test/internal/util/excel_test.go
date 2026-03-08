package util

import "testing"

func Test_isRowEmpty(t *testing.T) {
	type args struct {
		head []string
		row  []string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "t1",
			args: args{
				head: []string{
					"index",
					"page_content",
					"title",
				},
				row: []string{
					"",
					"",
					"",
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRowEmpty(tt.args.head, tt.args.row); got != tt.want {
				t.Errorf("isRowEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}
