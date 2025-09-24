package common

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestGetRealConnectionAddress(t *testing.T) {
	type args struct {
		dnsLink string
		address common.Address
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
		// TODO: Add test cases.
		{
			name: "test_case_1",
			args: args{
				dnsLink: "http://127.0.0.1:7080/api/dns/connection-address/",
				address: common.HexToAddress("51bdebc98ad4e158b7bc02220ab8ab4cf18af6bd"),
			},
			want:    "127.0.0.1:3002",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetRealConnectionAddress(tt.args.dnsLink, tt.args.address)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetRealConnectionAddress() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetRealConnectionAddress() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadLastLine(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "test_case_1",
			args: args{
				filePath: "/Users/hieuphandinhminh/Desktop/Repo/meta-node/cmd/validator/pkg/chain/tmp/test/chain/block/chain.csv",
			},
			want: "11,0x6f641b71850f0555d074cab2ee73b53f26964107fdfd63ce75fdb37b39105a67",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadLastLine(tt.args.filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadLastLine() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ReadLastLine() = %v, want %v", got, tt.want)
			}
		})
	}
}
