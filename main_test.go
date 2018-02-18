package main

import (
	"reflect"
	"testing"
)

func Test_firehoseDataConvert(t *testing.T) {
	type args struct {
		input []byte
	}
	var j, _ = json.Marshal(AccessLog{
		Host: "7.248.7.119",
		Ident: "-",
		AuthUser: "-",
		Timestamp: "2017-12-14T22:16:45+09:00",
		TimestampUtc: "2017-12-14T13:16:45Z",
		Request: "GET /explore",
		Response: 200,
		Bytes: 9947,
	})
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "success",
			args: args {
				input:  []byte("7.248.7.119 - - [14/Dec/2017:22:16:45 +09:00] \"GET /explore\" 200 9947 \"-\" \"Mozilla/5.0 (Windows NT 6.2; WOW64; rv:8.5) Gecko/20100101 Firefox/8.5.1\" "),
			},
			want: j,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := firehoseDataConvert(tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("firehoseDataConvert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("firehoseDataConvert() = %v, want %v", got, tt.want)
			}
		})
	}
}
