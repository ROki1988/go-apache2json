package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/json-iterator/go"
	"log"
	"regexp"
	"strconv"
	"time"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary
var re = regexp.MustCompile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[\\+\\-]\\d{2}:?\\d{2}){0,1}\\] \"(.+?)\" (\\d{3}) (\\d+)")
const dateLayout = "02/Jan/2006:15:04:05 -07:00"

// AccessLog is Apache access log
type AccessLog struct {
	Host         string `json:"host"`
	Ident        string `json:"ident"`
	AuthUser     string `json:"authuser"`
	Timestamp    string `json:"@timestamp"`
	TimestampUtc string `json:"@timestamp_utc"`
	Request      string `json:"request"`
	Response     uint32 `json:"response"`
	Bytes        uint32 `json:"bytes"`
}

func firehoseDataConvert(input []byte) ([]byte, error) {
	var m = re.FindStringSubmatch(string(input))
	var timestamp, timeError = time.Parse(dateLayout, m[4])
	if timeError != nil {
		return nil, timeError
	}
	var res, resError = strconv.ParseUint(m[6], 10, 32)
	if resError != nil {
		return nil, resError
	}
	var bytes, byteError = strconv.ParseUint(m[7], 10, 32)
	if byteError != nil {
		return nil, byteError
	}
	var r = AccessLog{
		Host:         m[1],
		Ident:        m[2],
		AuthUser:     m[3],
		Timestamp:    timestamp.Format(time.RFC3339),
		TimestampUtc: timestamp.UTC().Format(time.RFC3339),
		Request:      m[5],
		Response:     uint32(res),
		Bytes:        uint32(bytes),
	}
	return json.Marshal(&r)
}

func firehoseEventRecordConvert(input events.KinesisFirehoseEventRecord) (events.KinesisFirehoseResponseRecord) {
	var res = events.KinesisFirehoseResponseRecord{
		RecordID: input.RecordID,
	}

	var t, e = firehoseDataConvert(input.Data)
	if e != nil {
		log.Println(e)
		copy(res.Data, input.Data)
		res.Result = events.KinesisFirehoseTransformedStateProcessingFailed
	} else {
		res.Data = t
		res.Result = events.KinesisFirehoseTransformedStateOk
	}

	return res
}

func firehoseEventConvert(request events.KinesisFirehoseEvent) (events.KinesisFirehoseResponse, error) {
	var res = events.KinesisFirehoseResponse{}
	for _, v := range request.Records {
		res.Records = append(res.Records, firehoseEventRecordConvert(v))
		log.Println(v)
	}
	return res, nil
}

func main() {
	lambda.Start(firehoseEventConvert)
}
