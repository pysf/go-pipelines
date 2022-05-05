package pipeline

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type mockedReceiveMsgs struct {
	sqsiface.SQSAPI
	msg sqs.ReceiveMessageOutput
}

func (m *mockedReceiveMsgs) ReceiveMessage(in *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	return &m.msg, nil
}

const correctSQSMSG string = `{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"eu-central-1","eventTime":"2022-04-28T15:02:12.748Z","eventName":"ObjectCreated:CompleteMultipartUpload","userIdentity":{"principalId":"AWS:AIDAQ57GTTLQBWHPMCL2X"},"requestParameters":{"sourceIPAddress":"87.141.46.58"},"responseElements":{"x-amz-request-id":"FHNYVGN3ZQ9QEK8P","x-amz-id-2":"syNQ04BievqNMY8dnRoeiT1W4P7mqcCcAXPTQ6A14P4/ySkxTb3q/3UxAvknB9ErpIs/h+6o0a/eBnEZbyCDTTT9rNPISkEF"},"s3":{"s3SchemaVersion":"1.0","configurationId":"file-notification","bucket":{"name":"pysf-kafka-to-s3","ownerIdentity":{"principalId":"A17B0OXGJAG3T0"},"arn":"arn:aws:s3:::pysf-kafka-to-s3"},"object":{"key":"2mSalesRecords.csv","size":249602748,"eTag":"274024ceb1d84a2e5add322f5000e77b-24","sequencer":"00626AAC632158F7EA"}}}]}`
const correctEmptySQSMSG string = `{ "Records": [ { "eventVersion": "2.1", "eventSource": "aws:s3", "awsRegion": "eu-central-1", "eventTime": "2022-04-28T15:02:12.748Z", "eventName": "ObjectCreated:CompleteMultipartUpload", "userIdentity": { "principalId": "AWS:AIDAQ57GTTLQBWHPMCL2X" }, "requestParameters": { "sourceIPAddress": "87.141.46.58" }, "responseElements": { "x-amz-request-id": "FHNYVGN3ZQ9QEK8P", "x-amz-id-2": "syNQ04BievqNMY8dnRoeiT1W4P7mqcCcAXPTQ6A14P4/ySkxTb3q/3UxAvknB9ErpIs/h+6o0a/eBnEZbyCDTTT9rNPISkEF" } } ] }`

func TestGetMessage(t *testing.T) {

	cases := []struct {
		resp   sqs.ReceiveMessageOutput
		expect []Record
	}{
		{
			resp: sqs.ReceiveMessageOutput{
				Messages: []*sqs.Message{
					{Body: aws.String(correctSQSMSG)},
				},
			},
			expect: []Record{
				{
					S3Data: S3{
						Bucket: Bucket{
							Name: "pysf-kafka-to-s3"},
						Object: Object{
							Key:  "2mSalesRecords.csv",
							Size: 249602748,
						},
					},
				},
			},
		},
		{
			resp: sqs.ReceiveMessageOutput{
				Messages: []*sqs.Message{
					{Body: aws.String(correctEmptySQSMSG)},
				},
			},
			expect: []Record{
				{
					S3Data: S3{},
				},
			},
		},
	}

	for i, c := range cases {

		q := &sqsQueue{
			client: &mockedReceiveMsgs{msg: c.resp},
			url:    "test-queue",
		}

		resp, err := q.getMessages(10)
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}

		if !reflect.DeepEqual(resp, c.expect) {
			t.Fatalf("%d ,expected %v , got  %v", i, c.expect, resp)
		}

	}

}

func TestSQSStage(t *testing.T) {

	cases := []struct {
		queue  sqsQueue
		expect SQSS3Event
	}{
		{
			queue: sqsQueue{
				client: &mockedReceiveMsgs{
					msg: sqs.ReceiveMessageOutput{
						Messages: []*sqs.Message{
							{Body: aws.String(correctSQSMSG)},
						},
					},
				},
			},
			expect: SQSS3Event{
				message: Record{
					S3Data: S3{
						Bucket: Bucket{
							Name: "pysf-kafka-to-s3"},
						Object: Object{
							Key:  "2mSalesRecords.csv",
							Size: 249602748,
						},
					},
				},
			},
		},
		{
			queue: sqsQueue{
				client: &mockedReceiveMsgs{
					msg: sqs.ReceiveMessageOutput{
						Messages: []*sqs.Message{
							{Body: aws.String(correctEmptySQSMSG)},
						},
					},
				},
			},
			expect: SQSS3Event{
				message: Record{
					S3Data: S3{
						Bucket: Bucket{
							Name: ""},
						Object: Object{
							Key:  "",
							Size: 0,
						},
					},
				},
			},
		},
	}

	for i, c := range cases {
		sqss := sqsStage{
			sqsQueue: c.queue,
		}
		ctx := context.Background()
		resultCh := sqss.GetS3Message(ctx)

		select {
		case s3Nontif, ok := <-resultCh:

			if !ok {
				t.Fatalf("Channel closed!")
				break
			}

			bucket := c.expect.message.S3Data.Bucket.Name
			if !reflect.DeepEqual(s3Nontif.Bucket(), bucket) {
				t.Fatalf("case (%d)  expeting %v got %v", i, bucket, s3Nontif.Bucket())
			}

			key := c.expect.message.S3Data.Object.Key
			if !reflect.DeepEqual(s3Nontif.Key(), key) {
				t.Fatalf("case (%d) expeting %v got %v", i, key, s3Nontif.Key())
			}

		case <-time.After(1 * time.Second):
			t.Fatalf("test timedout")
		}

	}

}
