package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type s3Notification interface {
	bucket() string
	key() string
	getError() error
	done()
}

func messages(ctx context.Context, queue string) chan s3Notification {

	resultCh := make(chan s3Notification)

	go func() {
		defer close(resultCh)

		sqsMessages, err := latesEvents(queue)
		if err != nil {
			resultCh <- &s3PipelineEvent{
				err: fmt.Errorf("getSQSEvents: failed %w", err),
			}
			return
		}

		for _, message := range sqsMessages {
			select {
			case <-ctx.Done():
			case resultCh <- &s3PipelineEvent{
				message: message,
				onDone: func() {
					fmt.Printf("I need to remove this message from sqs %v", message.ReceiptHandle)
				},
			}:
			}
		}

	}()

	return resultCh

}

func latesEvents(queue string) ([]rawSQSRecord, error) {

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-central-1"),
	})

	if err != nil {
		return nil, fmt.Errorf("fetchLatesEvents: failed to connect to aws %w", err)
	}

	urlResult, err := queueUrl(sess, queue)
	if err != nil {
		return nil, fmt.Errorf("fetchLatesEvents: %w", err)
	}

	msgResult, err := sqsMessages(sess, urlResult.QueueUrl)
	if err != nil {
		return nil, fmt.Errorf("fetchLatesEvents: %w", err)
	}

	fmt.Println(len(msgResult.Messages))
	var result []rawSQSRecord
	for _, msg := range msgResult.Messages {
		// msg.ReceiptHandle
		var message rawSQSBody
		if err := json.Unmarshal([]byte(*msg.Body), &message); err != nil {
			return nil, fmt.Errorf("fetchLatesEvents: failed to parse sqs message %w", err)
		}
		if len(message.Records) > 1 {
			panic("more than one Record found in SQS msg")
		}
		for _, r := range message.Records {
			r.ReceiptHandle = *msg.ReceiptHandle
			result = append(result, r)
		}
	}

	return result, nil
}

func sqsMessages(sess *session.Session, queueUrl *string) (*sqs.ReceiveMessageOutput, error) {

	svc := sqs.New(sess)
	receivedMsg, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            queueUrl,
		MaxNumberOfMessages: aws.Int64(5),
	})

	fmt.Println(len(receivedMsg.Messages))
	if err != nil {
		return nil, fmt.Errorf("getMessages: faild to fetch messages from aws sqs %w", err)
	}

	return receivedMsg, nil
}

func queueUrl(sess *session.Session, queue string) (*sqs.GetQueueUrlOutput, error) {
	svc := sqs.New(sess)

	queueUrl, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &queue,
	})

	if err != nil {
		return nil, fmt.Errorf("getQueueUrl: failed to get sqs queue url %w", err)
	}

	return queueUrl, nil
}

type rawSQSBody struct {
	Records []rawSQSRecord `json:"Records"`
}

type rawSQSRecord struct {
	S3            rawSQSS3Data `json: "s3"`
	ReceiptHandle string
}

type rawSQSS3Data struct {
	Bucket rawBucketData `json:"bucket"`
	Object rawObjectData `json:"object"`
}

type rawBucketData struct {
	Name string
}

type rawObjectData struct {
	Key  string
	Size int
}
