package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func sqsEvents(ctx context.Context, queue *string) chan S3Stage {

	resultCh := make(chan S3Stage)

	go func() {
		defer close(resultCh)

		sqsMessages, err := latesEvents(queue)
		if err != nil {
			resultCh <- &S3StageEvent{
				err: fmt.Errorf("getSQSEvents: failed %w", err),
			}
			return
		}

		for _, message := range sqsMessages {
			select {
			case <-ctx.Done():
			case resultCh <- &S3StageEvent{
				bucket: message.S3.Bucket.Name,
				key:    message.S3.Object.Key,
				onDone: func() {
					fmt.Println(message.S3.Object.Key)
				},
			}:
			}
		}

	}()

	return resultCh

}

func latesEvents(queue *string) ([]sqsRecord, error) {

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
	var result []sqsRecord
	for _, msg := range msgResult.Messages {
		// msg.ReceiptHandle
		var message sqsBody
		if err := json.Unmarshal([]byte(*msg.Body), &message); err != nil {
			return nil, fmt.Errorf("fetchLatesEvents: failed to parse sqs message %w", err)
		}
		result = append(result, message.Records...)
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

func queueUrl(sess *session.Session, queue *string) (*sqs.GetQueueUrlOutput, error) {
	svc := sqs.New(sess)

	queueUrl, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: queue,
	})

	if err != nil {
		return nil, fmt.Errorf("getQueueUrl: failed to get sqs queue url %w", err)
	}

	return queueUrl, nil
}

type sqsBody struct {
	Records []sqsRecord `json:"Records"`
}

type sqsRecord struct {
	S3            s3Data `json: "s3"`
	ReceiptHandle string
}

type s3Data struct {
	Bucket bucket `json:"bucket"`
	Object object `json:"object"`
}

type bucket struct {
	Name string
}

type object struct {
	Key  string
	Size int
}
