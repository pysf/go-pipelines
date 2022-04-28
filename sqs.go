package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func messages(ctx context.Context, queue string) chan s3Notification {

	resultCh := make(chan s3Notification)
	go func() {
		defer close(resultCh)
		defer fmt.Println("sqs closing...")

		sqsMessages, err := fetchMessage(queue)
		if err != nil {
			resultCh <- &sqsS3Event{
				err: fmt.Errorf("messages: %w", err),
			}
			return
		}

		for _, message := range sqsMessages {

			select {
			case <-ctx.Done():
				return
			case resultCh <- &sqsS3Event{
				message: message,
				done:    getRemoveQueueFunc(message.S3.Object.Key),
			}:
			}
		}

	}()

	return resultCh

}

func getRemoveQueueFunc(name string) *func() {
	f := func() {
		fmt.Printf("Removing msg %v \n", name)
	}
	return &f
}

func fetchMessage(queue string) ([]rawSQSRecord, error) {

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-central-1"),
	})
	if err != nil {
		return nil, fmt.Errorf("fetchMessage: failed to connect to aws %w", err)
	}

	svc := sqs.New(sess)
	queueUrl, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &queue,
	})
	if err != nil {
		return nil, fmt.Errorf("fetchMessage: failed to get sqs queue url %w", err)
	}

	receivedMsg, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            queueUrl.QueueUrl,
		MaxNumberOfMessages: aws.Int64(10),
	})
	if err != nil {
		return nil, fmt.Errorf("fetchMessage: faild to fetch messages from aws sqs %w", err)
	}

	fmt.Printf("sqs msg: %v \n", len(receivedMsg.Messages))

	var result []rawSQSRecord
	for _, msg := range receivedMsg.Messages {
		var message rawSQSBody
		if err := json.Unmarshal([]byte(*msg.Body), &message); err != nil {
			return nil, fmt.Errorf("fetchMessage: failed to parse sqs message %w", err)
		}
		for _, r := range message.Records {
			r.ReceiptHandle = *msg.ReceiptHandle
			result = append(result, r)
		}
	}

	return result, nil
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

type sqsS3Event struct {
	err     error
	message rawSQSRecord
	done    *func()
}

func (e *sqsS3Event) bucket() string {
	return e.message.S3.Bucket.Name
}

func (e *sqsS3Event) key() string {
	return e.message.S3.Object.Key
}

func (e *sqsS3Event) getOnDone() *func() {
	return e.done
}

func (sqsEvent *sqsS3Event) getError() error {
	return sqsEvent.err
}
