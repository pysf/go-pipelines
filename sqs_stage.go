package pipeline

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

func NewSQSStage(queue string, awsCnf aws.Config) sqsStage {

	sess := session.Must(session.NewSession(&awsCnf))

	return sqsStage{
		sqsQueue: sqsQueue{
			client: sqs.New(sess),
			url:    queue,
		},
	}

}

type sqsStage struct {
	sqsQueue sqsQueue
}

func (s sqsStage) GetS3Message(ctx context.Context) chan S3Notification {

	resultCh := make(chan S3Notification)
	go func() {
		defer close(resultCh)

		sqsMessages, err := s.sqsQueue.getMessages(10)
		if err != nil {
			resultCh <- &SQSS3Event{
				err: fmt.Errorf("messages: %w", err),
			}
			return
		}

		for _, message := range sqsMessages {

			select {
			case <-ctx.Done():
				return
			case resultCh <- &SQSS3Event{
				message: message,
				done:    getRemoveQueueFunc(message.S3Data.Object.Key),
			}:
			}

		}

	}()

	return resultCh

}

type sqsQueue struct {
	client sqsiface.SQSAPI
	url    string
}

func (q *sqsQueue) getMessages(numberOfMSG int64) ([]Record, error) {

	receivedMsg, err := q.client.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            aws.String(q.url),
		MaxNumberOfMessages: aws.Int64(numberOfMSG),
	})

	if err != nil {
		return nil, fmt.Errorf("fetchMessage: faild to fetch messages from aws sqs %w", err)
	}

	// fmt.Printf("sqs msg: %v \n", len(receivedMsg.Messages))

	var result []Record
	for _, msg := range receivedMsg.Messages {

		var message Message
		if err := json.Unmarshal([]byte(*msg.Body), &message); err != nil {
			return nil, fmt.Errorf("fetchMessage: failed to parse sqs message %w", err)
		}
		result = append(result, message.Records...)
	}

	return result, nil
}

func getRemoveQueueFunc(name string) *func() {
	f := func() {
		fmt.Printf("Removing msg %v \n", name)
	}
	return &f
}

type Message struct {
	Records []Record
}

type Record struct {
	S3Data        S3 `json:"s3"`
	ReceiptHandle string
}

type S3 struct {
	Bucket Bucket
	Object Object
}

type Bucket struct {
	Name string
}

type Object struct {
	Key  string
	Size int
}

type SQSS3Event struct {
	err     error
	message Record
	done    *func()
}

func (sqsEvent *SQSS3Event) Bucket() string {
	return sqsEvent.message.S3Data.Bucket.Name
}

func (sqsEvent *SQSS3Event) Key() string {
	return sqsEvent.message.S3Data.Object.Key
}

func (sqsEvent *SQSS3Event) GetOnDone() *func() {
	return sqsEvent.done
}

func (sqsEvent *SQSS3Event) GetError() error {
	return sqsEvent.err
}
