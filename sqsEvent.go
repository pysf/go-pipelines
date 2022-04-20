package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type pipelineEvent struct {
	err       error
	sqsRecord sqsRecord
	file      *os.File
	processed bool
}

func getSQSEvents(ctx context.Context, queue *string) chan pipelineEvent {

	response := make(chan pipelineEvent)

	go func() {
		defer close(response)

		sqsMessages, err := fetchLatesEvents(queue)
		if err != nil {
			response <- pipelineEvent{
				err: fmt.Errorf("getSQSEvents: failed %w", err),
			}
			return
		}

		for _, message := range sqsMessages {

			select {
			case <-ctx.Done():
			case response <- pipelineEvent{
				sqsRecord: message,
			}:
			}
		}

	}()

	return response

}

func fetchLatesEvents(queue *string) ([]sqsRecord, error) {

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-central-1"),
	})

	if err != nil {
		return nil, fmt.Errorf("fetchLatesEvents: failed to connect to aws %w", err)
	}

	urlResult, err := getQueueUrl(sess, queue)
	if err != nil {
		return nil, fmt.Errorf("fetchLatesEvents: %w", err)
	}

	msgResult, err := getMessages(sess, urlResult.QueueUrl)
	if err != nil {
		return nil, fmt.Errorf("fetchLatesEvents: %w", err)
	}

	fmt.Println(len(msgResult.Messages))
	var result []sqsRecord
	for _, msg := range msgResult.Messages {
		var message sqsBody
		if err := json.Unmarshal([]byte(*msg.Body), &message); err != nil {
			return nil, fmt.Errorf("fetchLatesEvents: failed to parse sqs message %w", err)
		}
		result = append(result, message.Records...)
	}

	return result, nil
}

func getMessages(sess *session.Session, queueUrl *string) (*sqs.ReceiveMessageOutput, error) {

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

func getQueueUrl(sess *session.Session, queue *string) (*sqs.GetQueueUrlOutput, error) {
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
	S3 s3Data `json: "s3"`
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
