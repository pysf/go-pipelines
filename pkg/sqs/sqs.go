package sqs

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

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
