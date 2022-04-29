package sqs

import (
	"context"
	"fmt"

	"github.com/pysf/go-pipelines/pkg/pipeline"
)

func Messages(ctx context.Context, queue string) chan pipeline.S3Notification {

	resultCh := make(chan pipeline.S3Notification)
	go func() {
		defer close(resultCh)
		defer fmt.Println("sqs closing...")

		sqsMessages, err := fetchMessage(queue)
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
				done:    getRemoveQueueFunc(message.S3.Object.Key),
			}:
			}
		}

	}()

	return resultCh

}
