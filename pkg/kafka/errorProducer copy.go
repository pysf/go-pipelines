package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/pysf/go-pipelines/pkg/pipeline"
	"github.com/segmentio/kafka-go"
)

func CreateErrMsg(ctx context.Context, genericEventCh chan pipeline.GenericEventInt) chan pipeline.KafkaMessageInt {

	resultCh := make(chan pipeline.KafkaMessageInt)

	go func() {
		defer close(resultCh)
		defer fmt.Println("createKafkaErrMsg closing...")

		sendResult := func(e *KafkaMessage) {
			select {
			case <-ctx.Done():
			case resultCh <- e:
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case gEvnt, ok := <-genericEventCh:

				if !ok {
					return
				}

				if gEvnt.GetError() != nil {

					var kafkaErrMsg *KafkaMessage
					var appErr *pipeline.AppError

					if errors.As(gEvnt.GetError(), &appErr) {
						data, err := appErr.ToJSON()
						if err != nil {
							panic(err)
						}

						kafkaErrMsg = &KafkaMessage{
							msg: &kafka.Message{
								Key:   []byte(""),
								Value: data,
							},
						}

					} else {

						kafkaErrMsg = &KafkaMessage{
							msg: &kafka.Message{
								Key:   []byte(""),
								Value: []byte(gEvnt.GetError().Error()),
							},
						}

					}

					sendResult(kafkaErrMsg)

					if gEvnt.GetOnDone() != nil {
						f := *gEvnt.GetOnDone()
						f()
					}
				}

			}
		}

	}()
	return resultCh
}
