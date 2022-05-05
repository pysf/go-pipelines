package pipeline

import (
	"context"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func CreateErrMsg(ctx context.Context, genericEventCh chan GenericEventInt) chan KafkaMessageInt {

	resultCh := make(chan KafkaMessageInt)

	go func() {
		defer close(resultCh)
		defer fmt.Println("createKafkaErrMsg closing...")

		sendResult := func(e *kafkaMessage) {
			select {
			case <-ctx.Done():
			case resultCh <- e:
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-genericEventCh:

				if !ok {
					return
				}

				if event.GetError() != nil {

					var errEvent *kafkaMessage
					var appErr *AppError

					if errors.As(event.GetError(), &appErr) {
						data, err := appErr.toJSON()
						if err != nil {
							panic(err)
						}

						errEvent = &kafkaMessage{
							msg: &kafka.Message{
								Key:   []byte(""),
								Value: data,
							},
						}

					} else {

						errEvent = &kafkaMessage{
							msg: &kafka.Message{
								Key:   []byte(""),
								Value: []byte(event.GetError().Error()),
							},
						}

					}

					sendResult(errEvent)

					if event.GetOnDone() != nil {
						f := *event.GetOnDone()
						f()
					}
				}

			}
		}

	}()
	return resultCh
}
