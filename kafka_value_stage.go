package pipeline

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func CreateValueMsg(ctx context.Context, fileRowCh chan FileRow) chan KafkaMessageInt {

	resultCh := make(chan KafkaMessageInt)
	const stage string = "kafka"

	go func() {
		defer close(resultCh)
		defer fmt.Println("createKafkaValueMsg closing...")

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
			case rowEvent, ok := <-fileRowCh:
				if !ok {
					return
				}

				if rowEvent.GetError() != nil {
					sendResult(&kafkaMessage{
						err: rowEvent.GetError(),
					})
					break
				}

				row := (rowEvent.Data()).(map[string]string)
				jsonStr, err := json.Marshal(row)
				if err != nil {
					e := wrapError(fmt.Errorf("createKafkaMessage: json marshal failed %w", err))
					e.Misc = map[string]interface{}{
						"file":       rowEvent.FileName(),
						"lineNumber": rowEvent.LineNumber(),
						"stage":      stage,
					}
					sendResult(&kafkaMessage{
						err: e,
					})
					break
				}

				sendResult(&kafkaMessage{
					msg: &kafka.Message{
						Key:   []byte(rowEvent.FileName()),
						Value: jsonStr,
					},
				})

				if rowEvent.GetOnDone() != nil {
					f := *rowEvent.GetOnDone()
					f()
				}

			}
		}

	}()

	return resultCh
}
