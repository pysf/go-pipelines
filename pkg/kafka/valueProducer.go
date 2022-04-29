package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pysf/go-pipelines/pkg/pipeline"
	"github.com/segmentio/kafka-go"
)

func CreateValueMsg(ctx context.Context, fileRowCh chan pipeline.FileRow) chan pipeline.KafkaMessageInt {

	resultCh := make(chan pipeline.KafkaMessageInt)
	const stage string = "kafka"

	go func() {
		defer close(resultCh)
		defer fmt.Println("createKafkaValueMsg closing...")

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
			case rowEvent, ok := <-fileRowCh:
				if !ok {
					return
				}

				if rowEvent.GetError() != nil {
					sendResult(&KafkaMessage{
						err: rowEvent.GetError(),
					})
					break
				}

				row := (rowEvent.GetData()).(map[string]string)
				jsonStr, err := json.Marshal(row)
				if err != nil {
					e := pipeline.WrapError(fmt.Errorf("createKafkaMessage: json marshal failed %w", err))
					e.Misc = map[string]interface{}{
						"file":       rowEvent.FileName,
						"lineNumber": rowEvent.LineNumber(),
						"stage":      stage,
					}
					sendResult(&KafkaMessage{
						err: e,
					})
					break
				}

				sendResult(&KafkaMessage{
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
