package kafka

import (
	"context"
	"fmt"

	"github.com/pysf/go-pipelines/pkg/pipeline"
	"github.com/segmentio/kafka-go"
)

func SendMessage(ctx context.Context, kafkaMessageCh chan pipeline.KafkaMessageInt, topic string) chan pipeline.GenericEventInt {

	resultCh := make(chan pipeline.GenericEventInt)

	kw, err := kafkaWriter(topic)
	if err != nil {
		panic(err)
	}

	go func() {
		defer close(resultCh)
		defer fmt.Println("sendToKafka closing...")

		sendResult := func(r *pipeline.GenericEvent) {
			select {
			case <-ctx.Done():
				return
			case resultCh <- r:
			}

		}

		messageChunk := []kafka.Message{}
		for {
			select {
			case <-ctx.Done():
				return
			case kafkaMSG, ok := <-kafkaMessageCh:

				if !ok {
					if err := kw.WriteMessages(ctx, messageChunk...); err != nil {
						panic(err)
					}
					return
				}

				if kafkaMSG.GetError() != nil {
					sendResult(&pipeline.GenericEvent{
						Err: kafkaMSG.GetError(),
					})
					break
				}

				messageChunk = append(messageChunk, *kafkaMSG.Message())
				if len(messageChunk) >= 100 {
					if err := kw.WriteMessages(ctx, messageChunk...); err != nil {
						panic(err)
					}
					messageChunk = []kafka.Message{}
				}

				if kafkaMSG.GetOnDone() != nil {
					f := *kafkaMSG.GetOnDone()
					f()
				}

			}
		}

	}()
	return resultCh
}
