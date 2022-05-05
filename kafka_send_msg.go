package pipeline

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func SendMessage(ctx context.Context, kafkaMessageCh chan KafkaMessageInt, topic string) chan GenericEventInt {

	resultCh := make(chan GenericEventInt)

	kw, err := kafkaWriter(topic)
	if err != nil {
		panic(err)
	}

	go func() {
		defer close(resultCh)
		defer fmt.Println("sendToKafka closing...")

		sendResult := func(r *GenericEvent) {
			select {
			case <-ctx.Done():
				return
			case resultCh <- r:
			}

		}

		messageBatch := messageBatch{
			client: kw,
			ctx:    ctx,
		}
		for {
			select {
			case <-ctx.Done():
				return
			case kafkaMSG, ok := <-kafkaMessageCh:

				if !ok {
					if err := messageBatch.send(); err != nil {
						panic(err)
					}
					return
				}

				if kafkaMSG.GetError() != nil {
					sendResult(&GenericEvent{
						Err: kafkaMSG.GetError(),
					})
					break
				}

				messageBatch.add(*kafkaMSG.Message())
				if messageBatch.size() >= 100 {
					if err := messageBatch.send(); err != nil {
						panic(err)
					}
					messageBatch.flush()
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

type messageBatch struct {
	client   *kafka.Writer
	ctx      context.Context
	messages []kafka.Message
}

func (mb *messageBatch) size() int {
	return len(mb.messages)
}

func (mb *messageBatch) add(m kafka.Message) {
	mb.messages = append(mb.messages, m)
}

func (mb *messageBatch) flush() {
	mb.messages = make([]kafka.Message, 0)
}

func (mb *messageBatch) send() error {
	return mb.client.WriteMessages(mb.ctx, mb.messages...)
}
