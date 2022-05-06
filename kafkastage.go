package pipeline

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

func NewKafkaStege(valueTopic, errorTopic string) *kafkaStage {

	batcher := NewMessageBatcher()
	return &kafkaStage{
		valueTopic:     valueTopic,
		errorTopic:     errorTopic,
		messageBatcher: batcher,
	}
}

type kafkaStage struct {
	errorTopic string
	valueTopic string
	*messageBatcher
}

func (kafkaStage *kafkaStage) CreateMessage(ctx context.Context, fileRowCh chan FileRow) chan KafkaMessageInt {

	resultCh := make(chan KafkaMessageInt)

	go func() {
		defer close(resultCh)

		sendResult := func(msg *kafkaMessage) {
			select {
			case <-ctx.Done():
			case resultCh <- msg:
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case fileRow, ok := <-fileRowCh:
				if !ok {
					return
				}

				var errEvent *kafkaMessage
				if fileRow.GetError() != nil {

					var appErr *AppError

					if errors.As(fileRow.GetError(), &appErr) {
						data, err := appErr.toJSON()
						if err != nil {
							panic(fmt.Errorf("CreateKafkaMessage: error toJson failed %w", err))
						}

						errEvent = &kafkaMessage{
							msg: &kafka.Message{
								Topic: kafkaStage.errorTopic,
								Key:   []byte(fileRow.FileName()),
								Value: data,
							},
						}

					} else {

						errEvent = &kafkaMessage{
							msg: &kafka.Message{
								Topic: kafkaStage.errorTopic,
								Key:   []byte(fileRow.FileName()),
								Value: []byte(fileRow.GetError().Error()),
							},
						}

					}

					sendResult(errEvent)

				} else {
					row := (fileRow.Data()).(map[string]string)
					jsonStr, err := json.Marshal(row)
					if err != nil {
						panic(fmt.Errorf("CreateKafkaMessage: json marshal failed %w", err))
					}

					sendResult(&kafkaMessage{
						msg: &kafka.Message{
							Topic: kafkaStage.valueTopic,
							Key:   []byte(fileRow.FileName()),
							Value: jsonStr,
						},
					})

				}

				if fileRow.GetOnDone() != nil {
					f := *fileRow.GetOnDone()
					f()
				}

			}
		}

	}()

	return resultCh
}

type kafkaMessage struct {
	msg  *kafka.Message
	done *func()
	err  error
}

func (km *kafkaMessage) GetOnDone() *func() {
	return km.done
}

func (km *kafkaMessage) Message() *kafka.Message {
	return km.msg
}

func (km *kafkaMessage) GetError() error {
	return km.err
}

func (ks *kafkaStage) SendMessage(ctx context.Context, kafkaMessageCh chan KafkaMessageInt) chan GenericEventInt {

	resultCh := make(chan GenericEventInt)

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

		for {
			select {
			case <-ctx.Done():
				return
			case kafkaMSG, ok := <-kafkaMessageCh:

				if !ok {
					if err := ks.messageBatcher.send(ctx); err != nil {
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

				ks.messageBatcher.add(*kafkaMSG.Message())
				if ks.messageBatcher.size() >= 100 {
					if err := ks.messageBatcher.send(ctx); err != nil {
						panic(err)
					}
					ks.messageBatcher.flush()
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

func kafkaClient() (*kafka.Writer, error) {

	username, exist := os.LookupEnv("KAFKA_USERNAME")
	if !exist {
		panic(fmt.Errorf("sendToKafka: KAFKA_USERNAME is empty "))
	}

	password, exist := os.LookupEnv("KAFKA_PASSWORD")
	if !exist {
		panic(fmt.Errorf("sendToKafka: KAFKA_PASSWORD is empty "))
	}

	brokers, exist := os.LookupEnv("KAFKA_BROKERS")
	if !exist {
		panic(fmt.Errorf("sendToKafka: KAFKA_BROKERS is empty "))
	}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		SASLMechanism: plain.Mechanism{
			Username: username,
			Password: password,
		},
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:   strings.Split(brokers, ","),
		BatchSize: 100,
		Balancer:  &kafka.Hash{},
		Dialer:    dialer,
	}), nil
}

func NewMessageBatcher() *messageBatcher {
	client, err := kafkaClient()
	if err != nil {
		panic(fmt.Errorf("failed to create kafka client %w", err))
	}

	return &messageBatcher{
		kafkaClient: client,
	}
}

type messageBatcher struct {
	kafkaClient *kafka.Writer
	messages    []kafka.Message
}

func (mb *messageBatcher) size() int {
	return len(mb.messages)
}

func (mb *messageBatcher) add(m kafka.Message) {
	mb.messages = append(mb.messages, m)
}

func (mb *messageBatcher) flush() {
	mb.messages = make([]kafka.Message, 0)
}

func (mb *messageBatcher) send(ctx context.Context) error {
	return mb.kafkaClient.WriteMessages(ctx, mb.messages...)
}
